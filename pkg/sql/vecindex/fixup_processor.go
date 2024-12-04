// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/slices"
)

// fixupType enumerates the different kinds of fixups.
type fixupType int

const (
	// splitFixup is a fixup that includes the key of a partition to split as
	// well as the key of its parent partition.
	splitFixup fixupType = iota + 1
	// mergeFixup is a fixup that includes the key of a partition to merge as
	// well as the key of its parent partition.
	mergeFixup
	// vectorDeleteFixup is a fixup that includes the primary key of a vector to
	// delete from the index, as well as the key of the partition that contains
	// it.
	vectorDeleteFixup
)

// maxFixups specifies the maximum number of pending index fixups that can be
// enqueued by foreground threads, waiting for processing. Hitting this limit
// indicates the background goroutine has fallen far behind.
const maxFixups = 100

// fixup describes an index fixup so that it can be enqueued for processing.
// Each fixup type needs to have some subset of the fields defined.
type fixup struct {
	// Type is the kind of fixup.
	Type fixupType
	// PartitionKey is the key of the fixup's target partition, if the fixup
	// operates on a partition.
	PartitionKey vecstore.PartitionKey
	// ParentPartitionKey is the key of the parent of the fixup's target
	// partition, if the fixup operates on a partition
	ParentPartitionKey vecstore.PartitionKey
	// VectorKey is the primary key of the fixup vector.
	VectorKey vecstore.PrimaryKey
}

// partitionFixupKey is used as a key in a uniqueness map for partition fixups.
type partitionFixupKey struct {
	// Type is the kind of fixup.
	Type fixupType
	// PartitionKey is the key of the fixup's target partition.
	PartitionKey vecstore.PartitionKey
}

// fixupProcessor applies index fixups in a background goroutine. Fixups repair
// issues like dangling vectors and maintain the index by splitting and merging
// partitions. Rather than interrupt a search or insert by performing a fixup in
// a foreground goroutine, the fixup is enqueued and run later in a background
// goroutine. This scheme avoids adding unpredictable latency to foreground
// operations.
//
// In addition, it’s important that each fixup is performed in its own
// transaction, with no re-entrancy allowed. If a fixup itself triggers another
// fixup, then that will likewise be enqueued and performed in a separate
// transaction, in order to avoid contention and re-entrancy, both of which can
// cause problems.
//
// All entry methods (i.e. capitalized methods) in fixupProcess are thread-safe.
type fixupProcessor struct {
	// --------------------------------------------------
	// These fields can be accessed on any goroutine once the lock is acquired.
	// --------------------------------------------------
	mu struct {
		syncutil.Mutex

		// pendingPartitions tracks pending fixups that operate on a partition.
		pendingPartitions map[partitionFixupKey]bool

		// pendingVectors tracks pending fixups for deleting vectors.
		pendingVectors map[string]bool

		// waitForFixups broadcasts to any waiters when all fixups are processed.
		waitForFixups sync.Cond
	}

	// --------------------------------------------------
	// These fields can be accessed on any goroutine.
	// --------------------------------------------------

	// fixups is an ordered list of fixups to process.
	fixups chan fixup
	// fixupsLimitHit prevents flooding the log with warning messages when the
	// maxFixups limit has been reached.
	fixupsLimitHit log.EveryN

	// --------------------------------------------------
	// The following fields should only be accessed on a single background
	// goroutine (or a single foreground goroutine in deterministic tests).
	// --------------------------------------------------

	// index points back to the vector index to which fixups are applied.
	index *VectorIndex
	// rng is a random number generator. If nil, then the global random number
	// generator will be used.
	rng *rand.Rand
	// workspace is used to stack-allocate temporary memory.
	workspace internal.Workspace
	// searchCtx is reused to perform index searches and inserts.
	searchCtx searchContext

	// tempVectorsWithKeys is temporary memory for vectors and their keys.
	tempVectorsWithKeys []vecstore.VectorWithKey
}

// Init initializes the fixup processor. If "seed" is non-zero, then the fixup
// processor will use a deterministic random number generator. Otherwise, it
// will use the global random number generator.
func (fp *fixupProcessor) Init(index *VectorIndex, seed int64) {
	fp.index = index
	if seed != 0 {
		// Create a random number generator for the background goroutine.
		fp.rng = rand.New(rand.NewSource(seed))
	}
	fp.mu.pendingPartitions = make(map[partitionFixupKey]bool, maxFixups)
	fp.mu.pendingVectors = make(map[string]bool, maxFixups)
	fp.mu.waitForFixups.L = &fp.mu
	fp.fixups = make(chan fixup, maxFixups)
	fp.fixupsLimitHit = log.Every(time.Second)
}

// AddSplit enqueues a split fixup for later processing.
func (fp *fixupProcessor) AddSplit(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) {
	fp.addFixup(ctx, fixup{
		Type:               splitFixup,
		ParentPartitionKey: parentPartitionKey,
		PartitionKey:       partitionKey,
	})
}

// AddMerge enqueues a merge fixup for later processing.
func (fp *fixupProcessor) AddMerge(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) {
	fp.addFixup(ctx, fixup{
		Type:               mergeFixup,
		ParentPartitionKey: parentPartitionKey,
		PartitionKey:       partitionKey,
	})
}

// AddDeleteVector enqueues a vector deletion fixup for later processing.
func (fp *fixupProcessor) AddDeleteVector(
	ctx context.Context, partitionKey vecstore.PartitionKey, vectorKey vecstore.PrimaryKey,
) {
	fp.addFixup(ctx, fixup{
		Type:         vectorDeleteFixup,
		PartitionKey: partitionKey,
		VectorKey:    vectorKey,
	})
}

// Start is meant to be called on a background goroutine. It runs until the
// provided context is canceled, processing fixups as they are added to the
// fixup processor.
func (fp *fixupProcessor) Start(ctx context.Context) {
	ctx = internal.WithWorkspace(ctx, &fp.workspace)

	for {
		// Wait to run the next fixup in the queue.
		ok, err := fp.run(ctx, true /* wait */)
		if err != nil {
			// This is a background goroutine, so just log error and continue.
			log.Errorf(ctx, "fixup processor error: %v", err)
			continue
		}

		if !ok {
			// Context was canceled, so exit.
			return
		}
	}
}

// Wait blocks until all pending fixups have been processed by the background
// goroutine. This is useful in testing.
func (fp *fixupProcessor) Wait() {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	for len(fp.mu.pendingVectors) > 0 || len(fp.mu.pendingPartitions) > 0 {
		fp.mu.waitForFixups.Wait()
	}
}

// runAll processes all fixups in the queue. This should only be called by tests
// on one foreground goroutine, and only in cases where Start was not called.
func (fp *fixupProcessor) runAll(ctx context.Context) error {
	for {
		ok, err := fp.run(ctx, false /* wait */)
		if err != nil {
			return err
		}
		if !ok {
			// No more fixups to process.
			return nil
		}
	}
}

// run processes the next fixup in the queue and returns true. If "wait" is
// false, then run returns false if there are no fixups in the queue. If "wait"
// is true, then run blocks until it has processed a fixup or until the context
// is canceled, in which case it returns false.
func (fp *fixupProcessor) run(ctx context.Context, wait bool) (ok bool, err error) {
	// Get next fixup from the queue.
	var next fixup
	if wait {
		// Wait until a fixup is enqueued or the context is canceled.
		select {
		case next = <-fp.fixups:
			break

		case <-ctx.Done():
			// Context was canceled, abort.
			return false, nil
		}
	} else {
		// If no fixup is available, immediately return.
		select {
		case next = <-fp.fixups:
			break

		default:
			return false, nil
		}
	}

	// Invoke the fixup function. Note that we do not hold the lock while
	// processing the fixup.
	switch next.Type {
	case splitFixup:
		if err = fp.splitPartition(ctx, next.ParentPartitionKey, next.PartitionKey); err != nil {
			err = errors.Wrapf(err, "splitting partition %d", next.PartitionKey)
		}

	case mergeFixup:
		if err = fp.mergePartition(ctx, next.ParentPartitionKey, next.PartitionKey); err != nil {
			err = errors.Wrapf(err, "merging partition %d", next.PartitionKey)
		}

	case vectorDeleteFixup:
		if err = fp.deleteVector(ctx, next.PartitionKey, next.VectorKey); err != nil {
			err = errors.Wrap(err, "deleting vector")
		}

	default:
		return false, errors.AssertionFailedf("unknown fixup %d", next.Type)
	}

	// Delete already-processed fixup from its pending map, even if the fixup
	// failed, in order to avoid looping over the same fixup.
	fp.mu.Lock()
	defer fp.mu.Unlock()

	switch next.Type {
	case splitFixup, mergeFixup:
		key := partitionFixupKey{Type: next.Type, PartitionKey: next.PartitionKey}
		delete(fp.mu.pendingPartitions, key)

	case vectorDeleteFixup:
		delete(fp.mu.pendingVectors, string(next.VectorKey))
	}

	// If there are no more pending fixups, notify any waiters.
	if len(fp.mu.pendingPartitions) == 0 && len(fp.mu.pendingVectors) == 0 {
		fp.mu.waitForFixups.Broadcast()
	}

	return true, err
}

// addFixup enqueues the given fixup for later processing, assuming there is not
// already a duplicate fixup that's pending.
func (fp *fixupProcessor) addFixup(ctx context.Context, fixup fixup) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Check whether fixup limit has been reached.
	if len(fp.mu.pendingPartitions)+len(fp.mu.pendingVectors) >= maxFixups {
		// Don't enqueue the fixup.
		if fp.fixupsLimitHit.ShouldLog() {
			log.Warning(ctx, "reached limit of unprocessed fixups")
		}
		return
	}

	// Don't enqueue fixup if it's already pending.
	switch fixup.Type {
	case splitFixup, mergeFixup:
		key := partitionFixupKey{Type: fixup.Type, PartitionKey: fixup.PartitionKey}
		if _, ok := fp.mu.pendingPartitions[key]; ok {
			return
		}
		fp.mu.pendingPartitions[key] = true

	case vectorDeleteFixup:
		if _, ok := fp.mu.pendingVectors[string(fixup.VectorKey)]; ok {
			return
		}
		fp.mu.pendingVectors[string(fixup.VectorKey)] = true

	default:
		panic(errors.AssertionFailedf("unknown fixup %d", fixup.Type))
	}

	// Note that the channel send operation should never block, since it has
	// maxFixups capacity.
	fp.fixups <- fixup
}

// splitPartition splits the partition with the given key and parent key. This
// runs in its own set of transactions. The initial transaction replaces the
// splitting transaction with two new sibling partitions, each containing
// roughly one-half the vectors. Additional transactions move vectors in or out
// of those siblings in order to improve index quality.
func (fp *fixupProcessor) splitPartition(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) (err error) {
	split := splitData{
		ParentPartitionKey: parentPartitionKey,
		PartitionKey:       partitionKey,
	}

	// Replace the splitting partition with two new sibling partitions that each
	// contain roughly half of its vectors.
	var ok bool
	ok, err = fp.performSplit(ctx, &split)
	if !ok {
		// No-op if the split failed with an error or if it was not performed due
		// to starting conditions changing (e.g. it was already split by another
		// process).
		return err
	}

	// Improve quality of index by moving vectors to and from the new sibling
	// partitions, such that each vector is in the partition with the closest
	// centroid.
	if split.ParentPartition != nil {
		// Move any vectors to sibling partitions that have closer centroids.
		if err = fp.moveSplitVectorsToSiblings(ctx, &split, &split.Left); err != nil {
			return err
		}
		if err = fp.moveSplitVectorsToSiblings(ctx, &split, &split.Right); err != nil {
			return err
		}

		// Move any vectors at the same level that are closer to the new split
		// centroids than they are to their own centroids.
		if err = fp.moveVectorsFromSiblings(ctx, &split, &split.Left); err != nil {
			return err
		}
		if err = fp.moveVectorsFromSiblings(ctx, &split, &split.Right); err != nil {
			return err
		}
	}

	return nil
}

func (fp *fixupProcessor) performSplit(ctx context.Context, split *splitData) (ok bool, err error) {
	// Run the split within a transaction.
	var txn vecstore.Txn
	txn, err = fp.index.store.BeginTransaction(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		if err == nil {
			err = fp.index.store.CommitTransaction(ctx, txn)
		} else {
			err = errors.CombineErrors(err, fp.index.store.AbortTransaction(ctx, txn))
		}
	}()

	// Get the partition to be split from the store.
	split.Partition, err = fp.index.store.GetPartition(ctx, txn, split.PartitionKey)
	if errors.Is(err, vecstore.ErrPartitionNotFound) {
		log.VEventf(ctx, 2, "partition %d no longer exists, do not split", split.PartitionKey)
		return false, nil
	} else if err != nil {
		return false, errors.Wrapf(err, "getting partition %d to split", split.PartitionKey)
	}

	// Load the parent of the partition to split.
	if split.PartitionKey != vecstore.RootKey {
		split.ParentPartition, err = fp.index.store.GetPartition(ctx, txn, split.ParentPartitionKey)
		if errors.Is(err, vecstore.ErrPartitionNotFound) {
			log.VEventf(ctx, 2, "parent partition %d of partition %d no longer exists, do not split",
				split.ParentPartitionKey, split.PartitionKey)
			return false, nil
		} else if err != nil {
			return false, errors.Wrapf(err, "getting parent %d of partition %d to split",
				split.ParentPartitionKey, split.PartitionKey)
		}

		// Remove the splitting partition from the in-memory parent partition.
		if !split.ParentPartition.ReplaceWithLastByKey(vecstore.ChildKey{PartitionKey: split.PartitionKey}) {
			log.VEventf(ctx, 2, "partition %d is no longer child of partition %d, do not split",
				split.PartitionKey, split.ParentPartitionKey)
			return false, nil
		}
	}

	// Get the full vectors for the splitting partition's children.
	split.Vectors, err = fp.getFullVectorsForPartition(ctx, txn, split.PartitionKey, split.Partition)
	if err != nil {
		return false, errors.Wrapf(err,
			"getting full vectors for split of partition %d", split.PartitionKey)
	}
	if split.Vectors.Count < 2 {
		// This could happen if the partition had tons of dangling references that
		// need to be cleaned up.
		// TODO(andyk): We might consider cleaning up references and/or rewriting
		// the partition.
		log.VEventf(ctx, 2, "partition %d has only %d live vectors, do not split",
			split.PartitionKey, split.Vectors.Count)
		return false, nil
	}

	// Divide vectors from the splitting partition into new left and right sibling
	// partitions.
	fp.assignVectorsForSplit(ctx, split)

	log.VEventf(ctx, 2,
		"splitting partition %d (%d vectors) into left partition %d "+
			"(%d vectors) and right partition %d (%d vectors)",
		split.PartitionKey, len(split.Partition.ChildKeys()),
		split.Left.PartitionKey, len(split.Left.Partition.ChildKeys()),
		split.Right.PartitionKey, len(split.Right.Partition.ChildKeys()))

	if split.ParentPartition != nil {
		// De-link the splitting partition from its parent partition in the store.
		childKey := vecstore.ChildKey{PartitionKey: split.PartitionKey}
		_, err = fp.index.removeFromPartition(ctx, txn, split.ParentPartitionKey, childKey)
		if err != nil {
			return false, errors.Wrapf(err, "removing splitting partition %d from its parent %d",
				split.PartitionKey, split.ParentPartitionKey)
		}

		// Search for vectors at the same level that are closest to the centroids
		// of the new sibling partitions, as those vectors may need to be moved.
		// Do this only after de-linking the splitting partition to avoid finding
		// vectors in the new sibling partitions themselves.
		if err = fp.findClosestVectors(ctx, txn, &split.Left); err != nil {
			return false, errors.Wrapf(err, "finding closest vectors in left sibling partition %d",
				split.Left.PartitionKey)
		}

		if err = fp.findClosestVectors(ctx, txn, &split.Right); err != nil {
			return false, errors.Wrapf(err, "finding closest vectors in right sibling partition %d",
				split.Right.PartitionKey)
		}
	}

	// Insert the two new partitions into the index. This only adds their data
	// (and metadata) for the partition - they're not yet linked into the K-means
	// tree.
	split.Left.PartitionKey, err = fp.index.store.InsertPartition(ctx, txn, split.Left.Partition)
	if err != nil {
		return false, errors.Wrapf(err,
			"creating left partition for split of partition %d", split.PartitionKey)
	}
	split.Right.PartitionKey, err = fp.index.store.InsertPartition(ctx, txn, split.Right.Partition)
	if err != nil {
		return false, errors.Wrapf(err,
			"creating right partition for split of partition %d", split.PartitionKey)
	}

	// Now link the new partitions into the K-means tree.
	if split.PartitionKey == vecstore.RootKey {
		// Add a new level to the tree by setting a new root partition that points
		// to the two new partitions.
		centroids := vector.MakeSet(fp.index.rootQuantizer.GetRandomDims())
		centroids.EnsureCapacity(2)
		centroids.Add(split.Left.Partition.Centroid())
		centroids.Add(split.Right.Partition.Centroid())
		quantizedSet := fp.index.rootQuantizer.Quantize(ctx, &centroids)
		childKeys := []vecstore.ChildKey{
			{PartitionKey: split.Left.PartitionKey},
			{PartitionKey: split.Right.PartitionKey},
		}
		rootPartition := vecstore.NewPartition(
			fp.index.rootQuantizer, quantizedSet, childKeys, split.Partition.Level()+1)
		if err = fp.index.store.SetRootPartition(ctx, txn, rootPartition); err != nil {
			return false, errors.Wrapf(err,
				"setting new root for split of partition %d", split.PartitionKey)
		}

		log.VEventf(ctx, 2, "created new root level with child partitions %d and %d",
			split.Left.PartitionKey, split.Right.PartitionKey)
	} else {
		// Link the two new partitions into the K-means tree by inserting them
		// into the parent level. This can trigger a further split, this time of
		// the parent level.
		searchCtx := fp.reuseSearchContext(ctx, txn)
		searchCtx.Level = split.ParentPartition.Level() + 1

		searchCtx.Randomized = split.Left.Partition.Centroid()
		childKey := vecstore.ChildKey{PartitionKey: split.Left.PartitionKey}
		err = fp.index.insertHelper(searchCtx, childKey, true /* allowRetry */)
		if err != nil {
			return false, errors.Wrapf(err,
				"inserting left partition for split of partition %d", split.PartitionKey)
		}

		searchCtx.Randomized = split.Right.Partition.Centroid()
		childKey = vecstore.ChildKey{PartitionKey: split.Right.PartitionKey}
		err = fp.index.insertHelper(searchCtx, childKey, true /* allowRetry */)
		if err != nil {
			return false, errors.Wrapf(err,
				"inserting right partition for split of partition %d", split.PartitionKey)
		}

		// Delete the old partition.
		if err = fp.index.store.DeletePartition(ctx, txn, split.PartitionKey); err != nil {
			return false, errors.Wrapf(err, "deleting partition %d for split", split.PartitionKey)
		}
	}

	return true, nil
}

// assignVectorsForSplit divides the splitting partition's vectors into new left
// and right sibling partitions. It updates split.Left and split.Right with
// information about the new partitions. It clears split.Vectors after
// reordering it.
func (fp *fixupProcessor) assignVectorsForSplit(ctx context.Context, split *splitData) {
	// Determine which partition children should go into the left split partition
	// and which should go into the right split partition.
	tempOffsets := fp.workspace.AllocUint64s(split.Vectors.Count)
	defer fp.workspace.FreeUint64s(tempOffsets)
	kmeans := BalancedKmeans{Workspace: &fp.workspace, Rand: fp.rng}
	tempLeftOffsets, tempRightOffsets := kmeans.Compute(&split.Vectors, tempOffsets)

	// Copy centroid distances and child keys so they can be split.
	centroidDistances := slices.Clone(split.Partition.QuantizedSet().GetCentroidDistances())
	childKeys := slices.Clone(split.Partition.ChildKeys())

	tempVector := fp.workspace.AllocFloats(fp.index.quantizer.GetRandomDims())
	defer fp.workspace.FreeFloats(tempVector)

	// Any left offsets that point beyond the end of the left list indicate that
	// a vector needs to be moved from the right partition to the left partition.
	// The reverse is true for right offsets. Because the left and right offsets
	// are in sorted order, out-of-bounds offsets must be at the end of the left
	// list and the beginning of the right list. Therefore, the algorithm just
	// needs to iterate over those offsets and swap the positions of the
	// referenced vectors.
	li := len(tempLeftOffsets) - 1
	ri := 0

	var rightToLeft, leftToRight vector.T
	for li >= 0 {
		left := int(tempLeftOffsets[li])
		if left < len(tempLeftOffsets) {
			break
		}

		right := int(tempRightOffsets[ri])
		if right >= len(tempLeftOffsets) {
			panic(errors.AssertionFailedf(
				"expected equal number of left and right offsets that need to be swapped"))
		}

		// Swap vectors.
		rightToLeft = split.Vectors.At(left)
		leftToRight = split.Vectors.At(right)
		copy(tempVector, rightToLeft)
		copy(rightToLeft, leftToRight)
		copy(leftToRight, tempVector)

		// Swap centroid distances and child keys.
		centroidDistances[left], centroidDistances[right] =
			centroidDistances[right], centroidDistances[left]
		childKeys[left], childKeys[right] = childKeys[right], childKeys[left]

		li--
		ri++
	}

	leftVectorSet := split.Vectors
	rightVectorSet := leftVectorSet.SplitAt(len(tempLeftOffsets))

	// Clear the splitting vectors set, since it has been reordered and should
	// not be used after this point.
	split.Vectors = vector.Set{}

	leftCentroidDistances := centroidDistances[:len(tempLeftOffsets):len(tempLeftOffsets)]
	leftChildKeys := childKeys[:len(tempLeftOffsets):len(tempLeftOffsets)]
	split.Left.Init(ctx, fp.index.quantizer, leftVectorSet,
		leftCentroidDistances, leftChildKeys, split.Partition.Level())

	rightCentroidDistances := centroidDistances[len(tempLeftOffsets):]
	rightChildKeys := childKeys[len(tempLeftOffsets):]
	split.Right.Init(ctx, fp.index.quantizer, rightVectorSet,
		rightCentroidDistances, rightChildKeys, split.Partition.Level())
}

func (fp *fixupProcessor) findClosestVectors(
	ctx context.Context, txn vecstore.Txn, sibling *siblingSplitData,
) error {
	// TODO(andyk): Add way to filter search set in order to skip vectors deeper
	// down in the search rather than afterwards.
	searchCtx := fp.reuseSearchContext(ctx, txn)
	searchCtx.Options = SearchOptions{ReturnVectors: true}
	searchCtx.Level = sibling.Partition.Level()
	searchCtx.Randomized = sibling.Partition.Centroid()

	// Don't move more vectors than the number of remaining slots in the split
	// partition, to avoid triggering another split.
	maxResults := fp.index.options.MaxPartitionSize - sibling.Partition.Count()
	if maxResults < 1 {
		return nil
	}
	searchSet := vecstore.SearchSet{MaxResults: maxResults}
	err := fp.index.searchHelper(searchCtx, &searchSet, true /* allowRetry */)
	if err != nil {
		return err
	}

	sibling.Closest = searchSet.PopUnsortedResults()
	return nil
}

// moveSplitVectorsToSiblings checks each vector in the new split partition to
// see if it's now closer to a sibling partition's centroid than it is to its
// own centroid. If that's true, then move the vector to the sibling partition.
// Pass function to lazily fetch parent vectors, as it's expensive and is only
// needed if vectors actually need to be moved.
func (fp *fixupProcessor) moveSplitVectorsToSiblings(
	ctx context.Context, split *splitData, sibling *siblingSplitData,
) (err error) {
	for i := 0; i < sibling.Vectors.Count; i++ {
		if sibling.Vectors.Count == 1 && sibling.Partition.Level() != vecstore.LeafLevel {
			// Don't allow so many vectors to be moved that a non-leaf partition
			// ends up empty. This would violate a key constraint that the K-means
			// tree is always fully balanced.
			break
		}

		vector := sibling.Vectors.At(i)

		// If distance to new centroid is <= distance to old centroid, then skip.
		newCentroidDistance := sibling.Partition.QuantizedSet().GetCentroidDistances()[i]
		if newCentroidDistance <= sibling.OldCentroidDistances[i] {
			continue
		}

		// Ensure that the full vectors for the parent partition have been fetched.
		if split.ParentVectors.Dims == 0 {
			err = fp.runInTransaction(ctx, func(txn vecstore.Txn) error {
				split.ParentVectors, err = fp.getFullVectorsForPartition(
					ctx, txn, split.ParentPartitionKey, split.ParentPartition)
				return err
			})
			if err != nil {
				return err
			}
		}

		// Check whether the vector is closer to a sibling centroid than its own
		// new centroid.
		minDistanceOffset := -1
		for parent := 0; parent < split.ParentVectors.Count; parent++ {
			squaredDistance := num32.L2Distance(split.ParentVectors.At(parent), vector)
			if squaredDistance < newCentroidDistance {
				newCentroidDistance = squaredDistance
				minDistanceOffset = parent
			}
		}
		if minDistanceOffset == -1 {
			continue
		}

		siblingPartitionKey := split.ParentPartition.ChildKeys()[minDistanceOffset].PartitionKey
		log.VEventf(ctx, 3, "moving vector from splitting partition %d to sibling partition %d",
			split.PartitionKey, siblingPartitionKey)

		// Found a sibling child partition that's closer, so move the vector there.
		err = fp.runInTransaction(ctx, func(txn vecstore.Txn) error {
			childKey := sibling.Partition.ChildKeys()[i]
			err = fp.index.moveToPartition(ctx, txn, split.ParentPartitionKey,
				split.PartitionKey, siblingPartitionKey, vector, childKey)
			if err != nil {
				if errors.Is(err, vecstore.ErrPartitionNotFound) {
					// Another process must have deleted the partition, so vector
					// move is no-op.
					log.VEventf(ctx, 2,
						"vector move from partition %d to sibling partition %d failed, partition not found",
						split.PartitionKey, siblingPartitionKey)
					return nil
				} else if errors.Is(err, vecstore.ErrMoveNotAllowed) {
					// Vector can't be moved, so skip it.
					log.VEventf(ctx, 2,
						"vector move from partition %d to sibling partition %d failed, disallowed",
						split.PartitionKey, siblingPartitionKey)
					return nil
				}
				return errors.Wrapf(err, "moving vector from %d to sibling partition %d",
					split.PartitionKey, siblingPartitionKey)
			}
			return nil
		})
		if err != nil {
			return err
		}

		// Remove the vector's data from the new partition. The remove operation
		// backfills data at the current index with data from the last index.
		// Therefore, don't increment the iteration index, since the next item
		// is in the same location as the last.
		sibling.ReplaceWithLast(i)
		i--
	}

	return nil
}

// moveVectorsFromSiblings searches for vectors at the same level that are close
// to a split sibling partition's centroid. If the nearby vectors are closer
// than they are to their own centroid, then move them to the split sibling
// partition.
func (fp *fixupProcessor) moveVectorsFromSiblings(
	ctx context.Context, split *splitData, sibling *siblingSplitData,
) error {
	tempVector := fp.workspace.AllocVector(fp.index.quantizer.GetRandomDims())
	defer fp.workspace.FreeVector(tempVector)

	// Filter the closest results.
	for i := range sibling.Closest {
		result := &sibling.Closest[i]

		// Skip vectors that are closer to their own centroid than they are to
		// the split partition's centroid.
		if result.QuerySquaredDistance >= result.CentroidDistance*result.CentroidDistance {
			continue
		}

		log.VEventf(ctx, 3, "moving vector from partition %d to split sibling partition %d",
			result.ChildKey.PartitionKey, sibling.PartitionKey)

		// Leaf vectors from the primary index need to be randomized.
		vector := result.Vector
		if sibling.Partition.Level() == vecstore.LeafLevel {
			fp.index.quantizer.RandomizeVector(ctx, vector, tempVector, false /* invert */)
			vector = tempVector
		}

		// Move the vector to the sibling partition.
		err := fp.runInTransaction(ctx, func(txn vecstore.Txn) error {
			err := fp.index.moveToPartition(ctx, txn, split.ParentPartitionKey,
				result.ParentPartitionKey, sibling.PartitionKey, vector, result.ChildKey)
			if err != nil {
				if errors.Is(err, vecstore.ErrPartitionNotFound) {
					// Another process must have deleted the partition, so vector
					// move is no-op.
					log.VEventf(ctx, 2,
						"vector move from sibling partition %d to partition %d failed, partition not found",
						result.ParentPartitionKey, split.PartitionKey)
					return nil
				} else if errors.Is(err, vecstore.ErrMoveNotAllowed) {
					// Vector can't be moved, so skip it.
					log.VEventf(ctx, 2,
						"vector move from sibling partition %d to partition %d failed, disallowed",
						result.ParentPartitionKey, split.PartitionKey)
					return nil
				}
				return errors.Wrapf(err, "moving vector from sibling partition %d to partition %d",
					result.ParentPartitionKey, split.PartitionKey)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// mergePartition merges the partition with the given key and parent key. This
// runs in its own transaction. For a given index, there is at most one merge
// happening per SQL process. However, there can be multiple SQL processes, each
// running a merge.
func (fp *fixupProcessor) mergePartition(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) (err error) {
	if partitionKey == vecstore.RootKey {
		return errors.AssertionFailedf("cannot merge the root partition")
	}

	// Run the merge within a transaction.
	txn, err := fp.index.store.BeginTransaction(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = fp.index.store.CommitTransaction(ctx, txn)
		} else {
			err = errors.CombineErrors(err, fp.index.store.AbortTransaction(ctx, txn))
		}
	}()

	// Get the partition to be merged from the store.
	partition, err := fp.index.store.GetPartition(ctx, txn, partitionKey)
	if errors.Is(err, vecstore.ErrPartitionNotFound) {
		log.VEventf(ctx, 2, "partition %d no longer exists, do not merge", partitionKey)
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "getting partition %d to merge", partitionKey)
	}

	// Load the parent of the partition to merge.
	parentPartition, err := fp.index.store.GetPartition(ctx, txn, parentPartitionKey)
	if errors.Is(err, vecstore.ErrPartitionNotFound) {
		log.VEventf(ctx, 2, "parent partition %d of partition %d no longer exists, do not merge",
			parentPartitionKey, partitionKey)
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "getting parent %d of partition %d to merge",
			parentPartitionKey, partitionKey)
	}

	// This check ensures that the tree always stays fully balanced; removing a
	// level always happens at the root.
	if parentPartition.Count() == 1 && parentPartitionKey != vecstore.RootKey {
		log.VEventf(ctx, 2, "partition %d has no sibling partitions, do not merge", partitionKey)
		return nil
	}

	// Get the full vectors for the merging partition's children.
	vectors, err := fp.getFullVectorsForPartition(ctx, txn, partitionKey, partition)
	if err != nil {
		return errors.Wrapf(err, "getting full vectors for merge of partition %d", partitionKey)
	}

	log.VEventf(ctx, 2, "merging partition %d (%d vectors)",
		partitionKey, len(partition.ChildKeys()))

	// De-link the merging partition from its parent partition. This does not
	// delete data or metadata in the partition.
	childKey := vecstore.ChildKey{PartitionKey: partitionKey}
	if !parentPartition.ReplaceWithLastByKey(childKey) {
		log.VEventf(ctx, 2, "partition %d is no longer child of partition %d, do not merge",
			partitionKey, parentPartitionKey)
		return nil
	}
	if _, err = fp.index.removeFromPartition(ctx, txn, parentPartitionKey, childKey); err != nil {
		return errors.Wrapf(err, "remove partition %d from parent partition %d",
			partitionKey, parentPartitionKey)
	}

	// Delete the merging partition from the store. This actually deletes the
	// partition's data and metadata.
	if err = fp.index.store.DeletePartition(ctx, txn, partitionKey); err != nil {
		return errors.Wrapf(err, "deleting partition %d", partitionKey)
	}

	// Move vectors from the deleted partition to other partitions.
	if parentPartition.Count() == 0 {
		// The parent is now empty, which means the deleted partition was the last
		// partition in the parent. That means that the entire level needs to be
		// removed from the tree. This is only allowed if the parent partition is
		// the root partition (the sibling partition check made above ensure that
		// this is the case). Reduce the number of levels in the tree by one by
		// merging vectors in the merging partition into the root partition.
		if parentPartitionKey != vecstore.RootKey {
			return errors.AssertionFailedf("only root partition can have zero vectors")
		}
		quantizedSet := fp.index.rootQuantizer.Quantize(ctx, &vectors)
		rootPartition := vecstore.NewPartition(
			fp.index.rootQuantizer, quantizedSet, partition.ChildKeys(), partition.Level())
		if err = fp.index.store.SetRootPartition(ctx, txn, rootPartition); err != nil {
			return errors.Wrapf(err, "setting new root for merge of partition %d", partitionKey)
		}
	} else {
		// Re-insert vectors into remaining partitions at the same level.
		fp.searchCtx = searchContext{
			Ctx:       ctx,
			Workspace: fp.workspace,
			Txn:       txn,
			Level:     parentPartition.Level(),
		}

		childKeys := partition.ChildKeys()
		for i := range childKeys {
			fp.searchCtx.Randomized = vectors.At(i)
			err = fp.index.insertHelper(&fp.searchCtx, childKeys[i], true /* allowRetry */)
			if err != nil {
				return errors.Wrapf(err, "inserting vector from merged partition %d", partitionKey)
			}
		}
	}

	return nil
}

// deleteVector deletes a vector from the store that has had its primary key
// deleted in the primary index, but was never deleted from the secondary index.
func (fp *fixupProcessor) deleteVector(
	ctx context.Context, partitionKey vecstore.PartitionKey, vectorKey vecstore.PrimaryKey,
) (err error) {
	// Run the deletion within a transaction.
	txn, err := fp.index.store.BeginTransaction(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = fp.index.store.CommitTransaction(ctx, txn)
		} else {
			err = errors.CombineErrors(err, fp.index.store.AbortTransaction(ctx, txn))
		}
	}()

	log.VEventf(ctx, 2, "deleting dangling vector from partition %d", partitionKey)

	// Verify that the vector is still missing from the primary index. This guards
	// against a race condition where a row is created and deleted repeatedly with
	// the same primary key.
	childKey := vecstore.ChildKey{PrimaryKey: vectorKey}
	fp.tempVectorsWithKeys = ensureSliceLen(fp.tempVectorsWithKeys, 1)
	fp.tempVectorsWithKeys[0] = vecstore.VectorWithKey{Key: childKey}
	if err = fp.index.store.GetFullVectors(ctx, txn, fp.tempVectorsWithKeys); err != nil {
		return errors.Wrap(err, "getting full vector")
	}
	if fp.tempVectorsWithKeys[0].Vector != nil {
		log.VEventf(ctx, 2, "primary key row exists, do not delete vector")
		return nil
	}

	_, err = fp.index.removeFromPartition(ctx, txn, partitionKey, childKey)
	if errors.Is(err, vecstore.ErrPartitionNotFound) {
		log.VEventf(ctx, 2, "partition %d no longer exists, do not delete vector", partitionKey)
		return nil
	}
	return err
}

// getFullVectorsForPartition fetches the full-size vectors (potentially
// randomized by the quantizer) that are quantized by the given partition.
// Discard any dangling vectors in the partition.
// NOTE: This method can update the given partition.
func (fp *fixupProcessor) getFullVectorsForPartition(
	ctx context.Context,
	txn vecstore.Txn,
	partitionKey vecstore.PartitionKey,
	partition *vecstore.Partition,
) (vector.Set, error) {
	childKeys := partition.ChildKeys()
	fp.tempVectorsWithKeys = ensureSliceLen(fp.tempVectorsWithKeys, len(childKeys))
	for i := range childKeys {
		fp.tempVectorsWithKeys[i] = vecstore.VectorWithKey{Key: childKeys[i]}
	}
	err := fp.index.store.GetFullVectors(ctx, txn, fp.tempVectorsWithKeys)
	if err != nil {
		err = errors.Wrapf(err, "getting full vectors of partition %d to split", partitionKey)
		return vector.Set{}, err
	}

	// Remove dangling vector references.
	for i := 0; i < len(fp.tempVectorsWithKeys); i++ {
		if fp.tempVectorsWithKeys[i].Vector != nil {
			continue
		}

		// Move last reference to current location and reduce size of slice.
		count := len(fp.tempVectorsWithKeys) - 1
		fp.tempVectorsWithKeys[i] = fp.tempVectorsWithKeys[count]
		fp.tempVectorsWithKeys = fp.tempVectorsWithKeys[:count]
		partition.ReplaceWithLast(i)
		i--
	}

	vectors := vector.MakeSet(fp.index.quantizer.GetRandomDims())
	vectors.AddUndefined(len(fp.tempVectorsWithKeys))
	for i := range fp.tempVectorsWithKeys {
		// Leaf vectors from the primary index need to be randomized.
		if partition.Level() == vecstore.LeafLevel {
			fp.index.quantizer.RandomizeVector(
				ctx, fp.tempVectorsWithKeys[i].Vector, vectors.At(i), false /* invert */)
		} else {
			copy(vectors.At(i), fp.tempVectorsWithKeys[i].Vector)
		}
	}

	return vectors, nil
}

// runInTransaction runs the given function in the scope of a store transaction.
// If the function returns without error, the transaction is committed, else it
// is aborted.
func (fp *fixupProcessor) runInTransaction(
	ctx context.Context, fn func(txn vecstore.Txn) error,
) error {
	txn, err := fp.index.store.BeginTransaction(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = fp.index.store.CommitTransaction(ctx, txn)
		} else {
			err = errors.CombineErrors(err, fp.index.store.AbortTransaction(ctx, txn))
		}
	}()

	err = fn(txn)
	return err
}

// reuseSearchContext initializes the reusable search context, including reusing
// its temp slices.
func (fp *fixupProcessor) reuseSearchContext(ctx context.Context, txn vecstore.Txn) *searchContext {
	fp.searchCtx = searchContext{
		Ctx:                 ctx,
		Workspace:           fp.workspace,
		Txn:                 txn,
		tempKeys:            fp.searchCtx.tempKeys,
		tempCounts:          fp.searchCtx.tempCounts,
		tempVectorsWithKeys: fp.searchCtx.tempVectorsWithKeys,
	}
	return &fp.searchCtx
}
