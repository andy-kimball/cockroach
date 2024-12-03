// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/container/list"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// storeStatsAlpha specifies the ratio of new values to existing values in EMA
// calculations.
const storeStatsAlpha = 0.05

type txnId uint64

// inMemoryTxn tracks the transaction's state.
type inMemoryTxn struct {
	id txnId

	// current is immutable.
	current uint64

	// These fields should only be accessed on the same goroutine that created
	// the transaction.

	// updated is true if any in-memory state has been updated.
	updated bool
	// unbalancedKey, if non-zero, records a non-leaf partition that had all of
	// its vectors removed during the transaction. If, by the end of the
	// transaction, the partition is still empty, the store will panic, since
	// this violates the constraint that the K-means tree is always fully
	// balanced.
	unbalanced *inMemoryPartition

	lockedPartitions []*inMemoryPartition

	// This can only be accessed after the store mutex has been acquired.
	muStore struct {
		ended bool
	}
}

type inMemoryPartition struct {
	// Immutable.
	key PartitionKey

	lock struct {
		partitionLock

		partition *Partition
		start     uint64
		deleted   bool
	}
}

func (p *inMemoryPartition) isVisibleLocked(current uint64) bool {
	return !p.lock.deleted && (p.key != RootKey || current > p.lock.start)
}

type pendingAction struct {
	activeTxn        inMemoryTxn
	deletedPartition *inMemoryPartition
}

// InMemoryStore implements the Store interface over in-memory partitions and
// vectors. This is only used for testing and benchmarking.
type InMemoryStore struct {
	dims int
	seed int64

	mu struct {
		syncutil.Mutex

		clock      uint64
		partitions map[PartitionKey]*inMemoryPartition
		nextKey    PartitionKey
		vectors    map[string]vector.T
		stats      IndexStats
		pending    list.List[pendingAction]
	}
}

var _ Store = (*InMemoryStore)(nil)

// NewInMemoryStore constructs a new in-memory store for vectors of the given
// size. The seed determines how vectors are transformed as part of
// quantization and must be preserved if the store is saved to disk or loaded
// from disk.
func NewInMemoryStore(dims int, seed int64) *InMemoryStore {
	st := &InMemoryStore{
		dims: dims,
		seed: seed,
	}
	st.mu.partitions = make(map[PartitionKey]*inMemoryPartition)
	st.mu.nextKey = RootKey + 1

	// Create empty root partition.
	var empty vector.Set
	quantizer := quantize.NewUnQuantizer(dims)
	quantizedSet := quantizer.Quantize(context.Background(), &empty)
	inMemPartition := &inMemoryPartition{
		key: RootKey,
	}
	inMemPartition.lock.partition = &Partition{
		quantizer:    quantizer,
		quantizedSet: quantizedSet,
		level:        LeafLevel,
	}
	inMemPartition.lock.start = 1
	st.mu.partitions[RootKey] = inMemPartition

	st.mu.clock = 2
	st.mu.vectors = make(map[string]vector.T)
	st.mu.stats.NumPartitions = 1
	st.mu.pending.Init()
	return st
}

// BeginTransaction implements the Store interface.
func (s *InMemoryStore) BeginTransaction(ctx context.Context) (Txn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create new transaction with ID set to the current logical clock tick and
	// insert the transaction into the pending list.
	txn := inMemoryTxn{id: txnId(s.mu.clock), current: s.mu.clock}
	s.mu.clock++
	elem := s.mu.pending.PushBack(pendingAction{activeTxn: txn})
	return &elem.Value.activeTxn, nil
}

// CommitTransaction implements the Store interface.
func (s *InMemoryStore) CommitTransaction(ctx context.Context, txn Txn) error {
	// Release any exclusive partition locks held by the transaction.
	inMemTxn := txn.(*inMemoryTxn)
	for i := range inMemTxn.lockedPartitions {
		inMemTxn.lockedPartitions[i].lock.Release()
	}

	// Panic if the K-means tree contains an empty non-leaf partition after the
	// transaction ends, as this violates the constraint that the K-means tree
	// is always fully balanced.
	if inMemTxn.unbalanced != nil {
		// Need to acquire partition lock before inspecting the partition.
		func() {
			inMemTxn.unbalanced.lock.AcquireShared(inMemTxn.id)
			defer inMemTxn.unbalanced.lock.ReleaseShared()

			partition := inMemTxn.unbalanced.lock.partition
			if partition.Count() == 0 && partition.Level() > LeafLevel {
				if inMemTxn.unbalanced.isVisibleLocked(inMemTxn.current) {
					panic(errors.AssertionFailedf(
						"K-means tree is unbalanced, with empty non-leaf partition %d",
						inMemTxn.unbalanced.key))
				}
			}
		}()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	inMemTxn.muStore.ended = true

	// Iterate over pending actions:
	//   1. Remove any ended transactions that are at the front of the pending
	//      list (i.e. that have been pending for longest). Transactions are
	//      always removed in FIFO order, even if they actually ended in a
	//      different order.
	//   2. Garbage collect any deleted partitions that are at the front of the
	//      pending list.
	for s.mu.pending.Len() > 0 {
		elem := s.mu.pending.Front()
		if elem.Value.activeTxn.id != 0 {
			if !elem.Value.activeTxn.muStore.ended {
				// Oldest transaction is still active, no nothing more to do.
				break
			}
		} else {
			// Garbage collect the deleted partition.
			delete(s.mu.partitions, elem.Value.deletedPartition.key)
		}

		// Remove the action from the pending list.
		s.mu.pending.Remove(elem)
	}

	return nil
}

// AbortTransaction implements the Store interface.
func (s *InMemoryStore) AbortTransaction(ctx context.Context, txn Txn) error {
	inMemTxn := txn.(*inMemoryTxn)
	if inMemTxn.updated {
		// AbortTransaction is only trivially supported by the in-memory store.
		panic(errors.AssertionFailedf(
			"in-memory transaction cannot be aborted because state has already been updated"))
	}
	return s.CommitTransaction(ctx, txn)
}

// GetPartition implements the Store interface.
func (s *InMemoryStore) GetPartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey,
) (*Partition, error) {
	inMemPartition, err := s.getPartition(partitionKey)
	if err != nil {
		return nil, err
	}

	// Acquire exclusive lock on the partition for the duration of the
	// transaction.
	inMemTxn := txn.(*inMemoryTxn)
	inMemPartition.lock.Acquire(inMemTxn.id)
	inMemTxn.lockedPartitions = append(inMemTxn.lockedPartitions, inMemPartition)

	// Return an error if the partition has been deleted.
	if !inMemPartition.isVisibleLocked(inMemTxn.current) {
		return nil, ErrPartitionNotFound
	}

	// Make a deep copy of the partition, since modifications shouldn't impact
	// the store's copy.
	return inMemPartition.lock.partition.Clone(), nil
}

// SetRootPartition implements the Store interface.
func (s *InMemoryStore) SetRootPartition(ctx context.Context, txn Txn, partition *Partition) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	inMemTxn := txn.(*inMemoryTxn)

	existing, ok := s.mu.partitions[RootKey]
	if !ok {
		panic(errors.AssertionFailedf("the root partition cannot be found"))
	}

	// Existing root partition should have been locked by the transaction.
	if !existing.lock.IsAcquiredBy(inMemTxn.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire root partition lock", inMemTxn.id))
	}

	s.reportPartitionSizeLocked(partition.Count())

	// Grow or shrink CVStats slice if a new level is being added or removed.
	expectedLevels := int(partition.Level() - 1)
	if expectedLevels > len(s.mu.stats.CVStats) {
		s.mu.stats.CVStats = slices.Grow(s.mu.stats.CVStats, expectedLevels-len(s.mu.stats.CVStats))
	}
	s.mu.stats.CVStats = s.mu.stats.CVStats[:expectedLevels]

	// Delete previous partition and start new partition.
	existing.lock.deleted = true
	inMemPartition := &inMemoryPartition{key: RootKey}
	inMemPartition.lock.partition = partition
	inMemPartition.lock.start = s.mu.clock
	s.mu.clock++
	s.mu.partitions[RootKey] = inMemPartition

	inMemTxn.updated = true
	return nil
}

// InsertPartition implements the Store interface.
func (s *InMemoryStore) InsertPartition(
	ctx context.Context, txn Txn, partition *Partition,
) (PartitionKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inMemTxn := txn.(*inMemoryTxn)

	partitionKey := s.mu.nextKey
	s.mu.nextKey++

	inMemPartition := &inMemoryPartition{key: partitionKey}
	inMemPartition.lock.partition = partition
	inMemPartition.lock.start = s.mu.clock
	s.mu.clock++
	s.mu.partitions[partitionKey] = inMemPartition

	s.mu.stats.NumPartitions++
	s.reportPartitionSizeLocked(partition.Count())

	inMemTxn.updated = true
	return partitionKey, nil
}

// DeletePartition implements the Store interface.
func (s *InMemoryStore) DeletePartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if partitionKey == RootKey {
		panic(errors.AssertionFailedf("cannot delete the root partition"))
	}

	inMemPartition, ok := s.mu.partitions[partitionKey]
	if !ok {
		return ErrPartitionNotFound
	}

	inMemTxn := txn.(*inMemoryTxn)

	// Existing root partition should have been locked by the transaction.
	if !inMemPartition.lock.IsAcquiredBy(inMemTxn.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire root partition lock", inMemTxn.id))
	}

	if inMemTxn.updated == false {
		// Must be a merge operation.
		root := s.mu.partitions[RootKey]
		if !root.lock.TryAcquire(inMemTxn.id) {
			return ErrRestartTransaction
		}
		inMemTxn.lockedPartitions = append(inMemTxn.lockedPartitions, root)
	}

	if inMemPartition.lock.deleted {
		panic(errors.AssertionFailedf("partition %d is already deleted", partitionKey))
	}
	inMemPartition.lock.deleted = true

	// Add the partition to the pending list so that it will only be garbage
	// collected once all older transactions have ended.
	s.mu.pending.PushBack(pendingAction{deletedPartition: inMemPartition})

	s.mu.stats.NumPartitions--

	inMemTxn.updated = true
	return nil
}

// AddToPartition implements the Store interface.
func (s *InMemoryStore) AddToPartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey, vector vector.T, childKey ChildKey,
) (int, error) {
	inMemPartition, err := s.getPartition(partitionKey)
	if err != nil {
		return 0, err
	}

	inMemTxn := txn.(*inMemoryTxn)

	// Acquire exclusive lock on the partition.
	inMemPartition.lock.Acquire(inMemTxn.id)
	defer inMemPartition.lock.Release()

	// If the partition is deleted, then this transaction conflicted with another
	// transaction and needs to be restarted.
	if !inMemPartition.isVisibleLocked(inMemTxn.current) {
		s.mu.Lock()
		defer s.mu.Unlock()
		inMemTxn.current = s.mu.clock
		s.mu.clock++
		return 0, ErrRestartOperation
	}

	partition := inMemPartition.lock.partition

	if partition.Count() > 0 {
		if (childKey.PartitionKey == 0) != (partition.ChildKeys()[0].PartitionKey == 0) {
			fmt.Println("here")
		}
	}

	if partition.Add(ctx, vector, childKey) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.reportPartitionSizeLocked(partition.Count())
	}

	inMemTxn.updated = true
	return partition.Count(), nil
}

// RemoveFromPartition implements the Store interface.
func (s *InMemoryStore) RemoveFromPartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey, childKey ChildKey,
) (int, error) {
	inMemPartition, err := s.getPartition(partitionKey)
	if err != nil {
		return 0, err
	}

	inMemTxn := txn.(*inMemoryTxn)

	// Acquire exclusive lock on the partition.
	inMemPartition.lock.Acquire(inMemTxn.id)
	defer inMemPartition.lock.Release()

	// If the partition is deleted, then this transaction conflicted with another
	// transaction and needs to be restarted.
	if !inMemPartition.isVisibleLocked(inMemTxn.current) {
		s.mu.Lock()
		defer s.mu.Unlock()
		inMemTxn.current = s.mu.clock
		s.mu.clock++
		return 0, ErrRestartOperation
	}

	partition := inMemPartition.lock.partition
	if partition.ReplaceWithLastByKey(childKey) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.reportPartitionSizeLocked(partition.Count())
	}

	if partition.Count() == 0 && partition.Level() > LeafLevel {
		// A non-leaf partition has zero vectors. If this is still true at the
		// end of the transaction, the K-means tree will be unbalanced, which
		// violates a key constraint.
		txn.(*inMemoryTxn).unbalanced = inMemPartition
	}

	inMemTxn.updated = true
	return partition.Count(), nil
}

// SearchPartitions implements the Store interface.
func (s *InMemoryStore) SearchPartitions(
	ctx context.Context,
	txn Txn,
	partitionKeys []PartitionKey,
	queryVector vector.T,
	searchSet *SearchSet,
	partitionCounts []int,
) (level Level, err error) {
	inMemTxn := txn.(*inMemoryTxn)

	for i := 0; i < len(partitionKeys); i++ {
		inMemPartition, err := s.getPartition(partitionKeys[i])
		if err != nil {
			return 0, err
		}

		// Acquire shared lock on partition and search it. Note that we don't need
		// to check if the partition has been deleted, since the transaction that
		// deleted it must be concurrent with this transaction (or else the
		// deleted partition would not have been found by this transaction).
		func() {
			inMemPartition.lock.AcquireShared(inMemTxn.id)
			defer inMemPartition.lock.ReleaseShared()

			searchLevel, partitionCount := inMemPartition.lock.partition.Search(
				ctx, partitionKeys[i], queryVector, searchSet)
			if i == 0 {
				level = searchLevel
			} else if level != searchLevel {
				// Callers should only search for partitions at the same level.
				panic(errors.AssertionFailedf(
					"caller already searched a partition at level %d, cannot search at level %d",
					level, searchLevel))
			}
			partitionCounts[i] = partitionCount
		}()
	}

	return level, nil
}

// GetFullVectors implements the Store interface.
func (s *InMemoryStore) GetFullVectors(ctx context.Context, txn Txn, refs []VectorWithKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < len(refs); i++ {
		ref := &refs[i]
		if ref.Key.PartitionKey != InvalidKey {
			// Get the partition's centroid.
			inMemPartition, ok := s.mu.partitions[ref.Key.PartitionKey]
			if !ok {
				return ErrPartitionNotFound
			}

			// Don't need to acquire lock to call the Centroid method, since it
			// is immutable and thread-safe.
			ref.Vector = inMemPartition.lock.partition.Centroid()
		} else {
			vector, ok := s.mu.vectors[string(refs[i].Key.PrimaryKey)]
			if ok {
				ref.Vector = vector
			} else {
				ref.Vector = nil
			}
		}
	}

	return nil
}

// MergeStats implements the Store interface.
func (s *InMemoryStore) MergeStats(ctx context.Context, stats *IndexStats, skipMerge bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !skipMerge {
		// Merge CVStats.
		for i := range stats.CVStats {
			if i >= len(s.mu.stats.CVStats) {
				// More levels of incoming stats than the in-memory store has.
				break
			}

			// Calculate exponentially weighted moving averages of the mean and
			// variance samples that are reported by calling agents.
			sample := &stats.CVStats[i]
			cvstats := &s.mu.stats.CVStats[i]
			if cvstats.Mean == 0 {
				cvstats.Mean = sample.Mean
			} else {
				cvstats.Mean = storeStatsAlpha*sample.Mean + (1-storeStatsAlpha)*cvstats.Mean
				cvstats.Variance = storeStatsAlpha*sample.Variance + (1-storeStatsAlpha)*cvstats.Variance
			}
		}
	}

	// Update incoming stats with global stats.
	stats.NumPartitions = s.mu.stats.NumPartitions
	stats.VectorsPerPartition = s.mu.stats.VectorsPerPartition

	extra := len(s.mu.stats.CVStats) - len(stats.CVStats)
	if extra > 0 {
		stats.CVStats = slices.Grow(stats.CVStats, extra)
	}
	stats.CVStats = stats.CVStats[:len(s.mu.stats.CVStats)]
	copy(stats.CVStats, s.mu.stats.CVStats)

	return nil
}

// InsertVector inserts a new full-size vector into the in-memory store,
// associated with the given primary key. This mimics inserting a vector into
// the primary index, and is used during testing and benchmarking.
func (s *InMemoryStore) InsertVector(txn Txn, key PrimaryKey, vector vector.T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inMemTxn := txn.(*inMemoryTxn)
	inMemTxn.updated = true

	s.mu.vectors[string(key)] = vector
}

// DeleteVector deletes the vector associated with the given primary key from
// the in-memory store. This mimics deleting a vector from the primary index,
// and is used during testing and benchmarking.
func (s *InMemoryStore) DeleteVector(txn Txn, key PrimaryKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inMemTxn := txn.(*inMemoryTxn)
	inMemTxn.updated = true

	delete(s.mu.vectors, string(key))
}

// GetVector returns a single vector from the store, by its primary key. This
// is used for testing.
func (s *InMemoryStore) GetVector(key PrimaryKey) vector.T {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.mu.vectors[string(key)]
}

// GetAllVectors returns all vectors that have been added to the store as key
// and vector pairs. This is used for testing.
func (s *InMemoryStore) GetAllVectors() []VectorWithKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	refs := make([]VectorWithKey, 0, len(s.mu.vectors))
	for key, vec := range s.mu.vectors {
		refs = append(refs, VectorWithKey{Key: ChildKey{PrimaryKey: PrimaryKey(key)}, Vector: vec})
	}
	return refs
}

// MarshalBinary saves the in-memory store as a bytes. This allows the store to
// be saved and later loaded without needing to rebuild it from scratch.
func (s *InMemoryStore) MarshalBinary() (data []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.pending.Len() > 0 {
		panic(errors.AssertionFailedf("cannot save store if there are pending transactions"))
	}

	storeProto := StoreProto{
		Dims:       s.dims,
		Seed:       s.seed,
		Partitions: make([]PartitionProto, 0, len(s.mu.partitions)),
		NextKey:    s.mu.nextKey,
		Vectors:    make([]VectorProto, 0, len(s.mu.vectors)),
		Stats:      s.mu.stats,
	}

	// Remap partitions to protobufs.
	for partitionKey, inMemPartition := range s.mu.partitions {
		func() {
			inMemPartition.lock.AcquireShared(0)
			defer inMemPartition.lock.ReleaseShared()

			partition := inMemPartition.lock.partition
			partitionProto := PartitionProto{
				PartitionKey: partitionKey,
				ChildKeys:    partition.ChildKeys(),
				Level:        partition.Level(),
			}

			rabitq, ok := partition.quantizedSet.(*quantize.RaBitQuantizedVectorSet)
			if ok {
				partitionProto.RaBitQ = rabitq
			} else {
				partitionProto.UnQuantized = partition.quantizedSet.(*quantize.UnQuantizedVectorSet)
			}

			storeProto.Partitions = append(storeProto.Partitions, partitionProto)
		}()
	}

	// Remap vectors to protobufs.
	for key, vector := range s.mu.vectors {
		vectorWithKey := VectorProto{PrimaryKey: []byte(key), Vector: vector}
		storeProto.Vectors = append(storeProto.Vectors, vectorWithKey)
	}

	// Serialize the protobuf as bytes.
	return protoutil.Marshal(&storeProto)
}

// LoadInMemoryStore loads the in-memory store from bytes that were previously
// saved by MarshalBinary.
func LoadInMemoryStore(data []byte) (*InMemoryStore, error) {
	// Unmarshal bytes into a protobuf.
	var storeProto StoreProto
	if err := protoutil.Unmarshal(data, &storeProto); err != nil {
		return nil, err
	}

	// Construct the InMemoryStore object.
	inMemStore := &InMemoryStore{
		dims: storeProto.Dims,
		seed: storeProto.Seed,
	}
	inMemStore.mu.clock = 2
	inMemStore.mu.partitions = make(map[PartitionKey]*inMemoryPartition, len(storeProto.Partitions))
	inMemStore.mu.nextKey = storeProto.NextKey
	inMemStore.mu.vectors = make(map[string]vector.T, len(storeProto.Vectors))
	inMemStore.mu.stats = storeProto.Stats
	inMemStore.mu.pending.Init()

	raBitQuantizer := quantize.NewRaBitQuantizer(storeProto.Dims, storeProto.Seed)
	unquantizer := quantize.NewUnQuantizer(storeProto.Dims)

	// Construct the Partition objects.
	for i := range storeProto.Partitions {
		partitionProto := &storeProto.Partitions[i]
		var inMemPartition = &inMemoryPartition{
			key: partitionProto.PartitionKey,
		}
		inMemPartition.lock.start = 1

		partition := inMemPartition.lock.partition
		partition.childKeys = partitionProto.ChildKeys
		partition.level = partitionProto.Level

		if partitionProto.RaBitQ != nil {
			partition.quantizer = raBitQuantizer
			partition.quantizedSet = partitionProto.RaBitQ
		} else {
			partition.quantizer = unquantizer
			partition.quantizedSet = partitionProto.UnQuantized
		}
		inMemStore.mu.partitions[partitionProto.PartitionKey] = inMemPartition
	}

	// Insert vectors into the in-memory store.
	for i := range storeProto.Vectors {
		vectorProto := storeProto.Vectors[i]
		inMemStore.mu.vectors[string(vectorProto.PrimaryKey)] = vectorProto.Vector
	}

	return inMemStore, nil
}

func (s *InMemoryStore) getPartition(partitionKey PartitionKey) (*inMemoryPartition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partition, ok := s.mu.partitions[partitionKey]
	if !ok {
		return nil, ErrPartitionNotFound
	}
	return partition, nil
}

// reportPartitionSizeLocked updates the vectors per partition statistic. It is
// called with the count of vectors in a partition when a partition is inserted
// or updated.
// NOTE: Callers must have acquired the s.mu lock before calling.
func (s *InMemoryStore) reportPartitionSizeLocked(count int) {
	if s.mu.stats.VectorsPerPartition == 0 {
		// Use first value if this is the first update.
		s.mu.stats.VectorsPerPartition = float64(count)
	} else {
		// Calculate exponential moving average.
		s.mu.stats.VectorsPerPartition = (1 - storeStatsAlpha) * s.mu.stats.VectorsPerPartition
		s.mu.stats.VectorsPerPartition += storeStatsAlpha * float64(count)
	}
}
