// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

type splitData struct {
	ParentPartitionKey vecstore.PartitionKey

	// This does not have split partitions in it.
	ParentPartition *vecstore.Partition

	ParentVectors vector.Set

	PartitionKey vecstore.PartitionKey

	Partition *vecstore.Partition

	Vectors vector.Set

	Left siblingSplitData

	Right siblingSplitData
}

// siblingSplitData contains information about one of the new smaller sibling
// partitions created from splitting an over-sized partition.
type siblingSplitData struct {
	PartitionKey vecstore.PartitionKey
	// This does not have vectors from split partitions in it.
	Closest vecstore.SearchResults
	// Partition contains a subset of the quantized vectors and child keys from
	// the splitting partition.
	Partition *vecstore.Partition
	// Vectors is the subset of full-size vectors from the splitting partition.
	// The vectors are in randomized format.
	Vectors vector.Set
	// OldCentroidDistances are the exact distances from each vector to the
	// centroid of the splitting partition.
	OldCentroidDistances []float32
}

// Init initializes the split information by creating a new partition from the
// given subset of vectors from the splitting partition.
func (s *siblingSplitData) Init(
	ctx context.Context,
	quantizer quantize.Quantizer,
	vectors vector.Set,
	oldCentroidDistances []float32,
	childKeys []vecstore.ChildKey,
	level vecstore.Level,
) {
	s.Vectors = vectors
	s.OldCentroidDistances = oldCentroidDistances
	quantizedSet := quantizer.Quantize(ctx, &s.Vectors)
	s.Partition = vecstore.NewPartition(quantizer, quantizedSet, childKeys, level)
}

// ReplaceWithLast removes the vector at the given offset in the set, replacing
// it with the last vector in the set. The modified set has one less element and
// the last vector's position changes.
func (s *siblingSplitData) ReplaceWithLast(offset int) {
	s.Vectors.ReplaceWithLast(offset)
	s.OldCentroidDistances[offset] = s.OldCentroidDistances[len(s.OldCentroidDistances)-1]
	s.OldCentroidDistances = s.OldCentroidDistances[:len(s.OldCentroidDistances)-1]
	s.Partition.ReplaceWithLast(offset)
}
