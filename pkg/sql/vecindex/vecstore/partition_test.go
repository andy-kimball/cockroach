// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

func TestPartition(t *testing.T) {
	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})

	childKey10 := ChildKey{PartitionKey: 10}
	childKey20 := ChildKey{PartitionKey: 20}
	childKey30 := ChildKey{PartitionKey: 30}
	childKey40 := ChildKey{PartitionKey: 40}

	// Create new partition and add 4 vectors.
	quantizer := quantize.NewUnQuantizer(2)
	vectors := vector.MakeSetFromRawData([]float32{1, 2, 5, 2, 6, 6}, 2)
	quantizedSet := quantizer.Quantize(ctx, &vectors)
	childKeys := []ChildKey{childKey10, childKey20, childKey30}
	partition := NewPartition(quantizer, quantizedSet, childKeys, 1)
	require.True(t, partition.Add(ctx, vector.T{4, 3}, childKey40))

	// Try to add duplicate vector.
	require.False(t, partition.Add(ctx, vector.T{10, 10}, childKey20))

	require.Equal(t, 4, partition.Count())
	require.Equal(t, []ChildKey{childKey10, childKey20, childKey30, childKey40}, partition.ChildKeys())
	require.Equal(t, []float32{4, 3.33}, roundFloats(partition.Centroid(), 2))

	// Search method.
	searchSet := SearchSet{MaxResults: 3}
	level, count := partition.Search(ctx, RootKey, vector.T{1, 1}, &searchSet)
	require.Equal(t, Level(1), level)
	require.Equal(t, 4, count)
	result1 := SearchResult{QuerySquaredDistance: 1, ErrorBound: 0, CentroidDistance: 3.2830, ParentPartitionKey: 1, ChildKey: childKey10}
	result2 := SearchResult{QuerySquaredDistance: 13, ErrorBound: 0, CentroidDistance: 0.3333, ParentPartitionKey: 1, ChildKey: childKey40}
	result3 := SearchResult{QuerySquaredDistance: 17, ErrorBound: 0, CentroidDistance: 1.6667, ParentPartitionKey: 1, ChildKey: childKey20}
	results := roundResults(searchSet.PopResults(), 4)
	require.Equal(t, SearchResults{result1, result2, result3}, results)

	// Find method.
	require.Equal(t, 2, partition.Find(childKey30))
	require.Equal(t, 3, partition.Find(childKey40))
	require.Equal(t, -1, partition.Find(ChildKey{PrimaryKey: []byte{1, 2}}))

	// Remove vectors.
	require.True(t, partition.ReplaceWithLastByKey(childKey20))
	require.Equal(t, []ChildKey{childKey10, childKey40, childKey30}, partition.ChildKeys())
	require.False(t, partition.ReplaceWithLastByKey(childKey20))
	require.True(t, partition.ReplaceWithLastByKey(childKey30))
	require.Equal(t, []ChildKey{childKey10, childKey40}, partition.ChildKeys())
	require.True(t, partition.ReplaceWithLastByKey(childKey10))
	require.Equal(t, []ChildKey{childKey40}, partition.ChildKeys())
	require.True(t, partition.ReplaceWithLastByKey(childKey40))
	require.Equal(t, []ChildKey{}, partition.ChildKeys())
}

func roundResults(results SearchResults, prec int) SearchResults {
	for i := range results {
		result := &results[i]
		result.QuerySquaredDistance = float32(scalar.Round(float64(result.QuerySquaredDistance), prec))
		result.ErrorBound = float32(scalar.Round(float64(result.ErrorBound), prec))
		result.CentroidDistance = float32(scalar.Round(float64(result.CentroidDistance), prec))
		result.Vector = roundFloats(result.Vector, prec)
	}
	return results
}

func roundFloats(s []float32, prec int) []float32 {
	if s == nil {
		return nil
	}
	t := make([]float32, len(s))
	copy(t, s)
	num32.Round(t, prec)
	return t
}
