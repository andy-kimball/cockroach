// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package memo

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type RelNode interface {
	opt.Node
	Memo() *Memo
	Relational() *props.Relational
	Physical() *props.Physical
	FirstNode() RelNode
	NextNode() RelNode
	Cost() Cost

	group() nodeGroup
	setNext(nd RelNode)
}

var TrueSingleton = TrueExpr{}
var FalseSingleton = FalseExpr{}
var NullSingleton = NullExpr{Typ: types.Unknown}
var CountRowsSingleton = CountRowsExpr{}

var TrueFilter = FiltersExpr{}
var FalseFilter = FiltersExpr{{Condition: &FalseSingleton}}

var EmptyGroupingPrivate = GroupingPrivate{}

// EmptyTupleType represents an empty types.TTuple.
var EmptyTupleType types.T = types.TTuple{}

var EmptyTuple = TupleExpr{Typ: EmptyTupleType}

var EmptyTupleList = ScalarListExpr{&EmptyTuple}

const MaxAggChildren = 3

func LastGroupMember(member RelNode) RelNode {
	for {
		next := member.NextNode()
		if next == nil {
			return member
		}
		member = next
	}
}

func (n FiltersExpr) IsTrue() bool {
	return len(n) == 0
}

func (n FiltersExpr) IsFalse() bool {
	return len(n) == 1 && n[0].Condition.Op() == opt.FalseOp
}

func (n FiltersExpr) OuterCols(mem *Memo) opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.UnionWith(n[i].ScalarProps(mem).OuterCols)
	}
	return colSet
}

func (n AggregationsExpr) OutputCols() opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.Add(int(n[i].Col))
	}
	return colSet
}

type ScalarPropsExpr interface {
	opt.ScalarExpr
	ScalarProps(mem *Memo) *props.Scalar
}

// TupleOrdinal is an ordinal index into an expression of type Tuple. It is
// used by the ColumnAccess scalar expression.
type TupleOrdinal uint32

// ScanLimit is used for a limited table or index scan and stores the limit as
// well as the desired scan direction. A value of 0 means that there is no
// limit.
type ScanLimit int64

// MakeScanLimit initializes a ScanLimit with a number of rows and a direction.
func MakeScanLimit(rowCount int64, reverse bool) ScanLimit {
	if reverse {
		return ScanLimit(-rowCount)
	}
	return ScanLimit(rowCount)
}

// IsSet returns true if there is a limit.
func (sl ScanLimit) IsSet() bool {
	return sl != 0
}

// RowCount returns the number of rows in the limit.
func (sl ScanLimit) RowCount() int64 {
	if sl.Reverse() {
		return int64(-sl)
	}
	return int64(sl)
}

// Reverse returns true if the limit requires a reverse scan.
func (sl ScanLimit) Reverse() bool {
	return sl < 0
}

func (sl ScanLimit) String() string {
	if sl.Reverse() {
		return fmt.Sprintf("%d(rev)", -sl)
	}
	return fmt.Sprintf("%d", sl)
}

// ScanFlags stores any flags for the scan specified in the query (see
// tree.IndexFlags). These flags may be consulted by transformation rules or the
// coster.
type ScanFlags struct {
	// NoIndexJoin disallows use of non-covering indexes (index-join) for scanning
	// this table.
	NoIndexJoin bool

	// ForceIndex forces the use of a specific index (specified in Index).
	// ForceIndex and NoIndexJoin cannot both be set at the same time.
	ForceIndex bool
	Index      int
}

// Empty returns true if there are no flags set.
func (sf *ScanFlags) Empty() bool {
	return !sf.NoIndexJoin && !sf.ForceIndex
}

// CanProvideOrdering returns true if the scan operator returns rows that
// satisfy the given required ordering; it also returns whether the scan needs
// to be in reverse order to match the required ordering.
func (s *ScanPrivate) CanProvideOrdering(
	md *opt.Metadata, required *props.OrderingChoice,
) (ok bool, reverse bool) {
	// Scan naturally orders according to scanned index's key columns. A scan can
	// be executed either as a forward or as a reverse scan (unless it has a row
	// limit, in which case the direction is fixed).
	//
	// The code below follows the structure of OrderingChoice.Implies. We go
	// through the columns and determine if the ordering matches with either scan
	// direction.

	// We start off as accepting either a forward or a reverse scan. Until then,
	// the reverse variable is unset. Once the direction is known, reverseSet is
	// true and reverse indicates whether we need to do a reverse scan.
	const (
		either = 0
		fwd    = 1
		rev    = 2
	)
	direction := either
	if s.HardLimit.IsSet() {
		// When we have a limit, the limit forces a certain scan direction (because
		// it affects the results, not just their ordering).
		direction = fwd
		if s.HardLimit.Reverse() {
			direction = rev
		}
	}
	index := md.Table(s.Table).Index(s.Index)
	for left, right := 0, 0; right < len(required.Columns); {
		if left >= index.KeyColumnCount() {
			return false, false
		}
		indexCol := index.Column(left)
		indexColID := s.Table.ColumnID(indexCol.Ordinal)
		if required.Optional.Contains(int(indexColID)) {
			left++
			continue
		}
		reqCol := &required.Columns[right]
		if !reqCol.Group.Contains(int(indexColID)) {
			return false, false
		}
		// The directions of the index column and the required column impose either
		// a forward or a reverse scan.
		required := fwd
		if indexCol.Descending != reqCol.Descending {
			required = rev
		}
		if direction == either {
			direction = required
		} else if direction != required {
			// We already determined the direction, and according to it, this column
			// has the wrong direction.
			return false, false
		}
		left, right = left+1, right+1
	}
	// If direction is either, we prefer forward scan.
	return true, direction == rev
}

// CanProvideOrdering returns true if the row number operator returns rows that
// can satisfy the given required ordering.
func (r *RowNumberPrivate) CanProvideOrdering(required *props.OrderingChoice) bool {
	// By construction, any prefix of the ordering required of the input is also
	// ordered by the ordinality column. For example, if the required input
	// ordering is +a,+b, then any of these orderings can be provided:
	//
	//   +ord
	//   +a,+ord
	//   +a,+b,+ord
	//
	// As long as the optimizer is enabled, it will have already reduced the
	// ordering required of this operator to take into account that the ordinality
	// column is a key, so there will never be ordering columns after the
	// ordinality column in that case.
	ordCol := opt.MakeOrderingColumn(r.ColID, false)
	prefix := len(required.Columns)
	for i := range required.Columns {
		if required.MatchesAt(i, ordCol) {
			if i == 0 {
				return true
			}
			prefix = i
			break
		}
	}

	if prefix < len(required.Columns) {
		truncated := required.Copy()
		truncated.Truncate(prefix)
		return r.Ordering.Implies(&truncated)
	}

	return r.Ordering.Implies(required)
}

// CanProvideOrdering returns true if the MergeJoin operator returns rows that
// satisfy the given required ordering.
func (m *MergeJoinPrivate) CanProvideOrdering(required *props.OrderingChoice) bool {
	// TODO(radu): in principle, we could pass through an ordering that covers
	// more than the equality columns. For example, if we have a merge join
	// with left ordering a+,b+ and right ordering x+,y+ we could guarantee
	// a+,b+,c+ if we pass that requirement through to the left side. However,
	// this requires a specific contract on the execution side on which side's
	// ordering is preserved when multiple rows match on the equality columns.
	switch m.JoinType {
	case opt.InnerJoinOp:
		return m.LeftOrdering.Implies(required) || m.RightOrdering.Implies(required)

	case opt.LeftJoinOp, opt.SemiJoinOp, opt.AntiJoinOp:
		return m.LeftOrdering.Implies(required)

	case opt.RightJoinOp:
		return m.RightOrdering.Implies(required)

	default:
		return false
	}
}
