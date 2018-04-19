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

package norm

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// MatchedRuleFunc defines the callback function for the NotifyOnMatchedRule
// event supported by the optimizer and factory. It is invoked each time an
// optimization rule (Normalize or Explore) has been matched. The name of the
// matched rule is passed as a parameter. If the function returns false, then
// the rule is not applied (i.e. skipped).
type MatchedRuleFunc func(ruleName opt.RuleName) bool

// AppliedRuleFunc defines the callback function for the NotifyOnAppliedRule
// event supported by the optimizer and factory. It is invoked each time an
// optimization rule (Normalize or Explore) has been applied. The function is
// called with the name of the rule and the memo group it affected. If the rule
// was an exploration rule, then the added parameter gives the number of
// expressions added to the group by the rule.
type AppliedRuleFunc func(ruleName opt.RuleName, group memo.GroupID, added int)

//go:generate optgen -out factory.og.go factory ../ops/*.opt rules/*.opt

// Factory constructs a normalized expression tree within the memo. As each
// kind of expression is constructed by the factory, it transitively runs
// normalization transformations defined for that expression type. This may
// result in the construction of a different type of expression than what was
// requested. If, after normalization, the expression is already part of the
// memo, then construction is a no-op. Otherwise, a new memo group is created,
// with the normalized expression as its first and only expression.
//
// The result of calling each Factory Construct method is the id of the group
// that was constructed. Callers can access the normalized expression tree that
// the factory constructs by creating a memo.ExprView, like this:
//
//   ev := memo.MakeNormExprView(f.Memo(), group)
//
// Factory is largely auto-generated by optgen. The generated code can be found
// in factory.og.go. The factory.go file contains helper functions that are
// invoked by normalization patterns. While most patterns are specified in the
// optgen DSL, the factory always calls the `onConstruct` method as its last
// step, in order to allow any custom manual code to execute.
type Factory struct {
	mem     *memo.Memo
	evalCtx *tree.EvalContext

	// ruleCycles is used to detect cyclical rule invocations. Each rule with
	// the "DetectCycles" tag adds its expression fingerprint into this map
	// before constructing its replacement. If the replacement pattern recursively
	// invokes the same rule (or another rule with the DetectCycles tag) with that
	// same fingerprint, then the rule sees that the fingerprint is already in the
	// map, and will skip application of the rule.
	ruleCycles map[memo.Fingerprint]bool

	// matchedRule is the callback function that is invoked each time a normalize
	// rule has been matched by the factory. It can be set via a call to the
	// NotifyOnMatchedRule method.
	matchedRule MatchedRuleFunc

	// appliedRule is the callback function which is invoked each time a normalize
	// rule has been applied by the factory. It can be set via a call to the
	// NotifyOnAppliedRule method.
	appliedRule AppliedRuleFunc
}

// NewFactory returns a new Factory structure with a new, blank memo structure
// inside.
func NewFactory(evalCtx *tree.EvalContext) *Factory {
	return &Factory{
		mem:        memo.New(),
		evalCtx:    evalCtx,
		ruleCycles: make(map[memo.Fingerprint]bool),
	}
}

// DisableOptimizations disables all transformation rules. The unaltered input
// expression tree becomes the output expression tree (because no transforms
// are applied).
func (f *Factory) DisableOptimizations() {
	f.NotifyOnMatchedRule(func(opt.RuleName) bool { return false })
}

// NotifyOnMatchedRule sets a callback function which is invoked each time a
// normalize rule has been matched by the factory. If matchedRule is nil, then
// no further notifications are sent, and all rules are applied by default. In
// addition, callers can invoke the DisableOptimizations convenience method to
// disable all rules.
func (f *Factory) NotifyOnMatchedRule(matchedRule MatchedRuleFunc) {
	f.matchedRule = matchedRule
}

// NotifyOnAppliedRule sets a callback function which is invoked each time a
// normalize rule has been applied by the factory. If appliedRule is nil, then
// no further notifications are sent.
func (f *Factory) NotifyOnAppliedRule(appliedRule AppliedRuleFunc) {
	f.appliedRule = appliedRule
}

// Memo returns the memo structure that the factory is operating upon.
func (f *Factory) Memo() *memo.Memo {
	return f.mem
}

// Metadata returns the query-specific metadata, which includes information
// about the columns and tables used in this particular query.
func (f *Factory) Metadata() *opt.Metadata {
	return f.mem.Metadata()
}

// InternList adds the given list of group IDs to memo storage and returns an
// ID that can be used for later lookup. If the same list was added previously,
// this method is a no-op and returns the ID of the previous value.
func (f *Factory) InternList(items []memo.GroupID) memo.ListID {
	return f.mem.InternList(items)
}

// onConstruct is called as a final step by each factory construction method,
// so that any custom manual pattern matching/replacement code can be run.
func (f *Factory) onConstruct(group memo.GroupID) memo.GroupID {
	return group
}

// ----------------------------------------------------------------------
//
// Private extraction functions
//   Helper functions that make extracting common private types easier.
//
// ----------------------------------------------------------------------

func (f *Factory) extractColList(private memo.PrivateID) opt.ColList {
	return f.mem.LookupPrivate(private).(opt.ColList)
}

// ----------------------------------------------------------------------
//
// List functions
//   General custom match and replace functions used to test and construct
//   lists.
//
// ----------------------------------------------------------------------

// listOnlyHasNulls if every item in the given list is a Null op. If the list
// is empty, listOnlyHasNulls returns false.
func (f *Factory) listOnlyHasNulls(list memo.ListID) bool {
	if list.Length == 0 {
		return false
	}

	for _, item := range f.mem.LookupList(list) {
		if f.mem.NormExpr(item).Operator() != opt.NullOp {
			return false
		}
	}
	return true
}

// isSortedUniqueList returns true if the list is in sorted order, with no
// duplicates. See the comment for listSorter.compare for comparison rule
// details.
func (f *Factory) isSortedUniqueList(list memo.ListID) bool {
	ls := listSorter{f: f, list: f.mem.LookupList(list)}
	for i := 0; i < int(list.Length-1); i++ {
		if !ls.less(i, i+1) {
			return false
		}
	}
	return true
}

// constructSortedUniqueList sorts the given list and removes duplicates, and
// returns the resulting list. See the comment for listSorter.compare for
// comparison rule details.
func (f *Factory) constructSortedUniqueList(list memo.ListID) memo.ListID {
	// Make a copy of the list, since it needs to stay immutable.
	newList := make([]memo.GroupID, list.Length)
	copy(newList, f.mem.LookupList(list))
	ls := listSorter{f: f, list: newList}

	// Sort the list.
	sort.Slice(ls.list, ls.less)

	// Remove duplicates from the list.
	n := 0
	for i := 0; i < int(list.Length); i++ {
		if i == 0 || ls.compare(i-1, i) < 0 {
			newList[n] = newList[i]
			n++
		}
	}
	return f.mem.InternList(newList[:n])
}

// listSorter is a helper struct that implements the sort.Slice "less"
// comparison function.
type listSorter struct {
	f    *Factory
	list []memo.GroupID
}

// less returns true if item i in the list compares less than item j.
// sort.Slice uses this method to sort the list.
func (s listSorter) less(i, j int) bool {
	return s.compare(i, j) < 0
}

// compare returns -1 if item i compares less than item j, 0 if they are equal,
// and 1 if item i compares greater. Constants sort before non-constants, and
// are sorted and uniquified according to Datum comparison rules. Non-constants
// are sorted and uniquified by GroupID (arbitrary, but stable).
func (s listSorter) compare(i, j int) int {
	// If both are constant values, then use datum comparison.
	isLeftConst := s.f.mem.NormExpr(s.list[i]).IsConstValue()
	isRightConst := s.f.mem.NormExpr(s.list[j]).IsConstValue()
	if isLeftConst {
		if !isRightConst {
			// Constant always sorts before non-constant
			return -1
		}

		leftD := memo.ExtractConstDatum(memo.MakeNormExprView(s.f.mem, s.list[i]))
		rightD := memo.ExtractConstDatum(memo.MakeNormExprView(s.f.mem, s.list[j]))
		return leftD.Compare(s.f.evalCtx, rightD)
	} else if isRightConst {
		// Non-constant always sorts after constant.
		return 1
	}

	// Arbitrarily order by GroupID.
	if s.list[i] < s.list[j] {
		return -1
	} else if s.list[i] > s.list[j] {
		return 1
	}
	return 0
}

// ----------------------------------------------------------------------
//
// Typing functions
//   General custom match and replace functions used to test and construct
//   expression data types.
//
// ----------------------------------------------------------------------

// hasType returns true if the given expression has a static type that's
// equivalent to the requested type.
func (f *Factory) hasType(group memo.GroupID, typ memo.PrivateID) bool {
	groupType := f.lookupScalar(group).Type
	requestedType := f.mem.LookupPrivate(typ).(types.T)
	return groupType.Equivalent(requestedType)
}

func (f *Factory) boolType() memo.PrivateID {
	return f.InternType(types.Bool)
}

// canConstructBinary returns true if (op left right) has a valid binary op
// overload and is therefore legal to construct. For example, while
// (Minus <date> <int>) is valid, (Minus <int> <date>) is not.
func (f *Factory) canConstructBinary(op opt.Operator, left, right memo.GroupID) bool {
	leftType := f.lookupScalar(left).Type
	rightType := f.lookupScalar(right).Type
	return memo.BinaryOverloadExists(opt.MinusOp, rightType, leftType)
}

// ----------------------------------------------------------------------
//
// Property functions
//   General custom match and replace functions used to test expression
//   logical properties.
//
// ----------------------------------------------------------------------

// lookupLogical returns the given group's logical properties.
func (f *Factory) lookupLogical(group memo.GroupID) *memo.LogicalProps {
	return f.mem.GroupProperties(group)
}

// lookupRelational returns the given group's logical relational properties.
func (f *Factory) lookupRelational(group memo.GroupID) *memo.RelationalProps {
	return f.lookupLogical(group).Relational
}

// lookupScalar returns the given group's logical scalar properties.
func (f *Factory) lookupScalar(group memo.GroupID) *memo.ScalarProps {
	return f.lookupLogical(group).Scalar
}

// outputCols is a helper function that extracts the set of columns projected
// by the given operator. In addition to extracting columns from any relational
// operator, outputCols can also extract columns from the Projections and
// Aggregations scalar operators, which are used with Project and GroupBy.
func (f *Factory) outputCols(group memo.GroupID) opt.ColSet {
	// Handle columns projected by relational operators.
	logical := f.lookupLogical(group)
	if logical.Relational != nil {
		return f.lookupRelational(group).OutputCols
	}

	// Handle columns projected by Aggregations and Projections operators.
	var colList memo.PrivateID
	expr := f.mem.NormExpr(group)
	switch expr.Operator() {
	case opt.AggregationsOp:
		colList = expr.AsAggregations().Cols()
	case opt.ProjectionsOp:
		colList = expr.AsProjections().Cols()
	default:
		panic(fmt.Sprintf("outputCols doesn't support op %s", expr.Operator()))
	}

	return opt.ColListToSet(f.extractColList(colList))
}

// outerCols returns the set of outer columns associated with the given group,
// whether it be a relational or scalar operator.
func (f *Factory) outerCols(group memo.GroupID) opt.ColSet {
	return f.lookupLogical(group).OuterCols()
}

// synthesizedCols returns the set of columns which have been added by the given
// Project operator to its input columns. For example, the "x+1" column is a
// synthesized column in "SELECT x, x+1 FROM a".
func (f *Factory) synthesizedCols(project memo.GroupID) opt.ColSet {
	synth := f.outputCols(project).Copy()
	input := f.mem.NormExpr(project).AsProject().Input()
	synth.DifferenceWith(f.outputCols(input))
	return synth
}

// onlyConstants returns true if the scalar expression is a "constant
// expression tree", meaning that it will always evaluate to the same result.
// See the CommuteConst pattern comment for more details.
func (f *Factory) onlyConstants(group memo.GroupID) bool {
	// TODO(andyk): Consider impact of "impure" functions with side effects.
	return f.lookupScalar(group).OuterCols.Empty()
}

// hasNoCols returns true if the group has zero output columns.
func (f *Factory) hasNoCols(group memo.GroupID) bool {
	return f.outputCols(group).Empty()
}

// hasSameCols returns true if the two groups have an identical set of output
// columns.
func (f *Factory) hasSameCols(left, right memo.GroupID) bool {
	return f.outputCols(left).Equals(f.outputCols(right))
}

// hasSubsetCols returns true if the left group's output columns are a subset of
// the right group's output columns.
func (f *Factory) hasSubsetCols(left, right memo.GroupID) bool {
	return f.outputCols(left).SubsetOf(f.outputCols(right))
}

func (f *Factory) canBeNull(variable memo.GroupID)

// ----------------------------------------------------------------------
//
// Project Rules
//   Custom match and replace functions used with project.opt rules.
//
// ----------------------------------------------------------------------

// neededCols returns the set of columns needed by the given group. It is an
// alias for outerCols that's used for clarity with the UnusedCols patterns.
func (f *Factory) neededCols(group memo.GroupID) opt.ColSet {
	return f.outerCols(group)
}

// neededCols2 unions the set of columns needed by either of the given groups.
func (f *Factory) neededCols2(left, right memo.GroupID) opt.ColSet {
	return f.outerCols(left).Union(f.outerCols(right))
}

// neededCols3 unions the set of columns needed by any of the given groups.
func (f *Factory) neededCols3(group1, group2, group3 memo.GroupID) opt.ColSet {
	cols := f.outerCols(group1)
	cols.UnionWith(f.outerCols(group2))
	cols.UnionWith(f.outerCols(group3))
	return cols
}

// neededColsGroupBy unions the columns needed by either of a GroupBy's
// operands - either aggregations or groupingCols. This case doesn't fit any
// of the neededCols methods because groupingCols is a private, not a group.
func (f *Factory) neededColsGroupBy(aggs memo.GroupID, groupingCols memo.PrivateID) opt.ColSet {
	colSet := f.mem.LookupPrivate(groupingCols).(opt.ColSet)
	return f.outerCols(aggs).Union(colSet)
}

// neededColsLimit unions the columns needed by Projections with the columns in
// the Ordering of a Limit/Offset operator.
func (f *Factory) neededColsLimit(projections memo.GroupID, ordering memo.PrivateID) opt.ColSet {
	colSet := f.outerCols(projections).Copy()
	for _, col := range f.mem.LookupPrivate(ordering).(memo.Ordering) {
		colSet.Add(int(col.ID()))
	}
	return colSet
}

// hasUnusedColumns returns true if the target group has additional columns
// that are not part of the neededCols set.
func (f *Factory) hasUnusedColumns(target memo.GroupID, neededCols opt.ColSet) bool {
	return !f.outputCols(target).Difference(neededCols).Empty()
}

// filterUnusedColumns creates an expression that discards any outputs columns
// of the given group that are not used. If the target expression type supports
// column filtering (like Scan, Values, Projections, etc.), then create a new
// instance of that operator that does the filtering. Otherwise, construct a
// Project operator that wraps the operator and does the filtering.
func (f *Factory) filterUnusedColumns(target memo.GroupID, neededCols opt.ColSet) memo.GroupID {
	targetExpr := f.mem.NormExpr(target)
	switch targetExpr.Operator() {
	case opt.ScanOp:
		return f.filterUnusedScanColumns(target, neededCols)

	case opt.ValuesOp:
		return f.filterUnusedValuesColumns(target, neededCols)
	}

	// Get the subset of the target group's output columns that are in the
	// needed set (and discard those that aren't).
	colSet := f.outputCols(target).Intersection(neededCols)
	cnt := colSet.Len()

	// Create a new list of groups to project, along with the list of column
	// indexes to be projected. These will become inputs to the construction of
	// the Projections or Aggregations operator.
	groupList := make([]memo.GroupID, 0, cnt)
	colList := make(opt.ColList, 0, cnt)

	switch targetExpr.Operator() {
	case opt.ProjectionsOp, opt.AggregationsOp:
		// Get groups from existing lists.
		var existingList []memo.GroupID
		var existingCols opt.ColList
		if targetExpr.Operator() == opt.ProjectionsOp {
			existingList = f.mem.LookupList(targetExpr.AsProjections().Elems())
			existingCols = f.extractColList(targetExpr.AsProjections().Cols())
		} else {
			existingList = f.mem.LookupList(targetExpr.AsAggregations().Aggs())
			existingCols = f.extractColList(targetExpr.AsAggregations().Cols())
		}

		for i, group := range existingList {
			// Only add groups that are part of the needed columns.
			if neededCols.Contains(int(existingCols[i])) {
				groupList = append(groupList, group)
				colList = append(colList, existingCols[i])
			}
		}

	default:
		// Construct new variable groups for each output column that's needed.
		colSet.ForEach(func(i int) {
			v := f.ConstructVariable(f.InternColumnID(opt.ColumnID(i)))
			groupList = append(groupList, v)
			colList = append(colList, opt.ColumnID(i))
		})
	}

	if targetExpr.Operator() == opt.AggregationsOp {
		return f.ConstructAggregations(f.InternList(groupList), f.InternColList(colList))
	}

	projections := f.ConstructProjections(f.InternList(groupList), f.InternColList(colList))
	if targetExpr.Operator() == opt.ProjectionsOp {
		return projections
	}

	// Else wrap in Project operator.
	return f.ConstructProject(target, projections)
}

// filterUnusedScanColumns constructs a new Scan operator based on the given
// existing Scan operator, but projecting only the needed columns.
func (f *Factory) filterUnusedScanColumns(scan memo.GroupID, neededCols opt.ColSet) memo.GroupID {
	colSet := f.outputCols(scan).Intersection(neededCols)
	scanExpr := f.mem.NormExpr(scan).AsScan()
	existing := f.mem.LookupPrivate(scanExpr.Def()).(*memo.ScanOpDef)
	new := memo.ScanOpDef{Table: existing.Table, Cols: colSet}
	return f.ConstructScan(f.mem.InternScanOpDef(&new))
}

// filterUnusedValuesColumns constructs a new Values operator based on the
// given existing Values operator. The new operator will have the same set of
// rows, but containing only the needed columns. Other columns are discarded.
func (f *Factory) filterUnusedValuesColumns(
	values memo.GroupID, neededCols opt.ColSet,
) memo.GroupID {
	valuesExpr := f.mem.NormExpr(values).AsValues()
	existingCols := f.extractColList(valuesExpr.Cols())
	newCols := make(opt.ColList, 0, neededCols.Len())

	existingRows := f.mem.LookupList(valuesExpr.Rows())
	newRows := make([]memo.GroupID, 0, len(existingRows))

	// Create new list of columns that only contains needed columns.
	for _, colID := range existingCols {
		if !neededCols.Contains(int(colID)) {
			continue
		}
		newCols = append(newCols, colID)
	}

	// newElems is used to store tuple values, and can be allocated once and
	// reused repeatedly, since InternList will copy values to memo storage.
	newElems := make([]memo.GroupID, len(newCols))

	for _, row := range existingRows {
		existingElems := f.mem.LookupList(f.mem.NormExpr(row).AsTuple().Elems())

		n := 0
		for i, elem := range existingElems {
			if !neededCols.Contains(int(existingCols[i])) {
				continue
			}

			newElems[n] = elem
			n++
		}

		newRows = append(newRows, f.ConstructTuple(f.InternList(newElems)))
	}

	return f.ConstructValues(f.InternList(newRows), f.InternColList(newCols))
}

// projectNoCycle constructs a Project operator and adds its fingerprint to the
// ruleCycles map. Rules which have the DetectCycle tag will see that the
// expression is already in the map, and will not match. Adding to the map in
// this way is purely a performance optimization, and is used in patterns where
// it's known that re-matching the operator is unnecessary.
func (f *Factory) projectNoCycle(input, projections memo.GroupID) memo.GroupID {
	projectExpr := memo.MakeProjectExpr(input, projections)
	f.ruleCycles[projectExpr.Fingerprint()] = true
	return f.ConstructProject(input, projections)
}

// limitNoCycle is similar to projectNoCycle, except that it constructs a Limit
// operator. See projectNoCycle for details.
func (f *Factory) limitNoCycle(input, limit memo.GroupID, ordering memo.PrivateID) memo.GroupID {
	limitExpr := memo.MakeLimitExpr(input, limit, ordering)
	f.ruleCycles[limitExpr.Fingerprint()] = true
	return f.ConstructLimit(input, limit, ordering)
}

// offsetNoCycle is similar to projectNoCycle, except that it constructs an
// Offset operator. See projectNoCycle for details.
func (f *Factory) offsetNoCycle(input, limit memo.GroupID, ordering memo.PrivateID) memo.GroupID {
	offsetExpr := memo.MakeOffsetExpr(input, limit, ordering)
	f.ruleCycles[offsetExpr.Fingerprint()] = true
	return f.ConstructOffset(input, limit, ordering)
}

// ----------------------------------------------------------------------
//
// Select Rules
//   Custom match and replace functions used with select.opt rules.
//
// ----------------------------------------------------------------------

// emptyGroupingCols returns true if the given grouping columns for a GroupBy
// operator are empty.
func (f *Factory) emptyGroupingCols(cols memo.PrivateID) bool {
	return f.mem.LookupPrivate(cols).(opt.ColSet).Empty()
}

// isCorrelated returns true if variables in the source expression reference
// columns in the destination expression. For example:
//   (InnerJoin
//     (Scan a)
//     (Scan b)
//     (Eq (Variable a.x) (Const 1))
//   )
//
// The (Eq) expression is correlated with the (Scan a) expression because it
// references one of its columns. But the (Eq) expression is not correlated
// with the (Scan b) expression.
func (f *Factory) isCorrelated(src, dst memo.GroupID) bool {
	return f.outerCols(src).Intersects(f.outputCols(dst))
}

// isCorrelatedCols is similar to isCorrelated, except that it checks whether
// variables in the given expression reference any of the given columns. This:
//
//   (IsCorrelated $src $dst)
//
// is equivalent to this:
//
//   (IsCorrelatedCols $src (OutputCols $dts))
//
func (f *Factory) isCorrelatedCols(group memo.GroupID, cols opt.ColSet) bool {
	return f.outerCols(group).Intersects(cols)
}

// extractCorrelatedConditions returns a new list containing only those
// expressions from the given list that are correlated with the given set of
// columns. For example:
//   (InnerJoin
//     (Scan a)
//     (Scan b)
//     (Filters [
//       (Eq (Variable a.x) (Variable b.x))
//       (Gt (Variable a.x) (Const 1))
//     ])
//   )
//
// Calling extractCorrelatedConditions with the filter conditions list and the
// output columns of (Scan b) would extract the (Eq) expression, since it
// references columns from b.
func (f *Factory) extractCorrelatedConditions(list memo.ListID, cols opt.ColSet) memo.ListID {
	extracted := make([]memo.GroupID, 0, list.Length)
	for _, item := range f.mem.LookupList(list) {
		if f.isCorrelatedCols(item, cols) {
			extracted = append(extracted, item)
		}
	}
	return f.mem.InternList(extracted)
}

// extractUncorrelatedConditions is the inverse of extractCorrelatedConditions.
// Instead of extracting correlated expressions, it extracts list expressions
// that are *not* correlated with the destination.
func (f *Factory) extractUncorrelatedConditions(list memo.ListID, cols opt.ColSet) memo.ListID {
	extracted := make([]memo.GroupID, 0, list.Length)
	for _, item := range f.mem.LookupList(list) {
		if !f.isCorrelatedCols(item, cols) {
			extracted = append(extracted, item)
		}
	}
	return f.mem.InternList(extracted)
}

// concatFilters creates a new Filters operator that contains conditions from
// both the left and right boolean filter expressions. If the left or right
// expression is itself a Filters operator, then it is "flattened" by merging
// its conditions into the new Filters operator.
func (f *Factory) concatFilters(left, right memo.GroupID) memo.GroupID {
	leftExpr := f.mem.NormExpr(left)
	rightExpr := f.mem.NormExpr(right)

	// Handle cases where left/right filters are constant boolean values.
	if leftExpr.Operator() == opt.TrueOp || rightExpr.Operator() == opt.FalseOp {
		return right
	}
	if rightExpr.Operator() == opt.TrueOp || leftExpr.Operator() == opt.FalseOp {
		return left
	}

	// Determine how large to make the conditions slice (at least 2 slots).
	cnt := 2
	leftFiltersExpr := leftExpr.AsFilters()
	if leftFiltersExpr != nil {
		cnt += int(leftFiltersExpr.Conditions().Length) - 1
	}
	rightFiltersExpr := rightExpr.AsFilters()
	if rightFiltersExpr != nil {
		cnt += int(rightFiltersExpr.Conditions().Length) - 1
	}

	// Create the conditions slice and populate it.
	conditions := make([]memo.GroupID, 0, cnt)
	if leftFiltersExpr != nil {
		conditions = append(conditions, f.mem.LookupList(leftFiltersExpr.Conditions())...)
	} else {
		conditions = append(conditions, left)
	}
	if rightFiltersExpr != nil {
		conditions = append(conditions, f.mem.LookupList(rightFiltersExpr.Conditions())...)
	} else {
		conditions = append(conditions, right)
	}
	return f.ConstructFilters(f.InternList(conditions))
}

// ----------------------------------------------------------------------
//
// GroupBy Rules
//   Custom match and replace functions used with groupby.opt rules.
//
// ----------------------------------------------------------------------

// colsAreKey returns true if the given columns form a strong key for the output
// rows of the given group. A strong key means that the set of given column
// values are unique and not null.
func (f *Factory) colsAreKey(cols memo.PrivateID, group memo.GroupID) bool {
	colSet := f.mem.LookupPrivate(cols).(opt.ColSet)
	props := f.lookupLogical(group).Relational
	for _, weakKey := range props.WeakKeys {
		if weakKey.SubsetOf(colSet) && weakKey.SubsetOf(props.NotNullCols) {
			return true
		}
	}
	return false
}

// ----------------------------------------------------------------------
//
// Boolean Rules
//   Custom match and replace functions used with bool.opt rules.
//
// ----------------------------------------------------------------------

// simplifyAnd removes True operands from an And operator, and eliminates the
// And operator altogether if any operand is False. It also "flattens" any And
// operator child by merging its conditions into the top-level list. Only one
// level of flattening is necessary, since this pattern would have already
// matched any And operator children. If, after simplification, no operands
// remain, then simplifyAnd returns True.
func (f *Factory) simplifyAnd(conditions memo.ListID) memo.GroupID {
	list := make([]memo.GroupID, 0, conditions.Length+1)
	for _, item := range f.mem.LookupList(conditions) {
		itemExpr := f.mem.NormExpr(item)

		switch itemExpr.Operator() {
		case opt.AndOp:
			// Flatten nested And operands.
			list = append(list, f.mem.LookupList(itemExpr.AsAnd().Conditions())...)

		case opt.TrueOp:
			// And operator skips True operands.

		case opt.FalseOp:
			// Entire And evaluates to False if any operand is False.
			return item

		default:
			list = append(list, item)
		}
	}

	if len(list) == 0 {
		return f.ConstructTrue()
	}
	return f.ConstructAnd(f.mem.InternList(list))
}

// simplifyOr removes False operands from an Or operator, and eliminates the Or
// operator altogether if any operand is True. It also "flattens" any Or
// operator child by merging its conditions into the top-level list. Only one
// level of flattening is necessary, since this pattern would have already
// matched any Or operator children. If, after simplification, no operands
// remain, then simplifyOr returns False.
func (f *Factory) simplifyOr(conditions memo.ListID) memo.GroupID {
	list := make([]memo.GroupID, 0, conditions.Length+1)
	for _, item := range f.mem.LookupList(conditions) {
		itemExpr := f.mem.NormExpr(item)

		switch itemExpr.Operator() {
		case opt.OrOp:
			// Flatten nested Or operands.
			list = append(list, f.mem.LookupList(itemExpr.AsOr().Conditions())...)

		case opt.FalseOp:
			// Or operator skips False operands.

		case opt.TrueOp:
			// Entire Or evaluates to True if any operand is True.
			return item

		default:
			list = append(list, item)
		}
	}

	if len(list) == 0 {
		return f.ConstructFalse()
	}
	return f.ConstructOr(f.mem.InternList(list))
}

// simplifyFilters behaves the same way as simplifyAnd, with one addition: if
// the conditions include a Null value in any position, then the entire
// expression is False. This works because the Filters expression only appears
// as a Select or Join filter condition, both of which treat a Null filter
// conjunct exactly as if it were False.
func (f *Factory) simplifyFilters(conditions memo.ListID) memo.GroupID {
	list := make([]memo.GroupID, 0, conditions.Length+1)
	for _, item := range f.mem.LookupList(conditions) {
		itemExpr := f.mem.NormExpr(item)

		switch itemExpr.Operator() {
		case opt.AndOp:
			// Flatten nested And operands.
			list = append(list, f.mem.LookupList(itemExpr.AsAnd().Conditions())...)

		case opt.TrueOp:
			// Filters operator skips True operands.

		case opt.FalseOp:
			// Filters expression evaluates to False if any operand is False.
			return item

		case opt.NullOp:
			// Filters expression evaluates to False if any operand is False.
			return f.ConstructFalse()

		default:
			list = append(list, item)
		}
	}

	if len(list) == 0 {
		return f.ConstructTrue()
	}
	return f.ConstructFilters(f.mem.InternList(list))
}

func (f *Factory) negateConditions(conditions memo.ListID) memo.ListID {
	list := f.mem.LookupList(conditions)
	negCond := make([]memo.GroupID, len(list))
	for i := range list {
		negCond[i] = f.ConstructNot(list[i])
	}
	return f.mem.InternList(negCond)
}

// negateComparison negates a comparison op like:
//   a.x = 5
// to:
//   a.x <> 5
func (f *Factory) negateComparison(cmp opt.Operator, left, right memo.GroupID) memo.GroupID {
	switch cmp {
	case opt.EqOp:
		return f.ConstructNe(left, right)
	case opt.NeOp:
		return f.ConstructEq(left, right)
	case opt.GtOp:
		return f.ConstructLe(left, right)
	case opt.GeOp:
		return f.ConstructLt(left, right)
	case opt.LtOp:
		return f.ConstructGe(left, right)
	case opt.LeOp:
		return f.ConstructGt(left, right)
	case opt.InOp:
		return f.ConstructNotIn(left, right)
	case opt.NotInOp:
		return f.ConstructIn(left, right)
	case opt.LikeOp:
		return f.ConstructNotLike(left, right)
	case opt.NotLikeOp:
		return f.ConstructLike(left, right)
	case opt.ILikeOp:
		return f.ConstructNotILike(left, right)
	case opt.NotILikeOp:
		return f.ConstructILike(left, right)
	case opt.SimilarToOp:
		return f.ConstructNotSimilarTo(left, right)
	case opt.NotSimilarToOp:
		return f.ConstructSimilarTo(left, right)
	case opt.RegMatchOp:
		return f.ConstructNotRegMatch(left, right)
	case opt.NotRegMatchOp:
		return f.ConstructRegMatch(left, right)
	case opt.RegIMatchOp:
		return f.ConstructNotRegIMatch(left, right)
	case opt.NotRegIMatchOp:
		return f.ConstructRegIMatch(left, right)
	case opt.IsOp:
		return f.ConstructIsNot(left, right)
	case opt.IsNotOp:
		return f.ConstructIs(left, right)
	default:
		panic(fmt.Sprintf("unexpected operator: %v", cmp))
	}
}

// commuteInequality swaps the operands of an inequality comparison expression,
// changing the operator to compensate:
//   5 < x
// to:
//   x > 5
func (f *Factory) commuteInequality(op opt.Operator, left, right memo.GroupID) memo.GroupID {
	switch op {
	case opt.GeOp:
		return f.ConstructLe(right, left)
	case opt.GtOp:
		return f.ConstructLt(right, left)
	case opt.LeOp:
		return f.ConstructGe(right, left)
	case opt.LtOp:
		return f.ConstructGt(right, left)
	}
	panic(fmt.Sprintf("called commuteInequality with operator %s", op))
}

// ----------------------------------------------------------------------
//
// Comparison Rules
//   Custom match and replace functions used with comp.opt rules.
//
// ----------------------------------------------------------------------

// normalizeTupleEquality remaps the elements of two tuples compared for
// equality, like this:
//   (a, b, c) = (x, y, z)
// into this:
//   (a = x) AND (b = y) AND (c = z)
func (f *Factory) normalizeTupleEquality(left, right memo.ListID) memo.GroupID {
	if left.Length != right.Length {
		panic("tuple length mismatch")
	}

	leftList := f.mem.LookupList(left)
	rightList := f.mem.LookupList(right)
	conditions := make([]memo.GroupID, left.Length)
	for i := range conditions {
		conditions[i] = f.ConstructEq(leftList[i], rightList[i])
	}
	return f.ConstructAnd(f.InternList(conditions))
}

// ----------------------------------------------------------------------
//
// Scalar Rules
//   Custom match and replace functions used with scalar.opt rules.
//
// ----------------------------------------------------------------------

// simplifyCoalesce discards any leading null operands, and then if the next
// operand is a constant, replaces with that constant.
func (f *Factory) simplifyCoalesce(args memo.ListID) memo.GroupID {
	argList := f.mem.LookupList(args)
	for i := 0; i < int(args.Length-1); i++ {
		// If item is not a constant value, then its value may turn out to be
		// null, so no more folding. Return operands from then on.
		item := f.mem.NormExpr(argList[i])
		if !item.IsConstValue() {
			return f.ConstructCoalesce(f.InternList(argList[i:]))
		}

		if item.Operator() != opt.NullOp {
			return argList[i]
		}
	}

	// All operands up to the last were null (or the last is the only operand),
	// so return the last operand without the wrapping COALESCE function.
	return argList[args.Length-1]
}

// allowNullArgs returns true if the binary operator with the given inputs
// allows one of those inputs to be null. If not, then the binary operator will
// simply be replaced by null.
func (f *Factory) allowNullArgs(op opt.Operator, left, right memo.GroupID) bool {
	leftType := f.lookupScalar(left).Type
	rightType := f.lookupScalar(right).Type
	return memo.BinaryAllowsNullArgs(op, leftType, rightType)
}

// foldNullUnary replaces the unary operator with a typed null value having the
// same type as the unary operator would have.
func (f *Factory) foldNullUnary(op opt.Operator, input memo.GroupID) memo.GroupID {
	typ := f.lookupScalar(input).Type
	return f.ConstructNull(f.InternType(memo.InferUnaryType(op, typ)))
}

// foldNullBinary replaces the binary operator with a typed null value having
// the same type as the binary operator would have.
func (f *Factory) foldNullBinary(op opt.Operator, left, right memo.GroupID) memo.GroupID {
	leftType := f.lookupScalar(left).Type
	rightType := f.lookupScalar(right).Type
	return f.ConstructNull(f.InternType(memo.InferBinaryType(op, leftType, rightType)))
}

// ----------------------------------------------------------------------
//
// Numeric Rules
//   Custom match and replace functions used with numeric.opt rules.
//
// ----------------------------------------------------------------------

// isZero returns true if the input expression is a numeric constant with a
// value of zero.
func (f *Factory) isZero(input memo.GroupID) bool {
	d := f.mem.LookupPrivate(f.mem.NormExpr(input).AsConst().Value()).(tree.Datum)
	switch t := d.(type) {
	case *tree.DDecimal:
		return t.Decimal.Sign() == 0
	case *tree.DFloat:
		return *t == 0
	case *tree.DInt:
		return *t == 0
	}
	return false
}

// isOne returns true if the input expression is a numeric constant with a
// value of one.
func (f *Factory) isOne(input memo.GroupID) bool {
	d := f.mem.LookupPrivate(f.mem.NormExpr(input).AsConst().Value()).(tree.Datum)
	switch t := d.(type) {
	case *tree.DDecimal:
		return t.Decimal.Cmp(&tree.DecimalOne.Decimal) == 0
	case *tree.DFloat:
		return *t == 1.0
	case *tree.DInt:
		return *t == 1
	}
	return false
}
