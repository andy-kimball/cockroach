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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xfunc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// HasHoistableSubquery returns true if the given scalar group contains a
// subquery within its subtree that has at least one outer column, and if that
// subquery needs to be hoisted up into its parent query as part of query
// decorrelation.
func (c *CustomFuncs) HasHoistableSubquery(group memo.GroupID) bool {
	// Lazily calculate and store the HasHoistableSubquery value.
	scalar := c.LookupScalar(group)
	if scalar.IsAvailable(props.HasHoistableSubquery) {
		return scalar.Rule.HasHoistableSubquery
	}
	scalar.SetAvailable(props.HasHoistableSubquery)

	ev := memo.MakeNormExprView(c.f.mem, group)
	switch ev.Operator() {
	case opt.SubqueryOp, opt.ExistsOp, opt.AnyOp:
		scalar.Rule.HasHoistableSubquery = !scalar.OuterCols.Empty()
		return scalar.Rule.HasHoistableSubquery
	}

	// If HasHoistableSubquery is true for any child, then it's true for this
	// expression as well. The exception is Case/If branches that have side
	// effects. These can only be executed if the branch test evaluates to true,
	// and so it's not possible to hoist out subqueries, since they would then be
	// evaluated when they shouldn't be.
	for i, end := 0, ev.ChildCount(); i < end; i++ {
		child := ev.Child(i)
		if c.HasHoistableSubquery(child.Group()) {
			scalar.Rule.HasHoistableSubquery = true

			// Look in CASE WHEN and ELSE branches:
			//   (Case
			//     $input:*
			//     (When $cond1:* $branch1:*)  # optional
			//     (When $cond2:* $branch2:*)  # optional
			//     $else:*                     # optional
			//   )
			switch ev.Operator() {
			case opt.CaseOp:
				// Determine whether this is the Else child.
				if i > 0 && child.Operator() != opt.WhenOp {
					scalar.Rule.HasHoistableSubquery = !child.Logical().CanHaveSideEffects()
				}

			case opt.WhenOp:
				if i == 1 {
					scalar.Rule.HasHoistableSubquery = !child.Logical().CanHaveSideEffects()
				}
			}

			if scalar.Rule.HasHoistableSubquery {
				return true
			}
		}
	}
	return false
}

// HoistSelectSubquery searches the Select operator's filter for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
//
//   SELECT * FROM xy WHERE (SELECT u FROM uv WHERE u=x LIMIT 1) IS NULL
//   =>
//   SELECT xy.*
//   FROM xy
//   LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//   ON True
//   WHERE u IS NULL
//
func (c *CustomFuncs) HoistSelectSubquery(input, filter memo.GroupID) memo.GroupID {
	var hoister subqueryHoister
	hoister.init(c, input)
	replaced := hoister.hoistAll(filter)
	sel := c.f.ConstructSelect(hoister.input(), replaced)
	return c.f.ConstructSimpleProject(sel, c.OutputCols(input))
}

// HoistProjectSubquery searches the Project operator's projections for
// correlated subqueries. Any found queries are hoisted into LeftJoinApply
// or InnerJoinApply operators, depending on subquery cardinality:
//
//   SELECT (SELECT max(u) FROM uv WHERE u=x) FROM xy
//   =>
//   SELECT u
//   FROM xy
//   INNER JOIN LATERAL (SELECT max(u) FROM uv WHERE u=x)
//   ON True
//
func (c *CustomFuncs) HoistProjectSubquery(input, projections memo.GroupID) memo.GroupID {
	var hoister subqueryHoister
	hoister.init(c, input)
	replaced := hoister.hoistAll(projections)
	return c.f.ConstructProject(hoister.input(), replaced)
}

// HoistJoinSubquery searches the Join operator's filter for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
//
//   SELECT y, z
//   FROM xy
//   FULL JOIN yz
//   ON (SELECT u FROM uv WHERE u=x LIMIT 1) IS NULL
//   =>
//   SELECT y, z
//   FROM xy
//   FULL JOIN LATERAL
//   (
//     SELECT *
//     FROM yz
//     LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//     ON True
//   )
//   ON u IS NULL
//
func (c *CustomFuncs) HoistJoinSubquery(
	op opt.Operator, left, right, on memo.GroupID,
) memo.GroupID {
	var hoister subqueryHoister
	hoister.init(c, right)
	replaced := hoister.hoistAll(on)
	join := c.ConstructApplyJoin(op, left, hoister.input(), replaced)
	return c.f.ConstructSimpleProject(join, c.OutputCols(left).Union(c.OutputCols(right)))
}

// HoistValuesSubquery searches the Values operator's projections for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
//
//   SELECT (VALUES (SELECT u FROM uv WHERE u=x LIMIT 1)) FROM xy
//   =>
//   SELECT
//   (
//     SELECT vals.*
//     FROM (VALUES ())
//     LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//     ON True
//     INNER JOIN LATERAL (VALUES (u)) vals
//     ON True
//   )
//   FROM xy
//
// The dummy VALUES clause with a singleton empty row is added to the tree in
// order to use the hoister, which requires an initial input query. While a
// right join would be slightly better here, this is such a fringe case that
// it's not worth the extra code complication.
func (c *CustomFuncs) HoistValuesSubquery(rows memo.ListID, cols memo.PrivateID) memo.GroupID {
	var hoister subqueryHoister
	hoister.init(c, c.constructNoColsRow())

	lb := xfunc.MakeListBuilder(&c.CustomFuncs)
	for _, item := range c.f.mem.LookupList(rows) {
		lb.AddItem(hoister.hoistAll(item))
	}

	values := c.f.ConstructValues(lb.BuildList(), cols)
	projCols := c.f.mem.GroupProperties(values).Relational.OutputCols
	join := c.f.ConstructInnerJoinApply(hoister.input(), values, c.f.ConstructTrue())
	return c.f.ConstructSimpleProject(join, projCols)
}

// HoistZipSubquery searches the Zip operator's functions for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
//
//   SELECT generate_series
//   FROM xy
//   INNER JOIN LATERAL ROWS FROM
//   (
//     generate_series(1, (SELECT v FROM uv WHERE u=x))
//   )
//   =>
//   SELECT generate_series
//   FROM xy
//   ROWS FROM
//   (
//     SELECT generate_series
//     FROM (VALUES ())
//     LEFT JOIN LATERAL (SELECT v FROM uv WHERE u=x)
//     ON True
//     INNER JOIN LATERAL ROWS FROM (generate_series(1, v))
//     ON True
//   )
//
// The dummy VALUES clause with a singleton empty row is added to the tree in
// order to use the hoister, which requires an initial input query. While a
// right join would be slightly better here, this is such a fringe case that
// it's not worth the extra code complication.
func (c *CustomFuncs) HoistZipSubquery(funcs memo.ListID, cols memo.PrivateID) memo.GroupID {
	var hoister subqueryHoister
	hoister.init(c, c.constructNoColsRow())

	lb := xfunc.MakeListBuilder(&c.CustomFuncs)
	for _, item := range c.f.mem.LookupList(funcs) {
		lb.AddItem(hoister.hoistAll(item))
	}

	zip := c.f.ConstructZip(lb.BuildList(), cols)
	projCols := c.f.mem.GroupProperties(zip).Relational.OutputCols
	join := c.f.ConstructInnerJoinApply(hoister.input(), zip, c.f.ConstructTrue())
	return c.f.ConstructSimpleProject(join, projCols)
}

// ConstructNonApplyJoin constructs the non-apply join operator that corresponds
// to the given join operator type.
func (c *CustomFuncs) ConstructNonApplyJoin(
	joinOp opt.Operator, left, right, on memo.GroupID,
) memo.GroupID {
	switch joinOp {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		return c.f.ConstructInnerJoin(left, right, on)
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return c.f.ConstructLeftJoin(left, right, on)
	case opt.RightJoinOp, opt.RightJoinApplyOp:
		return c.f.ConstructRightJoin(left, right, on)
	case opt.FullJoinOp, opt.FullJoinApplyOp:
		return c.f.ConstructFullJoin(left, right, on)
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return c.f.ConstructSemiJoin(left, right, on)
	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return c.f.ConstructAntiJoin(left, right, on)
	}
	panic(fmt.Sprintf("unexpected join operator: %v", joinOp))
}

// ConstructApplyJoin constructs the apply join operator that corresponds
// to the given join operator type.
func (c *CustomFuncs) ConstructApplyJoin(
	joinOp opt.Operator, left, right, on memo.GroupID,
) memo.GroupID {
	switch joinOp {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		return c.f.ConstructInnerJoinApply(left, right, on)
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return c.f.ConstructLeftJoinApply(left, right, on)
	case opt.RightJoinOp, opt.RightJoinApplyOp:
		return c.f.ConstructRightJoinApply(left, right, on)
	case opt.FullJoinOp, opt.FullJoinApplyOp:
		return c.f.ConstructFullJoinApply(left, right, on)
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return c.f.ConstructSemiJoinApply(left, right, on)
	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return c.f.ConstructAntiJoinApply(left, right, on)
	}
	panic(fmt.Sprintf("unexpected join operator: %v", joinOp))
}

// CanAggsIgnoreNulls returns true if all the aggregate functions in the given
// Aggregations operator are able to ignore null values. In other words, any
// number of null values can be added to the grouping set and all the aggregate
// functions will return the same result.
//
// Note that the CountRows function (used for COUNT(*)) does not ignore null
// values on its own (they're included in the count). But it can be mapped to a
// Count function over a non-null column, so it's treated as a null-ignoring
// aggregate function here.
//
// Similarly, ConstAgg does not ignore nulls, but it can be converted to
// ConstNotNullAgg.
func (c *CustomFuncs) CanAggsIgnoreNulls(aggs memo.GroupID) bool {
	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	for _, elem := range c.f.mem.LookupList(aggsExpr.Aggs()) {
		op := c.f.mem.NormExpr(elem).Operator()
		if op != opt.CountRowsOp && op != opt.ConstAggOp && !opt.AggregateIgnoresNulls(op) {
			return false
		}
	}
	return true
}

// EnsureKey finds the shortest strong key for the input memo group. If no
// strong key exists, then EnsureKey wraps the input in a RowNumber operator,
// which provides a key column by uniquely numbering the rows. EnsureKey returns
// the input group (perhaps wrapped by RowNumber).
func (c *CustomFuncs) EnsureKey(in memo.GroupID) memo.GroupID {
	_, ok := c.CandidateKey(in)
	if ok {
		return in
	}

	colID := c.f.Metadata().AddColumn("rownum", types.Int)
	def := &memo.RowNumberDef{ColID: colID}
	return c.f.ConstructRowNumber(in, c.f.InternRowNumberDef(def))
}

// KeyCols returns a column set consisting of the columns that make up the
// candidate key for the given group (a key must be present).
func (c *CustomFuncs) KeyCols(group memo.GroupID) opt.ColSet {
	keyCols, ok := c.CandidateKey(group)
	if !ok {
		panic("expected group to have key")
	}
	return keyCols
}

// NonKeyCols returns a column set consisting of the output columns of the given
// group, minus the columns that make up its candidate key (which it must have).
func (c *CustomFuncs) NonKeyCols(group memo.GroupID) opt.ColSet {
	keyCols, ok := c.CandidateKey(group)
	if !ok {
		panic("expected group to have key")
	}
	return c.OutputCols(group).Difference(keyCols)
}

func (c *CustomFuncs) MinNotNullCol(group memo.GroupID) opt.ColSet {
	var colSet opt.ColSet
	col, ok := c.LookupLogical(group).Relational.NotNullCols.Next(0)
	if !ok {
		panic("expected group to have at least one not null column")
	}
	colSet.Add(col)
	return colSet
}

// MakeAggCols constructs a new Aggregations operator containing an aggregate
// function of the given operator type for each of column in the given set. For
// example, for ConstAggOp and columns (1,2), this expression is returned:
//
//   (Aggregations
//     [(ConstAgg (Variable 1)) (ConstAgg (Variable 2))]
//     [1,2]
//   )
//
func (c *CustomFuncs) MakeAggCols(aggOp opt.Operator, cols opt.ColSet) memo.GroupID {
	colsLen := cols.Len()
	outElems := make([]memo.GroupID, colsLen)
	outColList := make(opt.ColList, colsLen)
	c.makeAggCols(aggOp, cols, outElems, outColList)
	return c.f.ConstructAggregations(c.f.InternList(outElems), c.f.InternColList(outColList))
}

// MakeAggCols2 is similar to MakeAggCols, except that it allows two different
// sets of aggregate functions to be added to the resulting Aggregations
// operator, with one set appended to the other, like this:
//
//   (Aggregations
//     [(ConstAgg (Variable 1)) (ConstAgg (Variable 2)) (FirstAgg (Variable 3))]
//     [1,2,3]
//   )
//
func (c *CustomFuncs) MakeAggCols2(
	aggOp opt.Operator, cols opt.ColSet, aggOp2 opt.Operator, cols2 opt.ColSet,
) memo.GroupID {
	colsLen := cols.Len()
	outElems := make([]memo.GroupID, colsLen+cols2.Len())
	outColList := make(opt.ColList, len(outElems))

	c.makeAggCols(aggOp, cols, outElems, outColList)
	c.makeAggCols(aggOp2, cols2, outElems[colsLen:], outColList[colsLen:])

	return c.f.ConstructAggregations(c.f.InternList(outElems), c.f.InternColList(outColList))
}

// EnsureNotNullIfNeeded searches for a not-null output column in the given
// group. If such a column does not exist, it checks whether an aggregation
// which cannot ignore nulls exists.  If so, it synthesizes a new True constant
// column that is not-null. This becomes a kind of "canary" column that other
// expressions can inspect, since any null value in this column indicates that
// the row was added by an outer join as part of null extending.
//
// EnsureNotNullIfNeeded returns the input group, possibly wrapped in a new
// Project if a new column was synthesized.
//
// See the TryDecorrelateScalarGroupBy rule comment for more details.
func (c *CustomFuncs) EnsureNotNullIfNeeded(in, aggs memo.GroupID) memo.GroupID {
	_, ok := c.LookupLogical(in).Relational.NotNullCols.Next(0)
	if ok {
		return in
	}

	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())

	for _, elem := range aggsElems {
		if !opt.AggregateIgnoresNulls(c.f.mem.NormExpr(elem).Operator()) {
			notNullColID := c.f.Metadata().AddColumn("notnull", types.Bool)
			result := c.f.projectExtraCol(in, c.f.ConstructTrue(), notNullColID)
			return result
		}
	}
	return in
}

func (c *CustomFuncs) referencedCols(scalarExpr memo.GroupID) opt.ColSet {
	e := c.f.mem.NormExpr(scalarExpr)
	switch e.Operator() {
	case opt.VariableOp:
		col := c.f.mem.LookupPrivate(e.AsVariable().Col()).(opt.ColumnID)
		return util.MakeFastIntSet(int(col))
	default:
		var result opt.ColSet
		for i, n := 0, e.ChildCount(); i < n; i++ {
			result.UnionWith(c.referencedCols(e.ChildGroup(c.f.mem, i)))
		}
		return result
	}
}

// AggsCanBeDecorrelated returns true if every aggregate satisfies one of the
// following conditions:
//
//   * It is CountRows (because it will be translated into Count),
//   * It ignores nulls (because nothing extra must be done for it)
//   * It gives NULL on no input (because this is how we translate non-null
//     ignoring aggregates)
//
// TODO(justin): we can lift the third condition if we have a function that
// gives the correct "on empty" value for a given aggregate.
func (c *CustomFuncs) AggsCanBeDecorrelated(aggs memo.GroupID) bool {
	aggList := c.f.mem.LookupList(c.f.mem.NormExpr(aggs).AsAggregations().Aggs())

	for _, a := range aggList {
		agg := c.f.mem.NormExpr(a)
		op := agg.Operator()
		if op != opt.CountRowsOp && !opt.AggregateIsNullOnEmpty(op) && !opt.AggregateIgnoresNulls(op) {
			return false
		}
	}

	return true
}

// TranslateAggs takes an aggregation list which may or may not require
// translation due to non-null ignoring aggregations.
// If this isn't an issue for any aggregates, the input is returned unchanged.
// If there is an aggregate which requires translation, two things happen:
// * any null-accepting aggregates are given new column IDs so that the
//   original column IDs can be projected out properly, and
// * a const_not_null_agg aggregation is created over a non-nullable column in the
//   input in order to distinguish whether the aggregate should be considered to
//   have consumed a single NULL or nothing.
func (c *CustomFuncs) TranslateAggs(left, in, oldAggs memo.GroupID) memo.GroupID {
	toTranslate := c.aggsRequiringTranslation(oldAggs, left)
	if toTranslate.Empty() {
		return oldAggs
	}

	a := c.f.mem.NormExpr(oldAggs).AsAggregations()

	// If we have any null-accepting aggregates, then we must synthesize a
	// const_not_null_agg aggregation over a non-null column (which is assumed to
	// exist via EnsureNotNullIfNeeded having been called).
	id, ok := c.LookupLogical(in).Relational.NotNullCols.Next(0)
	if !ok {
		panic("expected a non-null col to exist")
	}
	canaryID := opt.ColumnID(id)

	aggs := c.f.mem.LookupList(a.Aggs())
	// The new aggregations should be the same as the old ones, just with one
	// extra space for the canary aggregation.
	newAggs := make([]memo.GroupID, len(aggs)+1)
	newAggCols := make(opt.ColList, len(aggs)+1)

	copy(newAggs, aggs)

	aggCols := c.f.mem.LookupPrivate(a.Cols()).(opt.ColList)
	for i := range aggCols {
		if toTranslate.Contains(int(aggCols[i])) {
			// These aggregations don't produce the right value, so we synthesize
			// a new column ID for them. This will later be projected properly by
			// TranslateNonIgnoreAggs.
			newAggCols[i] = c.f.Metadata().AddColumn("aggregate", c.f.Metadata().ColumnType(aggCols[i]))
		} else {
			newAggCols[i] = aggCols[i]
		}
	}

	aggCanaryIdx := len(newAggCols) - 1

	aggCanaryID := c.f.Metadata().AddColumn("aggregated_canary", types.Bool)
	newAggCols[aggCanaryIdx] = aggCanaryID

	canaryVar := c.f.ConstructVariable(c.f.InternColumnID(canaryID))
	newAggs[aggCanaryIdx] = c.f.ConstructConstNotNullAgg(canaryVar)

	return c.f.ConstructAggregations(
		c.f.InternList(newAggs),
		c.f.InternColList(newAggCols),
	)
}

// findCanaryAggregation takes a set of aggregations and the associated input
// and searches for a "canary aggregation".  That is, one which is a
// const_not_null_agg aggregation over a non-nullable column.  If found, it returns
// the id of the aggregation column and true, otherwise, it returns false.
// TODO(justin): this seems slightly ad-hoc and sketchy.
func (c *CustomFuncs) findCanaryAggregation(aggs, input memo.GroupID) (opt.ColumnID, bool) {
	a := c.f.mem.NormExpr(aggs).AsAggregations()
	aggCols := c.f.mem.LookupPrivate(a.Cols()).(opt.ColList)
	aggExprs := c.f.mem.LookupList(a.Aggs())
	for i := range aggExprs {
		id := aggExprs[i]
		agg := c.f.mem.NormExpr(id)
		if agg.Operator() == opt.ConstNotNullAggOp {
			in := c.f.mem.NormExpr(agg.AsConstNotNullAgg().Input())
			if in.Operator() == opt.VariableOp {
				v := in.AsVariable()
				col := c.f.mem.LookupPrivate(v.Col()).(opt.ColumnID)
				if c.f.mem.GroupProperties(input).Relational.NotNullCols.Contains(int(col)) {
					return aggCols[i], true
				}
			}
		}
	}

	return 0, false
}

// constructCanaryChecker returns a CASE expression which disambiguates an
// aggregation over a left join having received a NULL column because there
// were no matches on the right side of the join, and having received a NULL
// column because a NULL column was matched against.
func (c *CustomFuncs) constructCanaryChecker(
	aggCanaryVar memo.GroupID,
	inputCol opt.ColumnID,
) memo.GroupID {
	nullVal := c.f.ConstructNull(c.f.InternType(types.Unknown))
	return c.f.ConstructCase(
		c.f.ConstructTrue(),
		c.f.InternList([]memo.GroupID{
			c.f.ConstructWhen(
				c.f.ConstructIsNot(aggCanaryVar, nullVal),
				c.f.ConstructVariable(c.f.InternColumnID(inputCol)),
			),
			nullVal,
		}),
	)
}

// aggsRequiringTranslation returns the set of columns in aggregations which
// require a discriminating canary aggregation in TryDecorrelateScalarGroupBy.
// This is true for aggregations which read from the right input and cannot
// ignore NULL values.
func (c *CustomFuncs) aggsRequiringTranslation(aggregations, leftInput memo.GroupID) opt.ColSet {
	aggOp := c.f.mem.NormExpr(aggregations).AsAggregations()
	aggs := c.f.mem.LookupList(aggOp.Aggs())
	cols := c.f.mem.LookupPrivate(aggOp.Cols()).(opt.ColList)

	leftOutputCols := c.OutputCols(leftInput)

	var result opt.ColSet

	for i, a := range aggs {
		agg := c.f.mem.NormExpr(a)
		aggInput := agg.ChildGroup(c.f.mem, 0)

		// We don't need to do anything if the aggregate only references columns
		// originating from the left, since there are no ambiguous NULLs in such a
		// case.
		referencedCols := c.referencedCols(aggInput)

		if !opt.AggregateIgnoresNulls(agg.Operator()) &&
			!referencedCols.SubsetOf(leftOutputCols) {
			result.Add(int(cols[i]))
		}
	}

	return result
}

// are unable to ignore nulls. If that is the case, it inserts projections
// which check a "canary" aggregation that determines if a group actually had
// any things grouped into it or not.
func (c *CustomFuncs) TranslateNonIgnoreAggs2(
	in memo.GroupID,
	originalAggs memo.GroupID,
	newAggs memo.GroupID,
	leftWithKey memo.GroupID,
	newRight memo.GroupID,
) memo.GroupID {
	toTranslate := c.aggsRequiringTranslation(originalAggs, leftWithKey)
	if toTranslate.Empty() {
		return in
	}

	canaryAggregationID, ok := c.findCanaryAggregation(newAggs, newRight)
	if !ok {
		panic("didn't find aggregated canary")
	}

	pb := projectionsBuilder{f: c.f}

	key, ok := c.CandidateKey(leftWithKey)
	if !ok {
		panic("expected leftWithKey to have a key")
	}

	pb.addPassthroughCols(key)

	aggCanaryVar := c.f.ConstructVariable(c.f.InternColumnID(canaryAggregationID))

	origAggs := c.f.mem.NormExpr(originalAggs).AsAggregations()
	origAggCols := c.f.mem.LookupPrivate(origAggs.Cols()).(opt.ColList)

	// If a column appears in the original aggregation columns, we can safely
	// pass it through.
	var originalAggregations opt.ColSet
	for _, c := range origAggCols {
		originalAggregations.Add(int(c))
	}

	// Figure out how to project the columns in aggCols.  If a column also
	// appears in originalAggregations (that is, it was not translated by
	// TranslateAggs), it is safe to just pass it through. If not, that means
	// that TranslateAggs *did* change it, and we must check the canary to
	// determine what the correct value to project is.
	aggregations := c.f.mem.NormExpr(newAggs).AsAggregations()
	aggCols := c.f.mem.LookupPrivate(aggregations.Cols()).(opt.ColList)
	// TODO(justin): I don't really like this part - it feels pretty ad-hoc and
	// implicit, but I'm not sure how to clean it up.
	for i := range origAggCols {
		// The columns in aggCols align elementwise with those in origAggCols
		// (though there is one extra in aggCols).
		if toTranslate.Contains(int(origAggCols[i])) {
			pb.addSynthesized(
				c.constructCanaryChecker(aggCanaryVar, aggCols[i]),
				origAggCols[i],
			)
		} else {
			pb.addPassthroughCol(aggCols[i])
		}
	}

	return c.f.ConstructProject(in, pb.buildProjections())
}

// TranslateNonIgnoreAggs checks if any of the aggregates being decorrelated
// are unable to ignore nulls. If that is the case, it inserts projections
// which check a "canary" aggregation that determines if a group actually had
// any things grouped into it or not.
func (c *CustomFuncs) TranslateNonIgnoreAggs(
	newIn, newAggs, oldIn, oldAggs memo.GroupID, outCols opt.ColSet,
) memo.GroupID {
	aggsExpr := c.f.mem.NormExpr(newAggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())
	aggCols := c.ExtractColList(aggsExpr.Cols())
	oldAggCols := c.ExtractColList(c.f.mem.NormExpr(oldAggs).AsAggregations().Cols())

	var aggCanaryVar memo.GroupID
	pb := projectionsBuilder{f: c.f}

	var synthCols opt.ColSet
	for i, elem := range aggsElems {
		expr := c.f.mem.NormExpr(elem)
		if !opt.AggregateIgnoresNulls(expr.Operator()) {
			if aggCanaryVar == 0 {
				canaryAggID, ok := c.LookupLogical(oldIn).Relational.NotNullCols.Next(0)
				if !ok {
					panic("expected input expression to have not-null column")
				}
				aggCanaryVar = c.f.ConstructVariable(c.f.InternColumnID(opt.ColumnID(canaryAggID)))
			}

			pb.addSynthesized(
				c.constructCanaryChecker(aggCanaryVar, aggCols[i]),
				oldAggCols[i],
			)
			synthCols.Add(int(oldAggCols[i]))
		}
	}

	if synthCols.Empty() {
		return newIn
	}
	pb.addPassthroughCols(outCols.Difference(synthCols))
	return c.f.ConstructProject(newIn, pb.buildProjections())
}

// EnsureAggsCanIgnoreNulls scans the aggregate list to aggregation functions that
// don't ignore nulls but can be remapped so that they do:
//   - CountRows functions are are converted to Count functions that operate
//     over a not-null column from the given input group. The
//     EnsureNotNullIfNeeded method should already have been called in order
//     to guarantee such a column exists.
//   - ConstAgg is remapped to ConstNotNullAgg.
//   - Other aggregates that can use a canary column to detect nulls.
//
// CanAggsIgnoreNulls should already have been called in order to guarantee that
// remapping is possible.
//
// See the TryDecorrelateScalarGroupBy rule comment for more details.
func (c *CustomFuncs) EnsureAggsCanIgnoreNulls(in, aggs memo.GroupID) memo.GroupID {
	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())
	aggsCols := c.ExtractColList(aggsExpr.Cols())

	var outElems []memo.GroupID
	var outCols opt.ColList
	for i, elem := range aggsElems {
		newElem := elem
		newCol := aggsCols[i]
		expr := c.f.mem.NormExpr(elem)
		switch expr.Operator() {
		case opt.ConstAggOp:
			// Translate ConstAgg(...) to ConstNotNullAgg(...).
			newElem = c.f.ConstructConstNotNullAgg(expr.ChildGroup(c.f.mem, 0))

		case opt.CountRowsOp:
			// Translate CountRows() to Count(notNullCol).
			id, ok := c.LookupLogical(in).Relational.NotNullCols.Next(0)
			if !ok {
				panic("expected input expression to have not-null column")
			}
			notNullColID := c.f.InternColumnID(opt.ColumnID(id))
			newElem = c.f.ConstructCount(c.f.ConstructVariable(notNullColID))

		default:
			if !opt.AggregateIgnoresNulls(expr.Operator()) {
				// Allocate id for new intermediate agg column. The column will get
				// mapped back to the original id after the grouping (by the
				// TranslateNonIgnoreAggs method).
				md := c.f.Metadata()
				label := md.ColumnLabel(newCol)
				newCol = md.AddColumn(label, md.ColumnType(newCol))
			}
		}
		if outElems == nil {
			if newElem != elem || newCol != aggsCols[i] {
				outElems = make([]memo.GroupID, len(aggsElems))
				copy(outElems, aggsElems[:i])
				outCols = make(opt.ColList, len(aggsElems))
				copy(outCols, aggsCols[:i])
			}
		}
		if outElems != nil {
			outElems[i] = newElem
			outCols[i] = newCol
		}
	}
	if outElems == nil {
		// No changes.
		return aggs
	}
	return c.f.ConstructAggregations(c.f.InternList(outElems), c.f.InternColList(outCols))
}

func (c *CustomFuncs) EnsureCanaryAgg(in, aggs memo.GroupID) opt.ColSet {
	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())
	aggsCols := c.ExtractColList(aggsExpr.Cols())

	var canaryCols opt.ColSet
	id, ok := c.LookupLogical(in).Relational.NotNullCols.Next(0)
	if !ok {
		// No not null columns, so none of the aggs must have needed them (see
		// EnsureNotNullIfNeeded for more details).
		return canaryCols
	}

	needCanary := false
	for i, elem := range aggsElems {
		expr := c.f.mem.NormExpr(elem)
		if aggsCols[i] == opt.ColumnID(id) {
			// An aggregate already exists that can be used as a canary column.
			// This is most likely a ConstNotNull column that wraps a reference
			// to the not null column from the input.
			needCanary = false
			break
		}

		if !opt.AggregateIgnoresNulls(expr.Operator()) {
			needCanary = true
		}
	}

	if needCanary {
		canaryCols.Add(id)
	}
	return canaryCols
}

// MakeGroupByDef constructs a new unordered GroupByDef using the given grouping
// columns.
func (c *CustomFuncs) MakeGroupByDef(groupingCols opt.ColSet) memo.PrivateID {
	return c.f.InternGroupByDef(&memo.GroupByDef{
		GroupingCols: groupingCols,
	})
}

// MakeOrderedGroupByDef constructs a new GroupByDef using the given grouping
// columns and OrderingChoice private.
func (c *CustomFuncs) MakeOrderedGroupByDef(
	groupingCols opt.ColSet, ordering *props.OrderingChoice,
) memo.PrivateID {
	return c.f.InternGroupByDef(&memo.GroupByDef{
		GroupingCols: groupingCols,
		Ordering:     *ordering,
	})
}

func (c *CustomFuncs) ExtractGroupByOrdering(def memo.PrivateID) *props.OrderingChoice {
	return &c.f.mem.LookupPrivate(def).(*memo.GroupByDef).Ordering
}

// AddColsToGroupByDef returns a new GroupByDef that is a copy of the given
// GroupByDef, except with the given set of grouping columns union'ed with the
// existing grouping columns.
func (c *CustomFuncs) AddColsToGroupByDef(
	groupByDef memo.PrivateID, groupingCols opt.ColSet,
) memo.PrivateID {
	def := c.f.mem.LookupPrivate(groupByDef).(*memo.GroupByDef)
	return c.f.InternGroupByDef(&memo.GroupByDef{
		GroupingCols: def.GroupingCols.Union(groupingCols),
		Ordering:     def.Ordering,
	})
}

// ConstructAnyCondition builds an expression that compares the given scalar
// expression with the first (and only) column of the input rowset, using the
// given comparison operator.
func (c *CustomFuncs) ConstructAnyCondition(
	input, scalar memo.GroupID, cmp memo.PrivateID,
) memo.GroupID {
	inputVar := c.referenceSingleColumn(input)
	return c.ConstructBinary(c.f.mem.LookupPrivate(cmp).(opt.Operator), scalar, inputVar)
}

// ConstructBinary builds a dynamic binary expression, given the binary
// operator's type and its two arguments.
func (c *CustomFuncs) ConstructBinary(op opt.Operator, left, right memo.GroupID) memo.GroupID {
	return c.f.DynamicConstruct(
		op,
		memo.DynamicOperands{
			memo.DynamicID(left),
			memo.DynamicID(right),
		},
	)
}

// constructNoColsRow returns a Values operator having a single row with zero
// columns.
func (c *CustomFuncs) constructNoColsRow() memo.GroupID {
	lb := xfunc.MakeListBuilder(&c.CustomFuncs)
	lb.AddItem(c.f.ConstructTuple(
		c.f.InternList(nil), c.f.InternType(memo.EmptyTupleType),
	))
	return c.f.ConstructValues(lb.BuildList(), c.f.InternColList(opt.ColList{}))
}

// referenceSingleColumn returns a Variable operator that refers to the one and
// only column that is projected by the given group.
func (c *CustomFuncs) referenceSingleColumn(group memo.GroupID) memo.GroupID {
	cols := c.f.mem.GroupProperties(group).Relational.OutputCols
	if cols.Len() != 1 {
		panic("expression does not have exactly one column")
	}
	colID, _ := cols.Next(0)
	return c.f.ConstructVariable(c.f.InternColumnID(opt.ColumnID(colID)))
}

// subqueryHoister searches scalar expression trees looking for correlated
// subqueries which will be pulled up and joined to a higher level relational
// query. See the  hoistAll comment for more details on how this is done.
type subqueryHoister struct {
	c       *CustomFuncs
	f       *Factory
	mem     *memo.Memo
	hoisted memo.GroupID
}

func (r *subqueryHoister) init(c *CustomFuncs, input memo.GroupID) {
	r.c = c
	r.f = c.f
	r.mem = c.f.mem
	r.hoisted = input
}

// input returns a single expression tree that contains the input expression
// provided to the init method, but wrapped with any subqueries hoisted out of
// the scalar expression tree. See the hoistAll comment for more details.
func (r *subqueryHoister) input() memo.GroupID {
	return r.hoisted
}

// hoistAll searches the given subtree for each correlated Subquery, Exists, or
// Any operator, and lifts its subquery operand out of the scalar context and
// joins it with a higher-level relational expression. The original subquery
// operand is replaced by a Variable operator that refers to the first (and
// only) column of the hoisted relational expression.
//
// hoistAll returns the root of a new expression tree that incorporates the new
// Variable operators. The hoisted subqueries can be accessed via the input
// method. Each removed subquery wraps the one before, with the input query at
// the base. Each subquery adds a single column to its input and uses a
// JoinApply operator to ensure that it has no effect on the cardinality of its
// input. For example:
//
//   SELECT *
//   FROM xy
//   WHERE
//     (SELECT u FROM uv WHERE u=x LIMIT 1) IS NOT NULL
//     OR EXISTS(SELECT * FROM jk WHERE j=x)
//   =>
//   SELECT xy.*
//   FROM xy
//   LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//   ON True
//   INNER JOIN LATERAL
//   (
//     SELECT (CONST_AGG(True) IS NOT NULL) AS exists FROM jk WHERE j=x
//   )
//   ON True
//   WHERE u IS NOT NULL OR exists
//
// The choice of whether to use LeftJoinApply or InnerJoinApply depends on the
// cardinality of the hoisted subquery. If zero rows can be returned from the
// subquery, then LeftJoinApply must be used in order to preserve the
// cardinality of the input expression. Otherwise, InnerJoinApply can be used
// instead. In either case, the wrapped subquery must never return more than one
// row, so as not to change the cardinality of the result.
//
// See the comments for constructGroupByExists and constructGroupByAny for more
// details on how EXISTS and ANY subqueries are hoisted, including usage of the
// CONST_AGG function.
func (r *subqueryHoister) hoistAll(root memo.GroupID) memo.GroupID {
	// Match correlated subqueries.
	ev := memo.MakeNormExprView(r.mem, root)
	switch ev.Operator() {
	case opt.SubqueryOp, opt.ExistsOp, opt.AnyOp:
		if ev.Logical().OuterCols().Empty() {
			break
		}

		subquery := ev.ChildGroup(0)
		switch ev.Operator() {
		case opt.ExistsOp:
			subquery = r.constructGroupByExists(subquery)

		case opt.AnyOp:
			input := ev.ChildGroup(0)
			scalar := ev.ChildGroup(1)
			cmp := ev.Private().(opt.Operator)
			subquery = r.constructGroupByAny(scalar, cmp, input)
		}

		// Hoist the subquery into a single expression that can be accessed via
		// the subqueries method.
		subqueryProps := r.mem.GroupProperties(subquery).Relational
		if subqueryProps.Cardinality.CanBeZero() {
			// Zero cardinality allowed, so must use left outer join to preserve
			// outer row (padded with nulls) in case the subquery returns zero rows.
			r.hoisted = r.f.ConstructLeftJoinApply(r.hoisted, subquery, r.f.ConstructTrue())
		} else {
			// Zero cardinality not allowed, so inner join suffices. Inner joins
			// are preferable to left joins since null handling is much simpler
			// and they allow the optimizer more choices.
			r.hoisted = r.f.ConstructInnerJoinApply(r.hoisted, subquery, r.f.ConstructTrue())
		}

		// Replace the Subquery operator with a Variable operator referring to
		// the first (and only) column in the hoisted query.
		colID, _ := subqueryProps.OutputCols.Next(0)
		return r.f.ConstructVariable(r.mem.InternColumnID(opt.ColumnID(colID)))
	}

	return ev.Replace(r.f.evalCtx, func(child memo.GroupID) memo.GroupID {
		// Recursively hoist subqueries in each child that contains them.
		if r.c.HasHoistableSubquery(child) {
			return r.hoistAll(child)
		}

		// Return unchanged child.
		return child
	}).Group()
}

// constructGroupByExists transforms a scalar Exists expression like this:
//
//   EXISTS(SELECT * FROM a WHERE a.x=b.x)
//
// into a scalar GroupBy expression that returns a one row, one column relation:
//
//   SELECT (CONST_AGG(True) IS NOT NULL) AS exists
//   FROM (SELECT * FROM a WHERE a.x=b.x)
//
// The expression uses an internally-defined CONST_AGG aggregation function,
// since it's able to short-circuit on the first non-null it encounters. The
// above expression is equivalent to:
//
//   SELECT COUNT(True) > 0 FROM (SELECT * FROM a WHERE a.x=b.x)
//
// CONST_AGG (and COUNT) always return exactly one boolean value in the context
// of a scalar GroupBy expression. Because its operand is always True, the only
// way the final expression is False is when the input set is empty (since
// CONST_AGG returns NULL, which IS NOT NULL maps to False).
//
// However, later on, the TryDecorrelateScalarGroupBy rule will push a left join
// into the GroupBy, and null values produced by the join will flow into the
// CONST_AGG which will need to be changed to a CONST_NOT_NULL_AGG (which is
// defined to ignore those nulls so that its result will be unaffected).
func (r *subqueryHoister) constructGroupByExists(subquery memo.GroupID) memo.GroupID {
	trueColID := r.f.Metadata().AddColumn("true", types.Bool)
	pb := projectionsBuilder{f: r.f}
	pb.addSynthesized(r.f.ConstructTrue(), trueColID)
	trueProjection := pb.buildProjections()

	aggColID := r.f.Metadata().AddColumn("true_agg", types.Bool)
	aggCols := r.f.InternColList(opt.ColList{aggColID})
	aggVar := r.f.ConstructVariable(r.f.InternColumnID(aggColID))

	existsColID := r.f.Metadata().AddColumn("exists", types.Bool)
	nullVal := r.f.ConstructNull(r.f.InternType(types.Unknown))
	pb.addSynthesized(r.f.ConstructIsNot(aggVar, nullVal), existsColID)
	existsProjection := pb.buildProjections()

	return r.f.ConstructProject(
		r.f.ConstructScalarGroupBy(
			r.f.ConstructProject(
				subquery,
				trueProjection,
			),
			r.f.ConstructAggregations(
				r.f.funcs.InternSingletonList(
					r.f.ConstructConstAgg(
						r.f.ConstructVariable(r.f.InternColumnID(trueColID)),
					),
				),
				aggCols,
			),
			r.f.InternGroupByDef(&memo.GroupByDef{}),
		),
		existsProjection,
	)
}

// constructGroupByAny transforms a scalar Any expression like this:
//
//   z = ANY(SELECT x FROM xy)
//
// into a scalar GroupBy expression that returns a one row, one column relation
// that is equivalent to this:
//
//   SELECT
//     CASE
//       WHEN bool_or(notnull) AND z IS NOT Null THEN True
//       ELSE bool_or(notnull) IS NULL THEN False
//       ELSE Null
//     END
//   FROM
//   (
//     SELECT x IS NOT Null AS notnull
//     FROM xy
//     WHERE (z=x) IS NOT False
//   )
//
// BOOL_OR returns true if any input is true, else false if any input is false,
// else null. This is a mismatch with ANY, which returns true if any input is
// true, else null if any input is null, else false. In addition, the expression
// needs to be easy to decorrelate, which means that the outer column reference
// ("z" in the example) should not be part of a projection (since projections
// are difficult to hoist above left joins). The following procedure solves the
// mismatch between BOOL_OR and ANY, as well as avoids correlated projections:
//
//   1. Filter out false comparison rows with an initial filter. The result of
//      ANY does not change, no matter how many false rows are added or removed.
//      This step has the effect of mapping a set containing only false
//      comparison rows to the empty set (which is desirable).
//
//   2. Step #1 leaves only true and null comparison rows. A null comparison row
//      occurs when either the left or right comparison operand is null (Any
//      only allows comparison operators that propagate nulls). Map each null
//      row to a false row, but only in the case where the right operand is null
//      (i.e. the operand that came from the subquery). The case where the left
//      operand is null will be handled later.
//
//   3. Use the BOOL_OR aggregation function on the true/false values from step
//      #2. If there is at least one true value, then BOOL_OR returns true. If
//      there are no values (the empty set case), then BOOL_OR returns null.
//      Because of the previous steps, this indicates that the original set
//      contained only false values (or no values at all).
//
//   4. A True result from BOOL_OR is ambiguous. It could mean that the
//      comparison returned true for one of the rows in the group. Or, it could
//      mean that the left operand was null. The CASE statement ensures that
//      True is only returned if the left operand was not null.
//
//   5. In addition, the CASE statement maps a null return value to false, and
//      false to null. This matches ANY behavior.
//
// The following is a table showing the various interesting cases:
//
//         | subquery  | before        | after   | after
//     z   | x values  | BOOL_OR       | BOOL_OR | CASE
//   ------+-----------+---------------+---------+-------
//     1   | (1)       | (true)        | true    | true
//     1   | (1, null) | (true, false) | true    | true
//     1   | (1, 2)    | (true)        | true    | true
//     1   | (null)    | (false)       | false   | null
//    null | (1)       | (true)        | true    | null
//    null | (1, null) | (true, false) | true    | null
//    null | (null)    | (false)       | false   | null
//     2   | (1)       | (empty)       | null    | false
//   *any* | (empty)   | (empty)       | null    | false
//
// It is important that the set given to BOOL_OR does not contain any null
// values (the reason for step #2). Null is reserved for use by the
// TryDecorrelateScalarGroupBy rule, which will push a left join into the
// GroupBy. Null values produced by the left join will simply be ignored by
// BOOL_OR, and so cannot be used for any other purpose.
func (r *subqueryHoister) constructGroupByAny(
	scalar memo.GroupID, cmp opt.Operator, input memo.GroupID,
) memo.GroupID {
	// When the scalar value is not a simple variable or constant expression,
	// then cache its value using a projection, since it will be referenced
	// multiple times.
	scalarExpr := r.mem.NormExpr(scalar)
	if scalarExpr.Operator() != opt.VariableOp && !scalarExpr.IsConstValue() {
		typ := r.mem.GroupProperties(scalar).Scalar.Type
		scalarColID := r.f.Metadata().AddColumn("scalar", typ)
		r.hoisted = r.f.projectExtraCol(r.hoisted, scalar, scalarColID)
		scalar = r.f.ConstructVariable(r.f.InternColumnID(scalarColID))
	}

	inputVar := r.f.funcs.referenceSingleColumn(input)

	notNullColID := r.f.Metadata().AddColumn("notnull", types.Bool)
	notNullCols := memo.ProjectionsOpDef{SynthesizedCols: opt.ColList{notNullColID}}
	notNullVar := r.f.ConstructVariable(r.f.InternColumnID(notNullColID))

	aggColID := r.f.Metadata().AddColumn("bool_or", types.Bool)
	aggVar := r.f.ConstructVariable(r.f.InternColumnID(aggColID))

	caseColID := r.f.Metadata().AddColumn("case", types.Bool)
	caseCols := memo.ProjectionsOpDef{SynthesizedCols: opt.ColList{caseColID}}

	nullVal := r.f.ConstructNull(r.f.InternType(types.Unknown))

	return r.f.ConstructProject(
		r.f.ConstructScalarGroupBy(
			r.f.ConstructProject(
				r.f.ConstructSelect(
					input,
					r.f.ConstructIsNot(
						r.f.funcs.ConstructBinary(cmp, scalar, inputVar),
						r.f.ConstructFalse(),
					),
				),
				r.f.ConstructProjections(
					r.f.funcs.InternSingletonList(r.f.ConstructIsNot(inputVar, nullVal)),
					r.f.InternProjectionsOpDef(&notNullCols),
				),
			),
			r.f.ConstructAggregations(
				r.f.funcs.InternSingletonList(r.f.ConstructBoolOr(notNullVar)),
				r.f.InternColList(opt.ColList{aggColID}),
			),
			r.f.InternGroupByDef(&memo.GroupByDef{}),
		),
		r.f.ConstructProjections(
			r.f.funcs.InternSingletonList(
				r.f.ConstructCase(
					r.f.ConstructTrue(),
					r.f.InternList([]memo.GroupID{
						r.f.ConstructWhen(
							r.f.ConstructAnd(r.f.InternList([]memo.GroupID{
								aggVar,
								r.f.ConstructIsNot(scalar, nullVal),
							})),
							r.f.ConstructTrue(),
						),
						r.f.ConstructWhen(
							r.f.ConstructIs(aggVar, nullVal),
							r.f.ConstructFalse(),
						),
						nullVal,
					}),
				),
			),
			r.f.InternProjectionsOpDef(&caseCols),
		),
	)
}
