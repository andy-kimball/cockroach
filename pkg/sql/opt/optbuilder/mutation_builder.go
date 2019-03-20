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

package optbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// mutationBuilder is a helper struct that supports building Insert, Update,
// Upsert, and Delete operators in stages.
// TODO(andyk): Add support for Delete.
type mutationBuilder struct {
	b  *Builder
	md *opt.Metadata

	// op is InsertOp, UpdateOp, UpsertOp, or DeleteOp.
	op opt.Operator

	// tab is the target table.
	tab cat.Table

	// tabID is the metadata ID of the table.
	tabID opt.TableID

	// alias is the table alias specified in the mutation statement, or just the
	// resolved table name if no alias was specified.
	alias tree.TableName

	// targetColList is an ordered list of IDs of the table columns into which
	// values will be inserted, or which will be updated with new values. It is
	// incrementally built as the mutation operator is built.
	targetColList opt.ColList

	// targetColSet contains the same column IDs as targetColList, but as a set.
	targetColSet opt.ColSet

	// insertColList is an ordered list of IDs of input columns which provide
	// values to be inserted. Its length is always equal to the number of columns
	// in the target table, including mutation columns. Table columns which will
	// not have values inserted are set to zero (e.g. delete-only mutation
	// columns). insertColList is empty if this is not an Insert operator.
	insertColList opt.ColList

	// fetchColList is an ordered list of IDs of input columns which are fetched
	// from a target table in order to provide existing values that will form
	// lookup and update values. Its length is always equal to the number of
	// columns in the target table, including mutation columns. Table columns
	// which do not need to be fetched are set to zero. fetchColList is empty if
	// this is an Insert operator with no ON CONFLICT clause.
	fetchColList opt.ColList

	// updateColList is an ordered list of IDs of input columns which contain new
	// updated values for columns in a target table. Its length is always equal
	// to the number of columns in the target table, including mutation columns.
	// Table columns which do not need to be updated are set to zero.
	// updateColList is empty if this is an Insert operator with no ON CONFLICT
	// clause.
	updateColList opt.ColList

	// returnColList is an ordered list of IDs of input columns which contain the
	// final values for the target table. If a RETURNING clause is present, then
	// it will return the values from these columns. Note that some of the return
	// values represent brand new values that will be set by this mutation. But
	// others may be existing values that remain unchanged. This list is also
	// useful for computing check constraints, since they must be computed over
	// the final set of values, after all mutations have been applied. Note that
	// delete-only columns may be zero if they're never referenced by the operator
	// (never returned, never mutated, never part of check constraint expression,
	// etc).
	//
	// The length of returnColList is always equal to the number of columns in
	// the target table, including mutation columns. Table columns which do not
	// need to be updated are set to zero.
	returnScopeOrds []int

	// checkColList is an ordered list of IDs of input columns which contain the
	// boolean results of evaluating check constraint expressions defined on the
	// target table. Its length is always equal to the number of check constraints
	// on the table (see opt.Table.CheckCount).
	checkColList opt.ColList

	// canaryColID is the ID of the column that is used to decide whether to
	// insert or update each row. If the canary column's value is null, then it's
	// an insert; otherwise it's an update.
	canaryColID opt.ColumnID

	// subqueries temporarily stores subqueries that were built during initial
	// analysis of SET expressions. They will be used later when the subqueries
	// are joined into larger LEFT OUTER JOIN expressions.
	subqueries []*scope

	// parsedExprs is a cached set of parsed default and computed expressions
	// from the table schema. These are parsed once and cached for reuse.
	parsedExprs []tree.Expr

	// outScope contains the current set of columns that are in scope, as well as
	// the output expression as it is incrementally built. Once the final Insert
	// expression is completed, it will be contained in outScope.expr.
	outScope *scope
}

func (mb *mutationBuilder) init(b *Builder, op opt.Operator, tab cat.Table, alias tree.TableName) {
	mb.b = b
	mb.md = b.factory.Metadata()
	mb.op = op
	mb.tab = tab
	mb.alias = alias
	mb.targetColList = make(opt.ColList, 0, tab.DeletableColumnCount())

	// returnScopeOrds starts with uninitialized ordinals.
	mb.returnScopeOrds = make([]int, tab.DeletableColumnCount())
	for i := range mb.returnScopeOrds {
		mb.returnScopeOrds[i] = -1
	}

	// Add the table and its columns (including mutation columns) to metadata.
	mb.tabID = mb.md.AddTableWithAlias(tab, &mb.alias)
}

// buildInputForUpdateOrDelete constructs a Select expression from the fields in
// the Update or Delete operator, similar to this:
//
//   SELECT <cols>
//   FROM <table>
//   WHERE <where>
//   ORDER BY <order-by>
//   LIMIT <limit>
//
// All columns from the table to update are added to fetchColList.
// TODO(andyk): Do needed column analysis to project fewer columns if possible.
func (mb *mutationBuilder) buildInputForUpdateOrDelete(
	inScope *scope, where *tree.Where, limit *tree.Limit, orderBy tree.OrderBy,
) {
	// Fetch columns from different instance of the table metadata, so that it's
	// possible to remap columns, as in this example:
	//
	//   UPDATE abc SET a=b
	//
	inputTabID := mb.md.AddTableWithAlias(mb.tab, &mb.alias)

	// FROM
	mb.outScope = mb.b.buildScan(
		inputTabID,
		nil, /* ordinals */
		nil, /* indexFlags */
		includeMutations,
		inScope,
	)

	// WHERE
	mb.b.buildWhere(where, mb.outScope)

	// SELECT + ORDER BY (which may add projected expressions)
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)
	orderByScope := mb.b.analyzeOrderBy(orderBy, mb.outScope, projectionsScope)
	mb.b.buildOrderBy(mb.outScope, projectionsScope, orderByScope)
	mb.b.constructProjectForScope(mb.outScope, projectionsScope)

	// LIMIT
	if limit != nil {
		mb.b.buildLimit(limit, inScope, projectionsScope)
	}

	mb.outScope = projectionsScope

	// Set list of columns that will be fetched by the input expression.
	mb.fetchColList = make(opt.ColList, mb.tab.DeletableColumnCount())
	for i := range mb.outScope.cols {
		mb.fetchColList[i] = mb.outScope.cols[i].id
		mb.returnScopeOrds[i] = i
	}
}

// addTargetColsByName adds one target column for each of the names in the given
// list.
func (mb *mutationBuilder) addTargetColsByName(names tree.NameList) {
	for _, name := range names {
		// Determine the ordinal position of the named column in the table and
		// add it as a target column.
		if ord := cat.FindTableColumnByName(mb.tab, name); ord != -1 {
			mb.addTargetCol(ord)
			continue
		}
		panic(builderError{sqlbase.NewUndefinedColumnError(string(name))})
	}
}

// addTargetCol adds a target column by its ordinal position in the target
// table. It raises an error if a mutation or computed column is targeted, or if
// the same column is targeted multiple times.
func (mb *mutationBuilder) addTargetCol(ord int) {
	tabCol := mb.tab.Column(ord)

	// Don't allow targeting of mutation columns.
	if cat.IsMutationColumn(mb.tab, ord) {
		panic(builderError{makeBackfillError(tabCol.ColName())})
	}

	// Computed columns cannot be targeted with input values.
	if tabCol.IsComputed() {
		panic(builderError{sqlbase.CannotWriteToComputedColError(string(tabCol.ColName()))})
	}

	// Ensure that the name list does not contain duplicates.
	colID := mb.tabID.ColumnID(ord)
	if mb.targetColSet.Contains(int(colID)) {
		panic(pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"multiple assignments to the same column %q", tabCol.ColName()))
	}
	mb.targetColSet.Add(int(colID))

	mb.targetColList = append(mb.targetColList, colID)
}

// extractValuesInput tests whether the given input is a VALUES clause with no
// WITH, ORDER BY, or LIMIT modifier. If so, it's returned, otherwise nil is
// returned.
func (mb *mutationBuilder) extractValuesInput(inputRows *tree.Select) *tree.ValuesClause {
	if inputRows == nil {
		return nil
	}

	// Only extract a simple VALUES clause with no modifiers.
	if inputRows.With != nil || inputRows.OrderBy != nil || inputRows.Limit != nil {
		return nil
	}

	// Discard parentheses.
	if parens, ok := inputRows.Select.(*tree.ParenSelect); ok {
		return mb.extractValuesInput(parens.Select)
	}

	if values, ok := inputRows.Select.(*tree.ValuesClause); ok {
		return values
	}

	return nil
}

// replaceDefaultExprs looks for DEFAULT specifiers in input value expressions
// and replaces them with the corresponding default value expression for the
// corresponding column. This is only possible when the input is a VALUES
// clause. For example:
//
//   INSERT INTO t (a, b) (VALUES (1, DEFAULT), (DEFAULT, 2))
//
// Here, the two DEFAULT specifiers are replaced by the default value expression
// for the a and b columns, respectively.
//
// replaceDefaultExprs returns a VALUES expression with replaced DEFAULT values,
// or just the unchanged input expression if there are no DEFAULT values.
func (mb *mutationBuilder) replaceDefaultExprs(inRows *tree.Select) (outRows *tree.Select) {
	values := mb.extractValuesInput(inRows)
	if values == nil {
		return inRows
	}

	// Ensure that the number of input columns exactly matches the number of
	// target columns.
	numCols := len(values.Rows[0])
	mb.checkNumCols(len(mb.targetColList), numCols)

	var newRows []tree.Exprs
	for irow, tuple := range values.Rows {
		if len(tuple) != numCols {
			reportValuesLenError(numCols, len(tuple))
		}

		// Scan list of tuples in the VALUES row, looking for DEFAULT specifiers.
		var newTuple tree.Exprs
		for itup, val := range tuple {
			if _, ok := val.(tree.DefaultVal); ok {
				// Found DEFAULT, so lazily create new rows and tuple lists.
				if newRows == nil {
					newRows = make([]tree.Exprs, irow, len(values.Rows))
					copy(newRows, values.Rows[:irow])
				}

				if newTuple == nil {
					newTuple = make(tree.Exprs, itup, numCols)
					copy(newTuple, tuple[:itup])
				}

				val = mb.parseDefaultOrComputedExpr(mb.targetColList[itup])
			}
			if newTuple != nil {
				newTuple = append(newTuple, val)
			}
		}

		if newRows != nil {
			if newTuple != nil {
				newRows = append(newRows, newTuple)
			} else {
				newRows = append(newRows, tuple)
			}
		}
	}

	if newRows != nil {
		return &tree.Select{Select: &tree.ValuesClause{Rows: newRows}}
	}
	return inRows
}

// addSynthesizedCols is a helper method for addDefaultAndComputedColsForInsert
// and addComputedColsForUpdate that scans the list of table columns, looking
// for any that do not yet have values provided by the input expression. New
// columns are synthesized for any missing columns, as long as the addCol
// callback function returns true for that column.
func (mb *mutationBuilder) addSynthesizedCols(
	colList opt.ColList, addCol func(tabCol cat.Column) bool,
) {
	var projectionsScope *scope

	// Skip delete-only mutation columns, since they are ignored by all mutation
	// operators that synthesize columns.
	for i, n := 0, mb.tab.WritableColumnCount(); i < n; i++ {
		// Skip columns that are already specified.
		if colList[i] != 0 {
			continue
		}

		// Invoke addCol to determine whether column should be added.
		tabCol := mb.tab.Column(i)
		if !addCol(tabCol) {
			continue
		}

		// Construct a new Project operator that will contain the newly synthesized
		// column(s).
		if projectionsScope == nil {
			projectionsScope = mb.outScope.replace()
			projectionsScope.appendColumnsFromScope(mb.outScope)
		}
		tabColID := mb.tabID.ColumnID(i)
		expr := mb.parseDefaultOrComputedExpr(tabColID)
		texpr := mb.outScope.resolveAndRequireType(expr, tabCol.DatumType())
		scopeCol := mb.b.addColumn(projectionsScope, "" /* alias */, texpr)
		mb.b.buildScalar(texpr, mb.outScope, projectionsScope, scopeCol, nil)

		// Assign name to synthesized column. Computed columns may refer to default
		// columns in the table by name.
		scopeCol.table = *mb.tab.Name()
		scopeCol.name = tabCol.ColName()

		// Store id of newly synthesized column in corresponding list slots.
		colList[i] = scopeCol.id
		mb.returnScopeOrds[i] = len(projectionsScope.cols) - 1

		// Add corresponding target column.
		mb.targetColList = append(mb.targetColList, tabColID)
		mb.targetColSet.Add(int(tabColID))
	}

	if projectionsScope != nil {
		mb.b.constructProjectForScope(mb.outScope, projectionsScope)
		mb.outScope = projectionsScope
	}
}

/*
func (mb *mutationBuilder) limitCols() {
	var projections memo.ProjectionsExpr
	var passthrough opt.ColSet
	for i, n := 0, mb.tab.DeletableColumnCount(); i < n; i++ {
		colID := mb.mapToInputColID(i)
		col := mb.tab.Column(i)
		if maybeLimitColType(col.DatumType()) {

		} else {
			passthrough.Add(int())
		}

		colTyp := col.ColType()

	}

	return mb.b.factory.ConstructProject(input, projections, passthrough)
}

func maybeLimitColType(typ types.T) bool {
	typ = types.UnwrapType(typ)
	if typ.Equivalent(types.Decimal) {
		return true
	}
	if arr, ok := typ.(types.TArray); ok {
		return maybeLimitColType(arr.Typ)
	}
	return false
}
*/
// addCheckConstraintCols synthesizes a boolean output column for each check
// constraint defined on the target table. The mutation operator will report
// a constraint violation error if the value of the column is false.
func (mb *mutationBuilder) addCheckConstraintCols() {
	if mb.tab.CheckCount() > 0 {
		// Disambiguate names so that references in the constraint expression refer
		// to the correct columns.
		mb.disambiguateColumns()

		mb.checkColList = make(opt.ColList, mb.tab.CheckCount())
		projectionsScope := mb.outScope.replace()
		projectionsScope.appendColumnsFromScope(mb.outScope)

		for i, n := 0, mb.tab.CheckCount(); i < n; i++ {
			expr, err := parser.ParseExpr(string(mb.tab.Check(i)))
			if err != nil {
				panic(builderError{err})
			}

			alias := fmt.Sprintf("check%d", i+1)
			texpr := mb.outScope.resolveAndRequireType(expr, types.Bool)
			scopeCol := mb.b.addColumn(projectionsScope, alias, texpr)
			mb.b.buildScalar(texpr, mb.outScope, projectionsScope, scopeCol, nil)
			mb.checkColList[i] = scopeCol.id
		}

		mb.b.constructProjectForScope(mb.outScope, projectionsScope)
		mb.outScope = projectionsScope
	}
}

// disambiguateColumns ranges over the scope and ensures that at most one column
// has each table column name, and that name refers to the column with the final
// value that the mutation applies.
func (mb *mutationBuilder) disambiguateColumns() {
	// Get list of scope column ordinals that need to be preserved.
	var ordSet util.FastIntSet
	for _, ord := range mb.returnScopeOrds {
		if ord != -1 {
			ordSet.Add(ord)
		}
	}

	// Clear any names that are not part of ordSet. For remaining names, use table
	// name, not alias name, since computed column expressions will not reference
	// aliases.
	for i := range mb.outScope.cols {
		if ordSet.Contains(i) {
			mb.outScope.cols[i].table = *mb.tab.Name()
		} else {
			mb.outScope.cols[i].clearName()
		}
	}
}

// makeMutationPrivate builds a MutationPrivate struct containing the table and
// column metadata needed for the mutation operator.
func (mb *mutationBuilder) makeMutationPrivate(needResults bool) *memo.MutationPrivate {
	private := &memo.MutationPrivate{
		Table:      mb.tabID,
		InsertCols: mb.insertColList,
		FetchCols:  mb.fetchColList,
		UpdateCols: mb.updateColList,
		CanaryCol:  mb.canaryColID,
		CheckCols:  mb.checkColList,
	}

	if needResults {
		// Only non-mutation columns are output columns. ReturnCols needs to have
		// DeletableColumnCount entries, but only the first ColumnCount entries
		// can be non-zero.
		private.ReturnCols = make(opt.ColList, len(mb.returnScopeOrds))
		for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
			if mb.returnScopeOrds[i] == -1 {
				panic(fmt.Sprintf("column %d is not available in the mutation input", i))
			}
			private.ReturnCols[i] = mb.outScope.cols[mb.returnScopeOrds[i]].id
		}
	}

	return private
}

// buildReturning wraps the input expression with a Project operator that
// projects the given RETURNING expressions.
func (mb *mutationBuilder) buildReturning(returning tree.ReturningExprs) {
	// Handle case of no RETURNING clause.
	if returning == nil {
		mb.outScope = &scope{builder: mb.b, expr: mb.outScope.expr}
		return
	}

	// Start out by constructing a scope containing one column for each non-
	// mutation column in the target table, in the same order, and with the
	// same names. These columns can be referenced by the RETURNING clause.
	//
	//   1. Project only non-mutation columns.
	//   2. Alias columns to use table column names.
	//   3. Mark hidden columns.
	//   4. Project columns in same order as defined in table schema.
	//
	inScope := mb.outScope.replace()
	inScope.expr = mb.outScope.expr
	inScope.cols = make([]scopeColumn, 0, mb.tab.ColumnCount())
	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		tabCol := mb.tab.Column(i)
		inScope.cols = append(inScope.cols, scopeColumn{
			name:   tabCol.ColName(),
			table:  mb.alias,
			typ:    tabCol.DatumType(),
			id:     mb.tabID.ColumnID(i),
			hidden: tabCol.IsHidden(),
		})
	}

	// Construct the Project operator that projects the RETURNING expressions.
	outScope := inScope.replace()
	mb.b.analyzeReturningList(returning, nil /* desiredTypes */, inScope, outScope)
	mb.b.buildProjectionList(inScope, outScope)
	mb.b.constructProjectForScope(inScope, outScope)
	mb.outScope = outScope
}

// checkNumCols raises an error if the expected number of columns does not match
// the actual number of columns.
func (mb *mutationBuilder) checkNumCols(expected, actual int) {
	if actual != expected {
		more, less := "expressions", "target columns"
		if actual < expected {
			more, less = less, more
		}

		var kw string
		if mb.op == opt.InsertOp {
			kw = "INSERT"
		} else {
			kw = "UPSERT"
		}
		panic(pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"%s has more %s than %s, %d expressions for %d targets",
			kw, more, less, actual, expected))
	}
}

// parseDefaultOrComputedExpr parses the default (including nullable) or
// computed value expression for the given table column, and caches it for
// reuse.
func (mb *mutationBuilder) parseDefaultOrComputedExpr(colID opt.ColumnID) tree.Expr {
	if mb.parsedExprs == nil {
		mb.parsedExprs = make([]tree.Expr, mb.tab.DeletableColumnCount())
	}

	// Return expression from cache, if it was already parsed previously.
	ord := mb.tabID.ColumnOrdinal(colID)
	if mb.parsedExprs[ord] != nil {
		return mb.parsedExprs[ord]
	}

	var exprStr string
	tabCol := mb.tab.Column(ord)
	switch {
	case tabCol.IsComputed():
		exprStr = tabCol.ComputedExprStr()
	case tabCol.HasDefault():
		exprStr = tabCol.DefaultExprStr()
	default:
		return tree.DNull
	}

	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		panic(builderError{err})
	}

	mb.parsedExprs[ord] = expr
	return expr
}

// findNotNullIndexCol finds the first not-null column in the given index and
// returns its ordinal position in the owner table. There must always be such a
// column, even if it turns out to be an implicit primary key column.
func findNotNullIndexCol(index cat.Index) int {
	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
		indexCol := index.Column(i)
		if !indexCol.IsNullable() {
			return indexCol.Ordinal
		}
	}
	panic(pgerror.NewAssertionErrorf("should have found not null column in index"))
}

// resultsNeeded determines whether a statement that might have a RETURNING
// clause needs to provide values for result rows for a downstream plan.
func resultsNeeded(r tree.ReturningClause) bool {
	switch t := r.(type) {
	case *tree.ReturningExprs:
		return true
	case *tree.ReturningNothing, *tree.NoReturningClause:
		return false
	default:
		panic(pgerror.NewAssertionErrorf("unexpected ReturningClause type: %T", t))
	}
}

// getAliasedTableName returns the underlying table name for a TableExpr that
// could be either an alias or a normal table name. It also returns the alias,
// if there is one.
//
// This is not meant to perform name resolution, but rather simply to extract
// the name indicated after FROM in DELETE/INSERT/UPDATE/UPSERT.
func getAliasedTableName(n tree.TableExpr) (*tree.TableName, *tree.TableName) {
	var alias *tree.TableName
	if ate, ok := n.(*tree.AliasedTableExpr); ok {
		n = ate.Expr
		// It's okay to ignore the As columns here, as they're not permitted in
		// DML aliases where this function is used. The grammar does not allow
		// them, so the parser would have reported an error if they were present.
		if ate.As.Alias != "" {
			alias = tree.NewUnqualifiedTableName(ate.As.Alias)
		}
	}
	tn, ok := n.(*tree.TableName)
	if !ok {
		panic(pgerror.Unimplemented(
			"complex table expression in UPDATE/DELETE",
			"cannot use a complex table name with DELETE/UPDATE"))
	}
	return tn, alias
}

// checkDatumTypeFitsColumnType verifies that a given scalar value type is valid
// to be stored in a column of the given column type.
//
// For the purpose of this analysis, column type aliases are not considered to
// be different (eg. TEXT and VARCHAR will fit the same scalar type String).
//
// This is used by the UPDATE, INSERT and UPSERT code.
func checkDatumTypeFitsColumnType(col cat.Column, typ types.T) {
	if typ.Equivalent(col.DatumType()) {
		return
	}

	colName := string(col.ColName())
	panic(pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError,
		"value type %s doesn't match type %s of column %q",
		typ, col.ColType().String(), tree.ErrNameString(colName)))
}
