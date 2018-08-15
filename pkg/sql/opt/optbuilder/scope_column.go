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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

const scopeColInlineCount = 16

type scopeColAlloc struct {
	inline      [scopeColInlineCount]scopeColumn
	inlineCount int
	more        []scopeColumn
}

func (a *scopeColAlloc) alloc() *scopeColumn {
	if a.inlineCount < scopeColInlineCount {
		a.inlineCount++
		return &a.inline[a.inlineCount-1]
	} else if a.more == nil {
		a.more = make([]scopeColumn, 1, scopeColInlineCount)
		return &a.more[0]
	} else if len(a.more) == cap(a.more) {
		a.more = make([]scopeColumn, 1, len(a.more)*2)
		return &a.more[0]
	}
	a.more = append(a.more, scopeColumn{})
	return &a.more[len(a.more)-1]
}

type scopeColList struct {
	first, last *scopeColumn
}

func (l *scopeColList) empty() bool {
	return l.first == nil
}

func (l *scopeColList) count() int {
	cnt := 0
	for col := l.first; col != nil; col = col.next {
		cnt++
	}
	return cnt
}

func (l *scopeColList) lastCol() *scopeColumn {
	if l.last != nil {
		return l.last
	}
	return l.first
}

func (l *scopeColList) ith(i int) *scopeColumn {
	col := l.first
	for {
		if i <= 0 || col == nil {
			return col
		}
		col = col.next
		i--
	}
}

func (l *scopeColList) append(col *scopeColumn) {
	if col.next != nil {
		panic("cannot append column that is part of list; use appendList for that")
	}
	if l.first == nil {
		l.first = col
	} else if l.last == nil {
		l.first.next = col
		l.last = col
	} else {
		l.last.next = col
		l.last = col
	}
}

func (l *scopeColList) removeMatches(match func(col *scopeColumn) bool) {
	var prev *scopeColumn
	for col := l.first; col != nil; col = col.next {
		if match(col) {
			if col == l.first {
				l.first = col.next
			} else {
				prev.next = col.next
			}
		} else {
			prev = col
		}
	}
	if prev == l.first {
		l.last = nil
	} else {
		l.last = prev
	}
}

// scopeColumn holds per-column information that is scoped to a particular
// relational expression. Note that scopeColumn implements the tree.TypedExpr
// interface. During name resolution, unresolved column names in the AST are
// replaced with a scopeColumn.
type scopeColumn struct {
	// origName is the original name of this column, either in its origin table,
	// or when it was first synthesized.
	origName tree.Name

	// name is the current name of this column. It is usually the same as
	// origName, unless this column was renamed with an AS expression.
	name  tree.Name
	table tree.TableName
	typ   types.T

	// id is an identifier for this column, which is unique across all the
	// columns in the query.
	id     opt.ColumnID
	hidden bool

	// group is the GroupID of the scalar expression associated with this column.
	// group is 0 for columns that are just passed through from an inner scope
	// and for table columns.
	group memo.GroupID

	// expr is the AST expression that this column refers to, if any.
	// expr is nil if the column does not refer to an expression.
	expr tree.TypedExpr

	// exprStr contains a stringified representation of expr, or the original
	// column name if expr is nil. It is populated lazily inside getExprStr().
	exprStr string

	next *scopeColumn
}

// getExprStr gets a stringified representation of the expression that this
// column refers to, or the original column name if the column does not refer
// to an expression. It caches the result in exprStr.
func (c *scopeColumn) getExprStr() string {
	if c.exprStr == "" {
		if c.expr == nil {
			if tableStr := c.table.String(); tableStr != "" {
				c.exprStr = fmt.Sprintf("%s.%s", tableStr, c.origName)
			} else {
				c.exprStr = string(c.origName)
			}
		} else {
			c.exprStr = symbolicExprStr(c.expr)
		}
	}
	return c.exprStr
}

var _ tree.Expr = &scopeColumn{}
var _ tree.TypedExpr = &scopeColumn{}
var _ tree.VariableExpr = &scopeColumn{}

func (c *scopeColumn) String() string {
	return tree.AsString(c)
}

// Format implements the NodeFormatter interface.
func (c *scopeColumn) Format(ctx *tree.FmtCtx) {
	// FmtCheckEquivalence is used by getExprStr when comparing expressions for
	// equivalence. If that flag is present, then use the unique column id to
	// differentiate this column from other columns.
	if ctx.HasFlags(tree.FmtCheckEquivalence) {
		// Use double @ to distinguish from Cockroach column ordinals.
		ctx.Printf("@@%d", c.id)
		return
	}

	if ctx.HasFlags(tree.FmtShowTableAliases) && c.table.TableName != "" {
		if c.table.ExplicitSchema && c.table.SchemaName != "" {
			if c.table.ExplicitCatalog && c.table.CatalogName != "" {
				ctx.FormatNode(&c.table.CatalogName)
				ctx.WriteByte('.')
			}
			ctx.FormatNode(&c.table.SchemaName)
			ctx.WriteByte('.')
		}

		ctx.FormatNode(&c.table.TableName)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&c.name)
}

// Walk is part of the tree.Expr interface.
func (c *scopeColumn) Walk(v tree.Visitor) tree.Expr {
	return c
}

// TypeCheck is part of the tree.Expr interface.
func (c *scopeColumn) TypeCheck(_ *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	return c, nil
}

// ResolvedType is part of the tree.TypedExpr interface.
func (c *scopeColumn) ResolvedType() types.T {
	return c.typ
}

// Eval is part of the tree.TypedExpr interface.
func (*scopeColumn) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(fmt.Errorf("scopeColumn must be replaced before evaluation"))
}

// Variable is part of the tree.VariableExpr interface. This prevents the
// column from being evaluated during normalization.
func (*scopeColumn) Variable() {}
