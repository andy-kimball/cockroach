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
	"context"
	"fmt"

	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// Memo is a data structure for efficiently storing a forest of query plans.
// Conceptually, the memo is composed of a numbered set of equivalency classes
// called groups where each group contains a set of logically equivalent
// expressions. The different expressions in a single group are called memo
// expressions (memo-ized expressions). A memo expression has a list of child
// groups as its children rather than a list of individual expressions. The
// forest is composed of every possible combination of parent expression with
// its children, recursively applied.
//
// Memo expressions can be relational (e.g. join) or scalar (e.g. <). Operators
// are always both logical (specify results) and physical (specify results and
// a particular implementation). This means that even a "raw" unoptimized
// expression tree can be executed (naively). Both relational and scalar
// operators are uniformly represented as nodes in memo expression trees, which
// facilitates tree pattern matching and replacement.
//
// Because memo groups contain logically equivalent expressions, all the memo
// expressions in a group share the same logical properties. However, it's
// possible for two logically equivalent expression to be placed in different
// memo groups. This occurs because determining logical equivalency of two
// relational expressions is too complex to perform 100% correctly. A
// correctness failure (i.e. considering two expressions logically equivalent
// when they are not) results in invalid transformations and invalid plans.
// But placing two logically equivalent expressions in different groups has a
// much gentler failure mode: the memo and transformations are less efficient.
// Expressions within the memo may have different physical properties. For
// example, a memo group might contain both hash join and merge join
// expressions which produce the same set of output rows, but produce them in
// different orders.
//
// Expressions are inserted into the memo by the factory, which ensure that
// expressions have been fully normalized before insertion (see the comment in
// factory.go for more details). A new group is created only when unique
// normalized expressions are created by the factory during construction or
// rewrite of the tree. Uniqueness is determined by computing the fingerprint
// for a memo expression, which is simply the expression operator and its list
// of child groups. For example, consider this query:
//
//   SELECT * FROM a, b WHERE a.x = b.x
//
// After insertion into the memo, the memo would contain these six groups:
//
//   6: [inner-join [1 2 5]]
//   5: [eq [3 4]]
//   4: [variable b.x]
//   3: [variable a.x]
//   2: [scan b]
//   1: [scan a]
//
// The fingerprint for the inner-join expression is [inner-join [1 2 5]]. The
// memo maintains a map from expression fingerprint to memo group which allows
// quick determination of whether the normalized form of an expression already
// exists in the memo.
//
// The normalizing factory will never add more than one expression to a memo
// group. But the explorer does add denormalized expressions to existing memo
// groups, since oftentimes one of these equivalent, but denormalized
// expressions will have a lower cost than the initial normalized expression
// added by the factory. For example, the join commutativity transformation
// expands the memo like this:
//
//   6: [inner-join [1 2 5]] [inner-join [2 1 5]]
//   5: [eq [3 4]]
//   4: [variable b.x]
//   3: [variable a.x]
//   2: [scan b]
//   1: [scan a]
//
// TODO(andyk): See the comments in explorer.go for more details.
type Memo struct {
	// metadata provides information about the columns and tables used in this
	// particular query.
	metadata opt.Metadata

	// logPropsBuilder is inlined in the memo so that it can be reused each time
	// scalar or relational properties need to be built.
	logPropsBuilder logicalPropsBuilder

	interner interner

	// rootNode is the root node of the memo expression forest. It is set via
	// a call to SetRoot.
	rootNode opt.Node

	// rootProps are the physical properties required of the root memo node. It
	// is set via a call to SetRoot.
	rootProps *props.Physical

	// memEstimate is the approximate memory usage of the memo, in bytes.
	memEstimate int64

	// locName is the location which the memo is compiled against. This determines
	// the timezone, which is used for time-related data type construction and
	// comparisons. If the location changes, then this memo is invalidated.
	locName string

	// dbName is the current database at the time the memo was compiled. If this
	// changes, then the memo is invalidated.
	dbName string

	// searchPath is the current search path at the time the memo was compiled.
	// If this changes, then the memo is invalidated.
	searchPath sessiondata.SearchPath
}

// Init initializes a new empty memo instance, or resets existing state so it
// can be reused. It must be called before use (or reuse). The memo collects
// information about the context in which it is compiled from the evalContext
// argument. If any of that changes, then the memo must be invalidated (see the
// IsStale method for more details).
func (m *Memo) Init(evalCtx *tree.EvalContext) {
	m.metadata.Init()
	m.logPropsBuilder.init(evalCtx, m)
	m.interner.Init()

	m.rootNode = nil
	m.rootProps = nil
	m.memEstimate = 0
	m.locName = evalCtx.GetLocation().String()
	m.dbName = evalCtx.SessionData.Database
	m.searchPath = evalCtx.SessionData.SearchPath
}

func (m *Memo) IsEmpty() bool {
	return m.interner.Count() == 0
}

// MemoryEstimate returns a rough estimate of the memo's memory usage, in bytes.
// It only includes memory usage that is proportional to the size and complexity
// of the query, rather than constant overhead bytes.
func (m *Memo) MemoryEstimate() int64 {
	// Multiply by 2 to take rough account of allocation fragmentation, private
	// data, list overhead, properties, etc.
	return m.memEstimate * 2
}

// Metadata returns the metadata instance associated with the memo.
func (m *Memo) Metadata() *opt.Metadata {
	return &m.metadata
}

// RootNode returns the root memo node previously set via a call to SetRoot.
func (m *Memo) RootNode() opt.Node {
	return m.rootNode
}

// RootProps returns the physical properties required of the root memo group,
// previously set via a call to SetRoot.
func (m *Memo) RootProps() *props.Physical {
	return m.rootProps
}

// SetRoot stores the root memo node, as well as the physical properties
// required of the root group.
func (m *Memo) SetRoot(rel RelNode, phys *props.Physical) {
	m.rootNode = rel
	if m.rootProps != phys {
		m.rootProps = m.InternPhysicalProps(phys)
	}

	// Once memo is optimized, release reference to the eval context and free up
	// the memory used by the interner.
	if m.IsOptimized() {
		m.logPropsBuilder.clear()
		m.interner.Clear()
	}
}

// SetRoot stores the root memo node, as well as the physical properties
// required of the root group.
func (m *Memo) SetScalarRoot(scalar opt.ScalarExpr) {
	if m.rootNode != nil {
		panic("cannot set scalar root multiple times")
	}
	m.rootNode = scalar
}

// HasPlaceholders returns true if the memo contains at least one placeholder
// operator.
func (m *Memo) HasPlaceholders() bool {
	rel, ok := m.rootNode.(RelNode)
	if !ok {
		panic(fmt.Sprintf("placeholders only supported when memo root is relational"))
	}

	return rel.Relational().HasPlaceholder
}

// IsStale returns true if the memo has been invalidated by changes to any of
// its dependencies. Once a memo is known to be stale, it must be ejected from
// any query cache or prepared statement and replaced with a recompiled memo
// that takes into account the changes. IsStale checks the following
// dependencies:
//
//   1. Current database: this can change name resolution.
//   2. Current search path: this can change name resolution.
//   3. Current location: this determines time zone, and can change how time-
//      related types are constructed and compared.
//   4. Data source schema: this determines most aspects of how the query is
//      compiled.
//   5. Data source privileges: current user may no longer have access to one or
//      more data sources.
//
func (m *Memo) IsStale(ctx context.Context, evalCtx *tree.EvalContext, catalog opt.Catalog) bool {
	// Memo is stale if the current database has changed.
	if m.dbName != evalCtx.SessionData.Database {
		return true
	}

	// Memo is stale if the search path has changed. Assume it's changed if the
	// slice length is different, or if it no longer points to the same underlying
	// array. If two slices are the same length and point to the same underlying
	// array, then they are guaranteed to be identical. Note that GetPathArray
	// already specifies that the slice must not be modified, so its elements will
	// never be modified in-place.
	left := m.searchPath.GetPathArray()
	right := evalCtx.SessionData.SearchPath.GetPathArray()
	if len(left) != len(right) {
		return true
	}
	if len(left) != 0 && &left[0] != &right[0] {
		return true
	}

	// Memo is stale if the location has changed.
	if m.locName != evalCtx.GetLocation().String() {
		return true
	}

	// Memo is stale if the fingerprint of any data source in the memo's metadata
	// has changed, or if the current user no longer has sufficient privilege to
	// access the data source.
	if !m.Metadata().CheckDependencies(ctx, catalog) {
		return true
	}
	return false
}

func (m *Memo) InternPhysicalProps(physical *props.Physical) *props.Physical {
	// Special case physical properties that require nothing of operator.
	if !physical.Defined() {
		return &props.MinPhysProps
	}
	return m.interner.InternPhysicalProps(physical)
}

func (m *Memo) SetBestProps(nd RelNode, phys *props.Physical, cost Cost) {
	if nd.Physical() != nil {
		if nd.Physical() != phys || nd.Cost() != cost {
			panic(fmt.Sprintf("cannot overwrite %s (%.9g) with %s (%.9g)",
				nd.Physical(), nd.Cost(), phys, cost))
		}
		return
	}

	// Enforcer nodes keep their own copy of physical properties and cost.
	switch t := nd.(type) {
	case *SortNode:
		t.phys = phys
		t.cst = cost

	default:
		nd.group().setBestProps(phys, cost)
	}
}

// IsOptimized returns true if the memo has been fully optimized.
func (m *Memo) IsOptimized() bool {
	// The memo is optimized once the root node has physical properties assigned.
	rel, ok := m.rootNode.(RelNode)
	return ok && rel.Physical() != nil
}

// --------------------------------------------------------------------
// String representation.
// --------------------------------------------------------------------

// FmtFlags controls how the memo output is formatted.
type FmtFlags int

// HasFlags tests whether the given flags are all set.
func (f FmtFlags) HasFlags(subset FmtFlags) bool {
	return f&subset == subset
}

const (
	// FmtPretty performs a breadth-first topological sort on the memo groups,
	// and shows the root group at the top of the memo.
	FmtPretty FmtFlags = iota
)

// String returns a human-readable string representation of this memo for
// testing and debugging.
func (m *Memo) String() string {
	return m.FormatString(FmtPretty)
}

// FormatString returns a string representation of this memo for testing
// and debugging. The given flags control which properties are shown.
func (m *Memo) FormatString(flags FmtFlags) string {
	return m.format(&memoFmtCtx{buf: &bytes.Buffer{}, flags: flags})
}
