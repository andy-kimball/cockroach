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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

type memoFmtCtx struct {
	buf       *bytes.Buffer
	flags     FmtFlags
	ordering  []opt.Node
	numbering map[opt.Node]int
	interner  interner
}

func (m *Memo) format(f *memoFmtCtx) string {
	m.interner.Init()

	// First assign group numbers to every node in the memo.
	f.numbering = make(map[opt.Node]int)
	m.numberMemo(f, m.RootNode())

	// Now recursively format the memo using treeprinter.
	tp := treeprinter.New()

	var tpRoot treeprinter.Node
	if m.IsOptimized() {
		tpRoot = tp.Childf("memo (optimized, ~%dKB)", m.MemoryEstimate()/1024)
	} else {
		tpRoot = tp.Childf("memo (not optimized, ~%dKB)", m.MemoryEstimate()/1024)
	}

	for i, nd := range f.ordering {
		f.buf.Reset()
		if rel, ok := nd.(RelNode); ok {
			m.formatGroup(f, rel)
			tpChild := tpRoot.Childf("G%d: %s", i+1, f.buf.String())
			m.formatBestSet(f, rel, tpChild)
		} else {
			m.formatNode(f, nd)
			tpRoot.Childf("G%d: %s", i+1, f.buf.String())
		}
	}

	return tp.String()
}

func (m *Memo) numberMemo(f *memoFmtCtx, root opt.Node) {
	f.numbering[root] = 0
	f.ordering = append(f.ordering, root)

	start := 0
	for end := len(f.ordering); start != end; start, end = end, len(f.ordering) {
		for i := start; i < end; i++ {
			nd := f.ordering[i]

			if member, ok := nd.(RelNode); ok {
				for member = member.FirstNode(); member != nil; member = member.NextNode() {
					f.numbering[member] = i
					m.numberNode(f, member, i)
				}
			} else {
				m.numberNode(f, nd, i)
			}
		}
	}
}

func (m *Memo) numberNode(f *memoFmtCtx, nd opt.Node, number int) {
	for i := 0; i < nd.ChildCount(); i++ {
		child := nd.Child(i)

		// Don't include list item nodes, as they don't communicate useful
		// information.
		if opt.IsListItemOp(child) {
			child = child.Child(0)
		}

		// Handle special case of list nodes, which are not interned and stored
		// as byval slices in parent nodes. Intern separately in order to detect
		// duplicate lists.
		if opt.IsListOp(child) {
			interned := m.interner.InternNode(child)
			if interned != child {
				// List has already been seen before, so give it same number.
				f.numbering[child] = f.numbering[interned]
				continue
			}
		}

		// Use the first node in each group to ensure no more than one node per
		// group is added to the ordering list.
		grp := child
		if rel, ok := grp.(RelNode); ok {
			grp = rel.FirstNode()
		}

		if existing, ok := f.numbering[grp]; ok {
			f.numbering[child] = existing
		} else {
			f.numbering[child] = len(f.ordering)
			f.ordering = append(f.ordering, child)
		}
	}
}

func (m *Memo) formatGroup(f *memoFmtCtx, best RelNode) {
	first := best.FirstNode()
	for member := first; member != nil; member = member.NextNode() {
		if member != first {
			f.buf.WriteByte(' ')
		}
		m.formatNode(f, member)
	}
}

func (m *Memo) formatNode(f *memoFmtCtx, nd opt.Node) {
	fmt.Fprintf(f.buf, "(%s", nd.Op())
	for i := 0; i < nd.ChildCount(); i++ {
		child := nd.Child(i)
		if opt.IsListItemOp(child) {
			child = child.Child(0)
		}
		fmt.Fprintf(f.buf, " G%d", f.numbering[child]+1)
	}
	m.formatPrivate(f, nd, &props.Physical{})
	f.buf.WriteString(")")
}

func (m *Memo) formatBestSet(f *memoFmtCtx, best RelNode, tp treeprinter.Node) {
	for {
		physical := best.Physical()
		if physical == nil {
			break
		}

		f.buf.Reset()
		var tpChild treeprinter.Node
		if physical.Defined() {
			tpChild = tp.Childf("%s", physical)
		} else {
			tpChild = tp.Childf("[]")
		}
		m.formatBest(f, best)
		tpChild.Childf("best: %s", f.buf.String())
		tpChild.Childf("cost: %.2f", best.Cost())

		if !opt.IsEnforcerOp(best) {
			break
		}

		best = best.Child(0).(RelNode)
	}
}

func (m *Memo) formatBest(f *memoFmtCtx, best RelNode) {
	fmt.Fprintf(f.buf, "(%s", best.Op())

	for i := 0; i < best.ChildCount(); i++ {
		bestChild := best.Child(i)
		fmt.Fprintf(f.buf, " G%d", f.numbering[bestChild]+1)

		// Print properties required of the child if they are interesting.
		if rel, ok := bestChild.(RelNode); ok {
			required := rel.Physical()
			if required != nil && required.Defined() {
				fmt.Fprintf(f.buf, "=\"%s\"", required)
			}
		}
	}

	m.formatPrivate(f, best, best.Physical())
	f.buf.WriteString(")")
}

func (m *Memo) formatPrivate(f *memoFmtCtx, nd opt.Node, physProps *props.Physical) {
	private := nd.Private()
	if private == nil {
		return
	}

	// Start by using private expression formatting.
	nf := MakeNodeFmtCtxBuffer(f.buf, NodeFmtHideAll, m)
	formatPrivate(&nf, private, physProps)

	// Now append additional information that's useful in the memo case.
	switch t := nd.(type) {
	case *ScanNode:
		tab := m.metadata.Table(t.Table)
		if tab.ColumnCount() != t.Cols.Len() {
			fmt.Fprintf(f.buf, ",cols=%s", t.Cols)
		}
		if t.Constraint != nil {
			fmt.Fprintf(f.buf, ",constrained")
		}
		if t.HardLimit.IsSet() {
			fmt.Fprintf(f.buf, ",lim=%s", t.HardLimit)
		}

	case *IndexJoinNode:
		fmt.Fprintf(f.buf, ",cols=%s", t.Cols)

	case *LookupJoinNode:
		fmt.Fprintf(f.buf, ",keyCols=%v,lookupCols=%s", t.KeyCols, t.LookupCols)

	case *ExplainNode:
		propsStr := t.Props.String()
		if propsStr != "" {
			fmt.Fprintf(f.buf, " %s", propsStr)
		}

	case *ProjectNode:
		t.Passthrough.ForEach(func(i int) {
			fmt.Fprintf(f.buf, " %s", m.metadata.ColumnLabel(opt.ColumnID(i)))
		})
	}
}
