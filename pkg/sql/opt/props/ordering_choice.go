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

package props

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util"
	"golang.org/x/tools/go/gcimporter15/testdata"
)

// Ordering defines the order of rows provided or required by an operator. A
// negative value indicates descending order on the column id "-(value)".

// OrderingChoice
type OrderingChoice struct {
	optional opt.ColSet

	// groups and optional are disjoint set.
	groups []equivGroup
}

type equivGroup struct {
	cols opt.ColSet
	desc bool
}

// Defined is true if a particular row ordering is required or provided.
func (o *OrderingChoice) Defined() bool {
	return len(o.groups) != 0
}

func (o *OrderingChoice) Ordering() opt.Ordering {
	return len(o.groups)
}

func (o *OrderingChoice) AddCol(id opt.ColumnID, descending bool) {
	o.groups.Add(len(o.ordCols))
	o.ordCols = append(o.ordCols, opt.MakeOrderingColumn(id, descending))
}

func (o *OrderingChoice) ColsSubsetOf(cs opt.ColSet) bool {
	for _, col := range o.ordCols {
		if !cs.Contains(int(col.ID())) {
			return false
		}
	}
	return true
}

// Equals returns true if the two orderings are identical.
func (o *OrderingChoice) Equals(rhs *OrderingChoice) bool {
	if !o.groups.Equals(rhs.groups) {
		return false
	}
	if !o.optional.Equals(rhs.optional) {
		return false
	}

	for i := range o.ordCols {
		if o.ordCols[i] != rhs.ordCols[i] {
			return false
		}
	}
	return true
}

// Provides returns true if every ordering in this set of ordering choices
// satisfies some ordering in the set of required choices. In other words, is
// this set of ordering choices at least as restrictive as the required set?
// However, to keep the algorithm simpler, there may be cases where Provides
// returns false even though
//
//   (+a)              provides (+a)
//   (+a),(+b)         provides (+a)
//   (+a)              provides (+a) opt=(b)
//   (-b),(+a)         provides (+a) opt=(b)
//   opt=(a)           provides opt=(a,b)
//   (+a),(+b) opt=(a) provides opt=(a)
//   (+a)              provides (+a|+b)
//   (+b)              provides (+a|+b)
//   (+a|+b)           provides (+a) opt=(b)
//
// While the general problem is equivalent to the "inclusion problem for regular
// expressions", some simplifying assumptions keep the algorithm from getting
// too complex:
//
//   1. Both sets have same set of equivalent columns. If a==b, then any time
//      "a" appears, it will
//   2. Both sets have same set of optional columns.
//   3.
//
// It is always OK to return false, even if th
// If any of these assumptions doesn't hold
//
// found to be false, the Provides will be
// conservative and simply return false.
func (oc *OrderingChoice) Provides(required *OrderingChoice) bool {
	if !oc.optional.SubsetOf(required.optional) {
		return false
	}

	reqIter := OrderingIterator{Choices: required}
	if reqIter.Next() {
		// Trivial case where required allows any ordering.
		return true
	}

	ocIter := OrderingIterator{Choices: oc}
	for ocIter.Next() {
		if ocIter.EquivOrdCols().SubsetOf(required.optional) {
			// Skip past optional columns.
			continue
		}

		if !ocIter.EquivOrdCols().Equals(reqIter.EquivOrdCols()) {
			break
		}

		if !reqIter.Next() {
			return true
		}
	}

	return false
}

// SELECT * FROM abc WHERE b=2 LIMIT 1 ORDER BY b
func (o *OrderingChoice) CanReduce(fdset *FuncDepSet) bool {
	if !fdset.ComputeClosure(o.optional).Equals(o.optional) {
		return true
	}

	allCols := o.allCols()
	if !fdset.ComputeEquivClosure(allCols).Equals(allCols) {
		return true
	}

	last, remaining := o.splitCols()
	if last.SubsetOf(fdset.ComputeClosure(remaining)) {
		return true
	}

	return false
}

// SELECT * FROM abc WHERE b=2 LIMIT 1 ORDER BY b
func (oc *OrderingChoice) Reduce(fdset *FuncDepSet) {
	oc.optional = fdset.ComputeClosure(oc.optional)

	var newGroups util.FastIntSet
	newOrdCols := make([]opt.OrderingColumn, len(oc.ordCols))

	added := oc.optional.Copy()
	iter := OrderingIterator{Choices: oc}
	for iter.Next() {
		// Constant columns from the FD set become optional ordering columns and so
		// can be removed.
		// Don't add duplicate ordering columns
		ordCols := iter.EquivOrdCols()



		if ordCols.SubsetOf(allCols) {
			continue
		}


		newGroups.Add(len(newOrdCols))
		ordCols = fdset.ComputeEquivClosure(ordCols)
		for c, ok := ordCols.Next(0); ok; c, ok = ordCols.Next(c + 1) {
			newOrdCols = append(newOrdCols, opt.OrderingColumn(c))
		}
	}



	cols := fdset.ComputeClosure(oc.optional)
	if !cols.Equals(oc.optional.Equals()
	n := 0
	remove := false
	for i, col := range oc.ordCols {
		if oc.optional.Contains(int(col)) {
			continue
		}
		if n != i {
			oc.ordCols[n] = oc.ordCols[i]
		}
		n++
	}
	oc.ordCols = oc.ordCols[:n]

	closure := oc.optional
	n := 0
	keep := true
	for i, col := range oc.ordCols {
		remove = remove ||

		if col == 0 {

		}

		if !closure.Contains(int(col.ID())) {

		}
	}

	// First expand the set of optional ordering columns if possible.
	oc.optional = fdset.ComputeClosure(oc.optional)

	n := 0
	it := OrderingIterator{Choices: oc}
	for it.Next() {
		if oc.optional.Contains(int(it.Col().ID())) {
			// Drop optional columns from the ordering columns.

		}
	}
}

// String returns the pattern formatted like this:
//   (+1)
//   (+1|+2)
//   (+1|+2),(+3)
//   (+3|+4),(+5) opt=(1,2)
func (o OrderingChoice) String() string {
	var buf bytes.Buffer
	o.format(&buf)
	return buf.String()
}

func (o OrderingChoice) format(buf *bytes.Buffer) {
	for g, ok := o.groups.Next(0); ok; g, ok = o.groups.Next(g + 1) {
		next, ok := o.groups.Next(g + 1)
		if !ok {
			next = len(o.ordCols)
		}

		for i, col := range o.ordCols[g:next] {
			if i != 0 {
				buf.WriteByte('|')
			}
			if col.Ascending() {
				buf.WriteByte('+')
			} else {
				buf.WriteByte('-')
			}
			fmt.Fprintf(buf, "%d", col.ID())
		}

		if ok {
			buf.WriteByte(',')
		}
	}
	if !o.optional.Empty() {
		fmt.Fprintf(buf, "opt=%s", o.optional)
	}
}

// ColSet returns the set of column IDs used in the ordering.
// not including constants
func (p *OrderingChoice) allCols() opt.ColSet {
	var cs opt.ColSet
	for _, col := range p.ordCols {
		if col != 0 {
			cs.Add(int(col.ID()))
		}
	}
	return cs
}

func (o *OrderingChoice) splitCols() (last, remaining opt.ColSet) {
	start, ok := o.groups.Next(0)
	if !ok {
		panic("splitCols should only be called when there's at least one group")
	}

	for {
		var end int
		end, ok = o.groups.Next(start + 1)
		if !ok {
			end = len(o.ordCols)
		}

		for _, col := range o.ordCols[start:end] {
			if ok {
				remaining.Add(int(col))
			} else {
				last.Add(int(col))
			}
		}

		if !ok {
			return last, remaining
		}
	}
}

type OrderingIterator struct {
	Choices   *OrderingChoice
	index     int
	group     *equivGroup
}

func (it *OrderingIterator) EquivCols() opt.ColSet {
	return it.group.cols
}

func (it *OrderingIterator) Descending() bool {
	return it.group.desc
}

// TODO(andyk): Add NextEquiv method if/when it's needed.
func (it *OrderingIterator) Next() bool {
	// Don't advance newly initialized iterator.
	if it.group == nil {
		if len(it.Choices.groups) == 0 {
			// No ordering columns.
			return false
		}
	} else {
		next, ok := it.Choices.groups.Next(it.index + 1)
		if !ok {
			return false
		}
		it.index = next
	}

	next, ok := it.Choices.groups.Next(it.index + 1)
	if !ok {
		next = len(it.Choices.ordCols)
	}

	it.cols = opt.ColSet{}
	for _, col := range it.Choices.ordCols[it.index:next] {
		it.cols.Add(int(col))
	}

	return true
}
