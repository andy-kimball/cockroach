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

package opt

import (
	"context"
	"fmt"
	"math/bits"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// TableID uniquely identifies the usage of a base table within the scope of a
// query. TableID 0 is reserved to mean "unknown table".
//
// Internally, the TableID consists of an index into the Metadata.dataSources
// slice, as well as the ColumnID of the first column in the table. Subsequent
// columns have sequential ids, relative to their ordinal position in the table.
//
// See the comment for Metadata for more details on identifiers.
type TableID uint64

const (
	tableIDMask = 0xffffffff
)

// index returns the index of the table in Metadata.dataSources. It's biased
// by 1, so that TableID 0 can be be reserved to mean "unknown table".
func (t TableID) index() int {
	return int((t>>32)&tableIDMask) - 1
}

// firstColID returns the ColumnID of the first column in the table.
func (t TableID) firstColID() ColumnID {
	return ColumnID(t & tableIDMask)
}

// ColumnID returns the metadata id of the column at the given ordinal position
// in the table.
//
// NOTE: This method does not do bounds checking, so it's up to the caller to
//       ensure that a column really does exist at this ordinal position.
func (t TableID) ColumnID(ord int) ColumnID {
	return t.firstColID() + ColumnID(ord)
}

// makeTableID constructs a new TableID from its component parts.
func makeTableID(index int, firstColID ColumnID) TableID {
	// Bias the data source index by 1.
	return TableID((uint64(index+1) << 32) | uint64(firstColID))
}

// TableAnnID uniquely identifies an annotation on an instance of table
// metadata. A table annotation allows arbitrary values to be cached with table
// metadata, which can be used to avoid recalculating base table properties or
// other information each time it's needed.
//
// To create a TableAnnID, call NewTableAnnID during Go's program
// initialization phase. The returned TableAnnID never clashes with other
// annotations on the same data source. Here is a usage example:
//
//   var myAnnID = NewTableAnnID()
//
//   md.SetTableAnnotation(DataSourceID(1), myAnnID, "foo")
//   ann := md.TableAnnotation(DataSourceID(1), myAnnID)
//
// Currently, the following annotations are in use:
//   - WeakKeys: weak keys derived from a base table
//   - Stats: statistics derived from a base table
//
// To add an additional annotation, increase the value of
// maxTableAnnIDCount and add a call to NewTableAnnID.
type TableAnnID int

// tableAnnIDCount counts the number of times NewTableAnnID is called.
var tableAnnIDCount TableAnnID

// maxTableAnnIDCount is the maximum number of times that
// NewTableAnnID can be called. Calling more than this number of times
// results in a panic. Having a maximum enables a static annotation array to be
// inlined into the metadata table struct.
const maxTableAnnIDCount = 2

// Metadata assigns unique ids to the columns, tables, sequences, and other
// metadata used within the scope of a particular query. Because it is specific
// to one query, the ids tend to be small integers that can be efficiently
// stored and manipulated.
//
// Within a query, every unique column and every projection (that is more than
// just a pass through of a column) is assigned a unique column id.
// Additionally, every separate reference to the same table in the query gets a
// new set of output column ids. Consider the query:
//
//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
//
// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not equivalent
// to `r.y`. In order to achieve this, we need to give these columns different
// ids.
//
// In all cases, the column ids are global to the query. For example, consider
// the query:
//
//   SELECT x FROM a WHERE y > 0
//
// There are 2 columns in the above query: x and y. During name resolution, the
// above query becomes:
//
//   SELECT [0] FROM a WHERE [1] > 0
//   -- [0] -> x
//   -- [1] -> y
type Metadata struct {
	// cols stores information about each metadata column, indexed by ColumnID.
	cols []mdColumn

	// dataSources stores information about each instance of base tables, virtual
	// tables, or other data sources used in the query.
	dataSources []mdDataSource

	// deps stores information about all unique data sources depended on by the
	// query, as well as the privileges required to access those data sources.
	// The map key is the data source so that each data source is referenced at
	// most once. The map value is the union of all required privileges, where
	// each bit is 1 << privilege.Kind so that multiple privileges can be stored.
	deps map[DataSource]uint64
}

// mdDataSource stores information about one of the data sources stored in the
// metadata. This could be a base table, a virtual table, or a sequence.
type mdDataSource struct {
	// ds is a reference to the data source in the catalog.
	ds DataSource

	// alias is an alternate name for the data source to be used for formatting,
	// debugging, EXPLAIN output, etc. If it is "", then ds.Name will be used
	// instead.
	alias string

	// anns annotates table data source metadata with arbitrary data.
	anns [maxTableAnnIDCount]interface{}
}

// mdColumn stores information about one of the columns stored in the metadata,
// including its label and type.
type mdColumn struct {
	// tabID is the identifier of the base table to which this column belongs. If
	// the column was synthesized (i.e. no base table), then the value is set
	// to UnknownTableID.
	tabID TableID

	// alias is the best-effort name of this column. Since the same column can
	// have multiple names, one of those is chosen to be used for pretty-printing
	// and debugging. This might be different than what is stored in the physical
	// properties and is presented to end users.
	alias string

	// typ is the scalar SQL type of this column.
	typ types.T
}

// Init prepares the metadata for use (or reuse).
func (md *Metadata) Init() {
	// Clear the columns and data sources to release memory (this clearing pattern
	// is optimized by Go).
	for i := range md.cols {
		md.cols[i] = mdColumn{}
	}
	for i := range md.dataSources {
		md.dataSources[i] = mdDataSource{}
	}
	md.cols = md.cols[:0]
	md.dataSources = md.dataSources[:0]
	md.deps = nil
}

// AddMetadata initializes the metadata with a copy of the provided metadata.
// This metadata can then be modified independent of the copied metadata.
func (md *Metadata) AddMetadata(from *Metadata) {
	if len(md.cols) != 0 || len(md.dataSources) != 0 || len(md.deps) != 0 {
		panic("AddMetadata not supported when destination metadata is not empty")
	}
	md.cols = append(md.cols, from.cols...)
	md.dataSources = append(md.dataSources, from.dataSources...)
	md.deps = make(map[DataSource]uint64, len(from.deps))
	for ds, privs := range from.deps {
		md.deps[ds] = privs
	}
}

// AddDependency tracks one of the data sources on which the query depends, as
// well as the privilege required to access that data source. If the Memo using
// this metadata is cached, then a call to CheckDependencies can detect if
// changes to schema or permissions on the data source has invalidated the
// cached metadata.
func (md *Metadata) AddDependency(ds DataSource, priv privilege.Kind) {
	if md.deps == nil {
		md.deps = make(map[DataSource]uint64)
	}

	// Use shift operator to store union of privileges required of the data
	// source.
	existing := md.deps[ds]
	md.deps[ds] = existing | (1 << priv)
}

// CheckDependencies resolves each data source on which this metadata depends,
// in order to check that the fully qualified data source names still resolve to
// the same version of the same data source, and that the user still has
// sufficient privileges to access the data source.
func (md *Metadata) CheckDependencies(ctx context.Context, catalog Catalog) bool {
	for dep, privs := range md.deps {
		ds, err := catalog.ResolveDataSource(ctx, dep.Name())
		if err != nil {
			return false
		}
		if dep.ID() != ds.ID() {
			return false
		}
		if dep.Version() != ds.Version() {
			return false
		}

		for privs != 0 {
			// Strip off each privilege bit and make call to CheckPrivilege for it.
			priv := privilege.Kind(bits.TrailingZeros64(privs))
			if err = catalog.CheckPrivilege(ctx, ds, priv); err != nil {
				return false
			}
			privs = privs >> (priv + 1)
		}
	}
	return true
}

// AddColumn assigns a new unique id to a column within the query and records
// its label and type. If the label is empty, a "column<ID>" label is created.
// Note that a metadata column can be added without requiring it to be owned by
// a base table.
func (md *Metadata) AddColumn(alias string, typ types.T) ColumnID {
	if alias == "" {
		alias = fmt.Sprintf("column%d", len(md.cols)+1)
	}
	md.cols = append(md.cols, mdColumn{alias: alias, typ: typ})
	return ColumnID(len(md.cols))
}

// NumColumns returns the count of columns tracked by this Metadata instance.
func (md *Metadata) NumColumns() int {
	return len(md.cols)
}

// ColumnTableID returns the identifier of the base table to which the given
// column belongs. If the column is not associated with a table (e.g. because it
// was synthesized), then ColumnTableID returns zero.
func (md *Metadata) ColumnTableID(id ColumnID) TableID {
	// ColumnID is biased so that 0 is never used (reserved to indicate the
	// unknown column).
	return md.cols[id-1].tabID
}

// ColumnAlias returns the alias of the given column. It is used for pretty-
// printing and debugging.
func (md *Metadata) ColumnAlias(id ColumnID) string {
	// ColumnID is biased so that 0 is never used (reserved to indicate the
	// unknown column).
	return md.cols[id-1].alias
}

// ColumnType returns the SQL scalar type of the given column.
func (md *Metadata) ColumnType(id ColumnID) types.T {
	// ColumnID is biased so that 0 is never used (reserved to indicate the
	// unknown column).
	return md.cols[id-1].typ
}

// ColumnOrdinal returns the ordinal position of the column in its base table.
// It panics if the column has no table because it was synthesized.
func (md *Metadata) ColumnOrdinal(id ColumnID) int {
	tabID := md.cols[id-1].tabID
	if tabID == 0 {
		panic("column was synthesized and has no ordinal position")
	}
	return int(id - tabID.firstColID())
}

// QualifiedColumnAlias returns the column alias, qualified with the table alias
// if either of these conditions is true:
//
//   1. fullyQualify is true
//   2. there's another column in the metadata with the same column name but
//      different table alias
//
// If the column alias is qualified, the table alias is prefixed to it and
// separated by a "." character. The table alias is qualified with catalog/
// schema only if fullyQualify is true.
func (md *Metadata) QualifiedColumnAlias(id ColumnID, fullyQualify bool) string {
	col := &md.cols[id-1]
	if col.tabID == 0 {
		// Column doesn't belong to a table, so no need to qualify it further.
		return col.alias
	}
	tab := md.Table(col.tabID)

	// If a fully qualified alias has not been requested, then only qualify it if
	// it would otherwise be ambiguous.
	var tabAlias string
	if !fullyQualify {
		for i := range md.cols {
			if i == int(id-1) {
				continue
			}

			// If there are two columns with same name, then column is ambiguous.
			otherCol := &md.cols[i]
			if otherCol.alias == col.alias {
				tabAlias = md.TableAlias(col.tabID)
				if otherCol.tabID == 0 {
					fullyQualify = true
				} else {
					// Only qualify if the qualified names are actually different.
					otherTabAlias := md.TableAlias(otherCol.tabID)
					if tabAlias != otherTabAlias {
						fullyQualify = true
					}
				}
			}
		}
	} else {
		// Use the fully qualified name of the table as its "alias".
		tabAlias = tab.Name().FQString()
	}

	if !fullyQualify {
		return col.alias
	}

	var sb strings.Builder
	sb.WriteString(tabAlias)
	sb.WriteRune('.')
	sb.WriteString(col.alias)
	return sb.String()
}

// DataSourceByStableID looks up the data source associated with the given
// StableID (unique across all data sources and stable across queries).
func (md *Metadata) DataSourceByStableID(id StableID) DataSource {
	for _, mdDS := range md.dataSources {
		if mdDS.ds.ID() == id {
			return mdDS.ds
		}
	}
	return nil
}

// AddTable indexes a new reference to a table within the query. Separate
// references to the same table are assigned different table ids (e.g. in a
// self-join query). All columns are added to the metadata. If mutation columns
// are present, they are added after active columns.
func (md *Metadata) AddTable(tab Table) TableID {
	tabID := makeTableID(len(md.dataSources), ColumnID(len(md.cols)+1))
	if md.dataSources == nil {
		md.dataSources = make([]mdDataSource, 0, 4)
	}
	md.dataSources = append(md.dataSources, mdDataSource{ds: tab})

	colCount := tab.ColumnCount()
	if md.cols == nil {
		md.cols = make([]mdColumn, 0, colCount)
	}

	for i := 0; i < colCount; i++ {
		col := tab.Column(i)
		md.cols = append(md.cols, mdColumn{
			tabID: tabID,
			alias: string(col.ColName()),
			typ:   col.DatumType(),
		})
	}

	return tabID
}

func (md *Metadata) SetTableAlias(tabID TableID, alias string) {
	md.dataSources[tabID.index()].alias = alias
}

func (md *Metadata) SetColumnAlias(colID ColumnID, alias string) {
	md.cols[colID].alias = alias
}

// Table looks up the catalog table associated with the given metadata id. The
// same table can be associated with multiple metadata ids.
func (md *Metadata) Table(tabID TableID) Table {
	return md.dataSources[tabID.index()].ds.(Table)
}

// TableIndexColumns returns the set of columns in the given table index.
// TODO(justin): cache this value in the table metadata.
func (md *Metadata) TableIndexColumns(tableID TableID, indexOrdinal int) ColSet {
	tab := md.Table(tableID)
	index := tab.Index(indexOrdinal)

	var indexCols ColSet
	for i, cnt := 0, index.ColumnCount(); i < cnt; i++ {
		ord := index.Column(i).Ordinal
		indexCols.Add(int(tableID.ColumnID(ord)))
	}
	return indexCols
}

// TableAlias returns the alias of the given table. It is used for pretty-
// printing and debugging.
func (md *Metadata) TableAlias(tabID TableID) string {
	// Use the alias field, or the table name if the alias is "".
	mdTab := &md.dataSources[tabID.index()]
	if mdTab.alias != "" {
		return mdTab.alias
	}
	return string(mdTab.ds.Name().TableName)
}

// TableAnnotation returns the given annotation that is associated with the
// given table. If the table has no such annotation, then TableAnnotation
// returns nil.
func (md *Metadata) TableAnnotation(tabID TableID, annID TableAnnID) interface{} {
	return md.dataSources[tabID.index()].anns[annID]
}

// SetTableAnnotation associates the given annotation with the given table. The
// annotation is associated by the given ID, which was allocated by calling
// NewTableAnnID. If an annotation with the ID already exists on the table, then
// it is overwritten.
//
// See the TableAnnID comment for more details and a usage example.
func (md *Metadata) SetTableAnnotation(
	tabID TableID, tabAnnID TableAnnID, ann interface{},
) {
	md.dataSources[tabID.index()].anns[tabAnnID] = ann
}

// NewTableAnnID allocates a unique annotation identifier that is used to
// associate arbitrary data with table metadata. Only up to maxTableAnnIDCount
// annotation ID's can exist in the system. Attempting to exceed the maximum
// results in a panic.
//
// This method is not thread-safe, and therefore should only be called during
// Go's program initialization phase (which uses a single goroutine to init
// variables).
//
// See the TableAnnID comment for more details and a usage example.
func NewTableAnnID() TableAnnID {
	if tableAnnIDCount == maxTableAnnIDCount {
		panic("can't allocate table annotation id; increase maxTableAnnIDCount to allow")
	}
	cnt := tableAnnIDCount
	tableAnnIDCount++
	return cnt
}
