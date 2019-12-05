// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// buildCreateTable constructs a CreateTable operator based on the CREATE TABLE
// statement.
func (b *Builder) buildCreateTable(ct *tree.CreateTable, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	sch, resName := b.resolveSchemaForCreate(&ct.Table)
	// TODO(radu): we are modifying the AST in-place here. We should be storing
	// the resolved name separately.
	ct.Table.TableNamePrefix = resName
	schID := b.factory.Metadata().AddSchema(sch)

	if ct.PartitionBy != nil && ct.PartitionBy.Locality {
		rewritePartitionByLocality(ct)
	}

	if ct.IsReplicated {
		rewriteReplicated(ct)
	}

	// HoistConstraints normalizes any column constraints in the CreateTable AST
	// node.
	ct.HoistConstraints()

	var input memo.RelExpr
	var inputCols physical.Presentation
	if ct.As() {
		// The execution code might need to stringify the query to run it
		// asynchronously. For that we need the data sources to be fully qualified.
		// TODO(radu): this interaction is pretty hacky, investigate moving the
		// generation of the string to the optimizer.
		b.qualifyDataSourceNamesInAST = true
		defer func() {
			b.qualifyDataSourceNamesInAST = false
		}()

		b.pushWithFrame()
		// Build the input query.
		outScope := b.buildStmt(ct.AsSource, nil /* desiredTypes */, inScope)
		b.popWithFrame(outScope)

		numColNames := 0
		for i := 0; i < len(ct.Defs); i++ {
			if _, ok := ct.Defs[i].(*tree.ColumnTableDef); ok {
				numColNames++
			}
		}
		numColumns := len(outScope.cols)
		if numColNames != 0 && numColNames != numColumns {
			panic(sqlbase.NewSyntaxError(fmt.Sprintf(
				"CREATE TABLE specifies %d column name%s, but data source has %d column%s",
				numColNames, util.Pluralize(int64(numColNames)),
				numColumns, util.Pluralize(int64(numColumns)))))
		}

		input = outScope.expr
		if !ct.AsHasUserSpecifiedPrimaryKey() {
			// Synthesize rowid column, and append to end of column list.
			props, overloads := builtins.GetBuiltinProperties("unique_rowid")
			private := &memo.FunctionPrivate{
				Name:       "unique_rowid",
				Typ:        types.Int,
				Properties: props,
				Overload:   &overloads[0],
			}
			fn := b.factory.ConstructFunction(memo.EmptyScalarListExpr, private)
			scopeCol := b.synthesizeColumn(outScope, "rowid", types.Int, nil /* expr */, fn)
			input = b.factory.CustomFuncs().ProjectExtraCol(outScope.expr, fn, scopeCol.id)
		}
		inputCols = outScope.makePhysicalProps().Presentation
	} else {
		// Create dummy empty input.
		input = b.factory.ConstructZeroValues()
	}

	expr := b.factory.ConstructCreateTable(
		input,
		&memo.CreateTablePrivate{
			Schema:    schID,
			InputCols: inputCols,
			Syntax:    ct,
		},
	)
	return &scope{builder: b, expr: expr}
}

func rewritePartitionByLocality(ct *tree.CreateTable) {
	// If region column already exists, do nothing.
	for _, def := range ct.Defs {
		switch t := def.(type) {
		case *tree.ColumnTableDef:
			if t.Name == "region" {
				return
			}
		}
	}

	hoistPrimaryKey(ct)
	ct.PartitionBy = createPartitionBy(&ct.Table, "")

	// Add locality key to all indexes (including primary).
	for _, def := range ct.Defs {
		switch t := def.(type) {
		case *tree.UniqueConstraintTableDef:
			// Inherit partition by from table and prefix with partition key.
			t.PartitionBy = createPartitionBy(&ct.Table, t.Name)
			t.Columns = append([]tree.IndexElem{{Column: "region"}}, t.Columns...)

		case *tree.IndexTableDef:
			// Inherit partition by from table and prefix with partition key.
			t.PartitionBy = createPartitionBy(&ct.Table, t.Name)
			t.Columns = append([]tree.IndexElem{{Column: "region"}}, t.Columns...)
		}
	}

	// Add synthetic region column.
	colDef := &tree.ColumnTableDef{
		Name: "region",
		Type: types.String,
	}
	colDef.DefaultExpr.Expr = &tree.FuncExpr{
		Func:  tree.WrapFunction("crdb_internal.locality_value"),
		Exprs: tree.Exprs{tree.NewStrVal("region")},
	}
	colDef.CheckExprs = []tree.ColumnTableDefCheckExpr{
		{
			Expr: &tree.ComparisonExpr{
				Operator: tree.In,
				Left:     &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"region"}},
				Right:    &tree.Tuple{Exprs: tree.Exprs{tree.NewStrVal("us-east-2"), tree.NewStrVal("eu-west-3"), tree.NewStrVal("ap-northeast-2")}},
			},
		},
	}
	ct.Defs = append(tree.TableDefs{colDef}, ct.Defs...)
}

func rewriteReplicated(ct *tree.CreateTable) {
	hoistPrimaryKey(ct)

	colNames := make(tree.NameList, 0, len(ct.Defs))
	for _, def := range ct.Defs {
		switch t := def.(type) {
		case *tree.ColumnTableDef:
			colNames = append(colNames, t.Name)
		}
	}

	defs := ct.Defs
	for _, region := range []string{"eu-west-3", "ap-northeast-2"} {
		abbr := region[:2]
		for _, def := range defs {
			switch t := def.(type) {
			case *tree.UniqueConstraintTableDef:
				if t.ZoneConfig == nil {
					t.ZoneConfig = createZoneConfig(&ct.Table, t.Name, "", "us-east-2")
				}

				// Copy index, but prefix name with geographic region.
				newIdx := *t

				if t.PrimaryKey {
					storing := make(tree.NameList, 0, len(colNames))
					for _, name := range colNames {
						found := false
						for _, col := range newIdx.Columns {
							if col.Column == name {
								found = true
								break
							}
						}

						if !found {
							storing = append(storing, name)
						}
					}

					newIdx.Name = tree.Name(fmt.Sprintf("%s_primary", abbr))
					newIdx.Storing = storing
					newIdx.PrimaryKey = false
				} else {
					newIdx.Name = tree.Name(fmt.Sprintf("%s_%s", abbr, newIdx.Name))
				}
				newIdx.ZoneConfig = createZoneConfig(&ct.Table, newIdx.Name, "", region)

				ct.Defs = append(ct.Defs, &newIdx)

			case *tree.IndexTableDef:
				if t.ZoneConfig == nil {
					t.ZoneConfig = createZoneConfig(&ct.Table, t.Name, "", "us-east-2")
				}

				// Copy index, but prefix name with geographic region.
				newIdx := *t
				newIdx.Name = tree.Name(fmt.Sprintf("%s_%s", abbr, newIdx.Name))
				newIdx.ZoneConfig = createZoneConfig(&ct.Table, newIdx.Name, "", region)
				ct.Defs = append(ct.Defs, &newIdx)
			}
		}
	}
}

func hoistPrimaryKey(ct *tree.CreateTable) {
	for _, def := range ct.Defs {
		switch t := def.(type) {
		case *tree.ColumnTableDef:
			if t.PrimaryKey {
				t.PrimaryKey = false
				constraintDef := &tree.UniqueConstraintTableDef{
					IndexTableDef: tree.IndexTableDef{Columns: []tree.IndexElem{{Column: t.Name}}},
					PrimaryKey:    true,
				}
				ct.Defs = append(ct.Defs, constraintDef)
				return
			}
		}
	}
}

func createPartitionBy(table *tree.TableName, idxName tree.Name) *tree.PartitionBy {
	partitions := []tree.ListPartition{
		{
			Name:       "US",
			Exprs:      tree.Exprs{tree.NewStrVal("us-east-2")},
			ZoneConfig: createZoneConfig(table, idxName, "US", "us-east-2"),
		},
		{
			Name:       "EUROPE",
			Exprs:      tree.Exprs{tree.NewStrVal("eu-west-3")},
			ZoneConfig: createZoneConfig(table, idxName, "EUROPE", "eu-west-3"),
		},
		{
			Name:       "ASIA",
			Exprs:      tree.Exprs{tree.NewStrVal("ap-northeast-2")},
			ZoneConfig: createZoneConfig(table, idxName, "ASIA", "ap-northeast-2"),
		},
	}
	return &tree.PartitionBy{
		Fields:   tree.NameList{"region"},
		List:     partitions,
		Locality: true,
	}
}

func createZoneConfig(
	table *tree.TableName, idxName, partition tree.Name, region string,
) *tree.SetZoneConfig {
	specifier := tree.ZoneSpecifier{
		TableOrIndex: tree.TableIndexName{
			Table: *table,
			Index: tree.UnrestrictedName(idxName),
		},
		Partition: partition,
	}

	option1 := tree.KVOption{
		Key:   tree.Name("constraints"),
		Value: tree.NewStrVal(fmt.Sprintf("[+region=%s]", region)),
	}

	option2 := tree.KVOption{
		Key:   tree.Name("lease_preferences"),
		Value: tree.NewStrVal(fmt.Sprintf("[[+region=%s]]", region)),
	}

	option3 := tree.KVOption{
		Key:   tree.Name("num_replicas"),
		Value: tree.NewDInt(1),
	}

	return &tree.SetZoneConfig{
		ZoneSpecifier: specifier,
		Options:       tree.KVOptions{option1, option2, option3},
	}
}
