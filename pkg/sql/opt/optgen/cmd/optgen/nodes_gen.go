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

package main

import (
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// nodesGen generates the memo expression structs used by the optimizer, as
// well as lookup tables used to implement the ExprView methods.
type nodesGen struct {
	compiled *lang.CompiledExpr
	md       *metadata
	w        io.Writer
}

func (g *nodesGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.md = newMetadata(compiled, "memo")
	g.w = w

	fmt.Fprintf(g.w, "package memo\n\n")

	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "  \"fmt\"\n")
	fmt.Fprintf(g.w, "  \"unsafe\"\n")
	fmt.Fprintf(g.w, "\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/coltypes\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt/props\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/sem/types\"\n")
	fmt.Fprintf(g.w, ")\n\n")

	for _, define := range g.compiled.Defines {
		g.genNodeDef(define)
	}

	g.genMemoizeFuncs()
	g.genAddToGroupFuncs()
	g.genInternFuncs()
}

// genNodeDef generates a node's type definition and its methods.
func (g *nodesGen) genNodeDef(define *lang.DefineExpr) {
	opTyp := g.md.typeOf(define)

	// Generate comment for the node struct.
	generateDefineComments(g.w, define, opTyp.name)

	// Generate the struct and methods.
	if define.Tags.Contains("List") {
		listItemTyp := opTyp.listItemType
		fmt.Fprintf(g.w, "type %s []%s\n\n", opTyp.name, listItemTyp.name)
		fmt.Fprintf(g.w, "var Empty%s = %s{}\n\n", opTyp.name, opTyp.name)
		g.genListNodeFuncs(define)
	} else if define.Tags.Contains("Enforcer") {
		g.genNodeStruct(define)
		g.genEnforcerFuncs(define)
	} else if define.Tags.Contains("Private") {
		g.genPrivateStruct(define)
	} else {
		g.genNodeStruct(define)
		g.genNodeFuncs(define)
	}

	// Generate the node's group struct.
	g.genGroupNodeStruct(define)
}

// genGroupNodeDef generates the group struct definition for a relational node.
func (g *nodesGen) genGroupNodeStruct(define *lang.DefineExpr) {
	if !define.Tags.Contains("Relational") {
		return
	}

	structType := fmt.Sprintf("%sNode", define.Name)
	groupStructType := fmt.Sprintf("%sGroup", unTitle(string(define.Name)))

	// Generate the type definition.
	fmt.Fprintf(g.w, "type %s struct {\n", groupStructType)
	fmt.Fprintf(g.w, "  mem *Memo\n")
	fmt.Fprintf(g.w, "  rel props.Relational\n")
	fmt.Fprintf(g.w, "  phys *props.Physical\n")
	fmt.Fprintf(g.w, "  cst Cost\n")
	fmt.Fprintf(g.w, "  first %s\n", structType)
	fmt.Fprintf(g.w, "}\n\n")
	fmt.Fprintf(g.w, "var _ nodeGroup = &%s{}\n\n", groupStructType)

	// Generate the memo method.
	fmt.Fprintf(g.w, "func (g *%s) memo() *Memo {\n", groupStructType)
	fmt.Fprintf(g.w, "  return g.mem\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the relational method.
	fmt.Fprintf(g.w, "func (g *%s) relational() *props.Relational {\n", groupStructType)
	fmt.Fprintf(g.w, "  return &g.rel\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the physical method.
	fmt.Fprintf(g.w, "func (g *%s) physical() *props.Physical {\n", groupStructType)
	fmt.Fprintf(g.w, "  return g.phys\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the firstNode method.
	fmt.Fprintf(g.w, "func (g *%s) firstNode() RelNode {\n", groupStructType)
	fmt.Fprintf(g.w, "  return &g.first\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the cost method.
	fmt.Fprintf(g.w, "func (g *%s) cost() Cost {\n", groupStructType)
	fmt.Fprintf(g.w, "  return g.cst\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the setBestProps method.
	fmt.Fprintf(g.w, "func (g *%s) setBestProps(physical *props.Physical, cost Cost) {\n", groupStructType)
	fmt.Fprintf(g.w, "  g.phys = physical\n")
	fmt.Fprintf(g.w, "  g.cst = cost\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *nodesGen) genPrivateStruct(define *lang.DefineExpr) {
	privTyp := g.md.typeOf(define)

	fmt.Fprintf(g.w, "type %s struct {\n", privTyp.name)
	for _, field := range define.Fields {
		fmt.Fprintf(g.w, "  %s %s\n", field.Name, g.md.typeOf(field).name)
	}
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *nodesGen) genNodeStruct(define *lang.DefineExpr) {
	opTyp := g.md.typeOf(define)

	fmt.Fprintf(g.w, "type %s struct {\n", opTyp.name)

	// Generate child fields.
	for _, field := range define.Fields {
		// If field's name is "_", then use Go embedding syntax.
		fieldName := g.md.fieldName(field)
		if isEmbeddedField(field) {
			fmt.Fprintf(g.w, "  %s\n", g.md.typeOf(field).name)
		} else {
			fmt.Fprintf(g.w, "  %s %s\n", fieldName, g.md.typeOf(field).name)
		}
	}

	if define.Tags.Contains("Scalar") {
		fmt.Fprintf(g.w, "\n")
		if g.needsDataTypeField(define) {
			fmt.Fprintf(g.w, "  Typ types.T\n")
		}
	} else if define.Tags.Contains("Enforcer") {
		fmt.Fprintf(g.w, "  Input RelNode\n")
		fmt.Fprintf(g.w, "  phys  *props.Physical\n")
		fmt.Fprintf(g.w, "  cst   Cost\n")
	} else {
		fmt.Fprintf(g.w, "\n")
		fmt.Fprintf(g.w, "  grp  nodeGroup\n")
		fmt.Fprintf(g.w, "  next RelNode\n")
	}
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *nodesGen) genNodeFuncs(define *lang.DefineExpr) {
	opTyp := g.md.typeOf(define)
	childFields := g.md.childFields(define)
	privateField := g.md.privateField(define)

	if define.Tags.Contains("Scalar") {
		fmt.Fprintf(g.w, "var _ opt.ScalarExpr = &%s{}\n\n", opTyp.name)
	} else {
		fmt.Fprintf(g.w, "var _ RelNode = &%s{}\n\n", opTyp.name)
	}

	// Generate the Op method.
	fmt.Fprintf(g.w, "func (n *%s) Op() opt.Operator {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return opt.%sOp\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the ChildCount method.
	fmt.Fprintf(g.w, "func (n *%s) ChildCount() int {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return %d\n", len(childFields))
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Child method.
	fmt.Fprintf(g.w, "func (n *%s) Child(nth int) opt.Node {\n", opTyp.name)
	if len(childFields) > 0 {
		fmt.Fprintf(g.w, "  switch nth {\n")
		n := 0
		for _, field := range childFields {
			fieldName := g.md.fieldName(field)

			fmt.Fprintf(g.w, "  case %d:\n", n)
			if g.md.typeOf(field).isPointer {
				fmt.Fprintf(g.w, "    return n.%s\n", fieldName)
			} else {
				fmt.Fprintf(g.w, "    return &n.%s\n", fieldName)
			}
			n++
		}
		fmt.Fprintf(g.w, "  }\n")
	}
	fmt.Fprintf(g.w, "  panic(\"child index out of range\")\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Private method.
	fmt.Fprintf(g.w, "func (n *%s) Private() interface{} {\n", opTyp.name)
	if privateField != nil {
		fieldName := g.md.fieldName(privateField)

		if g.md.typeOf(privateField).isPointer {
			fmt.Fprintf(g.w, "  return n.%s\n", fieldName)
		} else {
			fmt.Fprintf(g.w, "  return &n.%s\n", fieldName)
		}
	} else {
		fmt.Fprintf(g.w, "  return nil\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the String method.
	fmt.Fprintf(g.w, "func (n *%s) String() string {\n", opTyp.name)
	if define.Tags.Contains("Scalar") {
		fmt.Fprintf(g.w, "  f := MakeNodeFmtCtx(NodeFmtHideQualifications, nil)\n")
	} else {
		fmt.Fprintf(g.w, "  f := MakeNodeFmtCtx(NodeFmtHideQualifications, n.Memo())\n")
	}
	fmt.Fprintf(g.w, "  f.FormatNode(n)\n")
	fmt.Fprintf(g.w, "  return f.Buffer.String()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the SetChild method.
	fmt.Fprintf(g.w, "func (n *%s) SetChild(nth int, child opt.Node) {\n", opTyp.name)
	if len(childFields) > 0 {
		fmt.Fprintf(g.w, "  switch nth {\n")
		n := 0
		for _, field := range childFields {
			fieldTyp := g.md.typeOf(field)
			fieldName := g.md.fieldName(field)

			fmt.Fprintf(g.w, "  case %d:\n", n)
			if fieldTyp.isPointer {
				fmt.Fprintf(g.w, "    n.%s = child.(%s)\n", fieldName, fieldTyp.name)
			} else {
				fmt.Fprintf(g.w, "    n.%s = *child.(*%s)\n", fieldName, fieldTyp.name)
			}
			fmt.Fprintf(g.w, "    return\n")
			n++
		}
		fmt.Fprintf(g.w, "  }\n")
	}
	fmt.Fprintf(g.w, "  panic(\"child index out of range\")\n")
	fmt.Fprintf(g.w, "}\n\n")

	if define.Tags.Contains("Scalar") {
		// Generate the DataType method.
		fmt.Fprintf(g.w, "func (n *%s) DataType() types.T {\n", opTyp.name)
		dataType := g.constDataType(define)
		if dataType != "" {
			fmt.Fprintf(g.w, "  return %s\n", dataType)
		} else {
			fmt.Fprintf(g.w, "  return n.Typ\n")
		}
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the ScalarProps method.
		if name := g.scalarPropsFieldName(define); name != "" {
			fmt.Fprintf(g.w, "func (n *%s) ScalarProps(mem *Memo) *props.Scalar {\n", opTyp.name)
			fmt.Fprintf(g.w, "  if !n.scalar.Populated {\n")
			fmt.Fprintf(g.w, "    mem.logPropsBuilder.build%sProps(n, &n.%s)\n", define.Name, name)
			fmt.Fprintf(g.w, "  }\n")
			fmt.Fprintf(g.w, "  return &n.%s\n", name)
			fmt.Fprintf(g.w, "}\n\n")
		}
	} else {
		// Generate the Memo method.
		fmt.Fprintf(g.w, "func (n *%s) Memo() *Memo {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return n.grp.memo()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the Relational method.
		fmt.Fprintf(g.w, "func (n *%s) Relational() *props.Relational {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return n.grp.relational()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the FirstNode method.
		fmt.Fprintf(g.w, "func (n *%s) FirstNode() RelNode {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return n.grp.firstNode()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the NextNode method.
		fmt.Fprintf(g.w, "func (n *%s) NextNode() RelNode {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return n.next\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the Physical method.
		fmt.Fprintf(g.w, "func (n *%s) Physical() *props.Physical {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return n.grp.physical()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the Cost method.
		fmt.Fprintf(g.w, "func (n *%s) Cost() Cost {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return n.grp.cost()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the group method.
		fmt.Fprintf(g.w, "func (n *%s) group() nodeGroup {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return n.grp\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the setNext method.
		fmt.Fprintf(g.w, "func (n *%s) setNext(nd RelNode) {\n", opTyp.name)
		fmt.Fprintf(g.w, "  if n.next != nil {\n")
		fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"node already has its next defined: %%s\", n))\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  n.next = nd\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the setGroup method.
		fmt.Fprintf(g.w, "func (n *%s) setGroup(grp RelNode) {\n", opTyp.name)
		fmt.Fprintf(g.w, "  if n.grp != nil {\n")
		fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"node is already part of a group: %%s\", n))\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  n.grp = grp.group()\n")
		fmt.Fprintf(g.w, "  LastGroupMember(grp).setNext(n)\n")
		fmt.Fprintf(g.w, "}\n\n")
	}
}

func (g *nodesGen) genEnforcerFuncs(define *lang.DefineExpr) {
	opTyp := g.md.typeOf(define)

	// Generate the Op method.
	fmt.Fprintf(g.w, "func (n *%s) Op() opt.Operator {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return opt.%sOp\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the ChildCount method.
	fmt.Fprintf(g.w, "func (n *%s) ChildCount() int {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return 1\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Child method.
	fmt.Fprintf(g.w, "func (n *%s) Child(nth int) opt.Node {\n", opTyp.name)
	fmt.Fprintf(g.w, "  if nth == 0 {\n")
	fmt.Fprintf(g.w, "    return n.Input\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "  panic(\"child index out of range\")\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Private method.
	fmt.Fprintf(g.w, "func (n *%s) Private() interface{} {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the String method.
	fmt.Fprintf(g.w, "func (n *%s) String() string {\n", opTyp.name)
	fmt.Fprintf(g.w, "  f := MakeNodeFmtCtx(NodeFmtHideQualifications, n.Memo())\n")
	fmt.Fprintf(g.w, "  f.FormatNode(n)\n")
	fmt.Fprintf(g.w, "  return f.Buffer.String()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the SetChild method.
	fmt.Fprintf(g.w, "func (n *%s) SetChild(nth int, child opt.Node) {\n", opTyp.name)
	fmt.Fprintf(g.w, "  if nth == 0 {\n")
	fmt.Fprintf(g.w, "    n.Input = child.(RelNode)\n")
	fmt.Fprintf(g.w, "    return\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "  panic(\"child index out of range\")\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Memo method.
	fmt.Fprintf(g.w, "func (n *%s) Memo() *Memo {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return n.Input.Memo()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Relational method.
	fmt.Fprintf(g.w, "func (n *%s) Relational() *props.Relational {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return n.Input.Relational()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the FirstNode method.
	fmt.Fprintf(g.w, "func (n *%s) FirstNode() RelNode {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return n.Input.FirstNode()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the NextNode method.
	fmt.Fprintf(g.w, "func (n *%s) NextNode() RelNode {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Physical method.
	fmt.Fprintf(g.w, "func (n *%s) Physical() *props.Physical {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return n.phys\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Cost method.
	fmt.Fprintf(g.w, "func (n *%s) Cost() Cost {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return n.cst\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the group method.
	fmt.Fprintf(g.w, "func (n *%s) group() nodeGroup {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return n.Input.group()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the setNext method.
	fmt.Fprintf(g.w, "func (n *%s) setNext(nd RelNode) {\n", opTyp.name)
	fmt.Fprintf(g.w, "  panic(\"setNext cannot be called on enforcer nodes\")\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the setGroup method.
	fmt.Fprintf(g.w, "func (n *%s) setGroup(grp nodeGroup) {\n", opTyp.name)
	fmt.Fprintf(g.w, "  panic(\"setGroup cannot be called on enforcer nodes\")\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *nodesGen) genListNodeFuncs(define *lang.DefineExpr) {
	if define.Tags.Contains("Relational") {
		panic("relational list operators are not supported; use scalar list child instead")
	}

	opTyp := g.md.typeOf(define)
	fmt.Fprintf(g.w, "var _ opt.ScalarExpr = &%s{}\n\n", opTyp.name)

	// Generate the Op method.
	fmt.Fprintf(g.w, "func (n *%s) Op() opt.Operator {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return opt.%sOp\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the ChildCount method.
	fmt.Fprintf(g.w, "func (n *%s) ChildCount() int {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return len(*n)\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Child method.
	fmt.Fprintf(g.w, "func (n *%s) Child(nth int) opt.Node {\n", opTyp.name)
	if opTyp.listItemType.isGenerated {
		fmt.Fprintf(g.w, "  return &(*n)[nth]\n")
	} else {
		fmt.Fprintf(g.w, "  return (*n)[nth]\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Private method.
	fmt.Fprintf(g.w, "func (n *%s) Private() interface{} {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the String method.
	fmt.Fprintf(g.w, "func (n *%s) String() string {\n", opTyp.name)
	fmt.Fprintf(g.w, "  f := MakeNodeFmtCtx(NodeFmtHideQualifications, nil)\n")
	fmt.Fprintf(g.w, "  f.FormatNode(n)\n")
	fmt.Fprintf(g.w, "  return f.Buffer.String()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the SetChild method.
	fmt.Fprintf(g.w, "func (n *%s) SetChild(nth int, child opt.Node) {\n", opTyp.name)
	if opTyp.listItemType.isGenerated {
		fmt.Fprintf(g.w, "  (*n)[nth] = *child.(*%sItem)\n", define.Name)
	} else {
		fmt.Fprintf(g.w, "  (*n)[nth] = child.(%s)\n", opTyp.listItemType.name)
	}
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the DataType method.
	fmt.Fprintf(g.w, "func (n *%s) DataType() types.T {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return types.Any\n")
	fmt.Fprintf(g.w, "}\n\n")
}

// genMemoizeFuncs generates Memoize methods on the memo.
func (g *nodesGen) genMemoizeFuncs() {
	defines := g.compiled.Defines.
		WithoutTag("Enforcer").
		WithoutTag("List").
		WithoutTag("ListItem").
		WithoutTag("Private")

	for _, define := range defines {
		opTyp := g.md.typeOf(define)

		fmt.Fprintf(g.w, "func (m *Memo) Memoize%s(\n", define.Name)
		for _, field := range define.Fields {
			fieldTyp := g.md.typeOf(field)
			fieldName := g.md.fieldName(field)
			fmt.Fprintf(g.w, "  %s %s,\n", unTitle(fieldName), fieldTyp.asParam())
		}
		fmt.Fprintf(g.w, ") *%s {\n", opTyp.name)

		if len(define.Fields) == 0 {
			fmt.Fprintf(g.w, "  return &%sSingleton\n", define.Name)
			fmt.Fprintf(g.w, "}\n\n")
			continue
		}

		// Construct a new node and add it to the interning map.
		if define.Tags.Contains("Scalar") {
			fmt.Fprintf(g.w, "  const size = int64(unsafe.Sizeof(%s{}))\n", opTyp.name)
			fmt.Fprintf(g.w, "  nd := &%s{\n", opTyp.name)
		} else {
			groupName := fmt.Sprintf("%sGroup", unTitle(string(define.Name)))
			fmt.Fprintf(g.w, "  const size = int64(unsafe.Sizeof(%s{}))\n", groupName)
			fmt.Fprintf(g.w, "  grp := &%s{mem: m, first: %s{\n", groupName, opTyp.name)
		}

		for _, field := range define.Fields {
			fieldTyp := g.md.typeOf(field)
			fieldName := g.md.fieldName(field)

			if fieldTyp.passByVal {
				fmt.Fprintf(g.w, "   %s: %s,\n", fieldName, unTitle(fieldName))
			} else {
				fmt.Fprintf(g.w, "   %s: *%s,\n", fieldName, unTitle(fieldName))
			}
		}

		if define.Tags.Contains("Scalar") {
			fmt.Fprintf(g.w, "  }\n")
			if g.needsDataTypeField(define) {
				fmt.Fprintf(g.w, "  nd.Typ = InferType(m, nd)\n")
			}
			fmt.Fprintf(g.w, "  interned := m.interner.Intern%s(nd)\n", define.Name)
		} else {
			fmt.Fprintf(g.w, "  }}\n")
			fmt.Fprintf(g.w, "  nd := &grp.first\n")
			fmt.Fprintf(g.w, "  nd.grp = grp\n")
			fmt.Fprintf(g.w, "  m.logPropsBuilder.build%sProps(nd, &grp.rel)\n", define.Name)
			fmt.Fprintf(g.w, "  interned := m.interner.Intern%s(nd)\n", define.Name)
		}

		// Track memo memory usage.
		fmt.Fprintf(g.w, "  if interned == nd {\n")
		fmt.Fprintf(g.w, "    m.memEstimate += size\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  return interned\n")

		fmt.Fprintf(g.w, "}\n\n")
	}
}

// genAddToGroupFuncs generates AddToGroup methods on the memo.
func (g *nodesGen) genAddToGroupFuncs() {
	defines := g.compiled.Defines.WithTag("Relational").WithoutTag("List").WithoutTag("ListItem")
	for _, define := range defines {
		opTyp := g.md.typeOf(define)

		fmt.Fprintf(g.w, "func (m *Memo) Add%sToGroup(nd *%s, grp RelNode) *%s {\n",
			define.Name, opTyp.name, opTyp.name)
		fmt.Fprintf(g.w, "  const size = int64(unsafe.Sizeof(%s{}))\n", opTyp.name)
		fmt.Fprintf(g.w, "  interned := m.interner.Intern%s(nd)\n", define.Name)
		fmt.Fprintf(g.w, "  if interned == nd {\n")
		fmt.Fprintf(g.w, "    nd.setGroup(grp)\n")
		fmt.Fprintf(g.w, "    m.memEstimate += size\n")
		fmt.Fprintf(g.w, "  } else if interned.group() != grp.group() {\n")
		fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"%%s node cannot be added to multiple groups: %%s\", nd.Op(), nd))\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  return interned\n")
		fmt.Fprintf(g.w, "}\n\n")
	}
}

// genInternFuncs generates methods on the interner.
func (g *nodesGen) genInternFuncs() {
	fmt.Fprintf(g.w, "func (in *interner) InternNode(nd opt.Node) opt.Node {\n")
	fmt.Fprintf(g.w, "  switch t := nd.(type) {\n")
	for _, define := range g.compiled.Defines.WithoutTag("Enforcer").WithoutTag("Private") {
		opTyp := g.md.typeOf(define)
		fmt.Fprintf(g.w, "  case *%s:\n", opTyp.name)
		fmt.Fprintf(g.w, "    return in.Intern%s(t)\n", define.Name)
	}
	fmt.Fprintf(g.w, "  default:\n")
	fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"unhandled op: %%s\", nd.Op()))\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "}\n\n")

	for _, define := range g.compiled.Defines.WithoutTag("Enforcer").WithoutTag("Private") {
		opTyp := g.md.typeOf(define)

		fmt.Fprintf(g.w, "func (in *interner) Intern%s(val *%s) *%s {\n",
			define.Name, opTyp.name, opTyp.name)

		fmt.Fprintf(g.w, "  in.hash = offset64\n")

		// Generate code to compute hash.
		fmt.Fprintf(g.w, "  in.hashOperator(opt.%sOp)\n", define.Name)
		if opTyp.listItemType != nil {
			fmt.Fprintf(g.w, "  in.hash%s(*val)\n", title(opTyp.friendlyName))
		} else {
			for _, field := range expandFields(g.compiled, define) {
				fieldName := g.md.fieldName(field)

				if !isExportedField(field) {
					continue
				}
				fieldTyp := g.md.typeOf(field)
				fmt.Fprintf(g.w, "  in.hash%s(val.%s)\n", title(fieldTyp.friendlyName), fieldName)
			}
		}
		fmt.Fprintf(g.w, "\n")

		// Generate code to check for existing item with same hash.
		fmt.Fprintf(g.w, "  for in.lookupItem() {\n")
		fmt.Fprintf(g.w, "    if other, ok := in.item.(*%s); ok {\n", opTyp.name)

		// Generate code to check node equality when there's an existing item.
		first := true
		if opTyp.listItemType != nil {
			first = false
			fmt.Fprintf(g.w, "  if in.is%sEqual(*val, *other)", title(opTyp.friendlyName))
		} else {
			for _, field := range expandFields(g.compiled, define) {
				fieldName := g.md.fieldName(field)

				if !isExportedField(field) {
					continue
				}
				if !first {
					fmt.Fprintf(g.w, " && \n        ")
				} else {
					fmt.Fprintf(g.w, "      if ")
					first = false
				}

				fieldTyp := g.md.typeOf(field)
				fmt.Fprintf(g.w, "in.is%sEqual(val.%s, other.%s)",
					title(fieldTyp.friendlyName), fieldName, fieldName)
			}
		}

		if !first {
			fmt.Fprintf(g.w, " {\n")
			fmt.Fprintf(g.w, "        return other\n")
			fmt.Fprintf(g.w, "      }\n")
		} else {
			// Handle nodes with no children.
			fmt.Fprintf(g.w, "      return other\n")
		}
		fmt.Fprintf(g.w, "    }\n\n")

		// Generate code to handle hash collision.
		fmt.Fprintf(g.w, "    in.handleCollision()\n")
		fmt.Fprintf(g.w, "  }\n\n")

		// Generate code to add node to the cache.
		fmt.Fprintf(g.w, "  in.cache[in.hash] = internedItem{item: val}\n")
		fmt.Fprintf(g.w, "  return val\n")
		fmt.Fprintf(g.w, "}\n\n")
	}
}

func (g *nodesGen) scalarPropsFieldName(define *lang.DefineExpr) string {
	for _, field := range expandFields(g.compiled, define) {
		if field.Type == "ScalarProps" {
			return string(field.Name)
		}
	}
	return ""
}

func (g *nodesGen) needsDataTypeField(define *lang.DefineExpr) bool {
	for _, field := range expandFields(g.compiled, define) {
		if field.Name == "Typ" && field.Type == "DatumType" {
			return false
		}
	}
	return g.constDataType(define) == ""
}

func (g *nodesGen) constDataType(define *lang.DefineExpr) string {
	switch define.Name {
	case "Exists", "Any", "AnyScalar":
		return "types.Bool"
	case "CountRows":
		return "types.Int"
	}
	if define.Tags.Contains("Comparison") || define.Tags.Contains("Boolean") {
		return "types.Bool"
	}
	return ""
}
