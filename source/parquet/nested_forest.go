package parquet

import (
	"fmt"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
)

func planNativeForest(s *Source) (*Source, error) {
	root := s.meta.Schema.Root()
	fields := []memory.Field{}
	plan := &fixedPlan{}

	add := func(n schema.Node, parent string, ancestors nestedAncestorChain) (memory.Field, error) {
		if n.RepetitionType() == arrowparquet.Repetitions.Repeated {
			return memory.Field{}, fmt.Errorf("native fixed Struct rejects repeated field %q", n.Name())
		}
		index := -1
		for i := 0; i < s.meta.Schema.NumColumns(); i++ {
			if s.meta.Schema.Column(i).SchemaNode() == n {
				index = i
				break
			}
		}
		if index < 0 {
			return memory.Field{}, fmt.Errorf("native fixed Struct requires primitive field %q", n.Name())
		}
		descriptor := s.meta.Schema.Column(index)
		physical := descriptor.PhysicalType()
		switch logical := descriptor.LogicalType().(type) {
		case schema.DecimalLogicalType:
			if parent != "" || ancestors.count != 0 {
				return memory.Field{}, fmt.Errorf("native forest rejects nested Decimal field %q", n.Name())
			}
			// bindingAt owns exact precision, physical representation and footer encoding checks.
		case schema.StringLogicalType:
			if physical != arrowparquet.Types.ByteArray {
				return memory.Field{}, fmt.Errorf("native UTF8 String requires BYTE_ARRAY for %q", n.Name())
			}
		case schema.NoLogicalType:
		case schema.IntLogicalType:
			width := logical.BitWidth()
			if !(physical == arrowparquet.Types.Int32 && (width == 8 || width == 16 || width == 32) || physical == arrowparquet.Types.Int64 && width == 64) {
				return memory.Field{}, fmt.Errorf("invalid native integer physical/logical pair for %q", n.Name())
			}
		case schema.DateLogicalType:
			if physical != arrowparquet.Types.Int32 {
				return memory.Field{}, fmt.Errorf("DATE requires INT32 for %q", n.Name())
			}
		case schema.TimestampLogicalType:
			if physical != arrowparquet.Types.Int64 {
				return memory.Field{}, fmt.Errorf("TIMESTAMP requires INT64 for %q", n.Name())
			}
		default:
			return memory.Field{}, fmt.Errorf("native fixed Struct rejects logical type %s for %q", descriptor.LogicalType(), n.Name())
		}
		b, e := bindingAt(s.meta, index, n.Name())
		if e != nil {
			return memory.Field{}, e
		}
		switch b.Type {
		case memory.TypeDecimal:
			if parent != "" || ancestors.count != 0 {
				return memory.Field{}, fmt.Errorf("native forest rejects nested Decimal field %q", n.Name())
			}
		case memory.TypeString:
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeDate32, memory.TypeTimestamp:
		default:
			return memory.Field{}, fmt.Errorf("native fixed Struct does not support %s field %q", b.Type, n.Name())
		}
		b.Nullable = n.RepetitionType() == arrowparquet.Repetitions.Optional
		maxDef := ancestors.optionalCount()
		if b.Nullable {
			maxDef++
		}
		d := s.meta.Schema.Column(index)
		if d.MaxRepetitionLevel() != 0 || d.MaxDefinitionLevel() != maxDef {
			return memory.Field{}, fmt.Errorf("invalid fixed field levels for %q", n.Name())
		}
		plan.leaves = append(plan.leaves, fixedLeaf{binding: b, parent: parent, parentOptional: ancestors.optionalCount() > 0, childOptional: b.Nullable, maxDef: maxDef, ancestors: ancestors})
		if b.Type == memory.TypeString {
			s.nestedLists = append(s.nestedLists, nestedListPlan{plan: int64ListPlan{name: n.Name(), index: index, maxDef: maxDef, optional: b.Nullable}, kind: memory.TypeString, parent: parent, parentOptional: ancestors.optionalCount() > 0, ancestors: ancestors, scalar: true})
		}
		return fieldForBinding(b), nil
	}

	addList := func(g *schema.GroupNode, parent string, ancestors nestedAncestorChain) (memory.Field, error) {
		if _, ok := g.LogicalType().(schema.ListLogicalType); !ok {
			return memory.Field{}, fmt.Errorf("nested child must be a standard LIST, not recursive Struct")
		}
		if g.RepetitionType() == arrowparquet.Repetitions.Repeated || g.NumFields() != 1 {
			return memory.Field{}, fmt.Errorf("LIST must be standard three-level shape")
		}
		repeated, ok := g.Field(0).(*schema.GroupNode)
		if !ok || repeated.RepetitionType() != arrowparquet.Repetitions.Repeated || repeated.NumFields() != 1 {
			return memory.Field{}, fmt.Errorf("LIST must contain one repeated group")
		}
		if _, ok := repeated.LogicalType().(schema.NoLogicalType); !ok {
			return memory.Field{}, fmt.Errorf("repeated LIST group cannot be annotated")
		}
		if item, ok := repeated.Field(0).(*schema.GroupNode); ok {
			if _, nested := item.LogicalType().(schema.ListLogicalType); nested {
				return s.planArrayList(g, item, parent, ancestors, plan)
			}
			return s.planStructList(g, item, parent, ancestors, plan)
		}
		element, ok := repeated.Field(0).(*schema.PrimitiveNode)
		if !ok || element.RepetitionType() == arrowparquet.Repetitions.Repeated {
			return memory.Field{}, fmt.Errorf("LIST element must be primitive Int64 or String")
		}
		index := -1
		for j := 0; j < s.meta.Schema.NumColumns(); j++ {
			if s.meta.Schema.Column(j).SchemaNode() == element {
				index = j
				break
			}
		}
		if index < 0 {
			return memory.Field{}, fmt.Errorf("LIST leaf not found")
		}
		d := s.meta.Schema.Column(index)
		kind := memory.TypeInt64
		if d.PhysicalType() == arrowparquet.Types.ByteArray {
			if _, ok := d.LogicalType().(schema.StringLogicalType); !ok {
				return memory.Field{}, fmt.Errorf("String List requires UTF8 annotation")
			}
			kind = memory.TypeString
		} else if d.PhysicalType() == arrowparquet.Types.Int64 {
			switch t := d.LogicalType().(type) {
			case schema.NoLogicalType:
			case schema.IntLogicalType:
				if !t.IsSigned() || t.BitWidth() != 64 {
					return memory.Field{}, fmt.Errorf("Int List requires signed64")
				}
			case schema.TimeLogicalType:
				if parent != "" || ancestors.count != 0 || t.IsAdjustedToUTC() || t.TimeUnit() != schema.TimeUnitNanos {
					return memory.Field{}, fmt.Errorf("LIST TIME requires a top-level single LIST with local TIME(nanos)")
				}
				if err := validateFixedScalarPageEncodings(s.meta, index, "wall-clock list", time64ValueEncodingSupported); err != nil {
					return memory.Field{}, err
				}
				kind = memory.TypeTime64
			default:
				return memory.Field{}, fmt.Errorf("unsupported LIST logical type")
			}
		} else {
			return memory.Field{}, fmt.Errorf("nested LIST element must be Int64 or String")
		}
		p := &int64ListPlan{name: g.Name(), index: index, optional: g.RepetitionType() == arrowparquet.Repetitions.Optional, elementOptional: element.RepetitionType() == arrowparquet.Repetitions.Optional, maxDef: 1}
		if p.optional {
			p.maxDef++
		}
		if p.elementOptional {
			p.maxDef++
		}
		p.maxDef += ancestors.optionalCount()
		if d.MaxRepetitionLevel() != 1 || d.MaxDefinitionLevel() != p.maxDef {
			return memory.Field{}, fmt.Errorf("invalid LIST level thresholds")
		}

		s.nestedLists = append(s.nestedLists, nestedListPlan{plan: *p, kind: kind, parent: parent, parentOptional: ancestors.optionalCount() > 0, ancestors: ancestors})
		plan.leaves = append(plan.leaves, fixedLeaf{binding: columnBinding{Name: p.name, Index: p.index, Type: memory.TypeList}, parent: parent, parentOptional: ancestors.optionalCount() > 0, ancestors: ancestors})
		if kind == memory.TypeString {
			return memory.ListStringField(p.name, p.optional, p.elementOptional), nil
		}
		if kind == memory.TypeTime64 {
			return memory.ListTime64Field(p.name, p.optional, p.elementOptional), nil
		}
		return memory.ListInt64Field(p.name, p.optional, p.elementOptional), nil
	}

	var walk func(schema.Node, string, nestedAncestorChain) (memory.Field, error)
	walk = func(n schema.Node, rootName string, ancestors nestedAncestorChain) (memory.Field, error) {
		g, group := n.(*schema.GroupNode)
		if !group {
			return add(n, rootName, ancestors)
		}
		if _, list := g.LogicalType().(schema.ListLogicalType); list {
			return addList(g, rootName, ancestors)
		}
		if ancestors.count >= memory.MaxStructDepth {
			return memory.Field{}, fmt.Errorf("Struct depth exceeds %d", memory.MaxStructDepth)
		}
		if _, ok := g.LogicalType().(schema.NoLogicalType); !ok || g.RepetitionType() == arrowparquet.Repetitions.Repeated || g.NumFields() == 0 {
			return memory.Field{}, fmt.Errorf("Struct must be nonempty nonrepeated and unannotated")
		}
		if rootName == "" {
			rootName = g.Name()
		}
		optional := g.RepetitionType() == arrowparquet.Repetitions.Optional
		threshold := ancestors.optionalCount()
		if optional {
			threshold++
		}
		id := len(s.structNodes)
		s.structNodes = append(s.structNodes, nestedStructNode{name: g.Name(), root: rootName, parent: ancestors.parentID(), depth: ancestors.count + 1, threshold: threshold, children: g.NumFields()})
		ancestors.ids[ancestors.count] = id
		ancestors.thresholds[ancestors.count] = threshold
		ancestors.count++
		children := make([]memory.Field, 0, g.NumFields())
		names := map[string]bool{}
		for j := 0; j < g.NumFields(); j++ {
			child := g.Field(j)
			if child.Name() == "" || names[child.Name()] {
				return memory.Field{}, fmt.Errorf("duplicate or empty child name")
			}
			names[child.Name()] = true
			f, e := walk(child, rootName, ancestors)
			if e != nil {
				return memory.Field{}, e
			}
			children = append(children, f)
		}
		// StructField copies the direct child slice; NewSchemaChecked below
		// performs the deep snapshot after every descendant has been bounded.
		return memory.StructField(g.Name(), optional, children...), nil
	}
	seen := map[string]bool{}
	for i := 0; i < root.NumFields(); i++ {
		n := root.Field(i)
		if n.Name() == "" || seen[n.Name()] {
			return nil, fmt.Errorf("duplicate or empty top name")
		}
		seen[n.Name()] = true
		f, e := walk(n, "", nestedAncestorChain{})
		if e != nil {
			return nil, e
		}
		fields = append(fields, f)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("NativeNested requires fields")
	}
	s.fixed = plan
	s.nativeForest = true
	var err error
	s.baseSchema, err = memory.NewSchemaWithNestedListsChecked(fields...)
	if err != nil {
		return nil, err
	}
	s.schema = s.baseSchema
	return s, nil
}
