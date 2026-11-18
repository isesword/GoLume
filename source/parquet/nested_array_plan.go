package parquet

import (
	"context"
	"fmt"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
	"math"
)

type repeatedBoundary struct{ present, slot int16 }
type nestedArrayPlan struct {
	field      memory.Field
	boundaries [2]repeatedBoundary
}

func (s *Source) planArrayList(outer, inner *schema.GroupNode, parent string, a nestedAncestorChain, fixed *fixedPlan) (memory.Field, error) {
	if inner.RepetitionType() == arrowparquet.Repetitions.Repeated || inner.NumFields() != 1 {
		return memory.Field{}, fmt.Errorf("inner LIST must be standard three-level")
	}
	wrapper, ok := inner.Field(0).(*schema.GroupNode)
	if !ok || wrapper.RepetitionType() != arrowparquet.Repetitions.Repeated || wrapper.NumFields() != 1 {
		return memory.Field{}, fmt.Errorf("inner LIST repeated wrapper invalid")
	}
	if _, ok = wrapper.LogicalType().(schema.NoLogicalType); !ok {
		return memory.Field{}, fmt.Errorf("inner repeated wrapper annotated")
	}
	leaf, ok := wrapper.Field(0).(*schema.PrimitiveNode)
	if !ok || leaf.RepetitionType() == arrowparquet.Repetitions.Repeated {
		return memory.Field{}, fmt.Errorf("two-level LIST terminal must be primitive")
	}
	index := -1
	for j := 0; j < s.meta.Schema.NumColumns(); j++ {
		if s.meta.Schema.Column(j).SchemaNode() == leaf {
			index = j
			break
		}
	}
	if index < 0 {
		return memory.Field{}, fmt.Errorf("array leaf missing")
	}
	desc := s.meta.Schema.Column(index)
	kind := memory.TypeInt64
	if desc.PhysicalType() == arrowparquet.Types.ByteArray {
		if _, ok := desc.LogicalType().(schema.StringLogicalType); !ok {
			return memory.Field{}, fmt.Errorf("array String needs UTF8")
		}
		kind = memory.TypeString
	} else if desc.PhysicalType() == arrowparquet.Types.Int64 {
		switch t := desc.LogicalType().(type) {
		case schema.NoLogicalType:
		case schema.IntLogicalType:
			if !t.IsSigned() || t.BitWidth() != 64 {
				return memory.Field{}, fmt.Errorf("array requires signed Int64")
			}
		default:
			return memory.Field{}, fmt.Errorf("array Int64 annotation invalid")
		}
	} else {
		return memory.Field{}, fmt.Errorf("array terminal must be Int64 or String")
	}
	o, i, e := outer.RepetitionType() == arrowparquet.Repetitions.Optional, inner.RepetitionType() == arrowparquet.Repetitions.Optional, leaf.RepetitionType() == arrowparquet.Repetitions.Optional
	d := a.optionalCount()
	if o {
		d++
	}
	bounds := [2]repeatedBoundary{{present: d, slot: d + 1}, {present: d + 1, slot: d + 2}}
	if i {
		bounds[1].present++
		bounds[1].slot++
	}
	max := bounds[1].slot
	if e {
		max++
	}
	if desc.MaxRepetitionLevel() != 2 || desc.MaxDefinitionLevel() != max {
		return memory.Field{}, fmt.Errorf("two-level LIST definition/repetition mismatch")
	}
	var item memory.Field
	if kind == memory.TypeInt64 {
		item = memory.ListInt64Field("item", i, e)
	} else {
		item = memory.ListStringField("item", i, e)
	}
	f, err := memory.NestedListField(outer.Name(), o, item)
	if err != nil {
		return memory.Field{}, err
	}
	p := int64ListPlan{name: outer.Name(), index: index, optional: o, elementOptional: e, maxDef: max}
	s.nestedLists = append(s.nestedLists, nestedListPlan{plan: p, kind: kind, parent: parent, ancestors: a, array: &nestedArrayPlan{field: f, boundaries: bounds}})
	fixed.leaves = append(fixed.leaves, fixedLeaf{binding: columnBinding{Name: p.name, Index: index, Type: memory.TypeList}, parent: parent, ancestors: a})
	return f, nil
}
func (s *Source) arrayMetadata(ctx context.Context) (int64, error) {
	var n int64
	for _, p := range s.nestedLists {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if p.array == nil {
			continue
		}
		root := p.plan.name
		if p.parent != "" {
			root = p.parent
		}
		if _, ok := s.schema.Field(root); !ok {
			continue
		}
		if n > math.MaxInt64-16384 {
			return 0, fmt.Errorf("array metadata overflow")
		}
		n += 16384
	}
	return n, nil
}
