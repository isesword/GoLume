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
	boundaries [memory.MaxRepeatedListDepth]repeatedBoundary
	depth      int
}

func (s *Source) planArrayList(outer, inner *schema.GroupNode, parent string, a nestedAncestorChain, fixed *fixedPlan) (memory.Field, error) {
	var groups [memory.MaxRepeatedListDepth]*schema.GroupNode
	var bounds [memory.MaxRepeatedListDepth]repeatedBoundary
	depth := 0
	g := outer
	def := a.optionalCount()
	var leaf *schema.PrimitiveNode
	for {
		if depth >= memory.MaxRepeatedListDepth || g.RepetitionType() == arrowparquet.Repetitions.Repeated || g.NumFields() != 1 {
			return memory.Field{}, fmt.Errorf("repeated LIST depth/shape unsupported")
		}
		if _, ok := g.LogicalType().(schema.ListLogicalType); !ok {
			return memory.Field{}, fmt.Errorf("repeated axis requires LIST annotation")
		}
		wrapper, ok := g.Field(0).(*schema.GroupNode)
		if !ok || wrapper.RepetitionType() != arrowparquet.Repetitions.Repeated || wrapper.NumFields() != 1 {
			return memory.Field{}, fmt.Errorf("LIST wrapper invalid")
		}
		if _, ok := wrapper.LogicalType().(schema.NoLogicalType); !ok {
			return memory.Field{}, fmt.Errorf("LIST wrapper annotated")
		}
		if g.RepetitionType() == arrowparquet.Repetitions.Optional {
			def++
		}
		groups[depth] = g
		bounds[depth] = repeatedBoundary{present: def, slot: def + 1}
		def++
		depth++
		element := wrapper.Field(0)
		if element.RepetitionType() == arrowparquet.Repetitions.Repeated {
			return memory.Field{}, fmt.Errorf("LIST element cannot be repeated")
		}
		if next, ok := element.(*schema.GroupNode); ok {
			g = next
			continue
		}
		leaf, ok = element.(*schema.PrimitiveNode)
		if !ok {
			return memory.Field{}, fmt.Errorf("LIST terminal must be primitive")
		}
		break
	}
	if depth < 2 {
		return memory.Field{}, fmt.Errorf("array requires repeated LIST axes")
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
	optional := leaf.RepetitionType() == arrowparquet.Repetitions.Optional
	if optional {
		def++
	}
	if int(desc.MaxRepetitionLevel()) != depth || desc.MaxDefinitionLevel() != def {
		return memory.Field{}, fmt.Errorf("LIST definition/repetition mismatch")
	}
	f := memory.Field{Name: "item", Type: kind, Nullable: optional, Encoding: memory.EncodingPlain}
	if kind == memory.TypeString {
		f.Encoding = memory.EncodingView
	}
	for i := depth - 1; i >= 0; i-- {
		name := "item"
		if i == 0 {
			name = outer.Name()
		}
		f = memory.Field{Name: name, Type: memory.TypeList, Nullable: groups[i].RepetitionType() == arrowparquet.Repetitions.Optional, Encoding: memory.EncodingView, Children: []memory.Field{f}}
	}
	if err := f.ValidateNestedList(); err != nil {
		return memory.Field{}, err
	}
	p := int64ListPlan{name: outer.Name(), index: index, optional: f.Nullable, elementOptional: optional, maxDef: def}
	s.nestedLists = append(s.nestedLists, nestedListPlan{plan: p, kind: kind, parent: parent, ancestors: a, array: &nestedArrayPlan{field: f, boundaries: bounds, depth: depth}})
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
