package parquet

import (
	"context"
	"fmt"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
	"math"
)

type nestedStructListPlan struct {
	field  memory.Field
	leaves []fixedLeaf
}

func (s *Source) planStructList(g, item *schema.GroupNode, parent string, ancestors nestedAncestorChain, plan *fixedPlan) (memory.Field, error) {
	if ancestors.count >= memory.MaxStructDepth || item.RepetitionType() == arrowparquet.Repetitions.Repeated || item.NumFields() == 0 {
		return memory.Field{}, fmt.Errorf("StructList requires flat nonempty item within depth8")
	}
	if _, ok := item.LogicalType().(schema.NoLogicalType); !ok {
		return memory.Field{}, fmt.Errorf("StructList item must be unannotated Struct")
	}
	group := &nestedStructListPlan{}
	p := int64ListPlan{name: g.Name(), optional: g.RepetitionType() == arrowparquet.Repetitions.Optional, elementOptional: item.RepetitionType() == arrowparquet.Repetitions.Optional}
	def := ancestors.optionalCount() + 1
	if p.optional {
		def++
	}
	if p.elementOptional {
		def++
	}
	children := make([]memory.Field, 0, item.NumFields())
	seen := map[string]bool{}
	for i := 0; i < item.NumFields(); i++ {
		n, ok := item.Field(i).(*schema.PrimitiveNode)
		if !ok || n.RepetitionType() == arrowparquet.Repetitions.Repeated || n.Name() == "" || seen[n.Name()] {
			return memory.Field{}, fmt.Errorf("StructList item requires unique flat children")
		}
		seen[n.Name()] = true
		index := -1
		for j := 0; j < s.meta.Schema.NumColumns(); j++ {
			if s.meta.Schema.Column(j).SchemaNode() == n {
				index = j
				break
			}
		}
		if index < 0 {
			return memory.Field{}, fmt.Errorf("StructList leaf missing")
		}
		desc := s.meta.Schema.Column(index)
		phy := desc.PhysicalType()
		switch t := desc.LogicalType().(type) {
		case schema.NoLogicalType:
		case schema.StringLogicalType:
			if phy != arrowparquet.Types.ByteArray {
				return memory.Field{}, fmt.Errorf("UTF8 requires BYTE_ARRAY")
			}
		case schema.IntLogicalType:
			w := t.BitWidth()
			if !(phy == arrowparquet.Types.Int32 && (w == 8 || w == 16 || w == 32) || phy == arrowparquet.Types.Int64 && w == 64) {
				return memory.Field{}, fmt.Errorf("invalid integer physical pair")
			}
		case schema.DateLogicalType:
			if phy != arrowparquet.Types.Int32 {
				return memory.Field{}, fmt.Errorf("DATE requires INT32")
			}
		case schema.TimestampLogicalType:
			if phy != arrowparquet.Types.Int64 {
				return memory.Field{}, fmt.Errorf("TIMESTAMP requires INT64")
			}
		default:
			return memory.Field{}, fmt.Errorf("unsupported StructList child logical type")
		}
		b, err := bindingAt(s.meta, index, n.Name())
		if err != nil {
			return memory.Field{}, err
		}
		b.Nullable = n.RepetitionType() == arrowparquet.Repetitions.Optional
		switch b.Type {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeDate32, memory.TypeTimestamp, memory.TypeString:
		default:
			return memory.Field{}, fmt.Errorf("unsupported StructList item type")
		}
		max := def
		if b.Nullable {
			max++
		}
		if desc.MaxRepetitionLevel() != 1 || desc.MaxDefinitionLevel() != max {
			return memory.Field{}, fmt.Errorf("invalid StructList leaf levels")
		}
		group.leaves = append(group.leaves, fixedLeaf{binding: b, maxDef: max, childOptional: b.Nullable})
		children = append(children, fieldForBinding(b))
		// Synthetic entries count every physical reader in stream admission; they
		// are removed before ordinary fixed sibling decoding.
		plan.leaves = append(plan.leaves, fixedLeaf{binding: columnBinding{Name: g.Name(), Index: index, Type: memory.TypeList}, parent: parent, ancestors: ancestors})
	}
	f, err := memory.ListStructField(g.Name(), p.optional, p.elementOptional, children...)
	if err != nil {
		return memory.Field{}, err
	}
	group.field = f
	p.index = group.leaves[0].binding.Index
	p.maxDef = def
	s.nestedLists = append(s.nestedLists, nestedListPlan{plan: p, kind: memory.TypeStruct, parent: parent, ancestors: ancestors, group: group})
	return f, nil
}
func (s *Source) structListGroupMetadata(ctx context.Context) (int64, error) {
	var total int64
	for _, p := range s.nestedLists {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if p.group == nil {
			continue
		}
		root := p.parent
		if root == "" {
			root = p.plan.name
		}
		if _, ok := s.schema.Field(root); !ok {
			continue
		}
		// Covers group/candidate descriptors, child tuples/readers, three element
		// Batch/maps/vector sets and child Field arrays; payload charged separately.
		count := int64(len(p.group.leaves))
		if count > (math.MaxInt64-8192)/8192 {
			return 0, fmt.Errorf("StructList metadata overflow")
		}
		n := int64(8192) + count*8192
		if total > math.MaxInt64-n {
			return 0, fmt.Errorf("StructList metadata overflow")
		}
		total += n
	}
	return total, nil
}
