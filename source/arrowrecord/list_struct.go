package arrowrecord

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"gocolumn/memory"
)

func checkStructListArrowType(dt arrow.DataType, depth int) error {
	if dt == nil {
		return fmt.Errorf("nil Arrow datatype")
	}
	switch t := dt.(type) {
	case *arrow.StructType:
		if depth >= memory.MaxStructDepth {
			return fmt.Errorf("Arrow Struct depth exceeds %d", memory.MaxStructDepth)
		}
		for _, c := range t.Fields() {
			if err := checkStructListArrowType(c.Type, depth+1); err != nil {
				return err
			}
		}
	case *arrow.ListType:
		switch e := t.Elem().(type) {
		case *arrow.StructType:
			if depth >= memory.MaxStructDepth {
				return fmt.Errorf("Arrow item Struct depth exceeds limit")
			}
			for _, c := range e.Fields() {
				if c.Type.ID() == arrow.LIST || c.Type.ID() == arrow.STRUCT {
					return fmt.Errorf("StructList item children must be flat")
				}
			}
		default:
			if t.Elem().ID() != arrow.INT64 && t.Elem().ID() != arrow.STRING {
				return fmt.Errorf("unsupported native List element")
			}
		}
	}
	return nil
}
func structListMemoryField(f arrow.Field) (memory.Field, error) {
	if err := checkStructListArrowType(f.Type, 0); err != nil {
		return memory.Field{}, err
	}
	out := mapStructListField(f)
	normalizeRecursiveStringFields(&out)
	if err := out.ValidateStructLists(); err != nil {
		return memory.Field{}, err
	}
	return out, nil
}
func mapStructListField(f arrow.Field) memory.Field {
	switch t := f.Type.(type) {
	case *arrow.StructType:
		out := memory.Field{Name: f.Name, Type: memory.TypeStruct, Nullable: f.Nullable, Encoding: memory.EncodingView, ArrowType: f.Type.String(), Children: make([]memory.Field, len(t.Fields()))}
		for i, c := range t.Fields() {
			out.Children[i] = mapStructListField(c)
		}
		return out
	case *arrow.ListType:
		out := memory.Field{Name: f.Name, Type: memory.TypeList, Nullable: f.Nullable, Encoding: memory.EncodingView, ArrowType: f.Type.String(), Children: []memory.Field{mapStructListField(t.ElemField())}}
		out.Children[0].Name = "item"
		return out
	default:
		return memoryField(f)
	}
}
func arrowStructListVector(a *array.List, f memory.Field, validity, ancestor memory.Bitmap) (*memory.StructListVector, error) {
	if err := f.ValidateListStruct(); err != nil {
		return nil, err
	}
	if a == nil || len(a.Offsets()) < a.Data().Offset()+a.Len()+1 {
		return nil, fmt.Errorf("invalid StructList offsets")
	}
	offsets := a.Offsets()[a.Data().Offset() : a.Data().Offset()+a.Len()+1]
	values, ok := a.ListValues().(*array.Struct)
	if !ok || values == nil {
		return nil, fmt.Errorf("StructList requires Struct values")
	}
	for i, o := range offsets {
		if o < 0 || int(o) > values.Len() || i > 0 && o < offsets[i-1] {
			return nil, fmt.Errorf("invalid StructList offsets")
		}
	}
	lo, hi := int(offsets[0]), int(offsets[len(offsets)-1])
	sliced := array.NewSlice(values, int64(lo), int64(hi))
	defer sliced.Release()
	items := sliced.(*array.Struct)
	parent := validity
	if ancestor != nil {
		parent = make(memory.Bitmap, memory.WordsFor(a.Len()))
		for r := 0; r < a.Len(); r++ {
			if memory.IsSet(ancestor, r) && (validity == nil || memory.IsSet(validity, r)) {
				memory.Set(parent, r)
			}
		}
	}
	elementDomain := make(memory.Bitmap, memory.WordsFor(hi-lo))
	for r := 0; r < a.Len(); r++ {
		if parent == nil || memory.IsSet(parent, r) {
			for e := int(offsets[r]) - lo; e < int(offsets[r+1])-lo; e++ {
				memory.Set(elementDomain, e)
			}
		}
	}
	item, err := arrowRecursiveStructVector(items, f.Children[0], validityBitmap(items), elementDomain)
	if err != nil {
		return nil, err
	}
	out := &memory.StructListVector{Offsets: make([]uint32, a.Len()+1), Validity: parent, Values: item, Encoding: memory.EncodingView}
	for i, o := range offsets {
		out.Offsets[i] = uint32(int(o) - lo)
	}
	return out, nil
}
