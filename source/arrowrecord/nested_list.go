package arrowrecord

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"gocolumn/memory"
	"reflect"
)

func checkNestedArrowType(dt arrow.DataType, structs int) error {
	if nilArrayDataType(dt) {
		return fmt.Errorf("nil native container datatype")
	}
	switch t := dt.(type) {
	case *arrow.StructType:
		if t == nil || structs >= memory.MaxStructDepth {
			return fmt.Errorf("native Struct depth exceeds limit")
		}
		for _, f := range t.Fields() {
			if err := checkNestedArrowType(f.Type, structs+1); err != nil {
				return err
			}
		}
	case *arrow.ListType:
		if t == nil || nilArrayDataType(t.Elem()) {
			return fmt.Errorf("nil List datatype")
		}
		if inner, ok := t.Elem().(*arrow.ListType); ok {
			if inner == nil || nilArrayDataType(inner.Elem()) || !nativeArrayTerminalType(inner.Elem()) {
				return fmt.Errorf("nested List requires Int64 or String terminal")
			}
			return nil
		}
		return checkStructListArrowType(t, structs)
	}
	return nil
}
func nestedMemoryField(f arrow.Field) (memory.Field, error) {
	if err := checkNestedArrowType(f.Type, 0); err != nil {
		return memory.Field{}, err
	}
	if f.Type.ID() != arrow.LIST && f.Type.ID() != arrow.STRUCT {
		return memoryField(f), nil
	}
	out := mapStructListField(f)
	normalizeRecursiveStringFields(&out)
	s, err := memory.NewSchemaWithNestedListsChecked(out)
	if err != nil {
		return memory.Field{}, err
	}
	return s.Fields[0], nil
}

// arrowArrayNode normalizes both offset axes over retained Arrow buffers.
// Constructor metadata belongs to the retained source; query views are separately admitted.
func arrowArrayNode(a arrow.Array, f memory.Field, depth int) (*memory.ArrayNode, error) {
	return arrowArrayNodeDomain(a, f, depth, nil)
}
func arrowArrayNodeDomain(a arrow.Array, f memory.Field, depth int, ancestor memory.Bitmap) (*memory.ArrayNode, error) {
	if nilArrayValue(a) || depth > memory.MaxNestedListDepth {
		return nil, fmt.Errorf("invalid native array node")
	}
	out := &memory.ArrayNode{Kind: f.Type, Encoding: memory.EncodingView}
	if f.Type == memory.TypeList {
		v, ok := a.(*array.List)
		if !ok || v == nil || depth == memory.MaxNestedListDepth || len(f.Children) != 1 {
			return nil, fmt.Errorf("invalid native List node")
		}
		raw := v.Offsets()
		base := v.Data().Offset()
		if base < 0 || len(raw) < base+v.Len()+1 {
			return nil, fmt.Errorf("short List offsets")
		}
		raw = raw[base : base+v.Len()+1]
		lo, hi := raw[0], raw[len(raw)-1]
		if lo < 0 || hi < lo || int64(hi) > int64(v.ListValues().Len()) {
			return nil, fmt.Errorf("invalid List child range")
		}
		for i, o := range raw {
			if o < lo || o > hi || i > 0 && o < raw[i-1] {
				return nil, fmt.Errorf("invalid List offsets")
			}
		}
		sliced := array.NewSlice(v.ListValues(), int64(lo), int64(hi))
		defer sliced.Release()
		domain := make(memory.Bitmap, memory.WordsFor(int(hi-lo)))
		for r := 0; r < v.Len(); r++ {
			if v.IsValid(r) && (ancestor == nil || memory.IsSet(ancestor, r)) {
				for e := raw[r]; e < raw[r+1]; e++ {
					memory.Set(domain, int(e-lo))
				}
			}
		}
		child, err := arrowArrayNodeDomain(sliced, f.Children[0], depth+1, domain)
		if err != nil {
			return nil, err
		}
		out.Child = child
		out.Offsets = offsets32(raw)
		out.Validity = validityBitmap(v)
		return out, nil
	}
	if depth != memory.MaxNestedListDepth {
		return nil, fmt.Errorf("invalid native terminal depth")
	}
	switch v := a.(type) {
	case *array.Dictionary:
		if v == nil || f.Type != memory.TypeString {
			return nil, fmt.Errorf("invalid dictionary terminal")
		}
		table, ok := v.Dictionary().(*array.String)
		if !ok || table == nil {
			return nil, fmt.Errorf("array dictionary requires String table")
		}
		raw := memory.StringViewVector{Offsets: offsets32(table.ValueOffsets()), Data: table.ValueBytes()}
		if err := raw.Validate(table.Len()); err != nil {
			return nil, err
		}
		if table.NullN() > 0 {
			return nil, fmt.Errorf("array String dictionary with NULL table entries is unsupported")
		}
		if nilArrayValue(v.Indices()) || v.Indices().Len() != v.Len() {
			return nil, fmt.Errorf("invalid dictionary indices")
		}
		text := &memory.StringViewVector{Offsets: make([]uint32, v.Len()+1), Validity: validityBitmap(v), Encoding: memory.EncodingView}
		for r := 0; r < v.Len(); r++ {
			if v.IsValid(r) && (ancestor == nil || memory.IsSet(ancestor, r)) {
				code := v.GetValueIndex(r)
				if code < 0 || code >= table.Len() {
					return nil, fmt.Errorf("array dictionary code out of bounds")
				}
				part := raw.ValueBytes(code)
				if uint64(len(text.Data))+uint64(len(part)) > uint64(^uint32(0)) {
					return nil, fmt.Errorf("array dictionary bytes exceed uint32")
				}
				text.Data = append(text.Data, part...)
			}
			text.Offsets[r+1] = uint32(len(text.Data))
		}
		out.Strings = text
	case *array.Int64:
		if v == nil || f.Type != memory.TypeInt64 {
			return nil, fmt.Errorf("invalid Int64 terminal")
		}
		out.Int64 = &memory.Int64Vector{Values: v.Values(), Validity: validityBitmap(v), Encoding: memory.EncodingView}
	case *array.String:
		if v == nil || f.Type != memory.TypeString {
			return nil, fmt.Errorf("invalid String terminal")
		}
		raw := v.ValueOffsets()
		if len(raw) != v.Len()+1 {
			return nil, fmt.Errorf("short String offsets")
		}
		for i, o := range raw {
			if o < 0 || i > 0 && o < raw[i-1] {
				return nil, fmt.Errorf("invalid String offsets")
			}
		}
		data := v.ValueBytes()
		if int64(raw[len(raw)-1])-int64(raw[0]) != int64(len(data)) {
			return nil, fmt.Errorf("invalid String bytes")
		}
		out.Strings = &memory.StringViewVector{Offsets: offsets32(raw), Data: data, Validity: validityBitmap(v), Encoding: memory.EncodingView}
	default:
		return nil, fmt.Errorf("unsupported native terminal %T", a)
	}
	return out, nil
}

func nilArrayDataType(dt arrow.DataType) bool {
	if dt == nil {
		return true
	}
	v := reflect.ValueOf(dt)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return v.IsNil()
	}
	return false
}

func nativeArrayTerminalType(dt arrow.DataType) bool {
	if nilArrayDataType(dt) {
		return false
	}
	if dt.ID() == arrow.INT64 || dt.ID() == arrow.STRING {
		return true
	}
	d, ok := dt.(*arrow.DictionaryType)
	return ok && d != nil && !nilArrayDataType(d.ValueType) && d.ValueType.ID() == arrow.STRING
}

func nilArrayValue(a arrow.Array) bool {
	if a == nil {
		return true
	}
	v := reflect.ValueOf(a)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return v.IsNil()
	}
	return false
}
