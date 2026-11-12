package physical

import (
	"fmt"
	"gocolumn/join"
	"gocolumn/memory"
	"math"
	"unsafe"
)

// joinBatchMetadataBytes charges schema-sized containers separately from vector
// data. The per-field allowance covers vector descriptors, map entries and map
// growth, plus allocation rounding of small buffers. A struct adds a Batch and
// child map headers. This is query-owned object accounting, not a Go RSS limit.
func joinBatchMetadataBytes(fields []memory.Field) (int64, error) {
	total := int64(4096) // Batch, map headers, local reservation objects and allocator slack
	for _, field := range fields {
		if err := addJoinEstimate(&total, 512); err != nil {
			return 0, err
		}
		if field.Type == memory.TypeList {
			if err := addJoinEstimate(&total, 512); err != nil {
				return 0, err
			}
		}
		if field.Type == memory.TypeStruct {
			children, err := joinBatchMetadataBytes(field.Children)
			if err != nil {
				return 0, err
			}
			// takeRows copies Fields. Bounded output borrows the immutable
			// schema, but sharing the same bound keeps this allowance prepaid.
			if int64(len(field.Children)) > math.MaxInt64/int64(unsafe.Sizeof(memory.Field{})) {
				return 0, fmt.Errorf("join struct metadata overflow")
			}
			if err := addJoinEstimate(&children, int64(len(field.Children))*int64(unsafe.Sizeof(memory.Field{}))); err != nil {
				return 0, err
			}
			if err := addJoinEstimate(&total, children); err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

// joinPlanSource follows exactly the NULL-parent rules of materialization.
func joinPlanSource(s *JoinSource, left *memory.Batch, binding joinOutputBinding, pair join.Pair, child *memory.Field) (*memory.Batch, string, int, bool, error) {
	src, row, ok := joinSourceRow(s, left, binding, pair)
	if !ok {
		return nil, "", 0, false, nil
	}
	if child == nil {
		return src, binding.in, row, true, nil
	}
	parent, err := src.Struct(binding.in)
	if err != nil {
		return nil, "", 0, false, err
	}
	if !parent.IsValid(row) {
		return nil, "", 0, false, nil
	}
	return parent.Values, child.Name, row, true, nil
}

// countJoinVariable counts exact capacities without allocating a plan array.
// Counts must fit both Go slices and the codec's uint32 offset representation.
func countJoinVariable(s *JoinSource, left *memory.Batch, binding joinOutputBinding, pairs []join.Pair, child *memory.Field) (int, int, error) {
	field := binding.field
	if child != nil {
		field = *child
	}
	var elements, bytes int64
	for _, pair := range pairs {
		src, name, row, valid, err := joinPlanSource(s, left, binding, pair, child)
		if err != nil {
			return 0, 0, err
		}
		if !valid {
			continue
		}
		if field.Type == memory.TypeList {
			if len(field.Children) == 1 && field.Children[0].Type == memory.TypeInt64 {
				n, err := joinGatherInt64ListSize(src.Int64Lists[name], []int{row}, nil)
				if err != nil {
					return 0, 0, err
				}
				if err := addJoinEstimate(&elements, int64(n)); err != nil {
					return 0, 0, err
				}
				continue
			}
			v, err := src.StringList(name)
			if err != nil {
				return 0, 0, err
			}
			if !v.IsValid(row) {
				continue
			}
			start, end, ok := v.ValueRange(row)
			if !ok {
				return 0, 0, fmt.Errorf("join list %q has malformed selected offsets", name)
			}
			if err := addJoinEstimate(&elements, int64(end-start)); err != nil {
				return 0, 0, err
			}
			for i := start; i < end; i++ {
				if v.Values.IsValid(i) {
					if err := addJoinEstimate(&bytes, int64(len(v.Values.ValueBytes(i)))); err != nil {
						return 0, 0, err
					}
				}
			}
		} else {
			v, err := src.String(name)
			if err != nil {
				return 0, 0, err
			}
			if v.IsValid(row) {
				if err := addJoinEstimate(&bytes, int64(len(v.ValueBytes(row)))); err != nil {
					return 0, 0, err
				}
			}
		}
	}
	if uint64(len(pairs)) >= math.MaxUint32 || elements >= math.MaxUint32 || bytes > math.MaxUint32 || elements >= math.MaxInt || bytes > math.MaxInt {
		return 0, 0, fmt.Errorf("join variable output exceeds columnar codec representation")
	}
	return int(elements), int(bytes), nil
}

func plannedJoinString(rows, bytes int) *memory.StringViewVector {
	return &memory.StringViewVector{Offsets: make([]uint32, 1, rows+1), Data: make([]byte, 0, bytes), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Encoding: memory.EncodingView}
}

func plannedJoinList(rows, elements, bytes int) *memory.StringListVector {
	return &memory.StringListVector{Offsets: make([]uint32, 1, rows+1), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Values: plannedJoinString(elements, bytes), Encoding: memory.EncodingView}
}

// initializeJoinMaterialization runs only after admission. Append-based nested
// helpers then fill exact capacities, without old/new backing arrays coexisting.
func initializeJoinMaterialization(dst *memory.Batch, s *JoinSource, left *memory.Batch, pairs []join.Pair) error {
	rows := len(pairs)
	for _, binding := range s.bindings {
		field := binding.field
		switch field.Type {
		case memory.TypeString, memory.TypeBinary:
			if field.Encoding == memory.EncodingDictionary {
				continue
			}
			_, bytes, err := countJoinVariable(s, left, binding, pairs, nil)
			if err != nil {
				return err
			}
			dst.Strings[binding.out] = &memory.StringViewVector{Offsets: make([]uint32, rows+1), Data: make([]byte, 0, bytes), Encoding: memory.EncodingView}
		case memory.TypeList:
			elements, bytes, err := countJoinVariable(s, left, binding, pairs, nil)
			if err != nil {
				return err
			}
			if len(field.Children) == 1 && field.Children[0].Type == memory.TypeInt64 {
				dst.Int64Lists[binding.out] = plannedJoinInt64List(rows, elements)
			} else {
				dst.StringLists[binding.out] = plannedJoinList(rows, elements, bytes)
			}
		case memory.TypeStruct:
			v := &memory.StructVector{Fields: field.Children, Values: memory.NewBatch(), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Encoding: memory.EncodingView}
			dst.Structs[binding.out] = v
			for _, child := range field.Children {
				var validity memory.Bitmap
				if child.Type != memory.TypeString && child.Type != memory.TypeList {
					validity = make(memory.Bitmap, memory.WordsFor(rows))
				}
				switch child.Type {
				case memory.TypeInt8:
					v.Values.Int8s[child.Name] = &memory.Int8Vector{Values: make([]int8, 0, rows), Validity: validity, Encoding: child.Encoding}
				case memory.TypeInt16:
					v.Values.Int16s[child.Name] = &memory.Int16Vector{Values: make([]int16, 0, rows), Validity: validity, Encoding: child.Encoding}
				case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
					v.Values.Int32s[child.Name] = &memory.Int32Vector{Values: make([]int32, 0, rows), Validity: validity, Encoding: child.Encoding}
				case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
					v.Values.Int64s[child.Name] = &memory.Int64Vector{Values: make([]int64, 0, rows), Validity: validity, Encoding: child.Encoding}
				case memory.TypeUint8:
					v.Values.Uint8s[child.Name] = &memory.Uint8Vector{Values: make([]uint8, 0, rows), Validity: validity, Encoding: child.Encoding}
				case memory.TypeUint16:
					v.Values.Uint16s[child.Name] = &memory.Uint16Vector{Values: make([]uint16, 0, rows), Validity: validity, Encoding: child.Encoding}
				case memory.TypeUint32, memory.TypeBool:
					v.Values.Uint32s[child.Name] = &memory.Uint32Vector{Values: make([]uint32, 0, rows), Validity: validity, Encoding: child.Encoding}
				case memory.TypeUint64:
					v.Values.Uint64s[child.Name] = &memory.Uint64Vector{Values: make([]uint64, 0, rows), Validity: validity, Encoding: child.Encoding}
				case memory.TypeFloat32:
					v.Values.Float32s[child.Name] = &memory.Float32Vector{Values: make([]float32, 0, rows), Validity: validity, Encoding: child.Encoding}
				case memory.TypeFloat64:
					v.Values.Float64s[child.Name] = &memory.Float64Vector{Values: make([]float64, 0, rows), Validity: validity, Encoding: child.Encoding}

				case memory.TypeString, memory.TypeList:
					elements, bytes, err := countJoinVariable(s, left, binding, pairs, &child)
					if err != nil {
						return err
					}
					if child.Type == memory.TypeString {
						v.Values.Strings[child.Name] = plannedJoinString(rows, bytes)
					} else {
						if len(child.Children) == 1 && child.Children[0].Type == memory.TypeInt64 {
							v.Values.Int64Lists[child.Name] = plannedJoinInt64List(rows, elements)
						} else {
							v.Values.StringLists[child.Name] = plannedJoinList(rows, elements, bytes)
						}
					}
				default:
					return fmt.Errorf("unsupported bounded join struct child %q", child.Name)
				}
			}
		}
	}
	return nil
}
