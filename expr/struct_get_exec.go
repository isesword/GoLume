package expr

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

func sameStructGetField(a, b memory.Field) bool {
	if a.Name != b.Name || a.Type != b.Type || a.Nullable != b.Nullable || a.Encoding != b.Encoding || a.DictionaryDomain != b.DictionaryDomain || a.ArrowType != b.ArrowType || a.DecimalPrecision != b.DecimalPrecision || a.DecimalScale != b.DecimalScale || len(a.Children) != len(b.Children) {
		return false
	}
	for i := range a.Children {
		if !sameStructGetField(a.Children[i], b.Children[i]) {
			return false
		}
	}
	return true
}

func evalStructGet(out, input *scalarRegister, plan *structGetPlan, rows int, masks ...memory.Bitmap) error {
	check := out.checkContext
	if check != nil {
		if err := check(); err != nil {
			return err
		}
	}
	if plan == nil || input.strct == nil || rows < 0 {
		return fmt.Errorf("STRUCT_GET requires a native Struct vector")
	}
	parent := input.strct
	extended := (memory.Schema{Fields: []memory.Field{plan.parent}}).HasStructLists()
	arrays := (memory.Schema{Fields: []memory.Field{plan.parent}}).HasNestedLists()
	recursive := extended || arrays
	for _, f := range plan.parent.Children {
		if f.Type == memory.TypeStruct {
			recursive = true
			break
		}
	}
	// ValidateWithContext allocates a schema-name map; admit its conservative
	// temporary envelope and the effective parent bitmap before either exists.
	scratchBytes := int64(1024)
	if !addJSONAllocation(&scratchBytes, int64(len(plan.parent.Children)), 512) || !addJSONAllocation(&scratchBytes, int64(memory.WordsFor(rows)), 8) {
		return fmt.Errorf("STRUCT_GET scratch size overflow")
	}
	if recursive {
		nodes, err := recursiveStructGetNodes(plan.parent, check)
		if err != nil {
			return err
		}
		if !addJSONAllocation(&scratchBytes, nodes, 512) {
			return fmt.Errorf("STRUCT_GET recursive scratch overflow")
		}
	}
	if out.acquire != nil {
		lease, err := out.acquire(scratchBytes)
		if err != nil {
			return err
		}
		defer lease.Release()
	}
	if parent.Rows != rows || parent.Values == nil || parent.Values.Rows != rows || len(parent.Fields) != len(plan.parent.Children) {
		return fmt.Errorf("STRUCT_GET parent shape mismatch")
	}
	if parent.Validity != nil && len(parent.Validity) < memory.WordsFor(rows) || input.validity != nil && len(input.validity) < memory.WordsFor(rows) {
		return fmt.Errorf("STRUCT_GET parent validity is too short")
	}
	for i := range parent.Fields {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if !recursive && !sameStructGetField(parent.Fields[i], plan.parent.Children[i]) {
			return fmt.Errorf("STRUCT_GET parent child schema mismatch at index %d", i)
		}
	}
	effective := make(memory.Bitmap, memory.WordsFor(rows))
	var active memory.Bitmap
	if len(masks) > 0 {
		active = masks[0]
	}
	if active != nil && len(active) < memory.WordsFor(rows) {
		return fmt.Errorf("STRUCT_GET active bitmap is too short")
	}
	for row := 0; row < rows; row++ {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if active != nil && !memory.IsSet(active, row) {
			continue
		}
		if !parent.IsValid(row) {
			if !plan.parent.Nullable {
				return fmt.Errorf("STRUCT_GET non-nullable parent contains NULL at row %d", row)
			}
			continue
		}
		if isValidRegister(input, row) {
			memory.Set(effective, row)
		}
	}
	view := *parent
	view.Validity = effective
	field := plan.parent
	field.Nullable = true // inactive rows are not schema violations.
	if field.Name == "" {
		field.Name = "expression"
	}
	validate := view.ValidateWithContext
	if recursive {
		validate = view.ValidateRecursiveWithContext
	}
	if extended {
		validate = view.ValidateStructListsWithContext
	}
	if arrays {
		validate = view.ValidateArrayContainersWithContext
	}
	if field.Name == "" {
		field.Name = "expression"
	}
	if err := validate(field, rows, check); err != nil {
		return fmt.Errorf("STRUCT_GET: %w", err)
	}
	childField := plan.parent.Children[plan.index]
	if memory.IsNestedList(childField) {
		return evalNestedListChild(out, parent.Values.Arrays[childField.Name], childField, rows, effective)
	}
	if memory.IsStructList(childField) {
		return evalStructListChild(out, parent.Values.StructLists[childField.Name], childField, rows, effective)
	}
	if childField.Type == memory.TypeStruct {
		return evalRecursiveStructGet(out, parent.Values.Structs[childField.Name], childField, rows, effective)
	}
	child := scalarRegister{typ: childField.Type}
	if err := bindScalarColumn(&child, parent.Values, childField.Name, childField.Type); err != nil {
		return fmt.Errorf("STRUCT_GET child: %w", err)
	}
	valid := func(row int) bool { return memory.IsSet(effective, row) && isValidRegister(&child, row) }
	var elements, dataBytes int64
	for row := 0; row < rows; row++ {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if !valid(row) {
			continue
		}
		if childField.Type == memory.TypeString {
			raw, err := registerString(&child, row)
			if err != nil {
				return err
			}
			if !addJSONAllocation(&dataBytes, int64(len(raw)), 1) {
				return fmt.Errorf("STRUCT_GET string size overflow")
			}
		} else if childField.Type == memory.TypeList {
			var lo, hi int
			if child.intList != nil {
				lo, hi, _ = child.intList.ValueRange(row)
			} else {
				lo, hi, _ = child.strList.ValueRange(row)
			}
			if !addJSONAllocation(&elements, int64(hi-lo), 1) {
				return fmt.Errorf("STRUCT_GET element count overflow")
			}
			if child.strList != nil {
				for j := lo; j < hi; j++ {
					if check != nil {
						if err := check(); err != nil {
							return err
						}
					}
					if child.strList.Values.IsValid(j) {
						if !addJSONAllocation(&dataBytes, int64(len(child.strList.Values.ValueBytes(j))), 1) {
							return fmt.Errorf("STRUCT_GET string size overflow")
						}
					}
				}
			}
		}
	}
	if dataBytes > math.MaxUint32 || elements > math.MaxUint32 || dataBytes > int64(int(^uint(0)>>1)) || elements >= int64(int(^uint(0)>>1)) {
		return fmt.Errorf("STRUCT_GET output exceeds offset/platform capacity")
	}
	outputBytes := int64(1024)
	add := func(n, width int64) bool { return addJSONAllocation(&outputBytes, n, width) }
	if !add(int64(memory.WordsFor(rows)), 8) {
		return fmt.Errorf("STRUCT_GET output size overflow")
	}
	width := int64(8)
	switch childField.Type {
	case memory.TypeInt8, memory.TypeUint8:
		width = 1
	case memory.TypeInt16, memory.TypeUint16:
		width = 2
	case memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32, memory.TypeDate32, memory.TypeTime32:
		width = 4
	case memory.TypeBool:
		width = 0
		if !add(int64(memory.WordsFor(rows)), 8) {
			return fmt.Errorf("STRUCT_GET bitmap size overflow")
		}
	case memory.TypeString:
		width = 0
		if !add(int64(rows)+1, 4) || !add(dataBytes, 1) {
			return fmt.Errorf("STRUCT_GET string size overflow")
		}
	case memory.TypeList:
		width = 0
		if !add(int64(rows)+1, 4) || !add(int64(memory.WordsFor(int(elements))), 8) {
			return fmt.Errorf("STRUCT_GET list size overflow")
		}
		if child.intList != nil {
			if !add(elements, 8) {
				return fmt.Errorf("STRUCT_GET list size overflow")
			}
		} else if !add(elements+1, 4) || !add(dataBytes, 1) {
			return fmt.Errorf("STRUCT_GET list string size overflow")
		}
	}
	if !add(int64(rows), width) {
		return fmt.Errorf("STRUCT_GET output size overflow")
	}
	var lease memory.Reservation
	if out.acquire != nil {
		var err error
		lease, err = out.acquire(outputBytes)
		if err != nil {
			return err
		}
	}
	committed := false
	defer func() {
		if !committed && lease != nil {
			lease.Release()
		}
	}()
	next := scalarRegister{typ: childField.Type, ownedValidity: make(memory.Bitmap, memory.WordsFor(rows))}
	next.validity = next.ownedValidity
	switch childField.Type {
	case memory.TypeInt8:
		next.i8 = make([]int8, rows)
	case memory.TypeInt16:
		next.i16 = make([]int16, rows)
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		next.i32 = make([]int32, rows)
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		next.i64 = make([]int64, rows)
	case memory.TypeUint8:
		next.u8 = make([]uint8, rows)
	case memory.TypeUint16:
		next.u16 = make([]uint16, rows)
	case memory.TypeUint32:
		next.u32 = make([]uint32, rows)
	case memory.TypeUint64:
		next.u64 = make([]uint64, rows)
	case memory.TypeFloat32:
		next.f32 = make([]float32, rows)
	case memory.TypeFloat64:
		next.f64 = make([]float64, rows)
	case memory.TypeBool:
		next.Truth = make(memory.Bitmap, memory.WordsFor(rows))
	case memory.TypeString:
		next.offsets = make([]uint32, rows+1)
		next.data = make([]byte, int(dataBytes))
	case memory.TypeList:
		if child.intList != nil {
			next.intList = &memory.Int64ListVector{Offsets: make([]uint32, rows+1), Validity: next.validity, Encoding: memory.EncodingView, Values: &memory.Int64Vector{Values: make([]int64, int(elements)), Validity: make(memory.Bitmap, memory.WordsFor(int(elements))), Encoding: memory.EncodingPlain}}
		} else {
			next.strList = &memory.StringListVector{Offsets: make([]uint32, rows+1), Validity: next.validity, Encoding: memory.EncodingView, Values: &memory.StringViewVector{Offsets: make([]uint32, int(elements)+1), Data: make([]byte, int(dataBytes)), Validity: make(memory.Bitmap, memory.WordsFor(int(elements))), Encoding: memory.EncodingView}}
		}
	default:
		return fmt.Errorf("STRUCT_GET unsupported child %s", childField.Type)
	}
	elementAt, byteAt := 0, 0
	copyBytes := func(dst, src []byte) error {
		for len(src) > 0 {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			n := min(len(src), 32<<10)
			copy(dst[:n], src[:n])
			dst, src = dst[n:], src[n:]
		}
		return nil
	}
	for row := 0; row < rows; row++ {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if valid(row) {
			memory.Set(next.validity, row)
			switch childField.Type {
			case memory.TypeInt8:
				next.i8[row] = child.i8[row]
			case memory.TypeInt16:
				next.i16[row] = child.i16[row]
			case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
				next.i32[row] = child.i32[row]
			case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
				next.i64[row] = child.i64[row]
			case memory.TypeUint8:
				next.u8[row] = child.u8[row]
			case memory.TypeUint16:
				next.u16[row] = child.u16[row]
			case memory.TypeUint32:
				next.u32[row] = child.u32[row]
			case memory.TypeUint64:
				next.u64[row] = child.u64[row]
			case memory.TypeFloat32:
				next.f32[row] = child.f32[row]
			case memory.TypeFloat64:
				next.f64[row] = child.f64[row]
			case memory.TypeBool:
				if child.u32[row] != 0 {
					memory.Set(next.Truth, row)
				}
			case memory.TypeString:
				raw, err := registerString(&child, row)
				if err != nil {
					return err
				}
				if err := copyBytes(next.data[byteAt:byteAt+len(raw)], raw); err != nil {
					return err
				}
				byteAt += len(raw)
			case memory.TypeList:
				var lo, hi int
				if child.intList != nil {
					lo, hi, _ = child.intList.ValueRange(row)
				} else {
					lo, hi, _ = child.strList.ValueRange(row)
				}
				for j := lo; j < hi; j++ {
					if check != nil {
						if err := check(); err != nil {
							return err
						}
					}
					if child.intList != nil {
						if child.intList.Values.IsValid(j) {
							next.intList.Values.Values[elementAt] = child.intList.Values.Values[j]
							memory.Set(next.intList.Values.Validity, elementAt)
						}
					} else {
						if child.strList.Values.IsValid(j) {
							raw := child.strList.Values.ValueBytes(j)
							if err := copyBytes(next.strList.Values.Data[byteAt:byteAt+len(raw)], raw); err != nil {
								return err
							}
							byteAt += len(raw)
							memory.Set(next.strList.Values.Validity, elementAt)
						}
						next.strList.Values.Offsets[elementAt+1] = uint32(byteAt)
					}
					elementAt++
				}
			}
		}
		if next.offsets != nil {
			next.offsets[row+1] = uint32(byteAt)
		}
		if next.intList != nil {
			next.intList.Offsets[row+1] = uint32(elementAt)
		}
		if next.strList != nil {
			next.strList.Offsets[row+1] = uint32(elementAt)
		}
	}
	if check != nil {
		if err := check(); err != nil {
			return err
		}
	}
	// Dedicated owner: swap only payload fields, retaining workspace hooks and
	// any separately admitted active-mask buffer. Old payload remains charged
	// through the last possible failure above.
	previous := out.structGetReservation
	out.i8, out.i16, out.i32, out.i64 = next.i8, next.i16, next.i32, next.i64
	out.u8, out.u16, out.u64 = next.u8, next.u16, next.u64
	// Bool's Uint32 output view is admitted separately by outputValue and
	// retained across evaluations; it is not part of the Truth payload lease.
	if childField.Type != memory.TypeBool {
		out.u32 = next.u32
	}
	out.f32, out.f64 = next.f32, next.f64
	out.offsets, out.data = next.offsets, next.data
	out.strView, out.strDict = nil, nil
	out.strList, out.intList = next.strList, next.intList
	out.ownedValidity, out.validity, out.Truth = next.ownedValidity, next.validity, next.Truth
	out.structGetReservation, out.structGetBytes = lease, outputBytes
	if previous != nil {
		previous.Release()
	}
	committed = true
	return nil
}

func recursiveStructGetNodes(f memory.Field, check func() error) (int64, error) {
	if check != nil {
		if err := check(); err != nil {
			return 0, err
		}
	}
	n := int64(1)
	for _, c := range f.Children {
		m, err := recursiveStructGetNodes(c, check)
		if err != nil {
			return 0, err
		}
		if m > math.MaxInt64-n {
			return 0, fmt.Errorf("STRUCT_GET metadata count overflow")
		}
		n += m
	}
	return n, nil
}
