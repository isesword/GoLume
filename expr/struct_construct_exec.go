package expr

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

type structConstructColumn struct {
	elements, bytes int64
	list            listGetView
}

func structConstructOffsets(offsets []uint32, dataBytes, rows int, check func() error) error {
	if rows == 0 && len(offsets) == 0 {
		return nil
	}
	if rows < 0 || len(offsets) <= rows {
		return fmt.Errorf("STRUCT_FROM short string offsets")
	}
	for i := 0; i <= rows; i++ {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if uint64(offsets[i]) > uint64(dataBytes) || i > 0 && offsets[i] < offsets[i-1] {
			return fmt.Errorf("STRUCT_FROM malformed string offset at %d", i)
		}
	}
	return nil
}

func structConstructShape(r *scalarRegister, field memory.Field, rows int, check func() error) (listGetView, error) {
	var list listGetView
	if r.typ != field.Type {
		return list, fmt.Errorf("STRUCT_FROM child storage type mismatch")
	}
	if r.validity != nil && len(r.validity) < memory.WordsFor(rows) {
		return list, fmt.Errorf("STRUCT_FROM short child validity")
	}
	length := rows
	switch field.Type {
	case memory.TypeInt8:
		length = len(r.i8)
	case memory.TypeInt16:
		length = len(r.i16)
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		length = len(r.i32)
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		length = len(r.i64)
	case memory.TypeUint8:
		length = len(r.u8)
	case memory.TypeUint16:
		length = len(r.u16)
	case memory.TypeUint32:
		length = len(r.u32)
	case memory.TypeUint64:
		length = len(r.u64)
	case memory.TypeFloat32:
		length = len(r.f32)
	case memory.TypeFloat64:
		length = len(r.f64)
	case memory.TypeBool:
		if len(r.Truth) < memory.WordsFor(rows) {
			return list, fmt.Errorf("STRUCT_FROM short bool truth")
		}
	case memory.TypeList:
		return validateListGet(r, &field, rows, check)
	case memory.TypeString:
		if r.strView != nil {
			return list, structConstructOffsets(r.strView.Offsets, len(r.strView.Data), rows, check)
		}
		if r.strDict != nil {
			if len(r.strDict.Indices) < rows {
				return list, fmt.Errorf("STRUCT_FROM short dictionary indices")
			}
			return list, structConstructOffsets(r.strDict.Offsets, len(r.strDict.Data), r.strDict.DictionaryLen(), check)
		}
		return list, structConstructOffsets(r.offsets, len(r.data), rows, check)
	default:
		return list, fmt.Errorf("STRUCT_FROM unsupported storage type %s", field.Type)
	}
	if length < rows {
		return list, fmt.Errorf("STRUCT_FROM short child values")
	}
	return list, nil
}

func structConstructValid(r *scalarRegister, row int, active memory.Bitmap) bool {
	return (active == nil || memory.IsSet(active, row)) && isValidRegister(r, row)
}

func preflightStructColumn(r *scalarRegister, field memory.Field, rows int, active memory.Bitmap, check func() error) (structConstructColumn, error) {
	var plan structConstructColumn
	list, err := structConstructShape(r, field, rows, check)
	if err != nil {
		return plan, err
	}
	plan.list = list
	for row := 0; row < rows; row++ {
		if check != nil {
			if err := check(); err != nil {
				return plan, err
			}
		}
		if active != nil && !memory.IsSet(active, row) {
			continue
		}
		valid := isValidRegister(r, row)
		if field.Type == memory.TypeList && list.parentValidity != nil {
			valid = valid && memory.IsSet(list.parentValidity, row)
		}
		if !valid {
			if !field.Nullable {
				return plan, fmt.Errorf("STRUCT_FROM required child %q is NULL at row %d", field.Name, row)
			}
			continue
		}
		switch field.Type {
		case memory.TypeString:
			if r.strDict != nil && uint64(r.strDict.Indices[row]) >= uint64(r.strDict.DictionaryLen()) {
				return plan, fmt.Errorf("STRUCT_FROM dictionary code out of range at row %d", row)
			}
			value, err := registerString(r, row)
			if err != nil {
				return plan, err
			}
			if !addJSONAllocation(&plan.bytes, int64(len(value)), 1) {
				return plan, fmt.Errorf("STRUCT_FROM string size overflow")
			}
		case memory.TypeList:
			lo, hi := int(list.offsets[row]), int(list.offsets[row+1])
			if !addJSONAllocation(&plan.elements, int64(hi-lo), 1) {
				return plan, fmt.Errorf("STRUCT_FROM list size overflow")
			}
			for child := lo; child < hi; child++ {
				if check != nil {
					if err := check(); err != nil {
						return plan, err
					}
				}
				validChild := list.childValidity == nil || memory.IsSet(list.childValidity, child)
				if !validChild {
					if !field.Children[0].Nullable {
						return plan, fmt.Errorf("STRUCT_FROM required list element is NULL at row %d", row)
					}
					continue
				}
				if list.strings != nil {
					if !addJSONAllocation(&plan.bytes, int64(list.strings.Offsets[child+1]-list.strings.Offsets[child]), 1) {
						return plan, fmt.Errorf("STRUCT_FROM list string size overflow")
					}
				}
			}
		}
	}
	if plan.bytes > math.MaxUint32 || plan.bytes > int64(math.MaxInt) || plan.elements > math.MaxUint32 || plan.elements >= int64(math.MaxInt) {
		return plan, fmt.Errorf("STRUCT_FROM output exceeds offset/platform bounds")
	}
	return plan, nil
}

func structConstructBytes(total *int64, field memory.Field, plan structConstructColumn, rows int) bool {
	add := func(n, w int64) bool { return addJSONAllocation(total, n, w) }
	if !add(int64(memory.WordsFor(rows)), 8) {
		return false
	}
	width := int64(8)
	switch field.Type {
	case memory.TypeInt8, memory.TypeUint8:
		width = 1
	case memory.TypeInt16, memory.TypeUint16:
		width = 2
	case memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32, memory.TypeBool, memory.TypeDate32, memory.TypeTime32:
		width = 4
	case memory.TypeString:
		return add(int64(rows)+1, 4) && add(plan.bytes, 1)
	case memory.TypeList:
		if !add(int64(rows)+1, 4) || !add(int64(memory.WordsFor(int(plan.elements))), 8) {
			return false
		}
		if field.Children[0].Type == memory.TypeInt64 {
			return add(plan.elements, 8)
		}
		return add(plan.elements+1, 4) && add(plan.bytes, 1)
	}
	return add(int64(rows), width)
}

func copyStructBytes(dst, src []byte, check func() error) error {
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

func buildStructColumn(dst *memory.Batch, r *scalarRegister, field memory.Field, plan structConstructColumn, rows int, active memory.Bitmap, check func() error) error {
	validity := make(memory.Bitmap, memory.WordsFor(rows))
	valid := func(row int) bool { return structConstructValid(r, row, active) }
	switch field.Type {
	case memory.TypeInt8:
		values := make([]int8, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				values[row] = r.i8[row]
			}
		}
		dst.Int8s[field.Name] = &memory.Int8Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeInt16:
		values := make([]int16, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				values[row] = r.i16[row]
			}
		}
		dst.Int16s[field.Name] = &memory.Int16Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		values := make([]int32, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				values[row] = r.i32[row]
			}
		}
		dst.Int32s[field.Name] = &memory.Int32Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		values := make([]int64, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				values[row] = r.i64[row]
			}
		}
		dst.Int64s[field.Name] = &memory.Int64Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeUint8:
		values := make([]uint8, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				values[row] = r.u8[row]
			}
		}
		dst.Uint8s[field.Name] = &memory.Uint8Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeUint16:
		values := make([]uint16, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				values[row] = r.u16[row]
			}
		}
		dst.Uint16s[field.Name] = &memory.Uint16Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeUint32:
		values := make([]uint32, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				values[row] = r.u32[row]
			}
		}
		dst.Uint32s[field.Name] = &memory.Uint32Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeUint64:
		values := make([]uint64, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				values[row] = r.u64[row]
			}
		}
		dst.Uint64s[field.Name] = &memory.Uint64Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeFloat32:
		values := make([]float32, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				values[row] = r.f32[row]
			}
		}
		dst.Float32s[field.Name] = &memory.Float32Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeFloat64:
		values := make([]float64, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				values[row] = r.f64[row]
			}
		}
		dst.Float64s[field.Name] = &memory.Float64Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeBool:
		values := make([]uint32, rows)
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				if memory.IsSet(r.Truth, row) {
					values[row] = 1
				}
			}
		}
		dst.Uint32s[field.Name] = &memory.Uint32Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case memory.TypeString:
		values := &memory.StringViewVector{Offsets: make([]uint32, rows+1), Data: make([]byte, int(plan.bytes)), Validity: validity, Encoding: memory.EncodingView}
		position := 0
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) {
				memory.Set(validity, row)
				raw, err := registerString(r, row)
				if err != nil {
					return err
				}
				if err := copyStructBytes(values.Data[position:position+len(raw)], raw, check); err != nil {
					return err
				}
				position += len(raw)
			}
			values.Offsets[row+1] = uint32(position)
		}
		dst.Strings[field.Name] = values
	case memory.TypeList:
		offsets := make([]uint32, rows+1)
		elementValidity := make(memory.Bitmap, memory.WordsFor(int(plan.elements)))
		list := plan.list
		position, bytes := 0, 0
		var ints []int64
		var strings *memory.StringViewVector
		if field.Children[0].Type == memory.TypeInt64 {
			ints = make([]int64, int(plan.elements))
		} else {
			strings = &memory.StringViewVector{Offsets: make([]uint32, int(plan.elements)+1), Data: make([]byte, int(plan.bytes)), Validity: elementValidity, Encoding: memory.EncodingView}
		}
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if valid(row) && (list.parentValidity == nil || memory.IsSet(list.parentValidity, row)) {
				memory.Set(validity, row)
				for child := int(list.offsets[row]); child < int(list.offsets[row+1]); child++ {
					if check != nil {
						if err := check(); err != nil {
							return err
						}
					}
					if list.childValidity == nil || memory.IsSet(list.childValidity, child) {
						memory.Set(elementValidity, position)
						if strings == nil {
							ints[position] = list.ints[child]
						} else {
							raw := list.strings.Data[list.strings.Offsets[child]:list.strings.Offsets[child+1]]
							if err := copyStructBytes(strings.Data[bytes:bytes+len(raw)], raw, check); err != nil {
								return err
							}
							bytes += len(raw)
						}
					}
					position++
					if strings != nil {
						strings.Offsets[position] = uint32(bytes)
					}
				}
			}
			offsets[row+1] = uint32(position)
		}
		if strings == nil {
			dst.Int64Lists[field.Name] = &memory.Int64ListVector{Offsets: offsets, Values: &memory.Int64Vector{Values: ints, Validity: elementValidity, Encoding: memory.EncodingPlain}, Validity: validity, Encoding: memory.EncodingView}
		} else {
			dst.StringLists[field.Name] = &memory.StringListVector{Offsets: offsets, Values: strings, Validity: validity, Encoding: memory.EncodingView}
		}
	}
	return nil
}

func evalStructFrom(out *scalarRegister, regs []*scalarRegister, inputs []int, fields []memory.Field, rows int, active memory.Bitmap) error {
	if structConstructExtendedFields(fields) {
		return evalStructFromExtended(out, regs, inputs, fields, rows, active)
	}
	check := out.checkContext
	if rows < 0 || rows == math.MaxInt || len(inputs) != len(fields) {
		return fmt.Errorf("STRUCT_FROM invalid execution shape")
	}
	if active != nil && len(active) < memory.WordsFor(rows) {
		return fmt.Errorf("STRUCT_FROM short active mask")
	}
	scratchBytes := int64(1024)
	if !addJSONAllocation(&scratchBytes, int64(len(fields)), 1024) {
		return fmt.Errorf("STRUCT_FROM scratch size overflow")
	}
	scratch, err := out.acquire(scratchBytes)
	if err != nil {
		return err
	}
	defer scratch.Release()
	plans := make([]structConstructColumn, len(fields))
	outputBytes := int64(2048)
	if !addJSONAllocation(&outputBytes, int64(len(fields)), 1024) || !addJSONAllocation(&outputBytes, int64(memory.WordsFor(rows)), 8) {
		return fmt.Errorf("STRUCT_FROM output size overflow")
	}
	for i, field := range fields {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if inputs[i] < 0 || inputs[i] >= len(regs) {
			return fmt.Errorf("STRUCT_FROM invalid input register")
		}
		plans[i], err = preflightStructColumn(regs[inputs[i]], field, rows, active, check)
		if err != nil {
			return err
		}
		if !structConstructBytes(&outputBytes, field, plans[i], rows) {
			return fmt.Errorf("STRUCT_FROM output size overflow")
		}
	}
	reservation, err := out.acquire(outputBytes)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			reservation.Release()
		}
	}()
	result := &memory.StructVector{Rows: rows, Fields: make([]memory.Field, len(fields)), Values: memory.NewBatch(), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Encoding: memory.EncodingView}
	result.Values.Rows = rows
	for row := 0; row < rows; row++ {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if active == nil || memory.IsSet(active, row) {
			memory.Set(result.Validity, row)
		}
	}
	for i, field := range fields {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		result.Fields[i] = cloneBoundField(field)
		if err := buildStructColumn(result.Values, regs[inputs[i]], field, plans[i], rows, active, check); err != nil {
			return err
		}
	}
	if check != nil {
		if err := check(); err != nil {
			return err
		}
	}
	previous := out.structFromReservation
	out.strct, out.validity = result, result.Validity
	out.structFromReservation, out.structFromBytes = reservation, outputBytes
	if previous != nil {
		previous.Release()
	}
	committed = true
	return nil
}
