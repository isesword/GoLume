package expr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"unicode/utf8"

	"gocolumn/memory"
)

func evalJSONDecode(out, input *scalarRegister, fields []memory.Field, rows int) error {
	// Context checks surround each row and the field/list loops below. The
	// standard library decoder itself is not interruptible within one call.
	outputBytes, scratchBytes, err := jsonAllocationSizes(out, input, len(fields), rows)
	if err != nil {
		return err
	}
	// Reserve the complete new result before constructing it. The previous
	// result remains alive and charged until the successful swap below.
	var outputReservation, scratchReservation memory.Reservation
	committed := false
	defer func() {
		if !committed && outputReservation != nil {
			outputReservation.Release()
		}
	}()
	if out.acquire != nil {
		outputReservation, err = out.acquire(outputBytes)
		if err != nil {
			return err
		}
		scratchReservation, err = out.acquire(scratchBytes)
		if err != nil {
			return err
		}
		defer func() {
			if scratchReservation != nil {
				scratchReservation.Release()
			}
		}()
	}
	// The result schema belongs to this register, never the immutable program.
	// Reuse it across batches under the read-only borrowed-result contract.
	var ownedFields []memory.Field
	if out.strct != nil {
		ownedFields = out.strct.Fields
	} else {
		ownedFields = make([]memory.Field, len(fields))
		for i, field := range fields {
			ownedFields[i] = cloneBoundField(field)
		}
	}
	result := &memory.StructVector{Fields: ownedFields, Values: memory.NewBatch(), Encoding: memory.EncodingView}
	if rows == 0 {
		// With no append, the result still needs schema-complete typed vectors
		// and offset sentinels. Do not synthesize or decode a row: required
		// fields and malformed literals have no value to validate on empty input.
		if err := jsonEmptyChildren(result, out.checkContext); err != nil {
			return err
		}
	}
	// Keep one row of vector metadata for the whole batch. AppendStructRow owns
	// the output payload; the scratch row must never retain variable payloads
	// across rows, which would turn the largest-row scratch allowance into a sum
	// of per-field high-water marks.
	var temp *memory.StructVector
	if rows > 0 {
		temp = &memory.StructVector{Rows: 1, Fields: ownedFields, Values: memory.NewBatch(), Encoding: memory.EncodingView}
		temp.Values.Rows = 1
	}
	for row := 0; row < rows; row++ {
		if out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		parentValid := isValidRegister(input, row)
		var object map[string]json.RawMessage
		if parentValid {
			raw, err := registerString(input, row)
			if err != nil {
				return err
			}
			if !utf8.Valid(raw) {
				return fmt.Errorf("JSON_DECODE at row %d: invalid UTF-8", row)
			}
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if err := decoder.Decode(&object); err != nil {
				return fmt.Errorf("JSON_DECODE at row %d: %w", row, err)
			}
			var extra json.RawMessage
			if err := decoder.Decode(&extra); err != io.EOF {
				return fmt.Errorf("JSON_DECODE at row %d: trailing JSON value", row)
			}
			if object == nil {
				parentValid = false
			}
		}
		if err := jsonStructRow(temp, fields, object, parentValid, row, out.checkContext); err != nil {
			return err
		}
		if err := memory.AppendStructRow(result, temp, 0, parentValid); err != nil {
			return fmt.Errorf("JSON_DECODE at row %d: %w", row, err)
		}
		clearJSONRowPayloads(temp)
	}
	if out.checkContext != nil {
		if err := out.checkContext(); err != nil {
			return err
		}
	}
	previous := out.jsonReservation
	out.strct = result
	out.validity = result.Validity
	out.jsonReservation = outputReservation
	if previous != nil {
		previous.Release()
	}
	committed = true
	return nil
}

// jsonAllocationSizes is a conservative live-allocation policy for the JSON
// implementation, not a measurement of Go heap/RSS. The output allowance covers
// row-aligned vectors, offsets/validity, append capacity, maps, and vector/field
// metadata. Scratch covers the largest row's decoder/raw-message maps, decoded
// strings/list elements, temporary StructVector, and stdlib parser workspace.
// Encoded strings bound their decoded byte lengths; each list element consumes
// encoded bytes. The multipliers deliberately allow geometric capacities and
// small-object overhead. Scratch is returned after decoding, while the output
// allowance stays charged for the lifetime of the result.
func jsonAllocationSizes(out, input *scalarRegister, fieldCount, rows int) (int64, int64, error) {
	if out.checkContext != nil {
		if err := out.checkContext(); err != nil {
			return 0, 0, err
		}
	}
	if rows < 0 || fieldCount < 0 {
		return 0, 0, fmt.Errorf("JSON_DECODE negative shape")
	}
	var total, largest int64
	for row := 0; row < rows; row++ {
		if out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return 0, 0, err
			}
		}
		if !isValidRegister(input, row) {
			continue
		}
		var size int64
		if input.constant {
			// Avoid registerString's []byte conversion before reservation.
			size = int64(len(input.strlit))
		} else {
			raw, err := registerString(input, row)
			if err != nil {
				return 0, 0, err
			}
			size = int64(len(raw))
		}
		if total > math.MaxInt64-size {
			return 0, 0, fmt.Errorf("JSON_DECODE allocation estimate overflows int64")
		}
		total += size
		if size > largest {
			largest = size
		}
	}
	// Express the arithmetic as checked terms, without allocating a term list.
	output := int64(2048)
	if !addJSONAllocation(&output, total, 16) ||
		!addJSONAllocation(&output, int64(fieldCount), 512) {
		return 0, 0, fmt.Errorf("JSON_DECODE allocation estimate overflows int64")
	}
	rowBytes := int64(64)
	if !addJSONAllocation(&rowBytes, int64(fieldCount), 64) ||
		!addJSONAllocation(&output, int64(rows), rowBytes) {
		return 0, 0, fmt.Errorf("JSON_DECODE allocation estimate overflows int64")
	}
	scratch := int64(8192)
	if !addJSONAllocation(&scratch, largest, 128) ||
		!addJSONAllocation(&scratch, int64(fieldCount), 2048) {
		return 0, 0, fmt.Errorf("JSON_DECODE allocation estimate overflows int64")
	}
	return output, scratch, nil
}

func addJSONAllocation(total *int64, count, size int64) bool {
	if count < 0 || size < 0 || (size != 0 && count > (math.MaxInt64-*total)/size) {
		return false
	}
	*total += count * size
	return true
}

func jsonStructRow(v *memory.StructVector, fields []memory.Field, object map[string]json.RawMessage, parentValid bool, row int, checkContext func() error) error {
	for _, field := range fields {
		if checkContext != nil {
			if err := checkContext(); err != nil {
				return err
			}
		}
		raw, present := object[field.Name]
		isNull := !present || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
		valid := parentValid && !isNull
		if parentValid && isNull && !field.Nullable {
			return fmt.Errorf("JSON_DECODE at row %d field %q: required value is missing or null", row, field.Name)
		}
		switch field.Type {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			value := int64(0)
			var err error
			if valid {
				value, err = jsonSigned(raw)
				if err == nil {
					min, max := signed32Range(field.Type)
					if value < min || value > max {
						err = fmt.Errorf("value %d overflows %s", value, field.Type)
					}
				}
				if err != nil {
					return jsonFieldError(row, field.Name, err)
				}
			}
			switch field.Type {
			case memory.TypeInt8:
				vec := v.Values.Int8s[field.Name]
				if vec == nil {
					vec = &memory.Int8Vector{Values: make([]int8, 1), Encoding: field.Encoding}
					v.Values.Int8s[field.Name] = vec
				}
				vec.Values[0] = int8(value)
				vec.Validity = setJSONRowValidity(vec.Validity, valid)
			case memory.TypeInt16:
				vec := v.Values.Int16s[field.Name]
				if vec == nil {
					vec = &memory.Int16Vector{Values: make([]int16, 1), Encoding: field.Encoding}
					v.Values.Int16s[field.Name] = vec
				}
				vec.Values[0] = int16(value)
				vec.Validity = setJSONRowValidity(vec.Validity, valid)
			default:
				vec := v.Values.Int32s[field.Name]
				if vec == nil {
					vec = &memory.Int32Vector{Values: make([]int32, 1), Encoding: field.Encoding}
					v.Values.Int32s[field.Name] = vec
				}
				vec.Values[0] = int32(value)
				vec.Validity = setJSONRowValidity(vec.Validity, valid)
			}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			value := int64(0)
			var err error
			if valid {
				value, err = jsonSigned(raw)
				if err != nil {
					return jsonFieldError(row, field.Name, err)
				}
			}
			vec := v.Values.Int64s[field.Name]
			if vec == nil {
				vec = &memory.Int64Vector{Values: make([]int64, 1), Encoding: field.Encoding}
				v.Values.Int64s[field.Name] = vec
			}
			vec.Values[0] = value
			vec.Validity = setJSONRowValidity(vec.Validity, valid)
		case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeBool:
			value := uint64(0)
			var err error
			if valid {
				if field.Type == memory.TypeBool {
					var b bool
					if err = json.Unmarshal(raw, &b); err == nil && b {
						value = 1
					}
				} else {
					value, err = jsonUnsigned(raw)
				}
				if err == nil && field.Type != memory.TypeBool {
					max := unsigned32Max(field.Type)
					if value > max {
						err = fmt.Errorf("value %d overflows %s", value, field.Type)
					}
				}
				if err != nil {
					return jsonFieldError(row, field.Name, err)
				}
			}
			switch field.Type {
			case memory.TypeUint8:
				vec := v.Values.Uint8s[field.Name]
				if vec == nil {
					vec = &memory.Uint8Vector{Values: make([]uint8, 1), Encoding: field.Encoding}
					v.Values.Uint8s[field.Name] = vec
				}
				vec.Values[0] = uint8(value)
				vec.Validity = setJSONRowValidity(vec.Validity, valid)
			case memory.TypeUint16:
				vec := v.Values.Uint16s[field.Name]
				if vec == nil {
					vec = &memory.Uint16Vector{Values: make([]uint16, 1), Encoding: field.Encoding}
					v.Values.Uint16s[field.Name] = vec
				}
				vec.Values[0] = uint16(value)
				vec.Validity = setJSONRowValidity(vec.Validity, valid)
			default:
				vec := v.Values.Uint32s[field.Name]
				if vec == nil {
					vec = &memory.Uint32Vector{Values: make([]uint32, 1), Encoding: field.Encoding}
					v.Values.Uint32s[field.Name] = vec
				}
				vec.Values[0] = uint32(value)
				vec.Validity = setJSONRowValidity(vec.Validity, valid)
			}
		case memory.TypeUint64:
			value := uint64(0)
			var err error
			if valid {
				value, err = jsonUnsigned(raw)
				if err != nil {
					return jsonFieldError(row, field.Name, err)
				}
			}
			vec := v.Values.Uint64s[field.Name]
			if vec == nil {
				vec = &memory.Uint64Vector{Values: make([]uint64, 1), Encoding: field.Encoding}
				v.Values.Uint64s[field.Name] = vec
			}
			vec.Values[0] = value
			vec.Validity = setJSONRowValidity(vec.Validity, valid)
		case memory.TypeFloat32, memory.TypeFloat64:
			value := float64(0)
			var err error
			if valid {
				bits := 64
				if field.Type == memory.TypeFloat32 {
					bits = 32
				}
				value, err = strconv.ParseFloat(string(raw), bits)
				if math.IsInf(value, 0) {
					return jsonFieldError(row, field.Name, fmt.Errorf("value overflows %s", field.Type))
				}
				if err != nil {
					return jsonFieldError(row, field.Name, fmt.Errorf("expected number"))
				}
				if math.IsNaN(value) {
					return jsonFieldError(row, field.Name, fmt.Errorf("value overflows %s", field.Type))
				}
			}
			if field.Type == memory.TypeFloat32 {
				vec := v.Values.Float32s[field.Name]
				if vec == nil {
					vec = &memory.Float32Vector{Values: make([]float32, 1), Encoding: field.Encoding}
					v.Values.Float32s[field.Name] = vec
				}
				vec.Values[0] = float32(value)
				vec.Validity = setJSONRowValidity(vec.Validity, valid)
			} else {
				vec := v.Values.Float64s[field.Name]
				if vec == nil {
					vec = &memory.Float64Vector{Values: make([]float64, 1), Encoding: field.Encoding}
					v.Values.Float64s[field.Name] = vec
				}
				vec.Values[0] = value
				vec.Validity = setJSONRowValidity(vec.Validity, valid)
			}
		case memory.TypeString:
			value := ""
			var err error
			if valid {
				err = json.Unmarshal(raw, &value)
				if err != nil {
					return jsonFieldError(row, field.Name, fmt.Errorf("expected string"))
				}
			}
			vec := v.Values.Strings[field.Name]
			if vec == nil {
				vec = &memory.StringViewVector{Offsets: make([]uint32, 2), Encoding: memory.EncodingView}
				v.Values.Strings[field.Name] = vec
			}
			vec.Offsets[1] = uint32(len(value))
			vec.Data = []byte(value)
			vec.Validity = setJSONRowValidity(vec.Validity, valid)
		case memory.TypeList:
			if len(field.Children) == 1 && field.Children[0].Type == memory.TypeInt64 {
				if err := decodeJSONInt64List(v.Values, field, raw, valid, row, checkContext); err != nil {
					return err
				}
				continue
			}
			list := v.Values.StringLists[field.Name]
			if list == nil {
				list = &memory.StringListVector{Offsets: make([]uint32, 2), Values: &memory.StringViewVector{Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
				v.Values.StringLists[field.Name] = list
			}
			list.Offsets[1] = 0
			list.Values.Offsets = []uint32{0}
			list.Validity = setJSONRowValidity(list.Validity, valid)
			if valid {
				var elements []json.RawMessage
				if err := json.Unmarshal(raw, &elements); err != nil {
					return jsonFieldError(row, field.Name, fmt.Errorf("expected string array"))
				}
				if uint64(len(elements)) > math.MaxUint32 {
					return jsonFieldError(row, field.Name, fmt.Errorf("element count overflows uint32 offsets"))
				}
				list.Offsets[1] = uint32(len(elements))
				for index, element := range elements {
					if index%256 == 0 && checkContext != nil {
						if err := checkContext(); err != nil {
							return err
						}
					}
					null := bytes.Equal(bytes.TrimSpace(element), []byte("null"))
					if null && !field.Children[0].Nullable {
						return fmt.Errorf("JSON_DECODE at row %d field %q[%d]: null element is not allowed", row, field.Name, index)
					}
					if null {
						if list.Values.Validity == nil {
							list.Values.Validity = memory.EnsureBitmap(nil, len(elements))
							for i := 0; i < index; i++ {
								memory.Set(list.Values.Validity, i)
							}
						}
						list.Values.Offsets = append(list.Values.Offsets, uint32(len(list.Values.Data)))
						continue
					}
					var value string
					if err := json.Unmarshal(element, &value); err != nil {
						return fmt.Errorf("JSON_DECODE at row %d field %q[%d]: expected string", row, field.Name, index)
					}
					if list.Values.Validity != nil {
						memory.Set(list.Values.Validity, index)
					}
					if uint64(len(list.Values.Data))+uint64(len(value)) > math.MaxUint32 {
						return fmt.Errorf("JSON_DECODE at row %d field %q[%d]: string bytes overflow uint32 offsets", row, field.Name, index)
					}
					list.Values.Data = append(list.Values.Data, value...)
					list.Values.Offsets = append(list.Values.Offsets, uint32(len(list.Values.Data)))
				}
			}
		default:
			return jsonFieldError(row, field.Name, fmt.Errorf("unsupported type %s", field.Type))
		}
	}
	return nil
}

func signed32Range(typ memory.DataType) (int64, int64) {
	switch typ {
	case memory.TypeInt8:
		return math.MinInt8, math.MaxInt8
	case memory.TypeInt16:
		return math.MinInt16, math.MaxInt16
	default:
		return math.MinInt32, math.MaxInt32
	}
}

func unsigned32Max(typ memory.DataType) uint64 {
	switch typ {
	case memory.TypeUint8:
		return math.MaxUint8
	case memory.TypeUint16:
		return math.MaxUint16
	default:
		return math.MaxUint32
	}
}

func jsonSigned(raw []byte) (int64, error) {
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		return 0, fmt.Errorf("expected integer")
	}
	value, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("expected integer or value overflows int64")
	}
	return value, nil
}
func jsonUnsigned(raw []byte) (uint64, error) {
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		return 0, fmt.Errorf("expected unsigned integer")
	}
	value, err := strconv.ParseUint(number.String(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("expected unsigned integer or value overflows uint64")
	}
	return value, nil
}
func jsonFieldError(row int, name string, err error) error {
	return fmt.Errorf("JSON_DECODE at row %d field %q: %w", row, name, err)
}

// Allocate validity on the first NULL only, then reuse the one-row bitmap.
func setJSONRowValidity(bits memory.Bitmap, valid bool) memory.Bitmap {
	if bits == nil {
		if valid {
			return nil
		}
		bits = make(memory.Bitmap, 1)
	}
	bits[0] = 0
	if valid {
		bits[0] = 1
	}
	return bits
}

func clearJSONRowPayloads(v *memory.StructVector) {
	for _, vec := range v.Values.Strings {
		vec.Data = nil
	}
	for _, list := range v.Values.Int64Lists {
		list.Values.Values = nil
		list.Values.Validity = nil
	}
	for _, list := range v.Values.StringLists {
		list.Values.Data = nil
		list.Values.Offsets = nil
		list.Values.Validity = nil
	}
}

// jsonEmptyChildren initializes the supported physical children without row
// payloads. It runs only after the output and scratch reservations succeed.
func jsonEmptyChildren(v *memory.StructVector, checkContext func() error) error {
	for _, field := range v.Fields {
		if checkContext != nil {
			if err := checkContext(); err != nil {
				return err
			}
		}
		switch field.Type {
		case memory.TypeInt8:
			v.Values.Int8s[field.Name] = &memory.Int8Vector{Encoding: field.Encoding}
		case memory.TypeInt16:
			v.Values.Int16s[field.Name] = &memory.Int16Vector{Encoding: field.Encoding}
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			v.Values.Int32s[field.Name] = &memory.Int32Vector{Encoding: field.Encoding}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			v.Values.Int64s[field.Name] = &memory.Int64Vector{Encoding: field.Encoding}
		case memory.TypeUint8:
			v.Values.Uint8s[field.Name] = &memory.Uint8Vector{Encoding: field.Encoding}
		case memory.TypeUint16:
			v.Values.Uint16s[field.Name] = &memory.Uint16Vector{Encoding: field.Encoding}
		case memory.TypeUint32, memory.TypeBool:
			v.Values.Uint32s[field.Name] = &memory.Uint32Vector{Encoding: field.Encoding}
		case memory.TypeUint64:
			v.Values.Uint64s[field.Name] = &memory.Uint64Vector{Encoding: field.Encoding}
		case memory.TypeFloat32:
			v.Values.Float32s[field.Name] = &memory.Float32Vector{Encoding: field.Encoding}
		case memory.TypeFloat64:
			v.Values.Float64s[field.Name] = &memory.Float64Vector{Encoding: field.Encoding}
		case memory.TypeString:
			v.Values.Strings[field.Name] = &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
		case memory.TypeList:
			if len(field.Children) == 1 && field.Children[0].Type == memory.TypeInt64 {
				v.Values.Int64Lists[field.Name] = &memory.Int64ListVector{Offsets: []uint32{0}, Values: &memory.Int64Vector{Encoding: memory.EncodingPlain}, Encoding: memory.EncodingView}
				continue
			}
			v.Values.StringLists[field.Name] = &memory.StringListVector{Offsets: []uint32{0}, Values: &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
		default:
			return fmt.Errorf("JSON_DECODE field %q: unsupported type %s", field.Name, field.Type)
		}
	}
	return nil
}
