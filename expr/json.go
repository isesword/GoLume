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
		temp, err := jsonStructRow(fields, object, parentValid, row, out.checkContext)
		if err != nil {
			return err
		}
		if err := memory.AppendStructRow(result, temp, 0, parentValid); err != nil {
			return fmt.Errorf("JSON_DECODE at row %d: %w", row, err)
		}
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

func jsonStructRow(fields []memory.Field, object map[string]json.RawMessage, parentValid bool, row int, checkContext func() error) (*memory.StructVector, error) {
	v := &memory.StructVector{Rows: 1, Fields: append([]memory.Field(nil), fields...), Values: memory.NewBatch(), Encoding: memory.EncodingView}
	v.Values.Rows = 1
	for _, field := range fields {
		if checkContext != nil {
			if err := checkContext(); err != nil {
				return nil, err
			}
		}
		raw, present := object[field.Name]
		isNull := !present || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
		valid := parentValid && !isNull
		if parentValid && isNull && !field.Nullable {
			return nil, fmt.Errorf("JSON_DECODE at row %d field %q: required value is missing or null", row, field.Name)
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
					return nil, jsonFieldError(row, field.Name, err)
				}
			}
			var validity memory.Bitmap
			if !valid {
				validity = memory.EnsureBitmap(nil, 1)
			}
			switch field.Type {
			case memory.TypeInt8:
				v.Values.Int8s[field.Name] = &memory.Int8Vector{Values: []int8{int8(value)}, Validity: validity, Encoding: field.Encoding}
			case memory.TypeInt16:
				v.Values.Int16s[field.Name] = &memory.Int16Vector{Values: []int16{int16(value)}, Validity: validity, Encoding: field.Encoding}
			default:
				v.Values.Int32s[field.Name] = &memory.Int32Vector{Values: []int32{int32(value)}, Validity: validity, Encoding: field.Encoding}
			}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			value := int64(0)
			var err error
			if valid {
				value, err = jsonSigned(raw)
				if err != nil {
					return nil, jsonFieldError(row, field.Name, err)
				}
			}
			vec := &memory.Int64Vector{Values: []int64{value}, Encoding: field.Encoding}
			if !valid {
				vec.Validity = memory.EnsureBitmap(nil, 1)
			}
			v.Values.Int64s[field.Name] = vec
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
					return nil, jsonFieldError(row, field.Name, err)
				}
			}
			var validity memory.Bitmap
			if !valid {
				validity = memory.EnsureBitmap(nil, 1)
			}
			switch field.Type {
			case memory.TypeUint8:
				v.Values.Uint8s[field.Name] = &memory.Uint8Vector{Values: []uint8{uint8(value)}, Validity: validity, Encoding: field.Encoding}
			case memory.TypeUint16:
				v.Values.Uint16s[field.Name] = &memory.Uint16Vector{Values: []uint16{uint16(value)}, Validity: validity, Encoding: field.Encoding}
			default:
				v.Values.Uint32s[field.Name] = &memory.Uint32Vector{Values: []uint32{uint32(value)}, Validity: validity, Encoding: field.Encoding}
			}
		case memory.TypeUint64:
			value := uint64(0)
			var err error
			if valid {
				value, err = jsonUnsigned(raw)
				if err != nil {
					return nil, jsonFieldError(row, field.Name, err)
				}
			}
			vec := &memory.Uint64Vector{Values: []uint64{value}, Encoding: field.Encoding}
			if !valid {
				vec.Validity = memory.EnsureBitmap(nil, 1)
			}
			v.Values.Uint64s[field.Name] = vec
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
					return nil, jsonFieldError(row, field.Name, fmt.Errorf("value overflows %s", field.Type))
				}
				if err != nil {
					return nil, jsonFieldError(row, field.Name, fmt.Errorf("expected number"))
				}
				if math.IsNaN(value) {
					return nil, jsonFieldError(row, field.Name, fmt.Errorf("value overflows %s", field.Type))
				}
			}
			var validity memory.Bitmap
			if !valid {
				validity = memory.EnsureBitmap(nil, 1)
			}
			if field.Type == memory.TypeFloat32 {
				v.Values.Float32s[field.Name] = &memory.Float32Vector{Values: []float32{float32(value)}, Validity: validity, Encoding: field.Encoding}
			} else {
				v.Values.Float64s[field.Name] = &memory.Float64Vector{Values: []float64{value}, Validity: validity, Encoding: field.Encoding}
			}
		case memory.TypeString:
			value := ""
			var err error
			if valid {
				err = json.Unmarshal(raw, &value)
				if err != nil {
					return nil, jsonFieldError(row, field.Name, fmt.Errorf("expected string"))
				}
			}
			vec := &memory.StringViewVector{Offsets: []uint32{0, uint32(len(value))}, Data: []byte(value), Encoding: memory.EncodingView}
			if !valid {
				vec.Offsets[1] = 0
				vec.Data = nil
				vec.Validity = memory.EnsureBitmap(nil, 1)
			}
			v.Values.Strings[field.Name] = vec
		case memory.TypeList:
			list := &memory.StringListVector{Offsets: []uint32{0, 0}, Values: &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
			if !valid {
				list.Validity = memory.EnsureBitmap(nil, 1)
			} else {
				var elements []json.RawMessage
				if err := json.Unmarshal(raw, &elements); err != nil {
					return nil, jsonFieldError(row, field.Name, fmt.Errorf("expected string array"))
				}
				if uint64(len(elements)) > math.MaxUint32 {
					return nil, jsonFieldError(row, field.Name, fmt.Errorf("element count overflows uint32 offsets"))
				}
				list.Offsets[1] = uint32(len(elements))
				for index, element := range elements {
					if index%256 == 0 && checkContext != nil {
						if err := checkContext(); err != nil {
							return nil, err
						}
					}
					null := bytes.Equal(bytes.TrimSpace(element), []byte("null"))
					if null && !field.Children[0].Nullable {
						return nil, fmt.Errorf("JSON_DECODE at row %d field %q[%d]: null element is not allowed", row, field.Name, index)
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
						return nil, fmt.Errorf("JSON_DECODE at row %d field %q[%d]: expected string", row, field.Name, index)
					}
					if list.Values.Validity != nil {
						memory.Set(list.Values.Validity, index)
					}
					if uint64(len(list.Values.Data))+uint64(len(value)) > math.MaxUint32 {
						return nil, fmt.Errorf("JSON_DECODE at row %d field %q[%d]: string bytes overflow uint32 offsets", row, field.Name, index)
					}
					list.Values.Data = append(list.Values.Data, value...)
					list.Values.Offsets = append(list.Values.Offsets, uint32(len(list.Values.Data)))
				}
			}
			v.Values.StringLists[field.Name] = list
		default:
			return nil, jsonFieldError(row, field.Name, fmt.Errorf("unsupported type %s", field.Type))
		}
	}
	return v, nil
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
