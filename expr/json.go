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
	result := &memory.StructVector{Fields: append([]memory.Field(nil), fields...), Values: memory.NewBatch(), Encoding: memory.EncodingView}
	for row := 0; row < rows; row++ {
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
			var extra any
			if err := decoder.Decode(&extra); err != io.EOF {
				return fmt.Errorf("JSON_DECODE at row %d: trailing JSON value", row)
			}
			if object == nil {
				parentValid = false
			}
		}
		temp, err := jsonStructRow(fields, object, parentValid, row)
		if err != nil {
			return err
		}
		projected := memory.BatchBytes(result.Values) + memory.BatchBytes(temp.Values) + int64(memory.WordsFor(row+1))*8
		if projected > out.charged && out.reserve != nil {
			if err := out.reserve(projected - out.charged); err != nil {
				return fmt.Errorf("JSON_DECODE at row %d: %w", row, err)
			}
			out.charged = projected
		}
		if err := memory.AppendStructRow(result, temp, 0, parentValid); err != nil {
			return fmt.Errorf("JSON_DECODE at row %d: %w", row, err)
		}
	}
	bytesNeeded := memory.BatchBytes(result.Values) + int64(len(result.Validity))*8
	if bytesNeeded > out.charged && out.reserve != nil {
		if err := out.reserve(bytesNeeded - out.charged); err != nil {
			return err
		}
		out.charged = bytesNeeded
	}
	out.strct = result
	out.validity = result.Validity
	return nil
}

func jsonStructRow(fields []memory.Field, object map[string]json.RawMessage, parentValid bool, row int) (*memory.StructVector, error) {
	v := &memory.StructVector{Rows: 1, Fields: append([]memory.Field(nil), fields...), Values: memory.NewBatch(), Encoding: memory.EncodingView}
	v.Values.Rows = 1
	for _, field := range fields {
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
			vec := &memory.Int32Vector{Values: []int32{int32(value)}, Encoding: field.Encoding}
			if !valid {
				vec.Validity = memory.EnsureBitmap(nil, 1)
			}
			v.Values.Int32s[field.Name] = vec
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
			vec := &memory.Uint32Vector{Values: []uint32{uint32(value)}, Encoding: field.Encoding}
			if !valid {
				vec.Validity = memory.EnsureBitmap(nil, 1)
			}
			v.Values.Uint32s[field.Name] = vec
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
				value, err = strconv.ParseFloat(string(raw), 64)
				if err != nil {
					return nil, jsonFieldError(row, field.Name, fmt.Errorf("expected number"))
				}
				if math.IsInf(value, 0) || (field.Type == memory.TypeFloat32 && math.Abs(value) > math.MaxFloat32) {
					return nil, jsonFieldError(row, field.Name, fmt.Errorf("value overflows %s", field.Type))
				}
			}
			vec := &memory.Float64Vector{Values: []float64{value}, Encoding: field.Encoding}
			if !valid {
				vec.Validity = memory.EnsureBitmap(nil, 1)
			}
			v.Values.Float64s[field.Name] = vec
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
