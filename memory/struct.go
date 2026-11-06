package memory

import (
	"fmt"
	"math"
)

// StructVector stores a nullable parent record and row-aligned child vectors.
// Phase 2.21 permits scalar and List<String> children without recursive Struct.
type StructVector struct {
	Rows     int
	Fields   []Field
	Values   *Batch
	Validity Bitmap
	Encoding Encoding
}

// Len returns the number of parent rows.
func (v *StructVector) Len() int {
	if v == nil {
		return 0
	}
	return v.Rows
}

// IsValid reports whether row is a non-NULL struct.
func (v *StructVector) IsValid(row int) bool {
	return v != nil && row >= 0 && row < v.Rows && (v.Validity == nil || IsSet(v.Validity, row))
}

// Validate checks the parent and supported child vector contracts.
func (v *StructVector) Validate(field Field, rows int) error {
	if err := field.ValidateStruct(); err != nil {
		return err
	}
	if v == nil || v.Rows != rows || v.Values == nil || v.Values.Rows != rows {
		return fmt.Errorf("struct %q rows do not match %d", field.Name, rows)
	}
	if len(v.Validity) != 0 && len(v.Validity) < WordsFor(rows) {
		return fmt.Errorf("struct %q validity is too short", field.Name)
	}
	if !field.Nullable && HasNulls(v.Validity, rows) {
		return fmt.Errorf("non-nullable struct %q contains NULL", field.Name)
	}
	if len(v.Fields) != len(field.Children) {
		return fmt.Errorf("struct %q child schema does not match", field.Name)
	}
	for i := range field.Children {
		if v.Fields[i].Name != field.Children[i].Name || v.Fields[i].Type != field.Children[i].Type {
			return fmt.Errorf("struct %q child schema does not match at index %d", field.Name, i)
		}
	}
	for _, child := range field.Children {
		for row := 0; row < rows; row++ {
			if !v.IsValid(row) {
				continue
			}
			if !child.Nullable && !batchFieldValid(v.Values, child, row) {
				return fmt.Errorf("struct %q non-nullable child %q is NULL at row %d", field.Name, child.Name, row)
			}
		}
		if child.Type == TypeList {
			list, ok := v.Values.StringLists[child.Name]
			if !ok {
				return fmt.Errorf("struct %q child %q is missing", field.Name, child.Name)
			}
			if err := list.Validate(rows); err != nil {
				return err
			}
		} else if !batchHasField(v.Values, child) {
			return fmt.Errorf("struct %q child %q is missing", field.Name, child.Name)
		}
	}
	return nil
}

// TakeStructRows returns an owned gathered struct vector.
func TakeStructRows(src *StructVector, rows []int) (*StructVector, error) {
	if src == nil || src.Values == nil {
		return nil, fmt.Errorf("cannot take nil struct")
	}
	out := &StructVector{Fields: append([]Field(nil), src.Fields...), Values: NewBatch(), Encoding: EncodingView}
	for _, row := range rows {
		if row < 0 || row >= src.Rows {
			return nil, fmt.Errorf("struct row %d out of range", row)
		}
		if err := AppendStructRow(out, src, row, src.IsValid(row)); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// AppendStructRow appends one source row or a synthesized NULL parent.
func AppendStructRow(dst, src *StructVector, row int, parentValid bool) error {
	if dst == nil || src == nil || src.Values == nil {
		return fmt.Errorf("struct append requires vectors")
	}
	if dst.Values == nil {
		dst.Values = NewBatch()
	}
	dstRow := dst.Rows
	dst.Validity = appendValidity(dst.Validity, parentValid, dstRow)
	if len(dst.Fields) == 0 {
		dst.Fields = append([]Field(nil), src.Fields...)
	}
	for _, field := range src.Fields {
		valid := parentValid && batchFieldValid(src.Values, field, row)
		switch field.Type {
		case TypeInt8:
			v := src.Values.Int8s[field.Name]
			out := dst.Values.Int8s[field.Name]
			if out == nil {
				out = &Int8Vector{Encoding: field.Encoding}
			}
			if valid {
				out.Values = append(out.Values, v.Values[row])
			} else {
				out.Values = append(out.Values, 0)
			}
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Int8s[field.Name] = out
		case TypeInt16:
			v := src.Values.Int16s[field.Name]
			out := dst.Values.Int16s[field.Name]
			if out == nil {
				out = &Int16Vector{Encoding: field.Encoding}
			}
			if valid {
				out.Values = append(out.Values, v.Values[row])
			} else {
				out.Values = append(out.Values, 0)
			}
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Int16s[field.Name] = out
		case TypeUint8:
			v := src.Values.Uint8s[field.Name]
			out := dst.Values.Uint8s[field.Name]
			if out == nil {
				out = &Uint8Vector{Encoding: field.Encoding}
			}
			if valid {
				out.Values = append(out.Values, v.Values[row])
			} else {
				out.Values = append(out.Values, 0)
			}
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Uint8s[field.Name] = out
		case TypeUint16:
			v := src.Values.Uint16s[field.Name]
			out := dst.Values.Uint16s[field.Name]
			if out == nil {
				out = &Uint16Vector{Encoding: field.Encoding}
			}
			if valid {
				out.Values = append(out.Values, v.Values[row])
			} else {
				out.Values = append(out.Values, 0)
			}
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Uint16s[field.Name] = out
		case TypeFloat32:
			v := src.Values.Float32s[field.Name]
			out := dst.Values.Float32s[field.Name]
			if out == nil {
				out = &Float32Vector{Encoding: field.Encoding}
			}
			if valid {
				out.Values = append(out.Values, v.Values[row])
			} else {
				out.Values = append(out.Values, 0)
			}
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Float32s[field.Name] = out

		case TypeInt32, TypeDate32, TypeTime32:
			v := src.Values.Int32s[field.Name]
			out := dst.Values.Int32s[field.Name]
			if out == nil {
				out = &Int32Vector{Encoding: field.Encoding}
			}
			if valid {
				out.Values = append(out.Values, v.Values[row])
			} else {
				out.Values = append(out.Values, 0)
			}
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Int32s[field.Name] = out
		case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
			v := src.Values.Int64s[field.Name]
			out := dst.Values.Int64s[field.Name]
			if out == nil {
				out = &Int64Vector{Encoding: field.Encoding}
			}
			if valid {
				out.Values = append(out.Values, v.Values[row])
			} else {
				out.Values = append(out.Values, 0)
			}
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Int64s[field.Name] = out
		case TypeUint32, TypeBool:
			v := src.Values.Uint32s[field.Name]
			out := dst.Values.Uint32s[field.Name]
			if out == nil {
				out = &Uint32Vector{Encoding: field.Encoding}
			}
			if valid {
				out.Values = append(out.Values, v.Values[row])
			} else {
				out.Values = append(out.Values, 0)
			}
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Uint32s[field.Name] = out
		case TypeUint64:
			v := src.Values.Uint64s[field.Name]
			out := dst.Values.Uint64s[field.Name]
			if out == nil {
				out = &Uint64Vector{Encoding: field.Encoding}
			}
			if valid {
				out.Values = append(out.Values, v.Values[row])
			} else {
				out.Values = append(out.Values, 0)
			}
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Uint64s[field.Name] = out
		case TypeFloat64:
			v := src.Values.Float64s[field.Name]
			out := dst.Values.Float64s[field.Name]
			if out == nil {
				out = &Float64Vector{Encoding: field.Encoding}
			}
			if valid {
				out.Values = append(out.Values, v.Values[row])
			} else {
				out.Values = append(out.Values, 0)
			}
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Float64s[field.Name] = out
		case TypeString:
			v := src.Values.Strings[field.Name]
			if v == nil {
				dictionary := src.Values.DictStrings[field.Name]
				if dictionary == nil {
					return fmt.Errorf("struct string child %q is missing", field.Name)
				}
				out := dst.Values.DictStrings[field.Name]
				if out == nil {
					out = &DictionaryStringVector{Offsets: append([]uint32(nil), dictionary.Offsets...), Data: append([]byte(nil), dictionary.Data...), DictionaryDomain: dictionary.DictionaryDomain, Encoding: dictionary.Encoding}
				}
				code := uint32(0)
				if valid {
					code = dictionary.Indices[row]
				}
				out.Indices = append(out.Indices, code)
				out.Validity = appendValidity(out.Validity, valid, dstRow)
				dst.Values.DictStrings[field.Name] = out
				continue
			}
			out := dst.Values.Strings[field.Name]
			if out == nil {
				out = &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}
			}
			if valid {
				value := v.ValueBytes(row)
				if uint64(len(out.Data))+uint64(len(value)) > math.MaxUint32 {
					return fmt.Errorf("struct string child %q data exceeds 4 GiB", field.Name)
				}
				out.Data = append(out.Data, value...)
			}
			out.Offsets = append(out.Offsets, uint32(len(out.Data)))
			out.Validity = appendValidity(out.Validity, valid, dstRow)
			dst.Values.Strings[field.Name] = out
		case TypeList:
			v := src.Values.StringLists[field.Name]
			out := dst.Values.StringLists[field.Name]
			if out == nil {
				out = &StringListVector{Offsets: []uint32{0}, Values: &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}, Encoding: EncodingView}
			}
			if valid {
				if err := AppendStringListRow(out, v, row); err != nil {
					return err
				}
			} else {
				if err := AppendNullStringList(out); err != nil {
					return err
				}
			}
			dst.Values.StringLists[field.Name] = out
		default:
			return fmt.Errorf("unsupported struct child %q type %s", field.Name, field.Type)
		}
	}
	dst.Rows++
	dst.Values.Rows = dst.Rows
	dst.Encoding = EncodingView
	return nil
}

func batchHasField(b *Batch, f Field) bool {
	switch f.Type {
	case TypeInt8:
		return b.Int8s[f.Name] != nil
	case TypeInt16:
		return b.Int16s[f.Name] != nil
	case TypeUint8:
		return b.Uint8s[f.Name] != nil
	case TypeUint16:
		return b.Uint16s[f.Name] != nil
	case TypeFloat32:
		return b.Float32s[f.Name] != nil

	case TypeInt32, TypeDate32, TypeTime32:
		return b.Int32s[f.Name] != nil
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		return b.Int64s[f.Name] != nil
	case TypeUint32, TypeBool:
		return b.Uint32s[f.Name] != nil
	case TypeUint64:
		return b.Uint64s[f.Name] != nil
	case TypeFloat64:
		return b.Float64s[f.Name] != nil
	case TypeString:
		return b.Strings[f.Name] != nil || b.DictStrings[f.Name] != nil
	case TypeList:
		return b.StringLists[f.Name] != nil
	}
	return false
}
func batchFieldValid(b *Batch, f Field, row int) bool {
	switch f.Type {
	case TypeInt8:
		v := b.Int8s[f.Name]
		return v != nil && v.IsValid(row)
	case TypeInt16:
		v := b.Int16s[f.Name]
		return v != nil && v.IsValid(row)
	case TypeUint8:
		v := b.Uint8s[f.Name]
		return v != nil && v.IsValid(row)
	case TypeUint16:
		v := b.Uint16s[f.Name]
		return v != nil && v.IsValid(row)
	case TypeFloat32:
		v := b.Float32s[f.Name]
		return v != nil && v.IsValid(row)

	case TypeInt32, TypeDate32, TypeTime32:
		v := b.Int32s[f.Name]
		return v != nil && v.IsValid(row)
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		v := b.Int64s[f.Name]
		return v != nil && v.IsValid(row)
	case TypeUint32, TypeBool:
		v := b.Uint32s[f.Name]
		return v != nil && v.IsValid(row)
	case TypeUint64:
		v := b.Uint64s[f.Name]
		return v != nil && v.IsValid(row)
	case TypeFloat64:
		v := b.Float64s[f.Name]
		return v != nil && v.IsValid(row)
	case TypeString:
		if v := b.Strings[f.Name]; v != nil {
			return v.IsValid(row)
		}
		v := b.DictStrings[f.Name]
		return v != nil && v.IsValid(row)
	case TypeList:
		v := b.StringLists[f.Name]
		return v != nil && v.IsValid(row)
	}
	return false
}
