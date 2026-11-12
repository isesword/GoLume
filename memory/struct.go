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
	if rows < 0 || v == nil || v.Rows != rows || v.Values == nil || v.Values.Rows != rows {
		return fmt.Errorf("struct %q rows do not match %d", field.Name, rows)
	}
	if !structValidityFits(v.Validity, rows) {
		return fmt.Errorf("struct %q validity is too short", field.Name)
	}
	if !field.Nullable && HasNulls(v.Validity, rows) {
		return fmt.Errorf("non-nullable struct %q contains NULL", field.Name)
	}
	if len(v.Fields) != len(field.Children) {
		return fmt.Errorf("struct %q child schema does not match", field.Name)
	}
	for i := range field.Children {
		if v.Fields[i].Name != field.Children[i].Name || v.Fields[i].Type != field.Children[i].Type || !sameListChildContract(v.Fields[i], field.Children[i]) {
			return fmt.Errorf("struct %q child schema does not match at index %d", field.Name, i)
		}
	}
	for _, child := range field.Children {
		// Validate storage before any validity lookup or row access. A NULL
		// parent hides child values, not malformed row-aligned buffers.
		if err := validateStructChildStorage(v.Values, child, rows); err != nil {
			return fmt.Errorf("struct %q child %q: %w", field.Name, child.Name, err)
		}
		var dictionary *DictionaryStringVector
		if child.Type == TypeString && v.Values.Strings[child.Name] == nil {
			dictionary = v.Values.DictStrings[child.Name]
		}
		var requiredInt64 *Int64ListVector
		if child.Type == TypeList && child.Children[0].Type == TypeInt64 && !child.Children[0].Nullable {
			requiredInt64 = v.Values.Int64Lists[child.Name]
		}
		var requiredElements *StringListVector
		if child.Type == TypeList && child.Children[0].Type == TypeString && !child.Children[0].Nullable {
			requiredElements = v.Values.StringLists[child.Name]
		}
		if child.Nullable && dictionary == nil && requiredElements == nil && requiredInt64 == nil {
			continue
		}
		for row := 0; row < rows; row++ {
			if !v.IsValid(row) {
				continue
			}
			valid := batchFieldValid(v.Values, child, row)
			if !child.Nullable && !valid {
				return fmt.Errorf("struct %q non-nullable child %q is NULL at row %d", field.Name, child.Name, row)
			}
			if !valid {
				continue
			}
			if dictionary != nil && uint64(dictionary.Indices[row]) >= uint64(dictionary.DictionaryLen()) {
				return fmt.Errorf("struct %q child %q dictionary code is out of range at row %d", field.Name, child.Name, row)
			}
			if requiredInt64 != nil {
				start, end, _ := requiredInt64.ValueRange(row)
				for element := start; element < end; element++ {
					if !requiredInt64.Values.IsValid(element) {
						return fmt.Errorf("struct %q child %q has NULL non-nullable element", field.Name, child.Name)
					}
				}
			}
			if requiredElements != nil {
				start, end, _ := requiredElements.ValueRange(row)
				for element := start; element < end; element++ {
					if !requiredElements.Values.IsValid(element) {
						return fmt.Errorf("struct %q child %q has NULL non-nullable element at row %d element %d", field.Name, child.Name, row, element-start)
					}
				}
			}
		}
	}
	return nil
}

// structValidityFits distinguishes an absent all-valid bitmap from a present
// but short bitmap. Division avoids overflowing rows+63 at extreme row counts.
func structValidityFits(validity Bitmap, rows int) bool {
	words := rows / 64
	if rows%64 != 0 {
		words++
	}
	return validity == nil || len(validity) >= words
}

func validateStructChildStorage(b *Batch, field Field, rows int) error {
	if !batchHasField(b, field) {
		return fmt.Errorf("vector is missing or unsupported")
	}
	var length int
	var validity Bitmap
	switch field.Type {
	case TypeInt8:
		v := b.Int8s[field.Name]
		length, validity = len(v.Values), v.Validity
	case TypeInt16:
		v := b.Int16s[field.Name]
		length, validity = len(v.Values), v.Validity
	case TypeInt32, TypeDate32, TypeTime32:
		v := b.Int32s[field.Name]
		length, validity = len(v.Values), v.Validity
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		v := b.Int64s[field.Name]
		length, validity = len(v.Values), v.Validity
	case TypeUint8:
		v := b.Uint8s[field.Name]
		length, validity = len(v.Values), v.Validity
	case TypeUint16:
		v := b.Uint16s[field.Name]
		length, validity = len(v.Values), v.Validity
	case TypeUint32, TypeBool:
		v := b.Uint32s[field.Name]
		length, validity = len(v.Values), v.Validity
	case TypeUint64:
		v := b.Uint64s[field.Name]
		length, validity = len(v.Values), v.Validity
	case TypeFloat32:
		v := b.Float32s[field.Name]
		length, validity = len(v.Values), v.Validity
	case TypeFloat64:
		v := b.Float64s[field.Name]
		length, validity = len(v.Values), v.Validity
	case TypeString:
		if v := b.Strings[field.Name]; v != nil {
			if err := v.Validate(rows); err != nil {
				return err
			}
			length, validity = v.Len(), v.Validity
		} else {
			v := b.DictStrings[field.Name]
			// Preserve the runtime dictionary fallback and non-zero-based
			// offset views. Domain identity is not a storage-shape property.
			table := StringViewVector{Offsets: v.Offsets, Data: v.Data}
			if err := table.Validate(v.DictionaryLen()); err != nil {
				return fmt.Errorf("dictionary: %w", err)
			}
			length, validity = len(v.Indices), v.Validity
		}
	case TypeList:
		if len(field.Children) == 1 && field.Children[0].Type == TypeInt64 {
			return b.Int64Lists[field.Name].Validate(rows)
		}
		return b.StringLists[field.Name].Validate(rows)
	}
	if length != rows {
		return fmt.Errorf("rows=%d want=%d", length, rows)
	}
	if !structValidityFits(validity, rows) {
		return fmt.Errorf("validity is too short for %d rows", rows)
	}
	return nil
}

// TakeStructRows returns an owned gathered struct vector. An empty selection
// preserves declared child vectors, including owned dictionary tables and
// canonical empty string/list offsets, without copying unselected row data.
func TakeStructRows(src *StructVector, rows []int) (*StructVector, error) {
	if src == nil || src.Values == nil {
		return nil, fmt.Errorf("cannot take nil struct")
	}
	out := &StructVector{Fields: append([]Field(nil), src.Fields...), Values: NewBatch(), Encoding: EncodingView}
	if len(rows) == 0 {
		if err := initEmptyStructChildren(out.Values, src); err != nil {
			return nil, err
		}
		return out, nil
	}
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

// initEmptyStructChildren mirrors AppendStructRow's declared-field selection
// and physical encodings. Cloning the whole Values batch would also copy
// undeclared columns and a shadowed dictionary when Strings takes precedence.
func initEmptyStructChildren(dst *Batch, src *StructVector) error {
	for _, field := range src.Fields {
		if !batchHasField(src.Values, field) {
			return fmt.Errorf("struct child %q type %s is missing or unsupported", field.Name, field.Type)
		}
		switch field.Type {
		case TypeInt8:
			dst.Int8s[field.Name] = &Int8Vector{Encoding: field.Encoding}
		case TypeInt16:
			dst.Int16s[field.Name] = &Int16Vector{Encoding: field.Encoding}
		case TypeInt32, TypeDate32, TypeTime32:
			dst.Int32s[field.Name] = &Int32Vector{Encoding: field.Encoding}
		case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
			dst.Int64s[field.Name] = &Int64Vector{Encoding: field.Encoding}
		case TypeUint8:
			dst.Uint8s[field.Name] = &Uint8Vector{Encoding: field.Encoding}
		case TypeUint16:
			dst.Uint16s[field.Name] = &Uint16Vector{Encoding: field.Encoding}
		case TypeUint32, TypeBool:
			dst.Uint32s[field.Name] = &Uint32Vector{Encoding: field.Encoding}
		case TypeUint64:
			dst.Uint64s[field.Name] = &Uint64Vector{Encoding: field.Encoding}
		case TypeFloat32:
			dst.Float32s[field.Name] = &Float32Vector{Encoding: field.Encoding}
		case TypeFloat64:
			dst.Float64s[field.Name] = &Float64Vector{Encoding: field.Encoding}
		case TypeString:
			if src.Values.Strings[field.Name] != nil {
				dst.Strings[field.Name] = &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}
			} else {
				v := src.Values.DictStrings[field.Name]
				dst.DictStrings[field.Name] = &DictionaryStringVector{Offsets: append([]uint32(nil), v.Offsets...), Data: append([]byte(nil), v.Data...), DictionaryDomain: v.DictionaryDomain, Encoding: v.Encoding}
			}
		case TypeList:
			if len(field.Children) == 1 && field.Children[0].Type == TypeInt64 {
				dst.Int64Lists[field.Name] = &Int64ListVector{Offsets: []uint32{0}, Values: &Int64Vector{Encoding: EncodingPlain}, Encoding: EncodingView}
				continue
			}
			dst.StringLists[field.Name] = &StringListVector{Offsets: []uint32{0}, Values: &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}, Encoding: EncodingView}
		}
	}
	return nil
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
			if len(field.Children) == 1 && field.Children[0].Type == TypeInt64 {
				v := src.Values.Int64Lists[field.Name]
				out := dst.Values.Int64Lists[field.Name]
				if out == nil {
					out = &Int64ListVector{}
				}
				var err error
				if valid {
					err = AppendInt64ListRow(out, v, row)
				} else {
					err = AppendNullInt64List(out)
				}
				if err != nil {
					return err
				}
				dst.Values.Int64Lists[field.Name] = out
				continue
			}
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
		if len(f.Children) == 1 && f.Children[0].Type == TypeInt64 {
			return b.Int64Lists[f.Name] != nil
		}
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
		if len(f.Children) == 1 && f.Children[0].Type == TypeInt64 {
			v := b.Int64Lists[f.Name]
			return v != nil && v.IsValid(row)
		}
		v := b.StringLists[f.Name]
		return v != nil && v.IsValid(row)
	}
	return false
}

// sameListChildContract keeps the runtime child binding and requested schema
// on the same physical list path. Scalar handling retains its existing rules.
func sameListChildContract(a, b Field) bool {
	if a.Type != TypeList || b.Type != TypeList {
		return true
	}
	if a.ValidateList() != nil || b.ValidateList() != nil {
		return false
	}
	return a.Nullable == b.Nullable && a.Children[0].Type == b.Children[0].Type && a.Children[0].Nullable == b.Children[0].Nullable
}
