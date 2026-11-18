package memory

import "fmt"

// ValidateWithContext checks the same Struct contract as Validate, with cancellation
// checkpoints during row/offset/element scans. check may be nil. The caller may
// supply a read-only parent-validity view to restrict value validation to its
// active domain; storage checks still cover all children and rows.
func (v *StructVector) ValidateWithContext(field Field, rows int, check func() error) error {
	if check != nil {
		if err := check(); err != nil {
			return err
		}
	}
	if err := validateStructSchemaContext(field, check); err != nil {
		return err
	}
	if rows < 0 || v == nil || v.Rows != rows || v.Values == nil || v.Values.Rows != rows {
		return fmt.Errorf("struct %q rows do not match %d", field.Name, rows)
	}
	if !structValidityFits(v.Validity, rows) {
		return fmt.Errorf("struct %q validity is too short", field.Name)
	}
	if !field.Nullable && v.Validity != nil {
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if !IsSet(v.Validity, row) {
				return fmt.Errorf("non-nullable struct %q contains NULL", field.Name)
			}
		}
	}
	if len(v.Fields) != len(field.Children) {
		return fmt.Errorf("struct %q child schema does not match", field.Name)
	}
	for i := range field.Children {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if v.Fields[i].Name != field.Children[i].Name || v.Fields[i].Type != field.Children[i].Type || !sameListChildContract(v.Fields[i], field.Children[i]) {
			return fmt.Errorf("struct %q child schema does not match at index %d", field.Name, i)
		}
	}
	for _, child := range field.Children {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		// Validate storage before any validity lookup or row access. A NULL
		// parent hides child values, not malformed row-aligned buffers.
		if err := validateStructChildStorageContext(v.Values, child, rows, check); err != nil {
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
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
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
					if check != nil {
						if err := check(); err != nil {
							return err
						}
					}
					if !requiredInt64.Values.IsValid(element) {
						return fmt.Errorf("struct %q child %q has NULL non-nullable element", field.Name, child.Name)
					}
				}
			}
			if requiredElements != nil {
				start, end, _ := requiredElements.ValueRange(row)
				for element := start; element < end; element++ {
					if check != nil {
						if err := check(); err != nil {
							return err
						}
					}
					if !requiredElements.Values.IsValid(element) {
						return fmt.Errorf("struct %q child %q has NULL non-nullable element at row %d element %d", field.Name, child.Name, row, element-start)
					}
				}
			}
		}
	}
	return nil
}

func validateStructStringContext(v *StringViewVector, rows int, check func() error) error {
	if v == nil || v.Len() != rows {
		return fmt.Errorf("string view rows do not match %d", rows)
	}
	if len(v.Offsets) > 0 && uint64(v.Offsets[len(v.Offsets)-1]) > uint64(len(v.Data)) {
		return fmt.Errorf("string view terminal offset exceeds data")
	}
	for i := 1; i < len(v.Offsets); i++ {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if v.Offsets[i] < v.Offsets[i-1] {
			return fmt.Errorf("string view offset %d decreases", i)
		}
	}
	return nil
}

func validateStructChildStorageContext(b *Batch, f Field, rows int, check func() error) error {
	if f.Type != TypeString && f.Type != TypeList {
		return validateStructChildStorage(b, f, rows)
	}
	if !batchHasField(b, f) {
		return fmt.Errorf("vector is missing or unsupported")
	}
	var validity Bitmap
	var length int
	if f.Type == TypeString {
		if v := b.Strings[f.Name]; v != nil {
			if err := validateStructStringContext(v, rows, check); err != nil {
				return err
			}
			validity, length = v.Validity, v.Len()
		} else {
			v := b.DictStrings[f.Name]
			table := StringViewVector{Offsets: v.Offsets, Data: v.Data}
			if err := validateStructStringContext(&table, v.DictionaryLen(), check); err != nil {
				return err
			}
			validity, length = v.Validity, len(v.Indices)
		}
	} else {
		var offsets []uint32
		var children int
		var childValidity Bitmap
		if f.Children[0].Type == TypeInt64 {
			v := b.Int64Lists[f.Name]
			if v.Values == nil {
				return fmt.Errorf("list child vector is nil")
			}
			offsets, validity, children, childValidity = v.Offsets, v.Validity, v.Values.Len(), v.Values.Validity
		} else {
			v := b.StringLists[f.Name]
			if v.Values == nil {
				return fmt.Errorf("list child vector is nil")
			}
			offsets, validity, children, childValidity = v.Offsets, v.Validity, v.Values.Len(), v.Values.Validity
			if err := validateStructStringContext(v.Values, children, check); err != nil {
				return err
			}
		}
		if rows < 0 || len(offsets) == 0 || len(offsets)-1 != rows {
			return fmt.Errorf("list offsets do not match rows")
		}
		if offsets[0] != 0 {
			return fmt.Errorf("list first offset must be zero")
		}
		for i := 1; i < len(offsets); i++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if offsets[i] < offsets[i-1] {
				return fmt.Errorf("list offset %d decreases", i)
			}
		}
		if uint64(offsets[rows]) != uint64(children) {
			return fmt.Errorf("list terminal offset does not match child rows")
		}
		if !structValidityFits(childValidity, children) {
			return fmt.Errorf("list child validity is too short")
		}
		length = rows
	}
	if length != rows {
		return fmt.Errorf("rows=%d want=%d", length, rows)
	}
	if !structValidityFits(validity, rows) {
		return fmt.Errorf("validity is too short for %d rows", rows)
	}
	return nil
}

func validateStructSchemaContext(f Field, check func() error) error {
	if f.Type != TypeStruct {
		return fmt.Errorf("field %q: expected struct, got %s", f.Name, f.Type)
	}
	if len(f.Children) == 0 {
		return fmt.Errorf("struct field %q requires at least one child", f.Name)
	}
	seen := make(map[string]struct{}, len(f.Children))
	for _, child := range f.Children {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if child.Name == "" {
			return fmt.Errorf("struct field %q has unnamed child", f.Name)
		}
		if _, ok := seen[child.Name]; ok {
			return fmt.Errorf("struct field %q has duplicate child %q", f.Name, child.Name)
		}
		seen[child.Name] = struct{}{}
		if child.Type == TypeList {
			if err := child.ValidateList(); err != nil {
				return fmt.Errorf("struct field %q: %w", f.Name, err)
			}
			continue
		}
		if child.Type == TypeString && child.Encoding == EncodingDictionary {
			return fmt.Errorf("struct field %q child %q cannot use dictionary encoding", f.Name, child.Name)
		}
		switch child.Type {
		case TypeInt8, TypeInt16, TypeInt32, TypeInt64, TypeUint8, TypeUint16, TypeUint32, TypeUint64, TypeFloat32, TypeFloat64, TypeString, TypeBool, TypeDate32, TypeDate64, TypeTimestamp, TypeTime32, TypeTime64, TypeDuration:
		default:
			return fmt.Errorf("struct field %q child %q has unsupported type %s", f.Name, child.Name, child.Type)
		}
	}
	return nil
}
