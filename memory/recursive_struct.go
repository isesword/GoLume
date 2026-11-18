package memory

import "fmt"

// MaxStructDepth is the native recursive Struct engineering limit, counting a
// root Struct as depth one. It is not a Parquet or Arrow format limitation.
const MaxStructDepth = 8

// NewSchemaChecked validates bounded native tree structure before taking an
// independent schema snapshot. Unlike NewSchema, it safely rejects cyclic or
// excessively deep child graphs. NewSchema retains its valid-acyclic contract.
func NewSchemaChecked(fields ...Field) (Schema, error) {
	for _, f := range fields {
		if err := checkRecursiveField(f, 0, nil); err != nil {
			return Schema{}, err
		}
	}
	return NewSchema(fields...), nil
}

// ValidateRecursiveStruct checks a bounded recursive native Struct schema.
// Lists remain restricted to primitive Int64 or String elements.
func (f Field) ValidateRecursiveStruct() error {
	if f.Type != TypeStruct {
		return fmt.Errorf("field %q: expected Struct", f.Name)
	}
	return checkRecursiveField(f, 0, nil)
}

func checkRecursiveField(f Field, depth int, check func() error) error {
	if check != nil {
		if err := check(); err != nil {
			return err
		}
	}
	if f.Type != TypeStruct {
		if f.Type == TypeList {
			if err := f.ValidateList(); err != nil {
				return err
			}
			item := f.Children[0]
			if f.DictionaryDomain != "" || item.DictionaryDomain != "" ||
				(f.Encoding != EncodingPlain && f.Encoding != EncodingView) ||
				(item.Encoding != EncodingPlain && item.Encoding != EncodingView) {
				return fmt.Errorf("recursive Struct requires primitive non-dictionary list metadata")
			}
			if len(f.Children[0].Children) != 0 {
				return fmt.Errorf("recursive List elements are unsupported")
			}
			return nil
		}
		if len(f.Children) != 0 {
			return fmt.Errorf("non-Struct field %q has children", f.Name)
		}
		return nil
	}
	depth++
	if depth > MaxStructDepth {
		return fmt.Errorf("Struct depth exceeds %d (cyclic or excessively deep schema)", MaxStructDepth)
	}
	if len(f.Children) == 0 {
		return fmt.Errorf("Struct %q requires children", f.Name)
	}
	for i, child := range f.Children {
		if child.Name == "" {
			return fmt.Errorf("Struct %q has unnamed child", f.Name)
		}
		for j := 0; j < i; j++ {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if f.Children[j].Name == child.Name {
				return fmt.Errorf("Struct %q has duplicate child %q", f.Name, child.Name)
			}
		}
		if child.Type != TypeStruct {
			// Reuse the established bounded terminal type contract, without
			// traversing an unvalidated recursive child graph.
			if err := (Field{Name: f.Name, Type: TypeStruct, Children: []Field{child}}).ValidateStruct(); err != nil {
				return err
			}
		}
		if err := checkRecursiveField(child, depth, check); err != nil {
			return err
		}
	}
	return nil
}

// ValidateRecursiveWithContext validates all storage while restricting required
// values and dictionary/List element checks to rows visible through every
// ancestor. It does not allocate effective per-row ancestor bitmaps.
func (v *StructVector) ValidateRecursiveWithContext(field Field, rows int, check func() error) error {
	if err := checkRecursiveField(field, 0, check); err != nil {
		return err
	}
	if field.Type != TypeStruct {
		return fmt.Errorf("expected Struct schema")
	}
	var ancestors [MaxStructDepth]*StructVector
	return validateRecursiveVector(v, field, rows, check, &ancestors, 0)
}

func recursiveCheck(check func() error) error {
	if check != nil {
		return check()
	}
	return nil
}

func validateRecursiveVector(v *StructVector, f Field, rows int, check func() error, ancestors *[MaxStructDepth]*StructVector, depth int) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	if depth >= MaxStructDepth || rows < 0 || v == nil || v.Rows != rows || v.Values == nil || v.Values.Rows != rows || len(v.Fields) != len(f.Children) {
		return fmt.Errorf("recursive Struct %q shape mismatch", f.Name)
	}
	if !structValidityFits(v.Validity, rows) {
		return fmt.Errorf("Struct %q validity is too short", f.Name)
	}
	for i := range f.Children {
		equal, err := recursiveFieldEqual(v.Fields[i], f.Children[i], 0, check)
		if err != nil {
			return err
		}
		if !equal {
			return fmt.Errorf("Struct %q child metadata mismatch", f.Name)
		}
	}
	visible := func(row int) bool {
		for i := 0; i < depth; i++ {
			if !ancestors[i].IsValid(row) {
				return false
			}
		}
		return true
	}
	for row := 0; row < rows; row++ {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		if visible(row) && !f.Nullable && !v.IsValid(row) {
			return fmt.Errorf("required Struct %q is NULL at row %d", f.Name, row)
		}
	}
	ancestors[depth] = v
	for _, child := range f.Children {
		if child.Type == TypeStruct {
			if err := validateRecursiveVector(v.Values.Structs[child.Name], child, rows, check, ancestors, depth+1); err != nil {
				return err
			}
			continue
		}
		if err := validateStructChildStorageContext(v.Values, child, rows, check); err != nil {
			return fmt.Errorf("child %q: %w", child.Name, err)
		}
		for row := 0; row < rows; row++ {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if !visible(row) || !v.IsValid(row) {
				continue
			}
			valid := batchFieldValid(v.Values, child, row)
			if !valid {
				if !child.Nullable {
					return fmt.Errorf("required child %q is NULL at row %d", child.Name, row)
				}
				continue
			}
			if child.Type == TypeString && v.Values.Strings[child.Name] == nil {
				d := v.Values.DictStrings[child.Name]
				if uint64(d.Indices[row]) >= uint64(d.DictionaryLen()) {
					return fmt.Errorf("child %q dictionary code out of range", child.Name)
				}
			}
			if child.Type == TypeList && !child.Children[0].Nullable {
				var start, end int
				var bitmap Bitmap
				if child.Children[0].Type == TypeInt64 {
					l := v.Values.Int64Lists[child.Name]
					start, end, _ = l.ValueRange(row)
					bitmap = l.Values.Validity
				} else {
					l := v.Values.StringLists[child.Name]
					start, end, _ = l.ValueRange(row)
					bitmap = l.Values.Validity
				}
				for e := start; e < end; e++ {
					if err := recursiveCheck(check); err != nil {
						return err
					}
					if bitmap != nil && !IsSet(bitmap, e) {
						return fmt.Errorf("child %q has required NULL element", child.Name)
					}
				}
			}
		}
	}
	return nil
}

func recursiveFieldEqual(a, b Field, depth int, check func() error) (bool, error) {
	if err := recursiveCheck(check); err != nil {
		return false, err
	}
	if depth > MaxStructDepth+1 {
		return false, nil
	}
	if a.Name != b.Name || a.Type != b.Type || a.Nullable != b.Nullable || a.Encoding != b.Encoding || a.DictionaryDomain != b.DictionaryDomain || a.ArrowType != b.ArrowType || a.DecimalPrecision != b.DecimalPrecision || a.DecimalScale != b.DecimalScale || len(a.Children) != len(b.Children) {
		return false, nil
	}
	for i := range a.Children {
		equal, err := recursiveFieldEqual(a.Children[i], b.Children[i], depth+1, check)
		if err != nil {
			return false, err
		}
		if !equal {
			return false, nil
		}
	}
	return true, nil
}
