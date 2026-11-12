package memory

import (
	"fmt"
	"math"
	"strconv"
)

// EstimateColumnarRunSelectionBytes bounds the encoded payload of a gathered
// selection without allocating the gather or any row-sized scratch space. Rows
// may be repeated and need not be sorted. Only schema fields are included.
//
// String, list<string>, and struct payloads follow the specialized gather
// semantics: top-level strings retain NULL-row bytes, while NULL list elements
// and NULL struct children have empty byte spans. Dictionaries retain their
// entire normalized dictionary and the vector's actual domain exactly once.
// Every output validity bitmap is conservatively included, even when the source
// has no bitmap (a NULL struct parent can introduce child NULLs).
//
// Source shapes are checked, but nullability applies only to selected values
// whose parents are valid. Unsupported opaque and recursive nested types fail.
// An empty selection is sized as a fully materialized zero-row codec batch;
// callers must still materialize empty struct child vectors before encoding.
func EstimateColumnarRunSelectionBytes(schema Schema, batch *Batch, rows []int) (int64, error) {
	if batch == nil || batch.Rows < 0 || batch.Rows > math.MaxInt-63 {
		return 0, fmt.Errorf("columnar selection requires a valid source row count")
	}
	if _, err := columnarRunMetadataLength(uint64(len(schema.Fields))); err != nil {
		return 0, err
	}
	for _, row := range rows {
		if row < 0 || row >= batch.Rows {
			return 0, fmt.Errorf("columnar selection row %d out of range [0,%d)", row, batch.Rows)
		}
	}
	if _, err := selectionValidityBytes(int64(len(rows))); err != nil {
		return 0, err
	}
	size := int64(columnarRunHeader)
	for _, field := range schema.Fields {
		fieldSize, err := selectionRunFieldSize(batch, field, rows, nil)
		if err != nil {
			return 0, fmt.Errorf("columnar selection field %q: %w", field.Name, err)
		}
		size, err = addRunSizes(size, fieldSize)
		if err != nil {
			return 0, err
		}
	}
	if size > int64(math.MaxInt) {
		return 0, fmt.Errorf("columnar selection exceeds addressable memory")
	}
	return size, nil
}

func selectionRunFieldSize(batch *Batch, field Field, rows []int, parent *StructVector) (int64, error) {
	validityBytes, err := selectionValidityBytes(int64(len(rows)))
	if err != nil {
		return 0, err
	}
	switch field.Type {
	case TypeStruct:
		v := batch.Structs[field.Name]
		if err := validateSelectionStruct(v, field, batch.Rows); err != nil {
			return 0, err
		}
		if err := validateSelectionNulls(v.Validity, field, rows, nil); err != nil {
			return 0, err
		}
		size := validityBytes
		for _, child := range field.Children {
			childBytes, err := selectionRunFieldSize(v.Values, child, rows, v)
			if err != nil {
				return 0, err
			}
			size, err = addRunSizes(size, childBytes)
			if err != nil {
				return 0, err
			}
		}
		return size, nil
	case TypeList:
		if err := field.ValidateList(); err != nil {
			return 0, err
		}
		if field.Children[0].Type == TypeInt64 {
			v := batch.Int64Lists[field.Name]
			if err := v.Validate(batch.Rows); err != nil {
				return 0, err
			}
			if err := validateSelectionNulls(v.Validity, field, rows, parent); err != nil {
				return 0, err
			}
			var children int64
			for _, row := range rows {
				if parent != nil && !parent.IsValid(row) || !v.IsValid(row) {
					continue
				}
				start, end, _ := v.ValueRange(row) // Validate checked physical offsets.
				children, err = addRunSizes(children, int64(end-start))
				if err != nil {
					return 0, err
				}
				// Bound child cardinality before multiplying the fixed-width payload.
				if _, err := selectionOffsetBytes(children); err != nil {
					return 0, err
				}
				for child := start; child < end; child++ {
					if !v.Values.IsValid(child) {
						if !field.Children[0].Nullable {
							return 0, fmt.Errorf("non-nullable list element is NULL at row %d", row)
						}
						continue
					}
				}
			}
			parentOffsets, err := selectionOffsetBytes(int64(len(rows)))
			if err != nil {
				return 0, err
			}
			childValidity, err := selectionValidityBytes(children)
			if err != nil {
				return 0, err
			}
			return addRunSizes(validityBytes, parentOffsets, childValidity, children*8)
		}
		v := batch.StringLists[field.Name]
		if err := v.Validate(batch.Rows); err != nil {
			return 0, err
		}
		if err := validateSelectionNulls(v.Validity, field, rows, parent); err != nil {
			return 0, err
		}
		var children, data int64
		for _, row := range rows {
			if parent != nil && !parent.IsValid(row) || !v.IsValid(row) {
				continue
			}
			start, end, _ := v.ValueRange(row) // Validate checked physical offsets.
			children, err = addRunSizes(children, int64(end-start))
			if err != nil {
				return 0, err
			}
			// The codec stores the child OFFSET COUNT, not just the child count.
			if _, err := selectionOffsetBytes(children); err != nil {
				return 0, err
			}
			for child := start; child < end; child++ {
				if !v.Values.IsValid(child) {
					if !field.Children[0].Nullable {
						return 0, fmt.Errorf("non-nullable list element is NULL at row %d", row)
					}
					continue
				}
				data, err = selectionAddData(data, v.Values.Offsets[child+1]-v.Values.Offsets[child])
				if err != nil {
					return 0, err
				}
			}
		}
		parentOffsets, err := selectionOffsetBytes(int64(len(rows)))
		if err != nil {
			return 0, err
		}
		childOffsets, err := selectionOffsetBytes(children)
		if err != nil {
			return 0, err
		}
		childValidity, err := selectionValidityBytes(children)
		if err != nil {
			return 0, err
		}
		return addRunSizes(validityBytes, parentOffsets, childValidity, childOffsets, 8, data)
	case TypeString, TypeBinary:
		if field.Encoding == EncodingDictionary {
			v := batch.DictStrings[field.Name]
			if v == nil {
				return 0, fmt.Errorf("dictionary vector is missing")
			}
			if err := v.Validate(batch.Rows); err != nil {
				return 0, err
			}
			if _, err := validitySize(v.Validity, batch.Rows); err != nil {
				return 0, err
			}
			if err := validateSelectionNulls(v.Validity, field, rows, parent); err != nil {
				return 0, err
			}
			data, err := normalizedOffsetDataSize(v.Offsets, v.Data)
			if err != nil {
				return 0, err
			}
			for i := 1; i < len(v.Offsets); i++ {
				if v.Offsets[i] < v.Offsets[i-1] {
					return 0, fmt.Errorf("dictionary offset %d decreases", i)
				}
			}
			offsets, err := selectionOffsetBytes(int64(len(v.Offsets) - 1))
			if err != nil {
				return 0, err
			}
			domain, err := columnarRunMetadataLength(uint64(len(v.DictionaryDomain)))
			if err != nil {
				return 0, err
			}
			for _, row := range rows {
				if v.IsValid(row) && uint64(v.Indices[row]) >= uint64(v.DictionaryLen()) {
					return 0, fmt.Errorf("dictionary row %d index exceeds cardinality", row)
				}
			}
			indices, err := selectionFixedBytes(int64(len(rows)), 4)
			if err != nil {
				return 0, err
			}
			return addRunSizes(validityBytes, indices, offsets, 8, data, 4, domain)
		}
		v := batch.Strings[field.Name]
		if v == nil {
			return 0, fmt.Errorf("string vector is missing")
		}
		if err := v.Validate(batch.Rows); err != nil {
			return 0, err
		}
		if _, err := validitySize(v.Validity, batch.Rows); err != nil {
			return 0, err
		}
		if err := validateSelectionNulls(v.Validity, field, rows, parent); err != nil {
			return 0, err
		}
		var data int64
		for _, row := range rows {
			// takeRows copies even NULL string payloads. AppendStructRow instead
			// canonicalizes child NULLs and inherited parent NULLs to empty spans.
			if parent != nil && (!parent.IsValid(row) || !v.IsValid(row)) {
				continue
			}
			data, err = selectionAddData(data, v.Offsets[row+1]-v.Offsets[row])
			if err != nil {
				return 0, err
			}
		}
		offsets, err := selectionOffsetBytes(int64(len(rows)))
		if err != nil {
			return 0, err
		}
		return addRunSizes(validityBytes, offsets, 8, data)
	default:
		shape := field
		shape.Nullable = true
		if field.Type == TypeDecimal && field.ArrowType != "" {
			// ValidateDecimal128 formats ArrowType using fmt.Sprintf. Compare
			// its canonical spelling on the stack to keep successful sizing
			// allocation-free, then reuse the remaining codec validation.
			var spelling [48]byte
			b := append(spelling[:0], "decimal("...)
			b = strconv.AppendInt(b, int64(field.DecimalPrecision), 10)
			b = append(b, ", "...)
			b = strconv.AppendInt(b, int64(field.DecimalScale), 10)
			b = append(b, ')')
			if field.ArrowType != string(b) {
				return 0, fmt.Errorf("decimal Arrow type conflicts with precision/scale")
			}
			shape.ArrowType = ""
		}
		validity, width, err := fixedRunField(batch, shape, batch.Rows)
		if err != nil {
			return 0, err
		}
		if _, err := validitySize(validity, batch.Rows); err != nil {
			return 0, err
		}
		if err := validateSelectionNulls(validity, field, rows, parent); err != nil {
			return 0, err
		}
		data, err := selectionFixedBytes(int64(len(rows)), int64(width))
		if err != nil {
			return 0, err
		}
		metadata := int64(0)
		if field.Type == TypeDecimal {
			metadata = 8
		}
		return addRunSizes(validityBytes, metadata, data)
	}
}

func validateSelectionNulls(validity Bitmap, field Field, rows []int, parent *StructVector) error {
	if field.Nullable || validity == nil {
		return nil
	}
	for _, row := range rows {
		if (parent == nil || parent.IsValid(row)) && !IsSet(validity, row) {
			return fmt.Errorf("non-nullable field %q contains selected NULL at row %d", field.Name, row)
		}
	}
	return nil
}

func validateSelectionStruct(v *StructVector, field Field, rows int) error {
	// Match ValidateStruct's bounded schema without its map allocation and
	// without applying child nullability to unselected source rows.
	if len(field.Children) == 0 || v == nil || v.Values == nil || v.Rows != rows || v.Values.Rows != rows || len(v.Fields) != len(field.Children) {
		return fmt.Errorf("struct shape or child schema mismatch")
	}
	if _, err := validitySize(v.Validity, rows); err != nil {
		return err
	}
	for i, child := range field.Children {
		if child.Name == "" || child.Name != v.Fields[i].Name || child.Type != v.Fields[i].Type {
			return fmt.Errorf("struct child schema mismatch at index %d", i)
		}
		for j := 0; j < i; j++ {
			if field.Children[j].Name == child.Name {
				return fmt.Errorf("duplicate struct child %q", child.Name)
			}
		}
		switch child.Type {
		case TypeList:
			if err := child.ValidateList(); err != nil {
				return err
			}
			if err := v.Fields[i].ValidateList(); err != nil {
				return err
			}
			if child.Children[0].Type != v.Fields[i].Children[0].Type {
				return fmt.Errorf("struct list child type mismatch")
			}
		case TypeString:
			if child.Encoding == EncodingDictionary {
				return fmt.Errorf("struct child %q cannot use dictionary encoding", child.Name)
			}
		case TypeInt8, TypeInt16, TypeInt32, TypeInt64, TypeUint8, TypeUint16, TypeUint32, TypeUint64, TypeFloat32, TypeFloat64, TypeBool, TypeDate32, TypeDate64, TypeTimestamp, TypeTime32, TypeTime64, TypeDuration:
		default:
			return fmt.Errorf("unsupported struct child %q type %s", child.Name, child.Type)
		}
	}
	return nil
}

func selectionValidityBytes(rows int64) (int64, error) {
	if rows < 0 || rows > int64(math.MaxInt)-63 {
		return 0, fmt.Errorf("columnar selection validity row count overflows")
	}
	words := (rows + 63) / 64
	if _, err := columnarRunMetadataLength(uint64(words)); err != nil {
		return 0, err
	}
	return 4 + words*8, nil
}

// selectionOffsetBytes includes the uint32 offset-count prefix.
func selectionOffsetBytes(rows int64) (int64, error) {
	if rows < 0 || rows >= math.MaxUint32 || rows >= int64(math.MaxInt) {
		return 0, fmt.Errorf("columnar selection offset count exceeds representation")
	}
	return 4 + (rows+1)*4, nil
}

func selectionFixedBytes(rows, width int64) (int64, error) {
	if rows < 0 || width <= 0 || rows > math.MaxInt64/width {
		return 0, fmt.Errorf("columnar selection fixed-width size overflow")
	}
	return rows * width, nil
}

func selectionAddData(total int64, bytes uint32) (int64, error) {
	if total < 0 || total > math.MaxUint32-int64(bytes) {
		return 0, fmt.Errorf("columnar selection string data exceeds uint32")
	}
	return total + int64(bytes), nil
}
