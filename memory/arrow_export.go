package memory

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

// ArrowSchema converts the stable GoLume schema slice supported by native
// sinks into an Apache Arrow schema. Dictionary strings are intentionally
// emitted as ordinary UTF-8 so batches with different local dictionaries share
// one file schema.
func ArrowSchema(schema Schema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(schema.Fields))
	for index, field := range schema.Fields {
		// This leaf profile is a transport contract, not general List-expression admission.
		if IsListTime64(field) {
			child, err := arrowField(field.Children[0])
			if err != nil {
				return nil, err
			}
			fields[index] = arrow.Field{Name: field.Name, Type: arrow.ListOfField(child), Nullable: field.Nullable}
			continue
		}
		converted, err := arrowField(field)
		if err != nil {
			return nil, fmt.Errorf("arrow schema field %q: %w", field.Name, err)
		}
		fields[index] = converted
	}
	return arrow.NewSchema(fields, nil), nil
}

func arrowField(field Field) (arrow.Field, error) {
	dataType, err := arrowDataType(field)
	if err != nil {
		return arrow.Field{}, err
	}
	return arrow.Field{Name: field.Name, Type: dataType, Nullable: field.Nullable}, nil
}

func arrowDataType(field Field) (arrow.DataType, error) {
	switch field.Type {
	case TypeBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case TypeInt8:
		return arrow.PrimitiveTypes.Int8, nil
	case TypeInt16:
		return arrow.PrimitiveTypes.Int16, nil
	case TypeInt32:
		return arrow.PrimitiveTypes.Int32, nil
	case TypeInt64:
		return arrow.PrimitiveTypes.Int64, nil
	case TypeUint8:
		return arrow.PrimitiveTypes.Uint8, nil
	case TypeUint16:
		return arrow.PrimitiveTypes.Uint16, nil
	case TypeUint32:
		return arrow.PrimitiveTypes.Uint32, nil
	case TypeUint64:
		return arrow.PrimitiveTypes.Uint64, nil
	case TypeFloat32:
		return arrow.PrimitiveTypes.Float32, nil
	case TypeFloat64:
		return arrow.PrimitiveTypes.Float64, nil
	case TypeString:
		return arrow.BinaryTypes.String, nil
	case TypeBinary:
		return arrow.BinaryTypes.Binary, nil
	case TypeDate32:
		return arrow.FixedWidthTypes.Date32, nil
	case TypeDate64:
		return arrow.FixedWidthTypes.Date64, nil
	case TypeTimestamp:
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, nil
	case TypeTime32:
		return arrow.FixedWidthTypes.Time32ms, nil
	case TypeTime64:
		return arrow.FixedWidthTypes.Time64ns, nil
	case TypeDuration:
		return &arrow.DurationType{Unit: arrow.Nanosecond}, nil
	case TypeList:
		if err := field.ValidateArrayContainers(); err != nil {
			return nil, err
		}
		child, err := arrowField(field.Children[0])
		if err != nil {
			return nil, err
		}
		return arrow.ListOfField(child), nil
	case TypeStruct:
		if err := field.ValidateArrayContainers(); err != nil {
			return nil, err
		}
		children := make([]arrow.Field, len(field.Children))
		for index, child := range field.Children {
			converted, err := arrowField(child)
			if err != nil {
				return nil, fmt.Errorf("struct child %q: %w", child.Name, err)
			}
			children[index] = converted
		}
		return arrow.StructOf(children...), nil
	case TypeDecimal:
		if field.DecimalPrecision == 0 {
			return nil, fmt.Errorf("logical type %s requires an explicit Arrow type adapter", field.Type)
		}
		if err := field.ValidateDecimal128(); err != nil {
			return nil, err
		}
		return &arrow.Decimal128Type{Precision: field.DecimalPrecision, Scale: field.DecimalScale}, nil
	case TypeArrow:
		return nil, fmt.Errorf("logical type %s requires an explicit Arrow type adapter", field.Type)
	default:
		return nil, fmt.Errorf("logical type %s is not supported by Arrow sinks", field.Type)
	}
}

// ArrowRecordRange copies a row range into an owned Apache Arrow RecordBatch.
// The caller must Release the returned record. Conversion is batch-bounded and
// is intended for native file sinks rather than row-at-a-time user code.
func ArrowRecordRange(allocator arrowmem.Allocator, schema Schema, batch *Batch, offset, length int) (arrow.RecordBatch, error) {
	if allocator == nil {
		return nil, fmt.Errorf("arrow allocator is nil")
	}
	if batch == nil {
		return nil, fmt.Errorf("arrow record batch is nil")
	}
	if offset < 0 || length < 0 || offset > batch.Rows || length > batch.Rows-offset {
		return nil, fmt.Errorf("arrow record range [%d,%d) outside %d rows", offset, offset+length, batch.Rows)
	}
	if schema.HasNestedLists() {
		if err := validateNestedListsBatch(batch, schema, nil, true); err != nil {
			return nil, err
		}
		for _, f := range schema.Fields {
			if err := validateArrayArrowLimits(batch, f, offset, length, [MaxStructDepth]Bitmap{}, 0); err != nil {
				return nil, err
			}
		}
	}
	if schema.HasStructLists() && !schema.HasNestedLists() {
		if err := ValidateStructListsBatch(batch, schema, nil); err != nil {
			return nil, err
		}
	}
	if schema.HasStructLists() {
		for _, f := range schema.Fields {
			if err := validateStructListArrowLimits(batch, f, offset, length, [MaxStructDepth]Bitmap{}, 0); err != nil {
				return nil, err
			}
		}
	}
	arrowSchema, err := ArrowSchema(schema)
	if err != nil {
		return nil, err
	}
	for _, field := range schema.Fields {
		if err := validateArrowBinaryDictionary(batch, field, batch.Rows); err != nil {
			return nil, fmt.Errorf("arrow field %q: %w", field.Name, err)
		}
		if err := validateArrowInt64ListStorage(batch, field, batch.Rows); err != nil {
			return nil, fmt.Errorf("arrow field %q: %w", field.Name, err)
		}
		if field.Type == TypeDecimal {
			if err := batch.Decimals[field.Name].ValidateField(field, batch.Rows); err != nil {
				return nil, err
			}
		}
	}
	builder := array.NewRecordBuilder(allocator, arrowSchema)
	defer builder.Release()
	for row := offset; row < offset+length; row++ {
		for column, field := range schema.Fields {
			if err := appendArrowValue(builder.Field(column), batch, field, row); err != nil {
				return nil, fmt.Errorf("arrow field %q row %d: %w", field.Name, row, err)
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

// ArrowRecord copies a complete GoLume batch into an owned Apache Arrow
// RecordBatch. The caller must Release the returned record.
func ArrowRecord(allocator arrowmem.Allocator, schema Schema, batch *Batch) (arrow.RecordBatch, error) {
	if batch == nil {
		return nil, fmt.Errorf("arrow record batch is nil")
	}
	return ArrowRecordRange(allocator, schema, batch, 0, batch.Rows)
}

func appendArrowValue(builder array.Builder, batch *Batch, field Field, row int) error {
	valid, err := batchValueValid(batch, field, row)
	if err != nil {
		return err
	}
	if !valid {
		if !field.Nullable {
			return fmt.Errorf("non-nullable field contains NULL")
		}
		builder.AppendNull()
		return nil
	}
	switch field.Type {
	case TypeBool:
		builder.(*array.BooleanBuilder).Append(batch.Uint32s[field.Name].Values[row] != 0)
	case TypeInt8:
		builder.(*array.Int8Builder).Append(batch.Int8s[field.Name].Values[row])
	case TypeInt16:
		builder.(*array.Int16Builder).Append(batch.Int16s[field.Name].Values[row])
	case TypeInt32:
		builder.(*array.Int32Builder).Append(batch.Int32s[field.Name].Values[row])
	case TypeDate32:
		builder.(*array.Date32Builder).Append(arrow.Date32(batch.Int32s[field.Name].Values[row]))
	case TypeTime32:
		builder.(*array.Time32Builder).Append(arrow.Time32(batch.Int32s[field.Name].Values[row]))
	case TypeInt64:
		builder.(*array.Int64Builder).Append(batch.Int64s[field.Name].Values[row])
	case TypeDate64:
		builder.(*array.Date64Builder).Append(arrow.Date64(batch.Int64s[field.Name].Values[row]))
	case TypeTimestamp:
		builder.(*array.TimestampBuilder).Append(arrow.Timestamp(batch.Int64s[field.Name].Values[row]))
	case TypeTime64:
		builder.(*array.Time64Builder).Append(arrow.Time64(batch.Int64s[field.Name].Values[row]))
	case TypeDuration:
		builder.(*array.DurationBuilder).Append(arrow.Duration(batch.Int64s[field.Name].Values[row]))
	case TypeUint8:
		builder.(*array.Uint8Builder).Append(batch.Uint8s[field.Name].Values[row])
	case TypeUint16:
		builder.(*array.Uint16Builder).Append(batch.Uint16s[field.Name].Values[row])
	case TypeUint32:
		builder.(*array.Uint32Builder).Append(batch.Uint32s[field.Name].Values[row])
	case TypeUint64:
		builder.(*array.Uint64Builder).Append(batch.Uint64s[field.Name].Values[row])
	case TypeFloat32:
		builder.(*array.Float32Builder).Append(batch.Float32s[field.Name].Values[row])
	case TypeDecimal:
		builder.(*array.Decimal128Builder).Append(batch.Decimals[field.Name].Values[row])
	case TypeFloat64:
		builder.(*array.Float64Builder).Append(batch.Float64s[field.Name].Values[row])
	case TypeString:
		value, err := batchStringBytes(batch, field, row)
		if err != nil {
			return err
		}
		builder.(*array.StringBuilder).Append(string(value))
	case TypeBinary:
		value, err := batchStringBytes(batch, field, row)
		if err != nil {
			return err
		}
		builder.(*array.BinaryBuilder).Append(value)
	case TypeList:
		if IsNestedList(field) {
			return appendArrowArrayNode(builder, batch.Arrays[field.Name], row)
		}
		if IsStructList(field) {
			v := batch.StructLists[field.Name]
			lo, hi, ok := v.ValueRange(row)
			if !ok {
				return fmt.Errorf("invalid StructList row")
			}
			lb := builder.(*array.ListBuilder)
			lb.Append(true)
			sb := lb.ValueBuilder().(*array.StructBuilder)
			for e := lo; e < hi; e++ {
				if !v.Values.IsValid(e) {
					sb.AppendNull()
					continue
				}
				sb.Append(true)
				for j, c := range field.Children[0].Children {
					if err := appendArrowValue(sb.FieldBuilder(j), v.Values.Values, c, e); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if field.Children[0].Type == TypeInt64 || IsListTime64(field) {
			list := batch.Int64Lists[field.Name]
			start, end, ok := list.ValueRange(row)
			if !ok {
				return fmt.Errorf("valid list row has no value range")
			}
			listBuilder := builder.(*array.ListBuilder)
			listBuilder.Append(true)
			values := listBuilder.ValueBuilder()
			if IsListTime64(field) && int64(values.Len())+int64(end-start) > math.MaxInt32 {
				return fmt.Errorf("wall-clock list exceeds Arrow offset range")
			}
			for index := start; index < end; index++ {
				if !list.Values.IsValid(index) {
					if !field.Children[0].Nullable {
						return fmt.Errorf("non-nullable list child contains NULL")
					}
					values.AppendNull()
					continue
				}
				value := list.Values.Values[index]
				if IsListTime64(field) {
					if value < 0 || value >= 86400000000000 {
						return fmt.Errorf("list wall-clock time outside [0, 24h)")
					}
					values.(*array.Time64Builder).Append(arrow.Time64(value))
				} else {
					values.(*array.Int64Builder).Append(value)
				}
			}
			return nil
		}
		list := batch.StringLists[field.Name]
		start, end, ok := list.ValueRange(row)
		if !ok {
			return fmt.Errorf("valid list row has no value range")
		}
		listBuilder := builder.(*array.ListBuilder)
		listBuilder.Append(true)
		values := listBuilder.ValueBuilder().(*array.StringBuilder)
		for index := start; index < end; index++ {
			if !list.Values.IsValid(index) {
				if !field.Children[0].Nullable {
					return fmt.Errorf("non-nullable list child contains NULL")
				}
				values.AppendNull()
				continue
			}
			values.Append(string(list.Values.ValueBytes(index)))
		}
	case TypeStruct:
		value := batch.Structs[field.Name]
		structBuilder := builder.(*array.StructBuilder)
		structBuilder.Append(true)
		for index, child := range field.Children {
			if err := appendArrowValue(structBuilder.FieldBuilder(index), value.Values, child, row); err != nil {
				return fmt.Errorf("struct child %q: %w", child.Name, err)
			}
		}
	default:
		return fmt.Errorf("logical type %s is not supported by Arrow sinks", field.Type)
	}
	return nil
}

func batchValueValid(batch *Batch, field Field, row int) (bool, error) {
	switch field.Type {
	case TypeDecimal:
		v := batch.Decimals[field.Name]
		if v == nil || row < 0 || row >= len(v.Values) {
			return false, fmt.Errorf("decimal vector is missing or shorter than rows")
		}
		return v.IsValid(row), nil
	case TypeInt8:
		value := batch.Int8s[field.Name]
		if value == nil {
			return false, fmt.Errorf("int8 vector is missing")
		}
		return value.IsValid(row), nil
	case TypeInt16:
		value := batch.Int16s[field.Name]
		if value == nil {
			return false, fmt.Errorf("int16 vector is missing")
		}
		return value.IsValid(row), nil
	case TypeUint8:
		value := batch.Uint8s[field.Name]
		if value == nil {
			return false, fmt.Errorf("uint8 vector is missing")
		}
		return value.IsValid(row), nil
	case TypeUint16:
		value := batch.Uint16s[field.Name]
		if value == nil {
			return false, fmt.Errorf("uint16 vector is missing")
		}
		return value.IsValid(row), nil
	case TypeFloat32:
		value := batch.Float32s[field.Name]
		if value == nil {
			return false, fmt.Errorf("float32 vector is missing")
		}
		return value.IsValid(row), nil

	case TypeBool, TypeUint32:
		value, ok := batch.Uint32s[field.Name]
		if !ok {
			return false, fmt.Errorf("uint32 vector is missing")
		}
		return value.IsValid(row), nil
	case TypeInt32, TypeDate32, TypeTime32:
		value, ok := batch.Int32s[field.Name]
		if !ok {
			return false, fmt.Errorf("int32 vector is missing")
		}
		return value.IsValid(row), nil
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		value, ok := batch.Int64s[field.Name]
		if !ok {
			return false, fmt.Errorf("int64 vector is missing")
		}
		return value.IsValid(row), nil
	case TypeUint64:
		value, ok := batch.Uint64s[field.Name]
		if !ok {
			return false, fmt.Errorf("uint64 vector is missing")
		}
		return value.IsValid(row), nil
	case TypeFloat64:
		value, ok := batch.Float64s[field.Name]
		if !ok {
			return false, fmt.Errorf("float64 vector is missing")
		}
		return value.IsValid(row), nil
	case TypeString:
		if value := batch.Strings[field.Name]; value != nil {
			return value.IsValid(row), nil
		}
		if value := batch.DictStrings[field.Name]; value != nil {
			return value.IsValid(row), nil
		}
		return false, fmt.Errorf("string vector is missing")
	case TypeBinary:
		if value := batch.Strings[field.Name]; value != nil {
			return value.IsValid(row), nil
		}
		if value := batch.DictStrings[field.Name]; value != nil {
			if row < 0 || row >= len(value.Indices) || (value.Validity != nil && row/64 >= len(value.Validity)) {
				return false, fmt.Errorf("binary dictionary row out of bounds")
			}
			if value.IsValid(row) {
				code := uint64(value.Indices[row])
				if code+1 >= uint64(len(value.Offsets)) || value.Offsets[code] > value.Offsets[code+1] || uint64(value.Offsets[code+1]) > uint64(len(value.Data)) {
					return false, fmt.Errorf("binary dictionary code or offsets out of bounds")
				}
			}
			return value.IsValid(row), nil
		}
		return false, fmt.Errorf("binary vector is missing")
	case TypeList:
		if IsNestedList(field) {
			v := batch.Arrays[field.Name]
			if v == nil {
				return false, fmt.Errorf("missing array node")
			}
			return v.IsValid(row), nil
		}
		if IsStructList(field) {
			v := batch.StructLists[field.Name]
			if v == nil {
				return false, fmt.Errorf("missing StructList")
			}
			return v.IsValid(row), nil
		}
		if field.Children[0].Type == TypeInt64 || IsListTime64(field) {
			value, ok := batch.Int64Lists[field.Name]
			if !ok {
				return false, fmt.Errorf("list vector is missing")
			}
			return value.IsValid(row), nil
		}
		value, ok := batch.StringLists[field.Name]
		if !ok {
			return false, fmt.Errorf("list vector is missing")
		}
		return value.IsValid(row), nil
	case TypeStruct:
		value, ok := batch.Structs[field.Name]
		if !ok {
			return false, fmt.Errorf("struct vector is missing")
		}
		return value.IsValid(row), nil
	default:
		return false, fmt.Errorf("logical type %s is not supported by Arrow sinks", field.Type)
	}
}

func batchStringBytes(batch *Batch, field Field, row int) ([]byte, error) {
	if value := batch.Strings[field.Name]; value != nil {
		return value.ValueBytes(row), nil
	}
	if value := batch.DictStrings[field.Name]; value != nil {
		return value.DictionaryValue(value.Indices[row]), nil
	}
	return nil, fmt.Errorf("string vector is missing")
}

// Validate native integer-list storage once per export, before row validity can
// hide malformed payloads. Even an empty exported range must have valid storage.
func validateArrowInt64ListStorage(batch *Batch, field Field, rows int) error {
	if fieldHasNestedLists(field, 0) {
		if IsNestedList(field) {
			return batch.Arrays[field.Name].ValidateWithContext(field, rows, NestedListParentDomain{}, nil)
		}
		return batch.Structs[field.Name].ValidateArrayContainersWithContext(field, rows, nil)
	}
	if fieldHasStructLists(field, 0) {
		if IsStructList(field) {
			return batch.StructLists[field.Name].ValidateWithContext(field, rows, StructListParentDomain{}, nil)
		}
		return batch.Structs[field.Name].ValidateStructListsWithContext(field, rows, nil)
	}
	if field.Type == TypeStruct {
		for _, child := range field.Children {
			if child.Type == TypeStruct {
				return batch.Structs[field.Name].ValidateRecursiveWithContext(field, rows, nil)
			}
		}
	}

	if field.Type == TypeList && (field.Children[0].Type == TypeInt64 || IsListTime64(field)) {
		return batch.Int64Lists[field.Name].Validate(rows)
	}
	if field.Type == TypeStruct {
		for _, child := range field.Children {
			if child.Type == TypeList && child.Children[0].Type == TypeInt64 {
				return batch.Structs[field.Name].Validate(field, rows)
			}
		}
	}
	return nil
}

// validateArrowBinaryDictionary validates the complete backing storage even for
// empty or partial exports. NULL row codes are unobserved and need not be valid.
func validateArrowBinaryDictionary(batch *Batch, field Field, rows int) error {
	if field.Type != TypeBinary || batch.Strings[field.Name] != nil {
		return nil
	}
	v := batch.DictStrings[field.Name]
	if v == nil {
		return nil
	}
	if err := v.Validate(rows); err != nil {
		return err
	}
	if v.Validity != nil && len(v.Validity) < WordsFor(rows) {
		return fmt.Errorf("binary dictionary validity is short")
	}
	if len(v.Offsets) == 0 {
		return fmt.Errorf("binary dictionary offsets are empty")
	}
	for i, offset := range v.Offsets {
		if uint64(offset) > uint64(len(v.Data)) || (i > 0 && offset < v.Offsets[i-1]) {
			return fmt.Errorf("binary dictionary offset %d is invalid", i)
		}
	}
	for row, code := range v.Indices {
		if v.IsValid(row) && uint64(code)+1 >= uint64(len(v.Offsets)) {
			return fmt.Errorf("binary dictionary code at row %d is invalid", row)
		}
	}
	return nil
}
