package memory

import (
	"fmt"

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
		if err := field.ValidateListString(); err != nil {
			return nil, err
		}
		child, err := arrowField(field.Children[0])
		if err != nil {
			return nil, err
		}
		return arrow.ListOfField(child), nil
	case TypeStruct:
		if err := field.ValidateStruct(); err != nil {
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
	case TypeDecimal, TypeArrow:
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
	arrowSchema, err := ArrowSchema(schema)
	if err != nil {
		return nil, err
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
	case TypeFloat64:
		builder.(*array.Float64Builder).Append(batch.Float64s[field.Name].Values[row])
	case TypeString:
		value, err := batchStringBytes(batch, field, row)
		if err != nil {
			return err
		}
		builder.(*array.StringBuilder).Append(string(value))
	case TypeBinary:
		builder.(*array.BinaryBuilder).Append(batch.Strings[field.Name].ValueBytes(row))
	case TypeList:
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
		value, ok := batch.Strings[field.Name]
		if !ok {
			return false, fmt.Errorf("binary vector is missing")
		}
		return value.IsValid(row), nil
	case TypeList:
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
