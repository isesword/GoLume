package memory

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestArrowRecordRangeCoversSupportedScalarFamilies(t *testing.T) {
	fields := []Field{
		{Name: "bool", Type: TypeBool, Nullable: true},
		{Name: "i8", Type: TypeInt8, Nullable: true},
		{Name: "i16", Type: TypeInt16, Nullable: true},
		{Name: "i32", Type: TypeInt32, Nullable: true},
		{Name: "i64", Type: TypeInt64, Nullable: true},
		{Name: "u8", Type: TypeUint8, Nullable: true},
		{Name: "u16", Type: TypeUint16, Nullable: true},
		{Name: "u32", Type: TypeUint32, Nullable: true},
		{Name: "u64", Type: TypeUint64, Nullable: true},
		{Name: "f32", Type: TypeFloat32, Nullable: true},
		{Name: "f64", Type: TypeFloat64, Nullable: true},
		{Name: "string", Type: TypeString, Nullable: true, Encoding: EncodingView},
		{Name: "dict", Type: TypeString, Nullable: true, Encoding: EncodingDictionary, DictionaryDomain: "domain"},
		{Name: "binary", Type: TypeBinary, Nullable: true, Encoding: EncodingView},
		{Name: "date32", Type: TypeDate32, Nullable: true},
		{Name: "date64", Type: TypeDate64, Nullable: true},
		{Name: "timestamp", Type: TypeTimestamp, Nullable: true},
		{Name: "time32", Type: TypeTime32, Nullable: true},
		{Name: "time64", Type: TypeTime64, Nullable: true},
		{Name: "duration", Type: TypeDuration, Nullable: true},
	}
	schema := NewSchema(fields...)
	validity := Bitmap{1}
	batch := NewBatch()
	batch.Uint32s["bool"] = &Uint32Vector{Values: []uint32{1, 0}, Validity: validity}
	batch.Int8s["i8"] = &Int8Vector{Values: []int8{-8, 0}, Validity: validity}
	batch.Int16s["i16"] = &Int16Vector{Values: []int16{-16, 0}, Validity: validity}
	batch.Int32s["i32"] = &Int32Vector{Values: []int32{-32, 0}, Validity: validity}
	batch.Int64s["i64"] = &Int64Vector{Values: []int64{-64, 0}, Validity: validity}
	batch.Uint8s["u8"] = &Uint8Vector{Values: []uint8{8, 0}, Validity: validity}
	batch.Uint16s["u16"] = &Uint16Vector{Values: []uint16{16, 0}, Validity: validity}
	batch.Uint32s["u32"] = &Uint32Vector{Values: []uint32{32, 0}, Validity: validity}
	batch.Uint64s["u64"] = &Uint64Vector{Values: []uint64{64, 0}, Validity: validity}
	batch.Float32s["f32"] = &Float32Vector{Values: []float32{3.25, 0}, Validity: validity}
	batch.Float64s["f64"] = &Float64Vector{Values: []float64{6.5, 0}, Validity: validity}
	batch.Strings["string"] = &StringViewVector{Offsets: []uint32{0, 3, 3}, Data: []byte("中"), Validity: validity, Encoding: EncodingView}
	batch.DictStrings["dict"] = &DictionaryStringVector{Indices: []uint32{1, 0}, Offsets: []uint32{0, 1, 2}, Data: []byte("ab"), Validity: validity, DictionaryDomain: "domain", Encoding: EncodingDictionary}
	batch.Strings["binary"] = &StringViewVector{Offsets: []uint32{0, 2, 2}, Data: []byte{0, 255}, Validity: validity, Encoding: EncodingView}
	batch.Int32s["date32"] = &Int32Vector{Values: []int32{12, 0}, Validity: validity}
	batch.Int64s["date64"] = &Int64Vector{Values: []int64{24, 0}, Validity: validity}
	batch.Int64s["timestamp"] = &Int64Vector{Values: []int64{36, 0}, Validity: validity}
	batch.Int32s["time32"] = &Int32Vector{Values: []int32{48, 0}, Validity: validity}
	batch.Int64s["time64"] = &Int64Vector{Values: []int64{60, 0}, Validity: validity}
	batch.Int64s["duration"] = &Int64Vector{Values: []int64{72, 0}, Validity: validity}
	batch.Reset(2)

	record, err := ArrowRecordRange(arrowmem.DefaultAllocator, schema, batch, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer record.Release()
	if record.NumRows() != 2 || record.NumCols() != int64(len(fields)) {
		t.Fatalf("record shape=(%d,%d)", record.NumRows(), record.NumCols())
	}
	for column := range fields {
		if record.Column(column).IsNull(0) || !record.Column(column).IsNull(1) {
			t.Fatalf("column %q validity=%s", fields[column].Name, record.Column(column))
		}
	}
	if value := record.Column(0).(*array.Boolean).Value(0); !value {
		t.Fatal("bool value was false")
	}
	if value := record.Column(11).(*array.String).Value(0); value != "中" {
		t.Fatalf("string value=%q", value)
	}
	if value := record.Column(12).(*array.String).Value(0); value != "b" {
		t.Fatalf("dictionary value=%q", value)
	}
	if value := record.Column(13).(*array.Binary).Value(0); len(value) != 2 || value[0] != 0 || value[1] != 255 {
		t.Fatalf("binary value=%v", value)
	}
}

func TestArrowRecordRangeRejectsUnsupportedNullAndBounds(t *testing.T) {
	if _, err := ArrowSchema(NewSchema(Field{Name: "decimal", Type: TypeDecimal})); err == nil || !strings.Contains(err.Error(), "explicit Arrow type adapter") {
		t.Fatalf("decimal schema error=%v", err)
	}
	batch := NewBatch()
	batch.Int64s["x"] = &Int64Vector{Values: []int64{1}, Validity: Bitmap{0}}
	batch.Reset(1)
	schema := NewSchema(Field{Name: "x", Type: TypeInt64})
	if _, err := ArrowRecord(arrowmem.DefaultAllocator, schema, batch); err == nil || !strings.Contains(err.Error(), "non-nullable field contains NULL") {
		t.Fatalf("non-nullable error=%v", err)
	}
	if _, err := ArrowRecordRange(arrowmem.DefaultAllocator, schema, batch, 1, 1); err == nil || !strings.Contains(err.Error(), "outside 1 rows") {
		t.Fatalf("range error=%v", err)
	}
	if _, err := ArrowRecordRange(nil, schema, batch, 0, 1); err == nil || !strings.Contains(err.Error(), "allocator is nil") {
		t.Fatalf("allocator error=%v", err)
	}
}
