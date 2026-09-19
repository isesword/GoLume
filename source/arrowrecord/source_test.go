package arrowrecord

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestSourceProjectionOpaqueAndDictionaryNormalization(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	rec1 := makeRecord(t, alloc, []int64{1, 10}, []int32{1, 2}, []string{"x", "y"}, []string{"a", "b"}, []uint8{0, 1})
	defer rec1.Release()
	rec2 := makeRecord(t, alloc, []int64{20, 30, 40}, []int32{3, 4, 5}, []string{"z", "w", "q"}, []string{"b", "a", "c"}, []uint8{0, 1, 2})
	defer rec2.Release()

	src, err := NewSource(rec1, rec2)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if got := src.Schema().Fields[3]; got.Type != memory.TypeString || got.Encoding != memory.EncodingDictionary || got.DictionaryDomain == "" {
		t.Fatalf("dictionary field=%+v", got)
	}
	if got := src.Schema().Fields[4]; got.Type != memory.TypeArrow || got.ArrowType == "" {
		t.Fatalf("list field should remain opaque: %+v", got)
	}

	pushed, report, err := src.ApplyPushdown(context.Background(), sourceRequest([]string{"pred", "dict"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Projection) != 2 || report.Projection[0] != "pred" || report.Projection[1] != "dict" {
		t.Fatalf("projection=%v", report.Projection)
	}
	legacy := pushed.(source.LegacyReaderSource)
	r, err := legacy.NewReader(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	parts, _ := pushed.Partitions(context.Background())
	b := memory.NewBatch()
	if err := r.ReadPartition(context.Background(), parts[0], b); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Opaque["list"]; ok {
		t.Fatal("unprojected opaque list was bound")
	}
	d1, err := b.DictionaryString("dict")
	if err != nil {
		t.Fatal(err)
	}
	if d1.Indices[0] != 0 || d1.Indices[1] != 1 {
		t.Fatalf("record1 codes=%v", d1.Indices)
	}
	if err := r.ReadPartition(context.Background(), parts[1], b); err != nil {
		t.Fatal(err)
	}
	d2, err := b.DictionaryString("dict")
	if err != nil {
		t.Fatal(err)
	}
	if want := []uint32{1, 0, 2}; !equalU32(d2.Indices, want) {
		t.Fatalf("record2 normalized codes=%v want=%v", d2.Indices, want)
	}
	if string(d2.DictionaryValue(0)) != "a" || string(d2.DictionaryValue(1)) != "b" || string(d2.DictionaryValue(2)) != "c" {
		t.Fatalf("canonical dictionary=%q,%q,%q", d2.DictionaryValue(0), d2.DictionaryValue(1), d2.DictionaryValue(2))
	}

	pruned, pruning, err := src.ApplyPushdown(context.Background(), sourceRequest([]string{"pred"}, expr.GT(expr.Col("pred"), expr.Lit(15))))
	if err != nil {
		t.Fatal(err)
	}
	parts2, _ := pruned.Partitions(context.Background())
	if pruning.PrunedPartitions != 1 || len(parts2) != 1 || parts2[0].ID != 1 {
		t.Fatalf("pruning=%+v parts=%v", pruning, parts2)
	}
}

func TestArrowListStringUsesSpecializedNestedVector(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	builder := array.NewListBuilderWithField(alloc, arrow.Field{Name: "item", Type: arrow.BinaryTypes.String, Nullable: true})
	values := builder.ValueBuilder().(*array.StringBuilder)
	builder.Append(true)
	values.Append("a")
	values.AppendNull()
	values.Append("中")
	builder.AppendNull()
	builder.Append(true)
	builder.Append(true)
	values.Append("")
	list := builder.NewArray()
	builder.Release()
	defer list.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "tags", Type: list.DataType(), Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{list}, 4)
	defer record.Release()

	src, err := NewSource(record)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	field := src.Schema().Fields[0]
	if field.Type != memory.TypeList || len(field.Children) != 1 || field.Children[0].Type != memory.TypeString || !field.Children[0].Nullable {
		t.Fatalf("list field=%+v", field)
	}
	reader, err := src.NewReader(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	batch := memory.NewBatch()
	if err := reader.ReadPartition(context.Background(), source.Partition{ID: 0, Rows: 4}, batch); err != nil {
		t.Fatal(err)
	}
	if _, opaque := batch.Opaque["tags"]; opaque {
		t.Fatal("list<string> should not remain opaque")
	}
	v, err := batch.StringList("tags")
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Validate(4); err != nil {
		t.Fatal(err)
	}
	if start, end, ok := v.ValueRange(0); !ok || end-start != 3 || string(v.Values.ValueBytes(start)) != "a" || v.Values.IsValid(start+1) || string(v.Values.ValueBytes(start+2)) != "中" {
		t.Fatalf("row 0 range=(%d,%d,%v) child=%+v", start, end, ok, v.Values)
	}
	if v.IsValid(1) || !v.IsValid(2) || !v.IsValid(3) {
		t.Fatalf("parent validity=%v", v.Validity)
	}
}

func TestArrowStructUsesSpecializedNestedVector(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	structType := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "tags", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.BinaryTypes.String, Nullable: true}), Nullable: true},
	)
	builder := array.NewStructBuilder(alloc, structType)
	id := builder.FieldBuilder(0).(*array.Int64Builder)
	name := builder.FieldBuilder(1).(*array.StringBuilder)
	tags := builder.FieldBuilder(2).(*array.ListBuilder)
	tagValues := tags.ValueBuilder().(*array.StringBuilder)
	id.Append(1)
	name.Append("one")
	tags.Append(true)
	tagValues.Append("go")
	tagValues.AppendNull()
	builder.Append(true)
	builder.AppendNull()
	id.Append(3)
	name.AppendNull()
	tags.Append(true)
	builder.Append(true)
	structArray := builder.NewArray().(*array.Struct)
	builder.Release()
	defer structArray.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: structType, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{structArray}, 3)
	defer record.Release()

	src, err := NewSource(record)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	field := src.Schema().Fields[0]
	if field.Type != memory.TypeStruct || len(field.Children) != 3 || field.Children[2].Type != memory.TypeList {
		t.Fatalf("struct field=%+v", field)
	}
	reader, err := src.NewReader(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	batch := memory.NewBatch()
	if err := reader.ReadPartition(context.Background(), source.Partition{ID: 0, Rows: 3}, batch); err != nil {
		t.Fatal(err)
	}
	if _, opaque := batch.Opaque["payload"]; opaque {
		t.Fatal("supported Struct must not remain opaque")
	}
	v, err := batch.Struct("payload")
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Validate(field, 3); err != nil {
		t.Fatal(err)
	}
	if !v.IsValid(0) || v.IsValid(1) || !v.IsValid(2) || v.Values.Strings["name"].IsValid(2) {
		t.Fatalf("struct validity parent=%v name=%v", v.Validity, v.Values.Strings["name"].Validity)
	}
	list := v.Values.StringLists["tags"]
	if start, end, ok := list.ValueRange(0); !ok || end-start != 2 || list.Values.IsValid(start+1) {
		t.Fatalf("list child=%+v range=(%d,%d,%v)", list, start, end, ok)
	}
}

func TestArrowStructRejectsRecursiveChild(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	inner := arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64})
	outer := arrow.StructOf(arrow.Field{Name: "nested", Type: inner})
	builder := array.NewStructBuilder(alloc, outer)
	nested := builder.FieldBuilder(0).(*array.StructBuilder)
	nested.FieldBuilder(0).(*array.Int64Builder).Append(1)
	nested.Append(true)
	builder.Append(true)
	arr := builder.NewArray().(*array.Struct)
	builder.Release()
	defer arr.Release()
	record := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "payload", Type: outer}}, nil), []arrow.Array{arr}, 1)
	defer record.Release()
	if src, err := NewSource(record); err == nil {
		src.Close()
		t.Fatal("recursive Struct was accepted")
	}
}

func sourceRequest(projection []string, predicate expr.Expr) source.PushdownRequest {
	return source.PushdownRequest{Projection: projection, Predicate: predicate}
}

func makeRecord(t *testing.T, alloc arrowmem.Allocator, pred []int64, key []int32, plain, dictValues []string, dictIndices []uint8) arrow.RecordBatch {
	t.Helper()
	predB := array.NewInt64Builder(alloc)
	defer predB.Release()
	predB.AppendValues(pred, nil)
	predA := predB.NewArray()
	defer predA.Release()
	keyB := array.NewInt32Builder(alloc)
	defer keyB.Release()
	keyB.AppendValues(key, nil)
	keyA := keyB.NewArray()
	defer keyA.Release()
	strB := array.NewStringBuilder(alloc)
	defer strB.Release()
	strB.AppendValues(plain, nil)
	strA := strB.NewArray()
	defer strA.Release()
	dictB := array.NewStringBuilder(alloc)
	defer dictB.Release()
	dictB.AppendValues(dictValues, nil)
	dictA := dictB.NewArray()
	defer dictA.Release()
	idxB := array.NewUint8Builder(alloc)
	defer idxB.Release()
	idxB.AppendValues(dictIndices, nil)
	idxA := idxB.NewArray()
	defer idxA.Release()
	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}
	dictCol := array.NewDictionaryArray(dt, idxA, dictA)
	defer dictCol.Release()
	listB := array.NewListBuilder(alloc, arrow.PrimitiveTypes.Int32)
	defer listB.Release()
	vals := listB.ValueBuilder().(*array.Int32Builder)
	for i := range pred {
		listB.Append(true)
		vals.Append(int32(i))
		vals.Append(int32(i + 1))
	}
	listA := listB.NewArray()
	defer listA.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pred", Type: arrow.PrimitiveTypes.Int64}, {Name: "key", Type: arrow.PrimitiveTypes.Int32},
		{Name: "plain", Type: arrow.BinaryTypes.String}, {Name: "dict", Type: dt}, {Name: "list", Type: listA.DataType()},
	}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{predA, keyA, strA, dictCol, listA}, int64(len(pred)))
}

func equalU32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSourceTypedFloatDateTimestampBindings(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	fb := array.NewFloat32Builder(alloc)
	defer fb.Release()
	fb.AppendValues([]float32{1.25, 2.5, 3.75}, nil)
	fa := fb.NewArray()
	defer fa.Release()

	db := array.NewDate32Builder(alloc)
	defer db.Release()
	db.AppendValues([]arrow.Date32{10, 20, 30}, nil)
	da := db.NewArray()
	defer da.Release()

	tst := arrow.FixedWidthTypes.Timestamp_ms.(*arrow.TimestampType)
	tb := array.NewTimestampBuilder(alloc, tst)
	defer tb.Release()
	tb.AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
	ta := tb.NewArray()
	defer ta.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f", Type: arrow.PrimitiveTypes.Float32},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32},
		{Name: "ts", Type: tst},
	}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{fa, da, ta}, 3)
	defer rec.Release()

	src, err := NewSource(rec)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if got, _ := src.Schema().Field("f"); got.Type != memory.TypeFloat32 {
		t.Fatalf("float field=%+v", got)
	}
	if got, _ := src.Schema().Field("d"); got.Type != memory.TypeDate32 {
		t.Fatalf("date field=%+v", got)
	}
	if got, _ := src.Schema().Field("ts"); got.Type != memory.TypeTimestamp {
		t.Fatalf("timestamp field=%+v", got)
	}

	r, err := src.NewReader(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	parts, _ := src.Partitions(context.Background())
	batch := memory.NewBatch()
	if err := r.ReadPartition(context.Background(), parts[0], batch); err != nil {
		t.Fatal(err)
	}
	if len(batch.Opaque) != 0 {
		t.Fatalf("typed columns should not retain opaque bindings: %v", batch.Opaque)
	}
	fv, _ := batch.Float32("f")
	if fv.Values[1] != 2.5 {
		t.Fatalf("float values=%v", fv.Values)
	}
	dv, _ := batch.Int32("d")
	if dv.Values[2] != 30 {
		t.Fatalf("date values=%v", dv.Values)
	}
	tv, _ := batch.Int64("ts")
	if tv.Values[0] != int64(1000)*int64(1e6) {
		t.Fatalf("timestamp values=%v", tv.Values)
	}
}

func TestSourcePreservesNativeArrowWidths(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	i8b := array.NewInt8Builder(alloc)
	i16b := array.NewInt16Builder(alloc)
	u8b := array.NewUint8Builder(alloc)
	u16b := array.NewUint16Builder(alloc)
	f32b := array.NewFloat32Builder(alloc)
	defer i8b.Release()
	defer i16b.Release()
	defer u8b.Release()
	defer u16b.Release()
	defer f32b.Release()
	i8b.AppendValues([]int8{-1, 2}, nil)
	i16b.AppendValues([]int16{-3, 4}, nil)
	u8b.AppendValues([]uint8{5, 6}, nil)
	u16b.AppendValues([]uint16{7, 8}, nil)
	f32b.AppendValues([]float32{1.5, 2.5}, nil)
	arrays := []arrow.Array{i8b.NewArray(), i16b.NewArray(), u8b.NewArray(), u16b.NewArray(), f32b.NewArray()}
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8}, {Name: "i16", Type: arrow.PrimitiveTypes.Int16},
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8}, {Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
	}, nil)
	rec := array.NewRecordBatch(schema, arrays, 2)
	defer rec.Release()
	src, err := NewSource(rec)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	r, err := src.NewReader(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	parts, _ := src.Partitions(context.Background())
	b := memory.NewBatch()
	if err := r.ReadPartition(context.Background(), parts[0], b); err != nil {
		t.Fatal(err)
	}
	if got, _ := b.Int8("i8"); got.Values[0] != -1 {
		t.Fatalf("i8=%v", got.Values)
	}
	if got, _ := b.Int16("i16"); got.Values[0] != -3 {
		t.Fatalf("i16=%v", got.Values)
	}
	if got, _ := b.Uint8("u8"); got.Values[0] != 5 {
		t.Fatalf("u8=%v", got.Values)
	}
	if got, _ := b.Uint16("u16"); got.Values[0] != 7 {
		t.Fatalf("u16=%v", got.Values)
	}
	if got, _ := b.Float32("f32"); got.Values[1] != 2.5 {
		t.Fatalf("f32=%v", got.Values)
	}
}

func TestSourceNormalizesTimestampUnitsAndRejectsTimezone(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	for _, tt := range []struct {
		name string
		unit arrow.TimeUnit
		raw  arrow.Timestamp
		want int64
		zone string
	}{
		{name: "seconds", unit: arrow.Second, raw: 2, want: 2 * int64(1e9)},
		{name: "millis", unit: arrow.Millisecond, raw: 2000, want: 2 * int64(1e9)},
		{name: "micros", unit: arrow.Microsecond, raw: 2000000, want: 2 * int64(1e9)},
		{name: "nanos", unit: arrow.Nanosecond, raw: arrow.Timestamp(2 * 1e9), want: 2 * int64(1e9)},
		{name: "utc", unit: arrow.Millisecond, raw: 2000, want: 2 * int64(1e9), zone: "UTC"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			typ := &arrow.TimestampType{Unit: tt.unit, TimeZone: tt.zone}
			b := array.NewTimestampBuilder(alloc, typ)
			defer b.Release()
			b.Append(tt.raw)
			a := b.NewArray()
			defer a.Release()
			rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "ts", Type: typ}}, nil), []arrow.Array{a}, 1)
			defer rec.Release()
			src, err := NewSource(rec)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			r, err := src.NewReader(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			parts, _ := src.Partitions(context.Background())
			batch := memory.NewBatch()
			if err := r.ReadPartition(context.Background(), parts[0], batch); err != nil {
				t.Fatal(err)
			}
			got, _ := batch.Int64("ts")
			if got.Values[0] != tt.want {
				t.Fatalf("timestamp=%d want=%d", got.Values[0], tt.want)
			}
		})
	}

	typ := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Asia/Shanghai"}
	b := array.NewTimestampBuilder(alloc, typ)
	defer b.Release()
	b.Append(1)
	a := b.NewArray()
	defer a.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "ts", Type: typ}}, nil), []arrow.Array{a}, 1)
	defer rec.Release()
	if _, err := NewSource(rec); err == nil {
		t.Fatal("expected non-UTC timestamp timezone rejection")
	}
}

func TestSourceNormalizesDurationUnitsAndRejectsOverflow(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	for _, tt := range []struct {
		name string
		unit arrow.TimeUnit
		raw  arrow.Duration
		want int64
	}{
		{name: "seconds", unit: arrow.Second, raw: 2, want: 2 * int64(1e9)},
		{name: "millis", unit: arrow.Millisecond, raw: 2000, want: 2 * int64(1e9)},
		{name: "micros", unit: arrow.Microsecond, raw: 2_000_000, want: 2 * int64(1e9)},
		{name: "nanos", unit: arrow.Nanosecond, raw: 2 * arrow.Duration(1e9), want: 2 * int64(1e9)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			typ := &arrow.DurationType{Unit: tt.unit}
			builder := array.NewDurationBuilder(alloc, typ)
			defer builder.Release()
			builder.Append(tt.raw)
			builder.AppendNull()
			arr := builder.NewArray()
			defer arr.Release()
			record := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "duration", Type: typ, Nullable: true}}, nil), []arrow.Array{arr}, 2)
			defer record.Release()
			src, err := NewSource(record)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			reader, err := src.NewReader(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			parts, _ := src.Partitions(context.Background())
			batch := memory.NewBatch()
			if err := reader.ReadPartition(context.Background(), parts[0], batch); err != nil {
				t.Fatal(err)
			}
			got, err := batch.Int64("duration")
			if err != nil {
				t.Fatal(err)
			}
			if got.Values[0] != tt.want || got.IsValid(1) {
				t.Fatalf("duration values=%v validity=%v want=%d and NULL", got.Values, got.Validity, tt.want)
			}
		})
	}

	typ := &arrow.DurationType{Unit: arrow.Second}
	builder := array.NewDurationBuilder(alloc, typ)
	defer builder.Release()
	builder.Append(arrow.Duration(math.MaxInt64))
	arr := builder.NewArray()
	defer arr.Release()
	record := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "duration", Type: typ}}, nil), []arrow.Array{arr}, 1)
	defer record.Release()
	if _, err := NewSource(record); err == nil {
		t.Fatal("expected duration nanosecond normalization overflow")
	}
}
