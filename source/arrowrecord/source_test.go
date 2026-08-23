package arrowrecord

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
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
	r, err := pushed.NewReader(context.Background())
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

	r, err := src.NewReader(context.Background())
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
	fv, _ := batch.Float64("f")
	if fv.Values[1] != 2.5 {
		t.Fatalf("float values=%v", fv.Values)
	}
	dv, _ := batch.Int32("d")
	if dv.Values[2] != 30 {
		t.Fatalf("date values=%v", dv.Values)
	}
	tv, _ := batch.Int64("ts")
	if tv.Values[0] != 1000 {
		t.Fatalf("timestamp values=%v", tv.Values)
	}
}
