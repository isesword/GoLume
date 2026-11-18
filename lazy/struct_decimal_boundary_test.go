package lazy

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
)

func TestStructConstructRejectsNativeDecimalChild(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	dt := &arrow.Decimal128Type{Precision: 9, Scale: 2}
	b := array.NewDecimal128Builder(alloc, dt)
	b.Append(decimal128.FromI64(123))
	b.AppendNull()
	a := b.NewArray()
	b.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "d", Type: dt, Nullable: true}}, nil), []arrow.Array{a}, 2)
	a.Release()
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	q, err := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, rec)
	rec.Release()
	if err != nil {
		t.Fatal(err)
	}
	input, err := q.Schema()
	if err != nil {
		t.Fatal(err)
	}
	f, ok := input.Field("d")
	if !ok || f.DecimalPrecision != 9 || f.DecimalScale != 2 {
		t.Fatal("top-level native Decimal lost", f)
	}
	// Top-level Decimal remains usable and exactly collected on the same source.
	df, err := q.SelectExprs(Col("d").Alias("out")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	col, err := df.Column("out")
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := col.Decimal128Chunks()
	if err != nil || len(chunks) != 1 {
		t.Fatal("native Decimal chunks", err)
	}
	value, valid, err := chunks[0].Value(0)
	if err != nil || !valid || value != decimal128.FromI64(123) {
		t.Fatal("native Decimal coefficient", value, valid, err)
	}
	_, valid, err = chunks[0].Value(1)
	if err != nil || valid {
		t.Fatal("native Decimal NULL", valid, err)
	}
	df.Close()
	if _, err := q.SelectExprs(StructFrom(Col("d").Alias("child")).Alias("record")).Schema(); err == nil {
		t.Fatal("Decimal child accepted by StructFrom")
	}
	// A forged parent schema must not let StructGet expand N1's supported types.
	child := f
	child.Name = "child"
	parent := memory.StructField("record", true, child)
	if _, err := expr.Compile(memory.NewSchema(parent), StructGet(Col("record"), "child")); err == nil {
		t.Fatal("Decimal child accepted by StructGet")
	}
	// N7 admits this existing recursive profile, while nested Decimal remains rejected above.
	rebuilt, err := q.SelectExprs(StructFrom(StructFrom(expr.I64(1).Alias("x")).Alias("inner")).Alias("record")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rebuiltColumn, err := rebuilt.Column("record")
	if err != nil {
		rebuilt.Close()
		t.Fatal(err)
	}
	values, err := rebuiltColumn.ToAnySlice()
	expected := []any{map[string]any{"inner": map[string]any{"x": int64(1)}}, map[string]any{"inner": map[string]any{"x": int64(1)}}}
	if err != nil || !reflect.DeepEqual(values, expected) {
		rebuilt.Close()
		t.Fatal("recursive StructFrom values", values, err)
	}
	rebuilt.Close()
	var deep expr.Expr = expr.I64(1)
	for depth := 0; depth < 9; depth++ {
		deep = StructFrom(expr.Named(deep, "child"))
	}
	if _, err := q.SelectExprs(expr.Named(deep, "record")).Schema(); err == nil {
		t.Fatal("depth9 StructFrom accepted")
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("boundary checks leaked query memory", s.Engine().Memory().Used())
	}
}
