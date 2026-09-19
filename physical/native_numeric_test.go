package physical

import (
	"context"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestNativeFloat32FilterProjectAndTake(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "f", Type: memory.TypeFloat32, Nullable: true})
	valid := memory.EnsureBitmap(nil, 5)
	for _, row := range []int{0, 1, 3, 4} {
		memory.Set(valid, row)
	}
	b := memory.NewBatch()
	b.Float32s["f"] = &memory.Float32Vector{Values: []float32{1, 4, 9, 16, 25}, Validity: valid}
	b.Reset(5)
	base, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(base, expr.GT(expr.Col("f"), expr.F64(8)))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.Col("f"), "alias")}, false)
	if err != nil {
		t.Fatal(err)
	}
	parts, err := projected.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStreamWithPartitions(context.Background(), projected, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)}, parts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	ok, err := stream.Next(context.Background(), out)
	if err != nil || !ok {
		t.Fatalf("next ok=%v err=%v", ok, err)
	}
	v, err := out.Float32("alias")
	if err != nil {
		t.Fatal(err)
	}
	if out.Rows != 2 || len(v.Values) != 2 || v.Values[0] != 16 || v.Values[1] != 25 {
		t.Fatalf("rows=%d values=%v", out.Rows, v.Values)
	}
	if v.Validity == nil || !v.IsValid(0) || !v.IsValid(1) {
		t.Fatalf("filtered valid output lost validity: %v", v.Validity)
	}
	if ok, err := stream.Next(context.Background(), out); ok || err != nil {
		t.Fatalf("expected eof ok=%v err=%v", ok, err)
	}
}

func TestCloneBatchPreservesNativeNumericVectors(t *testing.T) {
	src := memory.NewBatch()
	src.Int8s["i8"] = &memory.Int8Vector{Values: []int8{-1, 2}}
	src.Int16s["i16"] = &memory.Int16Vector{Values: []int16{-3, 4}}
	src.Uint8s["u8"] = &memory.Uint8Vector{Values: []uint8{5, 6}}
	src.Uint16s["u16"] = &memory.Uint16Vector{Values: []uint16{7, 8}}
	src.Float32s["f32"] = &memory.Float32Vector{Values: []float32{1.5, 2.5}}
	src.Reset(2)
	clone, err := cloneBatch(src)
	if err != nil {
		t.Fatal(err)
	}
	if clone.Int8s["i8"].Values[1] != 2 || clone.Int16s["i16"].Values[0] != -3 || clone.Uint8s["u8"].Values[0] != 5 || clone.Uint16s["u16"].Values[1] != 8 || clone.Float32s["f32"].Values[0] != 1.5 {
		t.Fatalf("native clone=%+v", clone)
	}
	clone.Int8s["i8"].Values[0] = 99
	if src.Int8s["i8"].Values[0] == 99 {
		t.Fatal("clone shares native value storage")
	}
}

func TestWindowNativeReaders(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "i8", Type: memory.TypeInt8}, memory.Field{Name: "u16", Type: memory.TypeUint16}, memory.Field{Name: "f32", Type: memory.TypeFloat32})
	b := memory.NewBatch()
	b.Int8s["i8"] = &memory.Int8Vector{Values: []int8{-3}}
	b.Uint16s["u16"] = &memory.Uint16Vector{Values: []uint16{65535}}
	b.Float32s["f32"] = &memory.Float32Vector{Values: []float32{1.25}}
	b.Reset(1)
	if v, ok, err := windowInt64(b, schema, "i8", 0); err != nil || !ok || v != -3 {
		t.Fatalf("i8=(%d,%v,%v)", v, ok, err)
	}
	if v, ok, err := windowUint64(b, schema, "u16", 0); err != nil || !ok || v != 65535 {
		t.Fatalf("u16=(%d,%v,%v)", v, ok, err)
	}
	if v, ok, err := windowFloat64(b, schema, "f32", 0); err != nil || !ok || v != 1.25 {
		t.Fatalf("f32=(%v,%v,%v)", v, ok, err)
	}
}
