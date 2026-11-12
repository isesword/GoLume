package lazy

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestInt64ListSeriesLifecycle(t *testing.T) {
	b := memory.NewBatch()
	b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: []uint32{0, 2, 2, 2}, Values: &memory.Int64Vector{Values: []int64{7, -9}, Validity: memory.Bitmap{1}}, Validity: memory.Bitmap{3}}
	b.Reset(3)
	src, err := inmemory.New(memory.NewSchema(memory.ListInt64Field("xs", true, true)), b)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	f, err := s.FromSource(src).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	col, err := f.Column("xs")
	if err != nil {
		t.Fatal(err)
	}
	values, valid, err := col.CopyInt64Lists()
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || len(values[0]) != 2 || values[0][0].Value != 7 || !values[0][0].Valid || values[0][1].Valid || values[1] == nil || values[2] != nil || !memory.IsSet(valid, 1) || memory.IsSet(valid, 2) {
		t.Fatalf("values=%v validity=%v", values, valid)
	}
	anyValues, err := col.ToAnySlice()
	if err != nil {
		t.Fatal(err)
	}
	if anyValues[2] != nil || anyValues[0].([]any)[1] != nil {
		t.Fatalf("any=%v", anyValues)
	}
	if col.NullCount() != 1 {
		t.Fatal("wrong null count")
	}
	chunks, err := col.Int64ListChunks()
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := chunks[0].Value(0)
	if err != nil || !ok {
		t.Fatal(err)
	}
	f.Close()
	if _, _, err := value.Element(0); !errors.Is(err, ErrDataFrameClosed) {
		t.Fatalf("after close: %v", err)
	}
	if _, _, err := col.CopyInt64Lists(); !errors.Is(err, ErrDataFrameClosed) {
		t.Fatalf("copy after close: %v", err)
	}
}

func TestInt64ListFilterSortProject(t *testing.T) {
	b := memory.NewBatch()
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}}
	b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: []uint32{0, 1, 1, 3, 3}, Values: &memory.Int64Vector{Values: []int64{1, 30, 31}, Validity: memory.Bitmap{3}}, Validity: memory.Bitmap{13}}
	b.Reset(4)
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.ListInt64Field("xs", true, true))
	src, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	f, err := s.FromSource(src).Filter(Col("id").Gt(Lit(1))).Sort(Desc("id")).Limit(3).SelectExprs(Col("xs").Alias("result")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	col, err := f.Column("result")
	if err != nil {
		t.Fatal(err)
	}
	got, valid, err := col.CopyInt64Lists()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] == nil || len(got[0]) != 0 || len(got[1]) != 2 || got[1][0].Value != 30 || got[1][1].Valid || memory.IsSet(valid, 2) {
		t.Fatalf("values=%v validity=%v", got, valid)
	}
}
