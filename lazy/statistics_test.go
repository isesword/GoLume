package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestStatisticsColumnarTypesAndEmpty(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "date", Type: memory.TypeDate32}, memory.Field{Name: "flag", Type: memory.TypeBool})
	for _, empty := range []bool{false, true} {
		var batches []*memory.Batch
		if !empty {
			b := memory.NewBatch()
			b.Rows = 3
			b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64 - 2, math.MaxUint64 - 1, math.MaxUint64}}
			b.Int32s["date"] = &memory.Int32Vector{Values: []int32{10, 20, 30}}
			b.Uint32s["flag"] = &memory.Uint32Vector{Values: []uint32{0, 1, 1}}
			batches = []*memory.Batch{b}
		}
		src, err := inmemory.New(schema, batches...)
		if err != nil {
			t.Fatal(err)
		}
		sess := NewSession(engine.Options{MemoryLimitBytes: 1 << 20, ExecutionPolicy: engine.ExecutionBounded})
		q := sess.FromSource(src).Agg(Var("u", "var"), Std("u", "std"), VarPopulation("u", "pop"), First("date", "date"), Last("flag", "flag"))
		outSchema, err := q.Schema()
		if err != nil {
			t.Fatal(err)
		}
		for i, want := range []memory.DataType{memory.TypeFloat64, memory.TypeFloat64, memory.TypeFloat64, memory.TypeDate32, memory.TypeBool} {
			if outSchema.Fields[i].Type != want {
				t.Fatal(outSchema)
			}
		}
		df, err := q.Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		out := df.batches()[0]
		if out.Rows != 1 || out.Float64s["var"].IsValid(0) == empty || out.Int32s["date"].IsValid(0) == empty || out.Uint32s["flag"].IsValid(0) == empty {
			t.Fatal("empty validity")
		}
		if !empty && (out.Float64s["var"].Values[0] != 1 || out.Float64s["std"].Values[0] != 1 || math.Abs(out.Float64s["pop"].Values[0]-2.0/3) > 1e-12 || out.Int32s["date"].Values[0] != 10 || out.Uint32s["flag"].Values[0] != 1) {
			t.Fatal("stats output")
		}
		df.Close()
		sess.Close()
	}
}

func TestStatisticsFloat32ResultType(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Float32s["f"] = &memory.Float32Vector{Values: []float32{1, 2, 3}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "f", Type: memory.TypeFloat32}), b)
	if err != nil {
		t.Fatal(err)
	}
	sess := NewSession(engine.Options{MemoryLimitBytes: 1 << 20, ExecutionPolicy: engine.ExecutionBounded})
	defer sess.Close()
	q := sess.FromSource(src).Agg(Var("f", "v"), Std("f", "s"), VarPopulation("f", "p"), StdPopulation("f", "ps"))
	schema, err := q.Schema()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range schema.Fields {
		if f.Type != memory.TypeFloat32 {
			t.Fatal(f)
		}
	}
	df, err := q.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	out := df.batches()[0]
	for name, want := range map[string]float32{"v": 1, "s": 1, "p": 2.0 / 3, "ps": float32(math.Sqrt(2.0 / 3))} {
		v := out.Float32s[name]
		if !v.IsValid(0) || v.Values[0] != want {
			t.Fatalf("%s %+v", name, v)
		}
	}
}
