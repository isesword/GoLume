package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestNativeAggregateDtypesAndComposition(t *testing.T) {
	for _, empty := range []bool{false, true} {
		schema := memory.NewSchema(memory.Field{Name: "small", Type: memory.TypeInt8}, memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "f", Type: memory.TypeFloat32})
		var batches []*memory.Batch
		if !empty {
			b := memory.NewBatch()
			b.Rows = 2
			b.Int8s["small"] = &memory.Int8Vector{Values: []int8{-128, 127}}
			b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64 - 2, 1}}
			b.Float32s["f"] = &memory.Float32Vector{Values: []float32{2, 4}}
			batches = []*memory.Batch{b}
		}
		src, err := inmemory.New(schema, batches...)
		if err != nil {
			t.Fatal(err)
		}
		session := NewSession(engine.Options{MemoryLimitBytes: 1 << 20, ExecutionPolicy: engine.ExecutionBounded})
		q := session.FromSource(src).Agg(Sum("small", "sum_small"), Min("small", "min_small"), Sum("u", "sum_u"), Min("u", "min_u"), Sum("f", "sum_f"), Avg("f", "avg_f"))
		outSchema, err := q.Schema()
		if err != nil {
			t.Fatal(err)
		}
		want := []memory.DataType{memory.TypeInt64, memory.TypeInt8, memory.TypeUint64, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat32}
		for i, field := range outSchema.Fields {
			if field.Type != want[i] {
				t.Fatalf("field %s dtype %s", field.Name, field.Type)
			}
		}
		df, err := q.Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		out := df.batches()[0]
		if out.Rows != 1 || out.Int64s["sum_small"].IsValid(0) == empty || out.Uint64s["sum_u"].IsValid(0) == empty || out.Float32s["avg_f"].IsValid(0) == empty {
			t.Fatal("native empty validity")
		}
		if !empty && (out.Int64s["sum_small"].Values[0] != -1 || out.Int8s["min_small"].Values[0] != -128 || out.Uint64s["sum_u"].Values[0] != math.MaxUint64-1 || out.Float32s["avg_f"].Values[0] != 3) {
			t.Fatal("native values")
		}
		df.Close()
		session.Close()
	}
}
