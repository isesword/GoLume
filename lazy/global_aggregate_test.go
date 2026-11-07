package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestGlobalAggregateColumnarOutput(t *testing.T) {
	for _, n := range []int{0, 3} {
		b := memory.NewBatch()
		b.Rows = n
		b.Int64s["v"] = &memory.Int64Vector{Values: []int64{2, 3, 7}[:n]}
		src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), b)
		if err != nil {
			t.Fatal(err)
		}
		session := NewSession(engine.Options{MemoryLimitBytes: 1 << 20, ExecutionPolicy: engine.ExecutionBounded})
		f := session.FromSource(src).Agg(Sum("v", "total"), Count("n"), Avg("v", "average"))
		schema, err := f.Schema()
		if err != nil {
			t.Fatal(err)
		}
		if len(schema.Fields) != 3 {
			t.Fatalf("schema=%+v", schema)
		}
		for repeat := 0; repeat < 2; repeat++ {
			df, err := f.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			batches := df.batches()
			if len(batches) != 1 || batches[0].Rows != 1 {
				t.Fatalf("batches=%+v", batches)
			}
			out := batches[0]
			if out.Uint64s["n"].Values[0] != uint64(n) {
				t.Fatalf("count=%v", out.Uint64s["n"])
			}
			if out.Int64s["total"].IsValid(0) != (n > 0) || out.Float64s["average"].IsValid(0) != (n > 0) {
				t.Fatal("wrong NULL identity")
			}
			if n > 0 && (out.Int64s["total"].Values[0] != 12 || out.Float64s["average"].Values[0] != 4) {
				t.Fatal("wrong aggregate values")
			}
			df.Close()
		}
		session.Close()
	}
}
