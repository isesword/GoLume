package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func TestMultiColumnAggregateColumnarAndComposition(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		t.Run(strategy, func(t *testing.T) {
			batch := memory.NewBatch()
			batch.Rows = 6
			batch.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 2, 3, 3}}
			batch.Int64s["a"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4, 5, 6}}
			batch.Int64s["b"] = &memory.Int64Vector{Values: []int64{10, 20, 30, 40, 50, 60}}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64}), batch)
			if err != nil {
				t.Fatal(err)
			}
			opts := engine.Options{Workers: 3, MemoryLimitBytes: 1 << 20}
			hint := strategy
			if strategy == "sorted" {
				src.WithOrdering(source.OrderingColumn{Name: "key"})
			}
			if strategy == "spill" {
				hint = ""
				opts.ExecutionPolicy = engine.ExecutionBounded
				opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
			}
			session := NewSession(opts)
			defer session.Close()
			f := session.FromSource(src).WithHints(expr.Hints{ForceStrategy: hint}).GroupBy("key").Agg(Sum("a", "sa"), Sum("b", "sb"), Count("n"))
			for repeat := 0; repeat < 2; repeat++ {
				result, err := f.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				seen := 0
				for _, b := range result.batches() {
					for row := 0; row < b.Rows; row++ {
						key := int64(b.Int32s["key"].Values[row])
						want := 4*key - 1
						if b.Int64s["sa"].Values[row] != want || b.Int64s["sb"].Values[row] != want*10 || b.Uint64s["n"].Values[row] != 2 {
							t.Fatalf("group %d mismatch", key)
						}
						seen++
					}
				}
				if seen != 3 {
					t.Fatalf("groups %d", seen)
				}
				result.Close()
			}
			result, err := f.Agg(Sum("sa", "grand_a"), Sum("sb", "grand_b")).Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer result.Close()
			out := result.batches()[0]
			if out.Rows != 1 || out.Int64s["grand_a"].Values[0] != 21 || out.Int64s["grand_b"].Values[0] != 210 {
				t.Fatal("nested multi aggregate mismatch")
			}
		})
	}
}
