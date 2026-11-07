package engine_test

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

// BenchmarkGlobalAggregate measures the new bounded path, not a SIMD speedup.
// Input construction and preparation are excluded; each measured execution
// consumes the source and materializes its one result row.
func BenchmarkGlobalAggregate(b *testing.B) {
	for _, rows := range []int{1024, 65536, 1048576} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			batch := memory.NewBatch()
			batch.Rows = rows
			values := make([]int64, rows)
			for i := range values {
				values[i] = 1
			}
			batch.Int64s["v"] = &memory.Int64Vector{Values: values}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), batch)
			if err != nil {
				b.Fatal(err)
			}
			e := engine.New(engine.Options{Workers: 1, MemoryLimitBytes: 64 << 20})
			defer e.Close()
			q, err := e.Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.Count("n")}}, engine.PrepareOptions{MaxConcurrency: 1})
			if err != nil {
				b.Fatal(err)
			}
			defer q.Close()
			b.ReportAllocs()
			b.SetBytes(int64(rows) * 8)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := q.Execute(context.Background(), engine.ExecuteOptions{Materialize: true})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || result.Rows[0].Sum != int64(rows) || result.Rows[0].Count != uint64(rows) {
					b.Fatalf("incorrect global result: %+v", result)
				}
			}
		})
	}
}
