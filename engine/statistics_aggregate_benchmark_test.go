package engine

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func BenchmarkStatisticsAggregate(b *testing.B) {
	const rows = 4096
	batch := memory.NewBatch()
	batch.Rows = rows
	k := make([]int32, rows)
	v := make([]uint64, rows)
	for i := range k {
		k[i] = int32(i % 64)
		v[i] = 1<<63 + uint64(i%97)
	}
	batch.Int32s["k"] = &memory.Int32Vector{Values: k}
	batch.Uint64s["v"] = &memory.Uint64Vector{Values: v}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeUint64}), batch)
	if err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"bitmap-direct", "partitioned-hash"} {
		b.Run(mode, func(b *testing.B) {
			opts := DefaultOptions()
			opts.Workers = 2
			opts.PreparedCacheBytes = 0
			e := New(opts)
			defer e.Close()
			p, err := e.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Var("v", "var"), expr.Std("v", "std"), expr.First("v", "first"), expr.Last("v", "last")}, Hints: expr.Hints{Cardinality: 64, ForceStrategy: mode}}, PrepareOptions{})
			if err != nil {
				b.Fatal(err)
			}
			defer p.Close()
			b.ReportAllocs()
			b.SetBytes(rows * 12)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
				if err != nil || len(r.Rows) != 64 {
					b.Fatalf("result groups%d error%v", len(r.Rows), err)
				}
			}
		})
	}
}
