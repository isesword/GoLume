package engine

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func BenchmarkBytesStatisticsAggregate(b *testing.B) {
	const rows = 4096
	batch := memory.NewBatch()
	batch.Rows = rows
	keys := make([]int32, rows)
	offsets := make([]uint32, rows+1)
	data := make([]byte, rows*32)
	for i := range keys {
		keys[i] = int32(i % 64)
		offsets[i] = uint32(i * 32)
		for j := 0; j < 32; j++ {
			data[i*32+j] = byte(i)
		}
	}
	offsets[rows] = uint32(len(data))
	batch.Int32s["k"] = &memory.Int32Vector{Values: keys}
	batch.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "s", Type: memory.TypeString}), batch)
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
			p, err := e.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.First("s", "first"), expr.Last("s", "last")}, Hints: expr.Hints{Cardinality: 64, ForceStrategy: mode}}, PrepareOptions{})
			if err != nil {
				b.Fatal(err)
			}
			defer p.Close()
			b.ReportAllocs()
			b.SetBytes(rows * 36)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
				if err != nil || len(r.Rows) != 64 {
					b.Fatalf("groups%d error%v", len(r.Rows), err)
				}
			}
		})
	}
}
