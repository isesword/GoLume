package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func BenchmarkMultiColumnAggregate(b *testing.B) {
	const rows = 65536
	for _, groups := range []int{0, 64, 4096} {
		for _, lanes := range []int{1, 2, 4} {
			b.Run(fmt.Sprintf("groups=%d/lanes=%d", groups, lanes), func(b *testing.B) {
				fields := []memory.Field{{Name: "key", Type: memory.TypeInt32}}
				batch := memory.NewBatch()
				batch.Rows = rows
				keys := make([]int32, rows)
				for i := range keys {
					if groups > 0 {
						keys[i] = int32(i % groups)
					}
				}
				batch.Int32s["key"] = &memory.Int32Vector{Values: keys}
				q := expr.Query{Hints: expr.Hints{Cardinality: max(1, groups)}}
				if groups > 0 {
					q.GroupBy = []string{"key"}
					q.Hints.ForceStrategy = "bitmap-direct"
				}
				for lane := 0; lane < lanes; lane++ {
					name := fmt.Sprintf("v%d", lane)
					fields = append(fields, memory.Field{Name: name, Type: memory.TypeInt64})
					values := make([]int64, rows)
					for i := range values {
						values[i] = int64(i%97 + lane)
					}
					batch.Int64s[name] = &memory.Int64Vector{Values: values}
					q.Aggregates = append(q.Aggregates, expr.Sum(name, name+"sum"))
				}
				q.Aggregates = append(q.Aggregates, expr.Count("rows"))
				src, err := inmemory.New(memory.NewSchema(fields...), batch)
				if err != nil {
					b.Fatal(err)
				}
				opts := DefaultOptions()
				opts.Workers = 1
				opts.PreparedCacheBytes = 0
				e := New(opts)
				defer e.Close()
				p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
				if err != nil {
					b.Fatal(err)
				}
				defer p.Close()
				b.ReportAllocs()
				b.SetBytes(int64(rows * (4 + 8*lanes)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := p.Execute(context.Background(), ExecuteOptions{}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
