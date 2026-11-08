package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

// The same fixture runs on the pre-widening tree: filtered aggregation includes
// expression dispatch, native promotion, nullable filtering and source batches.
func wideningQuery(tb testing.TB, rows, batchRows int, mixed, nullable, grouped bool) (*inmemory.Source, expr.Query) {
	tb.Helper()
	typ := memory.TypeFloat32
	if mixed {
		typ = memory.TypeInt32
	}
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeFloat32, Nullable: nullable}, memory.Field{Name: "b", Type: typ}, memory.Field{Name: "key", Type: memory.TypeInt32})
	batches := []*memory.Batch{}
	for base := 0; base < rows; base += batchRows {
		n := min(batchRows, rows-base)
		batch := memory.NewBatch()
		batch.Reset(n)
		a, b, i, key := make([]float32, n), make([]float32, n), make([]int32, n), make([]int32, n)
		var valid memory.Bitmap
		if nullable {
			valid = make(memory.Bitmap, memory.WordsFor(n))
		}
		for j := range a {
			r := base + j
			a[j] = float32(r%251) + 0.25
			b[j] = float32(r%31) + 0.75
			i[j] = int32(r%31) + 1
			key[j] = int32(r % 64)
			if nullable && r%17 != 0 {
				memory.Set(valid, j)
			}
		}
		batch.Float32s["a"] = &memory.Float32Vector{Values: a, Validity: valid}
		batch.Int32s["key"] = &memory.Int32Vector{Values: key}
		if mixed {
			batch.Int32s["b"] = &memory.Int32Vector{Values: i}
		} else {
			batch.Float32s["b"] = &memory.Float32Vector{Values: b}
		}
		batches = append(batches, batch)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		tb.Fatal(err)
	}
	value := expr.Col("a").Sub(expr.Col("b")).Mul(expr.Col("a")).Div(expr.Col("b"))
	q := expr.Query{Filter: expr.GT(value, expr.F64(1200)), Aggregates: []expr.Aggregate{expr.Sum("a", "sum"), expr.Count("count")}, Hints: expr.Hints{}}
	if grouped {
		q.GroupBy = []string{"key"}
		q.Hints.Cardinality = 64
		q.Hints.ForceStrategy = "bitmap-direct"
	}
	return src, q
}

func BenchmarkWideningQuery(b *testing.B) {
	for _, mixed := range []bool{false, true} {
		for _, nullable := range []bool{false, true} {
			for _, grouped := range []bool{false, true} {
				for _, batchRows := range []int{1021, 65539} {
					b.Run(fmt.Sprintf("mixed=%t/null=%t/grouped=%t/batch=%d", mixed, nullable, grouped, batchRows), func(b *testing.B) {
						src, q := wideningQuery(b, 65539, batchRows, mixed, nullable, grouped)
						opts := DefaultOptions()
						opts.Workers = 1
						opts.PreparedCacheBytes = 0
						opts.KernelMode = kernel.ModeAuto
						e := New(opts)
						defer e.Close()
						p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
						if err != nil {
							b.Fatal(err)
						}
						defer p.Close()
						if _, err := p.ExecuteTyped(context.Background(), ExecuteOptions{}); err != nil {
							b.Fatal(err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							if _, err := p.ExecuteTyped(context.Background(), ExecuteOptions{}); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			}
		}
	}
}
