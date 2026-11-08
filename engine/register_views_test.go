package engine

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func prepareRegisterViewsQuery(tb testing.TB, rows, batchRows, depth int, mode kernel.Mode) (*Engine, *PreparedQuery, uint64, uint64) {
	tb.Helper()
	var value expr.Expr = expr.Col("x")
	for range depth {
		value = expr.Add(value, expr.I64(1))
	}
	predicate := expr.GT(value, expr.I64(int64(50+depth)))
	var batches []*memory.Batch
	var wantCount, wantSum uint64
	for base := 0; base < rows; base += batchRows {
		n := min(batchRows, rows-base)
		batch := memory.NewBatch()
		batch.Rows = n
		x, id := make([]int64, n), make([]int32, n)
		valid := make(memory.Bitmap, memory.WordsFor(n))
		for row := range n {
			ordinal := base + row
			x[row], id[row] = int64(ordinal%100), int32(ordinal)
			if ordinal%11 != 3 {
				memory.Set(valid, row)
				if x[row] > 50 {
					wantCount++
					wantSum += uint64(id[row])
				}
			}
		}
		batch.Int64s["x"] = &memory.Int64Vector{Values: x, Validity: valid}
		batch.Int32s["id"] = &memory.Int32Vector{Values: id}
		batches = append(batches, batch)
	}
	source, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt32}), batches...)
	if err != nil {
		tb.Fatal(err)
	}
	opts := DefaultOptions()
	opts.Workers, opts.Pipeline.Workers = 1, 1
	opts.PreparedCacheBytes = 0
	opts.KernelMode = mode
	e := New(opts)
	p, err := e.Prepare(context.Background(), source, expr.Query{Filter: predicate, Aggregates: []expr.Aggregate{expr.Count("n"), expr.Sum("id", "sum")}}, PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		e.Close()
		tb.Fatal(err)
	}
	return e, p, wantCount, wantSum
}

func TestRegisterViewsWholeQueries(t *testing.T) {
	for _, depth := range []int{0, 8, 128} {
		for _, rows := range []int{0, 1, 65, 4097} {
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				t.Run(fmt.Sprintf("depth=%d/rows=%d/mode=%s", depth, rows, mode), func(t *testing.T) {
					e, p, count, sum := prepareRegisterViewsQuery(t, rows, 65, depth, mode)
					defer e.Close()
					defer p.Close()
					for range 2 {
						verifyLiteralInQuery(t, e, p, count, sum)
					}
				})
			}
		}
	}
}

// BenchmarkRegisterViewsWholeQuery measures the complete prepared execution:
// source iteration, nullable predicate, Count/Sum and budget cleanup. Preparation
// is excluded and the shallow predicate controls ordinary query overhead.
func BenchmarkRegisterViewsWholeQuery(b *testing.B) {
	for _, depth := range []int{0, 8, 128} {
		for _, batchRows := range []int{65, 4096} {
			b.Run(fmt.Sprintf("depth=%d/batch=%d", depth, batchRows), func(b *testing.B) {
				e, p, count, sum := prepareRegisterViewsQuery(b, 8192, batchRows, depth, kernel.ModeAuto)
				defer e.Close()
				defer p.Close()
				peak := verifyLiteralInQuery(b, e, p, count, sum)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					verifyLiteralInQuery(b, e, p, count, sum)
				}
				b.StopTimer()
				b.ReportMetric(float64(peak), "peak-managed-B")
			})
		}
	}
}
