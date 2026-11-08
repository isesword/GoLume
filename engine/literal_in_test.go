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

// Complete prepared query fixture, with real source batches, predicate
// execution, aggregate output, and query-owned budget cleanup in the timer.
// Only public baseline APIs are used for paired before/after measurements.
func prepareLiteralInQuery(tb testing.TB, kind string, count, rows, batchRows int, nullCandidate, not bool, mode kernel.Mode) (*Engine, *PreparedQuery, uint64, uint64) {
	tb.Helper()
	typ := memory.TypeInt64
	if kind == "string" {
		typ = memory.TypeString
	}
	candidates := make([]expr.Expr, count)
	for i := range candidates {
		if kind == "string" {
			candidates[i] = expr.StringLiteral{Value: fmt.Sprintf("key-%06d", i)}
		} else {
			candidates[i] = expr.I64(int64(i))
		}
	}
	if nullCandidate {
		candidates = append(candidates, expr.Null(typ))
	}
	predicate := expr.Expr(expr.In(expr.Col("x"), candidates...))
	if not {
		predicate = expr.Not(predicate)
	}
	var batches []*memory.Batch
	var wantCount, wantSum uint64
	for start := 0; start < rows; start += batchRows {
		n := min(batchRows, rows-start)
		b := memory.NewBatch()
		b.Rows = n
		v := make([]int64, n)
		id := make([]int32, n)
		valid := make(memory.Bitmap, memory.WordsFor(n))
		offsets := make([]uint32, n+1)
		var data []byte
		for row := range n {
			ordinal := start + row
			v[row] = int64(ordinal % (count * 2))
			id[row] = int32(ordinal)
			isValid := ordinal%11 != 3
			if isValid {
				memory.Set(valid, row)
			}
			if kind == "string" {
				data = append(data, fmt.Sprintf("key-%06d", v[row])...)
				offsets[row+1] = uint32(len(data))
			}
			selected := v[row] < int64(count)
			if not {
				selected = !selected && !nullCandidate
			}
			if selected && isValid {
				wantCount++
				wantSum += uint64(id[row])
			}
		}
		if kind == "string" {
			b.Strings["x"] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: valid}
		} else {
			b.Int64s["x"] = &memory.Int64Vector{Values: v, Validity: valid}
		}
		b.Int32s["id"] = &memory.Int32Vector{Values: id}
		batches = append(batches, b)
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: typ, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt32}), batches...)
	if err != nil {
		tb.Fatal(err)
	}
	opts := DefaultOptions()
	opts.Workers = 1
	opts.Pipeline.Workers = 1
	opts.PreparedCacheBytes = 0
	opts.KernelMode = mode
	opts.MemoryLimitBytes = 512 << 20
	opts.Pipeline.MaxMorselRows = 65536
	e := New(opts)
	p, err := e.Prepare(context.Background(), src, expr.Query{Filter: predicate, Aggregates: []expr.Aggregate{expr.Count("n"), expr.Sum("id", "sum")}}, PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		e.Close()
		tb.Fatal(err)
	}
	return e, p, wantCount, wantSum
}

func verifyLiteralInQuery(tb testing.TB, e *Engine, p *PreparedQuery, wantCount, wantSum uint64) int64 {
	tb.Helper()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		tb.Fatal(err)
	}
	result, err := p.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
	peak := budget.Snapshot().PeakBytes
	budget.Close()
	if err != nil {
		tb.Fatal(err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0].Aggregates) != 2 {
		tb.Fatalf("bad result: %+v", result)
	}
	a := result.Rows[0].Aggregates
	if a[0].Value.Bits != wantCount || !a[0].Value.Valid || a[1].Value.Bits != wantSum || a[1].Value.Valid != (wantCount > 0) {
		tb.Fatalf("got=%+v want count=%d sum=%d", a, wantCount, wantSum)
	}
	if budget.Used() != 0 || e.Memory().Used() != 0 {
		tb.Fatal("query memory leaked")
	}
	return peak
}

func TestLiteralInWholeQueries(t *testing.T) {
	for _, kind := range []string{"int64", "string"} {
		for _, count := range []int{4, 15, 16, 64, 1024} {
			for _, size := range [][2]int{{0, 63}, {1, 63}, {65, 63}, {4097, 127}} {
				for _, nulls := range []bool{false, true} {
					for _, not := range []bool{false, true} {
						for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
							t.Run(fmt.Sprintf("%s/count=%d/rows=%d/null=%v/not=%v/mode=%v", kind, count, size[0], nulls, not, mode), func(t *testing.T) {
								e, p, n, sum := prepareLiteralInQuery(t, kind, count, size[0], size[1], nulls, not, mode)
								defer e.Close()
								defer p.Close()
								verifyLiteralInQuery(t, e, p, n, sum)
								verifyLiteralInQuery(t, e, p, n, sum)
							})
						}
					}
				}
			}
		}
	}
}

func BenchmarkLiteralInWholeQuery(b *testing.B) {
	for _, kind := range []string{"int64", "string"} {
		for _, count := range []int{4, 16, 64, 256, 1024} {
			for _, size := range [][2]int{{32771, 1021}, {32771, 32771}} {
				b.Run(fmt.Sprintf("%s/count=%d/rows=%d/batch=%d", kind, count, size[0], size[1]), func(b *testing.B) {
					e, p, n, sum := prepareLiteralInQuery(b, kind, count, size[0], size[1], false, false, kernel.ModeAuto)
					defer e.Close()
					defer p.Close()
					peak := verifyLiteralInQuery(b, e, p, n, sum)
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						budget, err := e.BeginQueryMemory()
						if err != nil {
							b.Fatal(err)
						}
						result, err := p.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
						budget.Close()
						if err != nil {
							b.Fatal(err)
						}
						globalBindingResultSink = result
					}
					b.StopTimer()
					b.ReportMetric(float64(peak), "peak-managed-B")
				})
			}
		}
	}
}
