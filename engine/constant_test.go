package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

// These fixtures use public baseline APIs. The complete prepared execution,
// source batches, filter, aggregate result and budget cleanup are measured.
func prepareCompactLiteralQuery(tb testing.TB, kind string, rows, batchRows int, mode kernel.Mode) (*Engine, *PreparedQuery, uint64, uint64) {
	tb.Helper()
	needle := strings.Repeat("long-needle-", 22)
	var predicate expr.Expr
	switch kind {
	case "numeric_compare":
		predicate = expr.GT(expr.Add(expr.Col("x"), expr.I64(1)), expr.I64(50))
	case "string_slice":
		predicate = expr.EQ(expr.Slice(expr.Col("s"), expr.I64(0), expr.I64(5)), expr.Str("match"))
	case "string_find":
		predicate = expr.GE(expr.Find(expr.Concat(expr.Col("s"), expr.Str("!")), expr.Str(needle)), expr.I64(0))
	case "dense_control":
		predicate = expr.GT(expr.Add(expr.Col("x"), expr.I64(1)), expr.Col("y"))
	default:
		tb.Fatal("unknown fixture", kind)
	}
	var batches []*memory.Batch
	var wantCount, wantSum uint64
	for base := 0; base < rows; base += batchRows {
		n := min(batchRows, rows-base)
		b := memory.NewBatch()
		b.Rows = n
		x, y, id := make([]int64, n), make([]int64, n), make([]int32, n)
		valid := make(memory.Bitmap, memory.WordsFor(n))
		offsets := make([]uint32, n+1)
		var data []byte
		for row := range n {
			ordinal := base + row
			x[row], y[row], id[row] = int64(ordinal%100), 50, int32(ordinal)
			known := ordinal%11 != 3
			if known {
				memory.Set(valid, row)
			}
			text := "other"
			selected := ordinal%2 == 0
			if selected {
				text = "match"
			}
			if kind == "string_find" && selected {
				text = needle
			}
			data = append(data, text...)
			offsets[row+1] = uint32(len(data))
			if kind == "numeric_compare" || kind == "dense_control" {
				selected = x[row]+1 > 50
			}
			if known && selected {
				wantCount++
				wantSum += uint64(id[row])
			}
		}
		b.Int64s["x"] = &memory.Int64Vector{Values: x, Validity: valid}
		b.Int64s["y"] = &memory.Int64Vector{Values: y}
		b.Int32s["id"] = &memory.Int32Vector{Values: id}
		b.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: valid}
		batches = append(batches, b)
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "y", Type: memory.TypeInt64}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt32}), batches...)
	if err != nil {
		tb.Fatal(err)
	}
	opts := DefaultOptions()
	opts.Workers = 1
	opts.Pipeline.Workers = 1
	opts.PreparedCacheBytes = 0
	opts.KernelMode = mode
	e := New(opts)
	p, err := e.Prepare(context.Background(), src, expr.Query{Filter: predicate, Aggregates: []expr.Aggregate{expr.Count("n"), expr.Sum("id", "sum")}}, PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		e.Close()
		tb.Fatal(err)
	}
	return e, p, wantCount, wantSum
}

func TestCompactLiteralWholeQueries(t *testing.T) {
	for _, kind := range []string{"numeric_compare", "string_slice", "string_find", "dense_control"} {
		for _, rows := range []int{0, 1, 65, 4097} {
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
				t.Run(fmt.Sprintf("%s/rows=%d/mode=%s", kind, rows, mode), func(t *testing.T) {
					e, p, n, sum := prepareCompactLiteralQuery(t, kind, rows, 127, mode)
					defer e.Close()
					defer p.Close()
					for range 2 {
						verifyLiteralInQuery(t, e, p, n, sum)
					}
				})
			}
		}
	}
}

func BenchmarkCompactLiteralWholeQuery(b *testing.B) {
	for _, kind := range []string{"numeric_compare", "string_slice", "string_find", "dense_control"} {
		for _, batchRows := range []int{1021, 32771} {
			b.Run(fmt.Sprintf("%s/batch=%d", kind, batchRows), func(b *testing.B) {
				e, p, n, sum := prepareCompactLiteralQuery(b, kind, 32771, batchRows, kernel.ModeAuto)
				defer e.Close()
				defer p.Close()
				peak := verifyLiteralInQuery(b, e, p, n, sum)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					verifyLiteralInQuery(b, e, p, n, sum)
				}
				b.StopTimer()
				b.ReportMetric(float64(peak), "peak-managed-B")
			})
		}
	}
}
