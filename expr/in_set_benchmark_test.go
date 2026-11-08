package expr

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/memory"
)

// Fixtures deliberately use pre-optimization APIs. Compile/setup is excluded
// from execution timing. Cold includes workspace admission/evaluation/Close;
// warm keeps its buffers. Managed bytes are allowances, not heap/RSS metrics.
func literalInBenchFixture(tb testing.TB, kind, hit string, count, rows int) (Program, *memory.Batch) {
	tb.Helper()
	candidates := make([]Expr, count)
	for i := range candidates {
		candidates[i] = I64(int64(i))
		if kind == "string" {
			candidates[i] = StringLiteral{Value: fmt.Sprintf("key-%06d", i)}
		}
	}
	b := memory.NewBatch()
	b.Rows = rows
	typ := memory.TypeInt64
	values := make([]int64, rows)
	strings := make([]string, rows)
	for row := range rows {
		v := count + 1
		switch hit {
		case "first":
			v = 0
		case "last":
			v = count - 1
		case "mixed":
			v = row % (count * 2)
		}
		values[row] = int64(v)
		strings[row] = fmt.Sprintf("key-%06d", v)
	}
	if kind == "string" {
		typ = memory.TypeString
		b.Strings["x"] = phase32StringVector(strings, nil)
	} else {
		b.Int64s["x"] = &memory.Int64Vector{Values: values}
	}
	p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: typ}), In(Col("x"), candidates...))
	if err != nil {
		tb.Fatal(err)
	}
	return p, b
}

func BenchmarkLiteralIn(b *testing.B) {
	for _, kind := range []string{"int64", "string"} {
		for _, count := range []int{4, 15, 16, 64, 256, 1024} {
			for _, hit := range []string{"first", "miss", "mixed"} {
				for _, warm := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/count=%d/%s/warm=%v", kind, count, hit, warm), func(b *testing.B) {
						p, batch := literalInBenchFixture(b, kind, hit, count, 4096)
						m := memory.NewBudgetManager(0)
						w, err := NewWorkspace(context.Background(), p, m)
						if err != nil {
							b.Fatal(err)
						}
						defer func() { w.Close() }()
						kernels := testKernelSet()
						if _, err = w.EvaluateBool(context.Background(), batch, kernels); err != nil {
							b.Fatal(err)
						}
						retained := w.MemoryBytes()
						if !warm {
							w.Close()
							w, err = NewWorkspace(context.Background(), p, m)
							if err != nil {
								b.Fatal(err)
							}
						}
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							if _, err = w.EvaluateBool(context.Background(), batch, kernels); err != nil {
								b.Fatal(err)
							}
							if !warm {
								w.Close()
								w, err = NewWorkspace(context.Background(), p, m)
								if err != nil {
									b.Fatal(err)
								}
							}
						}
						b.StopTimer()
						b.ReportMetric(float64(retained), "retained-managed-B")
					})
				}
			}
		}
	}
}

func BenchmarkLiteralInCompile(b *testing.B) {
	for _, count := range []int{4, 16, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			candidates := make([]Expr, count)
			for i := range candidates {
				candidates[i] = I64(int64(i))
			}
			schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
			e := In(Col("x"), candidates...)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := Compile(schema, e); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
