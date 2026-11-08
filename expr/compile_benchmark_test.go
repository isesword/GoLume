package expr

import (
	"context"
	"fmt"
	"testing"
)

// Compilation includes binding, structural CSE and physical storage planning.
// Expressions are built outside the timer. Shared diamonds deliberately repeat
// source nodes; this measures the public API, including DAG-aware binding.
func BenchmarkExpressionCompile(b *testing.B) {
	for _, depth := range []int{8, 32, 128, 512} {
		chain := livenessChain(Col("x"), depth, false)
		cases := []struct {
			name    string
			outputs []NamedExpr
		}{
			{"chain", []NamedExpr{Named(chain, "out")}},
			{"outputs", []NamedExpr{Named(chain, "a"), Named(Add(chain, I64(2)), "b"), Named(Mul(chain, I64(3)), "c")}},
			{"regions", []NamedExpr{Named(If(Col("c"), chain, If(Not(Col("c")), chain, I64(0))), "out")}},
		}
		for _, tc := range cases {
			b.Run(fmt.Sprintf("%s/depth=%d", tc.name, depth), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					if _, err := CompileExpressions(livenessSchema(), tc.outputs); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
	for _, depth := range []int{4, 8, 12, 32, 128, 512} {
		var e Expr = Col("x")
		for range depth {
			e = Add(e, e)
		}
		outputs := []NamedExpr{Named(e, "out")}
		b.Run(fmt.Sprintf("diamond/depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := CompileExpressions(livenessSchema(), outputs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Sources and schemas are prepared outside the timer so costs reflect binding,
// compilation, CSE, and storage planning rather than source construction.
func BenchmarkExpressionBindDAG(b *testing.B) {
	for _, depth := range []int{4, 8, 12, 32, 128, 512} {
		var value Expr = Col("x")
		for range depth {
			value = Sub(value, value)
		}
		schema := livenessSchema()
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := Bind(schema, value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkExpressionCompileGuardedDAG(b *testing.B) {
	for _, depth := range []int{4, 8, 12, 32, 128, 512} {
		var value Expr = Col("x")
		for range depth {
			value = If(Col("c"), value, value)
		}
		schema := livenessSchema()
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := Compile(schema, value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkExpressionCompileSharedOutputs(b *testing.B) {
	for _, count := range []int{1, 8, 32} {
		var value Expr = Col("x")
		for range 12 {
			value = Sub(value, value)
		}
		named := make([]NamedExpr, count)
		for i := range named {
			named[i] = Named(value, fmt.Sprint(i))
		}
		schema := livenessSchema()
		b.Run(fmt.Sprintf("outputs=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := CompileExpressions(schema, named); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkExpressionCompileSimple(b *testing.B) {
	schema := livenessSchema()
	for _, tc := range []struct {
		name  string
		value Expr
	}{
		{"column", Col("x")},
		{"comparison", GT(Col("x"), I64(5))},
		{"conditional", If(Col("c"), Col("x"), I64(0))},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := Compile(schema, tc.value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Compiled programs and warmed workspaces are reused, exposing any runtime
// regression caused by sharing region code or its selection metadata.
func BenchmarkExpressionSelectedSteady(b *testing.B) {
	for _, depth := range []int{0, 4, 8} {
		var value Expr = Col("x")
		for range depth {
			value = If(Col("c"), value, value)
		}
		p, err := Compile(livenessSchema(), value)
		if err != nil {
			b.Fatal(err)
		}
		batch := livenessBatch(65, true)
		rows := []int{0, 1, 2, 3, 4, 64}
		ctx, kernels := context.Background(), testKernelSet()
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			w, err := NewWorkspace(ctx, p, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()
			if _, err := w.EvaluateSelected(ctx, batch, rows, kernels); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := w.EvaluateSelected(ctx, batch, rows, kernels); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
