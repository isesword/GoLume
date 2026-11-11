package expr

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

// BenchmarkComputedNumericComparison measures the public reusable expression
// workspace, including producing arithmetic/cast operands and validity. Run
// the same fixture on the parent revision to separate dispatch gains from the
// scalar-versus-SIMD comparison within the candidate.
func BenchmarkComputedNumericComparison(b *testing.B) {
	for _, rows := range []int{8, 65536} {
		for _, nullable := range []bool{false, true} {
			batch := memory.NewBatch()
			batch.Rows = rows
			av, bv := make([]int64, rows), make([]int64, rows)
			af, bf := make([]float64, rows), make([]float64, rows)
			var validA, validB memory.Bitmap
			if nullable {
				validA = memory.EnsureBitmap(nil, rows)
				validB = memory.EnsureBitmap(nil, rows)
			}
			for i := range av {
				av[i], bv[i] = int64(i%251), int64((i*7)%251)
				af[i], bf[i] = float64(av[i])+0.25, float64(bv[i])+0.5
				if nullable {
					if i%13 != 0 {
						memory.Set(validA, i)
					}
					if i%17 != 0 {
						memory.Set(validB, i)
					}
				}
			}
			batch.Int64s["a"] = &memory.Int64Vector{Values: av, Validity: validA}
			batch.Int64s["b"] = &memory.Int64Vector{Values: bv, Validity: validB}
			batch.Float64s["f"] = &memory.Float64Vector{Values: af, Validity: validA}
			batch.Float64s["g"] = &memory.Float64Vector{Values: bf, Validity: validB}
			schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: nullable}, memory.Field{Name: "b", Type: memory.TypeInt64, Nullable: nullable}, memory.Field{Name: "f", Type: memory.TypeFloat64, Nullable: nullable}, memory.Field{Name: "g", Type: memory.TypeFloat64, Nullable: nullable})
			for _, tc := range []struct {
				name       string
				expression Expr
			}{
				{"int64-columns", GT(Add(Col("a"), I64(1)), Add(Col("b"), I64(2)))},
				{"int64-literal", GT(Add(Col("a"), I64(1)), I64(127))},
				{"float64-columns", GT(Add(Col("f"), F64(1)), Add(Col("g"), F64(2)))},
				{"float64-literal", GT(Add(Col("f"), F64(1)), F64(127))},
				{"int32-columns", GT(CastTo(Col("a"), memory.TypeInt32), CastTo(Col("b"), memory.TypeInt32))},
				{"float32-columns", GT(CastTo(Col("f"), memory.TypeFloat32), CastTo(Col("g"), memory.TypeFloat32))},
			} {
				program, err := Compile(schema, tc.expression)
				if err != nil {
					b.Fatal(err)
				}
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
					b.Run(fmt.Sprintf("%s/rows=%d/null=%t/mode=%s", tc.name, rows, nullable, mode), func(b *testing.B) {
						ctx := context.Background()
						manager := memory.NewBudgetManager(0)
						workspace, err := NewWorkspace(ctx, program, manager)
						if err != nil {
							b.Fatal(err)
						}
						kernels := kernel.New(mode)
						if _, err := workspace.EvaluateBool(ctx, batch, kernels); err != nil {
							b.Fatal(err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							out, err := workspace.EvaluateBool(ctx, batch, kernels)
							if err != nil || len(out.Truth) != memory.WordsFor(rows) {
								b.Fatalf("evaluation: %v", err)
							}
						}
						b.StopTimer()
						b.ReportMetric(float64(workspace.MemoryBytes()), "retained-B")
						workspace.Close()
						if manager.Used() != 0 {
							b.Fatal("workspace leaked")
						}
					})
				}
			}
		}
	}
}
