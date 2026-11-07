package expr

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func BenchmarkMathExpression(b *testing.B) {
	for _, rows := range []int{64, 1024, 65536} {
		for _, name := range []string{"abs", "sqrt"} {
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				b.Run(fmt.Sprintf("%s/%s/%d", name, mode, rows), func(b *testing.B) {
					schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeFloat64})
					var e Expr = Abs(Col("x"))
					if name == "sqrt" {
						e = Sqrt(Col("x"))
					}
					p, err := Compile(schema, e)
					if err != nil {
						b.Fatal(err)
					}
					w, err := NewWorkspace(context.Background(), p, nil)
					if err != nil {
						b.Fatal(err)
					}
					defer w.Close()
					batch := memory.NewBatch()
					batch.Rows = rows
					values := make([]float64, rows)
					for i := range values {
						values[i] = float64(i) + .5
					}
					batch.Float64s["x"] = &memory.Float64Vector{Values: values}
					kernels := kernel.New(mode)
					if _, err := w.Evaluate(context.Background(), batch, kernels); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.SetBytes(int64(rows) * 8)
					b.ResetTimer()
					for b.Loop() {
						if _, err := w.Evaluate(context.Background(), batch, kernels); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
