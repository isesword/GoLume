package expr

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

// BenchmarkBitwiseExpression includes public Workspace evaluation, validity,
// and result export, but excludes input creation and compilation. Scalar and
// auto evaluate identical programs; this is not a comparison with an older API.
func BenchmarkBitwiseExpression(b *testing.B) {
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
		for _, rows := range []int{8, 65536} {
			for _, nullable := range []bool{false, true} {
				batch := memory.NewBatch()
				batch.Rows = rows
				var av, bv memory.Bitmap
				if nullable {
					av, bv = make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
					for i := range rows {
						if i%13 != 0 {
							memory.Set(av, i)
						}
						if i%17 != 0 {
							memory.Set(bv, i)
						}
					}
				}
				switch typ {
				case memory.TypeInt32:
					a, c := make([]int32, rows), make([]int32, rows)
					for i := range a {
						a[i], c[i] = int32(uint64(i)*2654435761), int32(uint64(i)*1664525+1013904223)
					}
					batch.Int32s["a"], batch.Int32s["b"] = &memory.Int32Vector{Values: a, Validity: av}, &memory.Int32Vector{Values: c, Validity: bv}
				case memory.TypeUint32:
					a, c := make([]uint32, rows), make([]uint32, rows)
					for i := range a {
						a[i], c[i] = uint32(uint64(i)*2654435761), uint32(uint64(i)*1664525+1013904223)
					}
					batch.Uint32s["a"], batch.Uint32s["b"] = &memory.Uint32Vector{Values: a, Validity: av}, &memory.Uint32Vector{Values: c, Validity: bv}
				case memory.TypeInt64:
					a, c := make([]int64, rows), make([]int64, rows)
					for i := range a {
						a[i], c[i] = int64(uint64(i)*11400714819323198485), int64(uint64(i)*1664525+1013904223)
					}
					batch.Int64s["a"], batch.Int64s["b"] = &memory.Int64Vector{Values: a, Validity: av}, &memory.Int64Vector{Values: c, Validity: bv}
				case memory.TypeUint64:
					a, c := make([]uint64, rows), make([]uint64, rows)
					for i := range a {
						a[i], c[i] = uint64(i)*11400714819323198485, uint64(uint64(i)*1664525+1013904223)
					}
					batch.Uint64s["a"], batch.Uint64s["b"] = &memory.Uint64Vector{Values: a, Validity: av}, &memory.Uint64Vector{Values: c, Validity: bv}
				}
				schema := memory.NewSchema(memory.Field{Name: "a", Type: typ, Nullable: nullable}, memory.Field{Name: "b", Type: typ, Nullable: nullable})
				for _, tc := range []struct {
					name string
					e    Expr
				}{
					{"and", BitAnd(Col("a"), Col("b"))}, {"or", BitOr(Col("a"), Col("b"))}, {"xor", BitXor(Col("a"), Col("b"))}, {"not", BitNot(Col("a"))}, {"literal", BitAnd(Col("a"), I64(127))},
				} {
					program, err := CompileExpressions(schema, []NamedExpr{Named(tc.e, "out")})
					if err != nil {
						b.Fatal(err)
					}
					for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
						b.Run(fmt.Sprintf("%s/%s/rows=%d/null=%t/mode=%s", typ, tc.name, rows, nullable, mode), func(b *testing.B) {
							ctx := context.Background()
							budget := memory.NewBudgetManager(0)
							w, err := NewWorkspace(ctx, program, budget)
							if err != nil {
								b.Fatal(err)
							}
							kernels := kernel.New(mode)
							if _, err := w.Evaluate(ctx, batch, kernels); err != nil {
								b.Fatal(err)
							}
							b.ReportAllocs()
							b.ResetTimer()
							for range b.N {
								if _, err := w.Evaluate(ctx, batch, kernels); err != nil {
									b.Fatal(err)
								}
							}
							b.StopTimer()
							b.ReportMetric(float64(w.MemoryBytes()), "retained-B")
							w.Close()
							if budget.Used() != 0 {
								b.Fatal("workspace leaked")
							}
						})
					}
				}
			}
		}
	}
}
