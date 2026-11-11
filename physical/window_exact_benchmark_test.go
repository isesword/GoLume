package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

// Identical benchmark code also runs on the pre-exact baseline. The benign
// integer-valued float workload has identical results under both contracts.
func BenchmarkWindowSumAverageState(b *testing.B) {
	for _, width := range []int64{1, 64, 4096} {
		for _, function := range []WindowFunction{WindowSum, WindowAvg} {
			b.Run(fmt.Sprintf("function%d/width%d", function, width), func(b *testing.B) {
				expr := WindowExpression{Function: function, Column: "v", Alias: "r", Frame: WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: width - 1}, End: WindowBound{Kind: WindowCurrentRow}}}
				state, err := newRollingWindowState(expr, memory.TypeFloat64, memory.TypeFloat64)
				if err != nil {
					b.Fatal(err)
				}
				state.initialize()
				out := memory.NewBatch()
				out.Float64s["r"] = &memory.Float64Vector{Values: make([]float64, 1), Validity: memory.EnsureBitmap(nil, 1)}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := state.advance(windowNumericCell{valid: true, f64: float64(i%127 - 63)}); err != nil {
						b.Fatal(err)
					}
					if err := state.write(out, 0); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				count := min(int(width), b.N)
				want := 0.0
				for i := b.N - count; i < b.N; i++ {
					want += float64(i%127 - 63)
				}
				if function == WindowAvg {
					want /= float64(count)
				}
				if out.Float64s["r"].Values[0] != want {
					b.Fatal("incorrect benchmark result", out.Float64s["r"].Values[0], want)
				}
			})
		}
	}
}

func BenchmarkWindowSumAverageStream(b *testing.B) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered} {
		for _, width := range []int64{1, 64} {
			b.Run(fmt.Sprintf("mode%d/width%d", mode, width), func(b *testing.B) {
				const rows = 4096
				batch := memory.NewBatch()
				batch.Rows = rows
				ids := make([]int64, rows)
				values := make([]float64, rows)
				for i := range ids {
					ids[i], values[i] = int64(i), float64(i%127-63)
				}
				batch.Int64s["id"] = &memory.Int64Vector{Values: ids}
				batch.Float64s["v"] = &memory.Float64Vector{Values: values}
				schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeFloat64})
				src, err := inmemory.New(schema, batch)
				if err != nil {
					b.Fatal(err)
				}
				frame := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: width - 1}, End: WindowBound{Kind: WindowCurrentRow}}
				exprs := []WindowExpression{{Function: WindowSum, Column: "v", Alias: "sum", Frame: frame}, {Function: WindowAvg, Column: "v", Alias: "avg", Frame: frame}}
				fields := []memory.Field{{Name: "sum", Type: memory.TypeFloat64, Nullable: true}, {Name: "avg", Type: memory.TypeFloat64, Nullable: true}}
				manager := memory.NewBudgetManager(8 << 20)
				win, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, exprs, fields, manager, WindowSourceOptions{Mode: mode})
				if err != nil {
					b.Fatal(err)
				}
				wantSum := 0.0
				for row := rows - int(width); row < rows; row++ {
					wantSum += values[row]
				}
				b.ReportAllocs()
				b.SetBytes(rows * 8)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					stream, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1024})
					if err != nil {
						b.Fatal(err)
					}
					out := memory.NewBatch()
					count := 0
					var lastSum, lastAverage float64
					for {
						ok, err := stream.Next(context.Background(), out)
						if err != nil {
							stream.Close()
							b.Fatal(err)
						}
						if !ok {
							break
						}
						count += out.Rows
						if out.Rows > 0 {
							lastSum = out.Float64s["sum"].Values[out.Rows-1]
							lastAverage = out.Float64s["avg"].Values[out.Rows-1]
						}
					}
					out.ClearColumns()
					stream.Close()
					if count != rows || manager.Used() != 0 || lastSum != wantSum || lastAverage != wantSum/float64(width) {
						b.Fatal(count, manager.Used(), lastSum, lastAverage)
					}
				}
			})
		}
	}
}
