package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func BenchmarkDirectionalFillLongNullRun(b *testing.B) {
	for _, rows := range []int{4096, 65536} {
		for _, function := range []WindowFunction{WindowForwardFill, WindowBackwardFill} {
			b.Run(fmt.Sprintf("%d/%d", function, rows), func(b *testing.B) {
				input := memory.NewBatch()
				input.Rows = rows
				input.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, rows)}
				input.Int64s["v"] = &memory.Int64Vector{Values: make([]int64, rows), Validity: memory.EnsureBitmap(nil, rows)}
				for i := range rows {
					input.Int64s["id"].Values[i] = int64(i)
				}
				input.Int64s["v"].Values[rows-1] = 99
				memory.Set(input.Int64s["v"].Validity, rows-1)
				schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true})
				src, err := inmemory.New(schema, input)
				if err != nil {
					b.Fatal(err)
				}
				e := WindowExpression{Function: function, Column: "v", Alias: "fill", Frame: WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}}
				manager := memory.NewBudgetManager(64 << 20)
				win, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{e}, []memory.Field{{Name: "fill", Type: memory.TypeInt64, Nullable: true}}, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(rows * 8))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 128})
					if err != nil {
						b.Fatal(err)
					}
					out := memory.NewBatch()
					count := 0
					for {
						ok, err := st.Next(context.Background(), out)
						if err != nil {
							b.Fatal(err)
						}
						if !ok {
							break
						}
						count += out.Rows
					}
					st.Close()
					if count != rows || manager.Used() != 0 {
						b.Fatal(count, manager.Used())
					}
				}
			})
		}
	}
}
