package physical

import (
	"context"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func BenchmarkOffsetWindowStream(b *testing.B) {
	const rows = 65536
	input := memory.NewBatch()
	input.Rows = rows
	input.Int64s["v"] = &memory.Int64Vector{Values: make([]int64, rows)}
	for i := range input.Int64s["v"].Values {
		input.Int64s["v"].Values[i] = int64(i)
	}
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64})
	src, err := inmemory.New(schema, input)
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		kind WindowBoundKind
	}{{"past", WindowPreceding}, {"future", WindowFollowing}} {
		b.Run(tc.name, func(b *testing.B) {
			bound := WindowBound{Kind: tc.kind, Offset: 3}
			e := WindowExpression{Function: WindowShift, Column: "v", Alias: "s", Frame: WindowFrame{Start: bound, End: bound}}
			manager := memory.NewBudgetManager(8 << 20)
			win, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "v"}}, []WindowExpression{e}, []memory.Field{{Name: "s", Type: memory.TypeInt64, Nullable: true}}, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
			if err != nil {
				b.Fatal(err)
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
				}
				stream.Close()
				if count != rows || manager.Used() != 0 {
					b.Fatal(count, manager.Used())
				}
			}
		})
	}
}
