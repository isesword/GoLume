package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func BenchmarkPhase30PipelineMorsels(b *testing.B) {
	const totalRows = 1 << 20
	for _, rowsPerBatch := range []int{1024, 4096, 16384, 65536} {
		b.Run(fmt.Sprintf("rows=%d", rowsPerBatch), func(b *testing.B) {
			schema := memory.NewSchema(
				memory.Field{Name: "pred", Type: memory.TypeInt32},
				memory.Field{Name: "value", Type: memory.TypeFloat64},
			)
			batches := make([]*memory.Batch, 0, totalRows/rowsPerBatch)
			for base := 0; base < totalRows; base += rowsPerBatch {
				n := rowsPerBatch
				if totalRows-base < n {
					n = totalRows - base
				}
				batch := memory.NewBatch()
				pred := make([]int32, n)
				values := make([]float64, n)
				for i := range pred {
					pred[i] = int32((base + i) & 1)
					values[i] = float64(base + i)
				}
				batch.Int32s["pred"] = &memory.Int32Vector{Values: pred}
				batch.Float64s["value"] = &memory.Float64Vector{Values: values}
				batch.Reset(n)
				batches = append(batches, batch)
			}
			src, err := inmemory.New(schema, batches...)
			if err != nil {
				b.Fatal(err)
			}
			opts := engine.DefaultOptions()
			opts.Workers = 1
			session := NewSession(opts)
			defer session.Close()
			prepared, err := session.FromSource(src).
				Filter(Col("pred").Gt(Lit(0))).
				Select("value").
				Prepare(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			defer prepared.Close()
			b.SetBytes(totalRows * (4 + 8))
			b.Run("reader-loop", func(b *testing.B) {
				b.ReportAllocs()
				rows := int64(0)
				for i := 0; i < b.N; i++ {
					reader, err := prepared.src.NewReader(context.Background(), source.ReadOptions{Kernels: prepared.kernels})
					if err != nil {
						b.Fatal(err)
					}
					batch := memory.NewBatch()
					for _, part := range prepared.parts {
						if err := reader.ReadPartition(context.Background(), part, batch); err != nil {
							b.Fatal(err)
						}
						rows += int64(batch.Rows)
					}
					batch.ReleaseOwned()
					if err := reader.Close(); err != nil {
						b.Fatal(err)
					}
				}
				if rows == 0 {
					b.Fatal("reader produced no rows")
				}
			})
			b.Run("batch-stream", func(b *testing.B) {
				b.ReportAllocs()
				rows := int64(0)
				for i := 0; i < b.N; i++ {
					stream, err := source.OpenBatchStreamWithPartitions(context.Background(), prepared.src, source.ReadOptions{Kernels: prepared.kernels}, prepared.parts)
					if err != nil {
						b.Fatal(err)
					}
					batch := memory.NewBatch()
					for {
						ok, nextErr := stream.Next(context.Background(), batch)
						if nextErr != nil {
							b.Fatal(nextErr)
						}
						if !ok {
							break
						}
						rows += int64(batch.Rows)
					}
					batch.ReleaseOwned()
					if err := stream.Close(); err != nil {
						b.Fatal(err)
					}
				}
				if rows == 0 {
					b.Fatal("stream produced no rows")
				}
			})
		})
	}
}
