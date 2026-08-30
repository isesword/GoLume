package lazy

import (
	"context"
	"path/filepath"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func BenchmarkPhase36ResultSinks(b *testing.B) {
	const (
		totalRows    = 1 << 20
		rowsPerBatch = 16 << 10
	)
	schema := memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	batches := make([]*memory.Batch, 0, totalRows/rowsPerBatch)
	for base := 0; base < totalRows; base += rowsPerBatch {
		predicates := make([]int64, rowsPerBatch)
		values := make([]int64, rowsPerBatch)
		for row := range values {
			predicates[row] = int64((base + row) & 1)
			values[row] = int64(base + row)
		}
		batch := memory.NewBatch()
		batch.Int64s["pred"] = &memory.Int64Vector{Values: predicates}
		batch.Int64s["value"] = &memory.Int64Vector{Values: values}
		batch.Reset(rowsPerBatch)
		batches = append(batches, batch)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		b.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 1
	opts.Pipeline.Workers = 1
	session := NewSession(opts)
	defer session.Close()
	query := session.FromSource(src).Filter(Col("pred").Gt(Lit(0))).Select("value")
	prepared, err := query.Prepare(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer prepared.Close()
	b.SetBytes(totalRows * 16)

	b.Run("CollectBatches", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			reader, err := prepared.ExecuteBatches(context.Background(), BatchOptions{})
			if err != nil {
				b.Fatal(err)
			}
			var rows int64
			for reader.Next(context.Background()) {
				rows += reader.Batch().Len()
			}
			if err := reader.Close(); err != nil {
				b.Fatal(err)
			}
			if rows != totalRows/2 {
				b.Fatalf("rows=%d", rows)
			}
		}
	})

	b.Run("Collect", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			frame, err := query.Collect(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			if frame.Height() != totalRows/2 {
				b.Fatalf("rows=%d", frame.Height())
			}
			if err := frame.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SinkIPC", func(b *testing.B) {
		b.ReportAllocs()
		path := filepath.Join(b.TempDir(), "result.arrow")
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := query.SinkIPC(context.Background(), path, IPCSinkOptions{SinkOptions: SinkOptions{Overwrite: iteration > 0}})
			if err != nil {
				b.Fatal(err)
			}
			if result.Rows != totalRows/2 {
				b.Fatalf("rows=%d", result.Rows)
			}
		}
	})
}
