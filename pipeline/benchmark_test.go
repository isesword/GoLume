package pipeline_test

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/physical"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func BenchmarkPipelineSchedulingSmallBatches(b *testing.B) {
	src := benchmarkIntSource(b, 256, 16, 1)
	parts, _ := src.Partitions(context.Background())
	readOpts := scalarReadOptions()
	cases := []struct {
		name    string
		workers int
		direct  bool
	}{
		{name: "direct", workers: 1, direct: true},
		{name: "auto-workers-4", workers: 4},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			opts := pipeline.Options{
				Workers: tc.workers, TargetMorselBytes: 4 << 10, MaxMorselBytes: 8 << 10, MaxInFlightBytes: 64 << 10,
				MinMorselRows: 1, MaxMorselRows: 256, Ordering: pipeline.OrderingStable,
			}
			manager := memory.NewBudgetManager(1 << 20)
			b.ReportAllocs()
			b.SetBytes(256 * 16 * 8)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var stream source.BatchStream
				var err error
				if tc.direct {
					stream, err = source.OpenBatchStreamWithPartitions(context.Background(), src, readOpts, parts)
				} else {
					stream, err = pipeline.OpenBatchStream(context.Background(), src, readOpts, parts, manager, opts, observer.Nop{})
				}
				if err != nil {
					b.Fatal(err)
				}
				if rows, err := drainBenchmarkStream(stream); err != nil || rows != 4096 {
					b.Fatalf("rows=%d err=%v", rows, err)
				}
			}
			b.StopTimer()
			if manager.Used() != 0 {
				b.Fatalf("reservation leak=%d", manager.Used())
			}
		})
	}
}

func BenchmarkPipelineWorkerScaling(b *testing.B) {
	base := benchmarkIntSource(b, 64, 4096, 8)
	named := make([]expr.NamedExpr, 0, 8)
	for column := 0; column < 8; column++ {
		name := fmt.Sprintf("x%d", column)
		named = append(named, expr.Named(expr.Add(expr.Col(name), expr.I64(int64(column+1))), name))
	}
	src, err := physical.NewProjectSource(base, named, false)
	if err != nil {
		b.Fatal(err)
	}
	parts, _ := src.Partitions(context.Background())
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			opts := pipeline.Options{
				Workers: workers, TargetMorselBytes: 1 << 20, MaxMorselBytes: 2 << 20, MaxInFlightBytes: 16 << 20,
				MinMorselRows: 256, MaxMorselRows: 16 << 10, Ordering: pipeline.OrderingRelaxed,
			}
			manager := memory.NewBudgetManager(32 << 20)
			b.ReportAllocs()
			b.SetBytes(64 * 4096 * 8 * 8)
			b.ResetTimer()
			var peak int64
			for i := 0; i < b.N; i++ {
				stream, err := pipeline.OpenBatchStream(context.Background(), src, scalarReadOptions(), parts, manager, opts, observer.Nop{})
				if err != nil {
					b.Fatal(err)
				}
				if rows, err := drainBenchmarkStream(stream); err != nil || rows != 64*4096 {
					b.Fatalf("rows=%d err=%v", rows, err)
				}
				if stats, ok := stream.(pipeline.StreamStatsProvider); ok && stats.PipelineStats().Exchange.PeakInFlightBytes > peak {
					peak = stats.PipelineStats().Exchange.PeakInFlightBytes
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(peak), "peak-inflight-B")
			if manager.Used() != 0 {
				b.Fatalf("reservation leak=%d", manager.Used())
			}
		})
	}
}

func BenchmarkPipelineWideTablePeakMemory(b *testing.B) {
	src := benchmarkWideStringSource(b, 16, 1024, 16, 64)
	parts, _ := src.Partitions(context.Background())
	opts := pipeline.Options{
		Workers: 4, TargetMorselBytes: 1 << 20, MaxMorselBytes: 2 << 20, MaxInFlightBytes: 8 << 20,
		MinMorselRows: 64, MaxMorselRows: 4096, Ordering: pipeline.OrderingStable,
	}
	manager := memory.NewBudgetManager(16 << 20)
	b.ReportAllocs()
	b.SetBytes(16 * 1024 * 16 * 64)
	b.ResetTimer()
	var peak int64
	for i := 0; i < b.N; i++ {
		stream, err := pipeline.OpenBatchStream(context.Background(), src, scalarReadOptions(), parts, manager, opts, observer.Nop{})
		if err != nil {
			b.Fatal(err)
		}
		if rows, err := drainBenchmarkStream(stream); err != nil || rows != 16*1024 {
			b.Fatalf("rows=%d err=%v", rows, err)
		}
		stats := stream.(pipeline.StreamStatsProvider).PipelineStats()
		if stats.Exchange.PeakInFlightBytes > peak {
			peak = stats.Exchange.PeakInFlightBytes
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(peak), "peak-inflight-B")
	if peak > opts.MaxInFlightBytes {
		b.Fatalf("peak in-flight=%d limit=%d", peak, opts.MaxInFlightBytes)
	}
	if manager.Used() != 0 {
		b.Fatalf("reservation leak=%d", manager.Used())
	}
}

func drainBenchmarkStream(stream source.BatchStream) (int, error) {
	out := memory.NewBatch()
	rows := 0
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			_ = stream.Close()
			return rows, err
		}
		if !ok {
			return rows, stream.Close()
		}
		rows += out.Rows
	}
}

func benchmarkIntSource(b *testing.B, partitions, rows, columns int) *inmemory.Source {
	b.Helper()
	fields := make([]memory.Field, columns)
	for column := range fields {
		fields[column] = memory.Field{Name: fmt.Sprintf("x%d", column), Type: memory.TypeInt64}
	}
	batches := make([]*memory.Batch, partitions)
	for partition := range batches {
		batch := memory.NewBatch()
		for column := 0; column < columns; column++ {
			values := make([]int64, rows)
			for row := range values {
				values[row] = int64(partition*rows + row + column)
			}
			batch.Int64s[fmt.Sprintf("x%d", column)] = &memory.Int64Vector{Values: values}
		}
		batch.Reset(rows)
		batches[partition] = batch
	}
	src, err := inmemory.New(memory.NewSchema(fields...), batches...)
	if err != nil {
		b.Fatal(err)
	}
	return src
}

func benchmarkWideStringSource(b *testing.B, partitions, rows, columns, width int) *inmemory.Source {
	b.Helper()
	fields := make([]memory.Field, columns)
	for column := range fields {
		fields[column] = memory.Field{Name: fmt.Sprintf("s%d", column), Type: memory.TypeString}
	}
	batches := make([]*memory.Batch, partitions)
	for partition := range batches {
		batch := memory.NewBatch()
		for column := 0; column < columns; column++ {
			data := make([]byte, rows*width)
			offsets := make([]uint32, rows+1)
			for row := 0; row < rows; row++ {
				for i := 0; i < width; i++ {
					data[row*width+i] = byte('a' + (partition+column+row+i)%26)
				}
				offsets[row+1] = uint32((row + 1) * width)
			}
			batch.Strings[fmt.Sprintf("s%d", column)] = &memory.StringViewVector{Offsets: offsets, Data: data, Encoding: memory.EncodingView}
		}
		batch.Reset(rows)
		batches[partition] = batch
	}
	src, err := inmemory.New(memory.NewSchema(fields...), batches...)
	if err != nil {
		b.Fatal(err)
	}
	return src
}
