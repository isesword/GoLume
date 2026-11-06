package parquet

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"gocolumn/memory"
	"gocolumn/source"
)

// BenchmarkParquetBatchStream compares the same source data through the legacy
// whole-row-group reader and the native bounded reader. FirstBatch is latency
// to the first available output, NOT an equal-row-count comparison: rows/op
// records that difference. FullScan consumes exactly the same rows in both.
// B/op and allocs/op are Go runtime allocations, including Arrow; managed-peak-B
// is only GoLume's concurrent reservations, not Arrow memory or process RSS.
func BenchmarkParquetBatchStream(b *testing.B) {
	benchmarkParquetBatchStreams(b, nil)
}

// BenchmarkParquetBatchStreamNumeric isolates batched numeric decode from the
// per-string page-view path while keeping rows and fixtures identical.
func BenchmarkParquetBatchStreamNumeric(b *testing.B) {
	benchmarkParquetBatchStreams(b, []string{"id", "n", "i32", "f32", "f64", "flag"})
}

func benchmarkParquetBatchStreams(b *testing.B, projection []string) {
	b.Helper()
	for _, rowGroupRows := range []int{1024, 16384, 65536} {
		b.Run(fmt.Sprintf("RowGroupRows=%d", rowGroupRows), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "benchmark.parquet")
			writeBatchStreamFixture(b, path, []int{rowGroupRows, rowGroupRows}, 32)
			src := openBatchStreamFixture(b, path)
			if len(projection) != 0 {
				projected, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: projection})
				if err != nil {
					b.Fatal(err)
				}
				src = projected.(*Source)
			}
			parts, err := src.Partitions(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			for _, native := range []bool{false, true} {
				name := "Legacy"
				if native {
					name = "Native"
				}
				for _, first := range []bool{true, false} {
					operation := "FullScan"
					if first {
						operation = "FirstBatch"
					}
					b.Run(name+"/"+operation, func(b *testing.B) {
						benchmarkParquetBatchRead(b, src, parts, native, first)
					})
				}
			}
		})
	}
}

func benchmarkParquetBatchRead(b *testing.B, src *Source, parts []source.Partition, native, first bool) {
	b.Helper()
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = 1024, 256<<10
	manager := newBatchStreamPeakManager(1 << 20)
	opts.MemoryManager = manager
	ctx := context.Background()
	var totalRows, totalBytes int64
	var firstLatency time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := memory.NewBatch()
		started := time.Now()
		if native {
			stream, err := source.OpenBatchStreamWithPartitions(ctx, src, opts, parts)
			if err != nil {
				b.Fatal(err)
			}
			for {
				ok, err := stream.Next(ctx, batch)
				if err != nil {
					stream.Close()
					b.Fatal(err)
				}
				if !ok {
					break
				}
				totalRows += int64(batch.Rows)
				totalBytes += memory.BatchBytes(batch)
				if first {
					firstLatency += time.Since(started)
					break
				}
				batch.ClearColumns()
			}
			if err := stream.Close(); err != nil {
				b.Fatal(err)
			}
			if manager.Used() != 0 {
				b.Fatalf("native scan leaked %d bytes", manager.Used())
			}
		} else {
			reader, err := src.NewReader(ctx, opts)
			if err != nil {
				b.Fatal(err)
			}
			for _, part := range parts {
				if err := reader.ReadPartition(ctx, part, batch); err != nil {
					reader.Close()
					b.Fatal(err)
				}
				totalRows += int64(batch.Rows)
				totalBytes += memory.BatchBytes(batch)
				if first {
					firstLatency += time.Since(started)
					break
				}
			}
			if err := reader.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	b.SetBytes(totalBytes / int64(b.N))
	b.ReportMetric(float64(totalRows)/float64(b.N), "rows/op")
	b.ReportMetric(float64(totalRows)/b.Elapsed().Seconds(), "rows/s")
	if first {
		b.ReportMetric(float64(firstLatency.Nanoseconds())/float64(b.N), "first-batch-ns")
	}
	if native {
		b.ReportMetric(float64(manager.peak.Load()), "managed-peak-B")
	}
}
