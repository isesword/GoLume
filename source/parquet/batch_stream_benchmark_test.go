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

// BenchmarkParquetBatchStream measures bounded native first-batch latency and
// equal-row-count full scans. Compare historical implementation snapshots using
// benchstat; production no longer contains a whole-row-group reader. B/op
// includes Arrow allocations; managed-peak-B excludes Arrow and process RSS.
func BenchmarkParquetBatchStream(b *testing.B) {
	benchmarkParquetBatchStreams(b, nil)
}

// BenchmarkParquetBatchStreamNumeric isolates batched numeric decode from the
// batched string page-view path while keeping rows and fixtures identical.
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
			for _, first := range []bool{true, false} {
				operation := "FullScan"
				if first {
					operation = "FirstBatch"
				}
				b.Run("Native/"+operation, func(b *testing.B) {
					benchmarkParquetBatchRead(b, src, parts, first)
				})
			}
		})
	}
}

func benchmarkParquetBatchRead(b *testing.B, src *Source, parts []source.Partition, first bool) {
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
		{
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
		}
	}
	b.StopTimer()
	b.SetBytes(totalBytes / int64(b.N))
	b.ReportMetric(float64(totalRows)/float64(b.N), "rows/op")
	b.ReportMetric(float64(totalRows)/b.Elapsed().Seconds(), "rows/s")
	if first {
		b.ReportMetric(float64(firstLatency.Nanoseconds())/float64(b.N), "first-batch-ns")
	}
	b.ReportMetric(float64(manager.peak.Load()), "managed-peak-B")
}

// BenchmarkParquetNullableScatter isolates the measured nullable-scatter hot
// loop from Arrow I/O and codecs. PerRow is a benchmark-only reference, not a
// second source implementation or reader API.
func BenchmarkParquetNullableScatter(b *testing.B) {
	const rows = 1024
	out := make([]int64, rows)
	defs := make([]int16, rows)
	valid := make(memory.Bitmap, memory.WordsFor(rows))
	var dense []int64
	for i := range defs {
		if i%7 != 0 {
			defs[i] = 1
			dense = append(dense, int64(i))
		}
	}
	b.Run("PerRowReference", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			clear(valid)
			next := 0
			for row := 0; row < rows; row++ {
				if defs[row] == 0 {
					out[row] = 0
					continue
				}
				if next >= len(dense) {
					b.Fatal("dense overflow")
				}
				out[row] = dense[next]
				next++
				memory.Set(valid, row)
			}
			if next != len(dense) {
				b.Fatal("dense mismatch")
			}
		}
	})
	b.Run("WordMask", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			if err := scatterNullable(out, dense, defs, valid, len(dense), "value"); err != nil {
				b.Fatal(err)
			}
		}
	})
}
