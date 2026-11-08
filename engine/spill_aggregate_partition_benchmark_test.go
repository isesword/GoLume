package engine

import (
	"context"
	"gocolumn/memory"
	"testing"
)

// BenchmarkSpillAggregatePartitionQuery includes input scan, forced initial
// spills, merging, all output batches and file cleanup. Every case verifies
// that it really spills and returns the expected cardinality and checksum.
func BenchmarkSpillAggregatePartitionQuery(b *testing.B) {
	for _, tc := range []struct {
		name         string
		rows, groups int
		limit        int64
	}{
		{"32K_rows_8K_groups_1MiB", 32768, 8192, 1 << 20},
		{"256K_rows_64K_groups_4MiB", 262144, 65536, 4 << 20},
		{"1M_rows_64K_groups_8MiB", 1048576, 65536, 8 << 20},
	} {
		b.Run(tc.name, func(b *testing.B) {
			opts := streamTestOptions()
			opts.ExecutionPolicy = ExecutionBounded
			opts.MemoryLimitBytes = tc.limit
			opts.Spill = SpillOptions{Directory: b.TempDir(), MaxBytes: 256 << 20}
			opts.Pipeline.MaxMorselRows = 1024
			opts.Pipeline.MaxMorselBytes = 16384
			opts.Pipeline.MaxInFlightBytes = 32768
			e := New(opts)
			defer e.Close()
			q := streamTestQuery()
			q.Hints.ForceStrategy = ""
			p, err := e.Prepare(context.Background(), &generatedStreamSource{rows: tc.rows, groups: tc.groups}, q, PrepareOptions{})
			if err != nil {
				b.Fatal(err)
			}
			defer p.Close()
			var spillBytes, spillRuns, peak int64
			b.ReportAllocs()
			b.SetBytes(int64(tc.rows) * 12)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				budget, err := e.BeginQueryMemory()
				if err != nil {
					b.Fatal(err)
				}
				ctx := memory.WithQueryBudget(context.Background(), budget)
				stream, err := p.NewSpillableAggregateStream(ctx, p.resultSchema(), 1024)
				if err != nil {
					b.Fatal(err)
				}
				out := memory.NewBatch()
				groups := 0
				var sum int64
				for {
					ok, err := stream.Next(ctx, out)
					if err != nil {
						b.Fatal(err)
					}
					if !ok {
						break
					}
					groups += out.Rows
					for _, v := range out.Int64s["sum"].Values {
						sum += v
					}
				}
				stats := stream.(*spillableAggregateStream).Stats()
				if stats.SpillRuns == 0 || groups != tc.groups || sum != int64(tc.rows) {
					b.Fatalf("runs=%d groups=%d sum=%d", stats.SpillRuns, groups, sum)
				}
				spillRuns += stats.SpillRuns
				spillBytes += stats.SpillBytes
				if err := stream.Close(); err != nil {
					b.Fatal(err)
				}
				snapshot := budget.Snapshot()
				peak += snapshot.PeakBytes
				if budget.Used() != 0 || snapshot.PeakBytes > tc.limit {
					b.Fatalf("budget leak or overrun: %+v", snapshot)
				}
				if err := budget.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(spillRuns)/float64(b.N), "spill-runs/op")
			b.ReportMetric(float64(spillBytes)/float64(b.N), "spill-B/op")
			b.ReportMetric(float64(peak)/float64(b.N), "peak-B/op")
		})
	}
}
