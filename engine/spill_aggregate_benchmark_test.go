package engine

import (
	"context"
	"gocolumn/memory"
	"testing"
)

// BenchmarkSpillAggregateCompleteQuery includes source scan, aggregate state,
// output consumption, spill I/O when needed, and execution-local cleanup.
func BenchmarkSpillAggregateCompleteQuery(b *testing.B) {
	for _, tc := range []struct {
		name         string
		rows, groups int
		limit        int64
	}{
		{"fits_small", 65536, 64, 8 << 20},
		{"fits_many", 65536, 4096, 8 << 20},
		{"pressure", 32768, 8192, 1 << 20},
	} {
		for _, sink := range []string{"stream", "summary", "materialized"} {
			if tc.name == "pressure" && sink == "materialized" {
				continue
			}
			b.Run(tc.name+"/"+sink, func(b *testing.B) {
				opts := streamTestOptions()
				opts.ExecutionPolicy = ExecutionBounded
				opts.MemoryLimitBytes = tc.limit
				opts.Spill = SpillOptions{Directory: b.TempDir(), MaxBytes: 64 << 20}
				opts.Pipeline.MaxMorselRows = 1024
				opts.Pipeline.MaxMorselBytes = 16384
				opts.Pipeline.MaxInFlightBytes = 32768
				e := New(opts)
				defer e.Close()
				q := streamTestQuery()
				q.Hints.ForceStrategy = ""
				src := &generatedStreamSource{rows: tc.rows, groups: tc.groups}
				p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
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
					if sink == "stream" {
						stream, err := p.NewSpillableAggregateStream(ctx, p.resultSchema(), 128)
						if err != nil {
							b.Fatal(err)
						}
						out := memory.NewBatch()
						count := 0
						var sum int64
						for {
							ok, err := stream.Next(ctx, out)
							if err != nil {
								b.Fatal(err)
							}
							if !ok {
								break
							}
							count += out.Rows
							for _, v := range out.Int64s["sum"].Values {
								sum += v
							}
						}
						stats := stream.(*spillableAggregateStream).Stats()
						spillRuns += stats.SpillRuns
						spillBytes += stats.SpillBytes
						if count != tc.groups || sum != int64(tc.rows) {
							b.Fatalf("groups=%d sum=%d", count, sum)
						}
						if err := stream.Close(); err != nil {
							b.Fatal(err)
						}
					} else {
						result, err := p.Execute(ctx, ExecuteOptions{Materialize: sink == "materialized"})
						if err != nil {
							b.Fatal(err)
						}
						if result.GroupCount != tc.groups || result.SelectedRows != tc.rows || result.Checksum != int64(tc.rows) {
							b.Fatalf("result %+v", result)
						}
					}
					peak += budget.Snapshot().PeakBytes
					if budget.Used() != 0 {
						b.Fatal("execution retained memory")
					}
					if err := budget.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if sink == "stream" {
					b.ReportMetric(float64(spillRuns)/float64(b.N), "spill-runs/op")
					b.ReportMetric(float64(spillBytes)/float64(b.N), "spill-B/op")
				}
				b.ReportMetric(float64(peak)/float64(b.N), "peak-B/op")
			})
		}
	}
}
