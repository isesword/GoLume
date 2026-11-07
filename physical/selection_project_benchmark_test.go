package physical

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

type selectionPeakManager struct {
	base *memory.BudgetManager
	peak int64
}

func (m *selectionPeakManager) Used() int64  { return m.base.Used() }
func (m *selectionPeakManager) Limit() int64 { return m.base.Limit() }
func (m *selectionPeakManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	r, err := m.base.Reserve(ctx, n)
	if err == nil && m.base.Used() > m.peak {
		m.peak = m.base.Used()
	}
	return r, err
}

// Wide input / narrow projection measures data that the public filter boundary
// used to compact even though the following projection never reads it. The
// source-row metric is logical input, not an instrumented kernel visit count.
// auto selects the private edge only when eligible (includeInput falls back).
func BenchmarkSelectionProject(b *testing.B) {
	const rows = 8192
	for _, width := range []int{2, 32} {
		for _, pct := range []int{1, 50, 99, 100} {
			for _, include := range []bool{false, true} {
				for _, fused := range []bool{false, true} {
					mode := "materialized"
					if fused {
						mode = "auto"
					}
					b.Run(fmt.Sprintf("width%d/pct%d/input%t/%s", width, pct, include, mode), func(b *testing.B) {
						input := selectionFixture(b, rows, width, 1, false, "")
						manager := &selectionPeakManager{base: memory.NewBudgetManager(0)}
						p := selectionProjection(b, input, rows*pct/100, fused, include, manager)
						opts := testReadOptions()
						opts.Kernels = kernel.New(kernel.ModeAuto)
						opts.BatchRows = rows
						opts.MemoryManager = manager
						ctx := context.Background()
						out := memory.NewBatch()
						produced := 0
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							stream, err := p.NewBatchStream(ctx, opts)
							if err != nil {
								b.Fatal(err)
							}
							produced = 0
							for {
								ok, err := stream.Next(ctx, out)
								if err != nil {
									b.Fatal(err)
								}
								if !ok {
									break
								}
								produced += out.Rows
							}
							if err := stream.Close(); err != nil {
								b.Fatal(err)
							}
							if manager.Used() != 0 {
								b.Fatalf("retained %d", manager.Used())
							}
						}
						b.StopTimer()
						b.ReportMetric(float64(manager.peak), "peak_bytes")
						b.ReportMetric(rows, "source_rows/op")
						b.ReportMetric(float64(produced), "projected_rows/op")
					})
				}
			}
		}
	}
}
