package physical

import (
	"fmt"
	"gocolumn/kernel"
	"testing"
)

// Complement the existing numeric and column32 query matrices with both
// Float32 comparison shapes. Arithmetic promotion remains unchanged.
func TestBitmapFloat32WholeQuery(t *testing.T) {
	for _, op := range []string{"gt", "ge", "lt", "le", "eq"} {
		for _, column := range []bool{false, true} {
			for _, nullable := range []bool{false, true} {
				c := numericQueryCase{"float32", op, column}
				input, expected := numericQueryFixture(t, c, nullable)
				var scalarPeak int64
				var scalarAllocs float64
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
					t.Run(fmt.Sprintf("op=%s/column=%t/nullable=%t/mode=%s", op, column, nullable, mode), func(t *testing.T) {
						drain, manager := numericQueryRunner(t, input, expected, c, mode)
						drain(true)
						allocs := testing.AllocsPerRun(10, func() { drain(false) })
						if mode == kernel.ModeScalar {
							scalarPeak = manager.peak
							scalarAllocs = allocs
						} else if manager.peak != scalarPeak || allocs > scalarAllocs {
							t.Fatalf("peak=%d scalar=%d allocations=%g scalar=%g", manager.peak, scalarPeak, allocs, scalarAllocs)
						}
					})
				}
			}
		}
	}
}

// This supplementary final-build benchmark is not part of the frozen
// before/after binary comparison; the baseline executable predates this case.
func BenchmarkBitmapFloat32WholeQuery(b *testing.B) {
	for _, column := range []bool{false, true} {
		for _, nullable := range []bool{false, true} {
			c := numericQueryCase{"float32", "gt", column}
			input, expected := numericQueryFixture(b, c, nullable)
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
				b.Run(fmt.Sprintf("column=%t/nullable=%t/mode=%s", column, nullable, mode), func(b *testing.B) {
					drain, manager := numericQueryRunner(b, input, expected, c, mode)
					drain(true)
					manager.peak = 0
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						drain(false)
					}
					b.StopTimer()
					b.ReportMetric(float64(manager.peak), "peak_bytes")
					b.ReportMetric(numericQueryRows, "source_rows/op")
					b.ReportMetric(float64(len(expected.rows)), "projected_rows/op")
				})
			}
		}
	}
}
