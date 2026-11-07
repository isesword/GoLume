package expr

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

// BenchmarkExpressionLiveness separates first-evaluation allocation costs from
// steady-state throughput. Compilation and input construction are excluded.
// peak-managed-B includes all expression reservations, while retained-B counts
// owned capacities after evaluation; neither includes the borrowed input.
func BenchmarkExpressionLiveness(b *testing.B) {
	const rows = 1 << 16
	for _, boolean := range []bool{false, true} {
		name, input := "int64", Col("x")
		if boolean {
			name, input = "bool", Col("c")
		}
		for _, depth := range []int{8, 32, 128} {
			p, err := Compile(livenessSchema(), livenessChain(input, depth, boolean))
			if err != nil {
				b.Fatal(err)
			}
			batch := livenessBatch(rows, true)
			for _, cold := range []bool{false, true} {
				state := "warm"
				if cold {
					state = "cold"
				}
				b.Run(fmt.Sprintf("%s/depth=%d/%s", name, depth, state), func(b *testing.B) {
					ctx, kernels := context.Background(), kernel.New(kernel.ModeScalar)
					manager := &scalarTestManager{base: memory.NewBudgetManager(0)}
					evaluate := func(w *Workspace) {
						if boolean {
							out, err := w.EvaluateBool(ctx, batch, kernels)
							if err != nil || len(out.Truth) != memory.WordsFor(rows) {
								b.Fatalf("boolean evaluation: %v", err)
							}
						} else {
							out, err := w.Evaluate(ctx, batch, kernels)
							if err != nil {
								b.Fatal(err)
							}
							if len(out[0].Int64.Values) != rows || out[0].Int64.Values[1] != int64(1+depth) {
								b.Fatal("incorrect arithmetic chain result")
							}
						}
					}
					var w *Workspace
					if !cold {
						w, err = NewWorkspace(ctx, p, manager)
						if err != nil {
							b.Fatal(err)
						}
						evaluate(w)
						defer w.Close()
					}
					var retained int64
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if cold {
							w, err = NewWorkspace(ctx, p, manager)
							if err != nil {
								b.Fatal(err)
							}
						}
						evaluate(w)
						if cold {
							retained = w.MemoryBytes()
							if err := w.Close(); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					if !cold {
						retained = w.MemoryBytes()
						if err := w.Close(); err != nil {
							b.Fatal(err)
						}
					}
					if manager.Used() != 0 {
						b.Fatalf("workspace leaked %d bytes", manager.Used())
					}
					b.ReportMetric(float64(manager.peak), "peak-managed-B")
					b.ReportMetric(float64(retained), "retained-B")
					b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
				})
			}
		}
	}
}
