package expr

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

// BenchmarkExpressionRegisterMetadata isolates metadata costs on small batches.
// Liveness benchmarks cover the same chain shapes on large, payload-bound inputs.
// Identity controls expose the extra input-view bookkeeping without owner reuse.
func BenchmarkExpressionRegisterMetadata(b *testing.B) {
	for _, boolean := range []bool{false, true} {
		kind, input := "int64", Col("x")
		if boolean {
			kind, input = "bool", Col("c")
		}
		for _, depth := range []int{0, 8, 128} {
			p, err := Compile(livenessSchema(), livenessChain(input, depth, boolean))
			if err != nil {
				b.Fatal(err)
			}
			for _, rows := range []int{1, 65} {
				batch := livenessBatch(rows, true)
				for _, cold := range []bool{false, true} {
					state := "warm"
					if cold {
						state = "cold"
					}
					b.Run(fmt.Sprintf("%s/depth=%d/rows=%d/%s", kind, depth, rows, state), func(b *testing.B) {
						ctx, kernels := context.Background(), kernel.New(kernel.ModeScalar)
						manager := memory.NewBudgetManager(0)
						var w *Workspace
						evaluate := func() {
							if boolean {
								v, err := w.EvaluateBool(ctx, batch, kernels)
								if err != nil || len(v.Truth) != memory.WordsFor(rows) {
									b.Fatalf("bool evaluation: %v", err)
								}
							} else {
								v, err := w.Evaluate(ctx, batch, kernels)
								if err != nil || len(v[0].Int64.Values) != rows {
									b.Fatalf("int64 evaluation: %v", err)
								}
								if rows > 1 && v[0].Int64.Values[1] != int64(1+depth) {
									b.Fatal("incorrect result")
								}
							}
						}
						if !cold {
							w, err = NewWorkspace(ctx, p, manager)
							if err != nil {
								b.Fatal(err)
							}
							evaluate()
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
							evaluate()
							if cold {
								retained = w.MemoryBytes()
								w.Close()
							}
						}
						b.StopTimer()
						if !cold {
							retained = w.MemoryBytes()
							w.Close()
						}
						if manager.Used() != 0 {
							b.Fatalf("leaked %d bytes", manager.Used())
						}
						b.ReportMetric(float64(retained), "retained-B")
					})
				}
			}
		}
	}
}
