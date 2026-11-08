//go:build !race

package engine

import (
	"context"
	"gocolumn/kernel"
	"testing"
)

// The race runtime intentionally discards some sync.Pool entries, so exact
// allocation-count equality is only meaningful without race instrumentation.
// All result, validity, charged-memory and release oracles still run under race.
func TestNarrowSIMDEngineAllocations(t *testing.T) {
	for _, c := range narrowEngineBenchmarkCases() {
		if c.rows == 7 {
			continue
		}
		f := narrowEngineMake(t, c)
		var scalar float64
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			t.Run(c.name()+"/mode="+mode.String(), func(t *testing.T) {
				e, p := narrowEnginePrepare(t, c, mode, f)
				defer e.Close()
				defer p.Close()
				narrowEngineVerify(t, c, f, e, p)
				allocs := testing.AllocsPerRun(3, func() {
					r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
					if err != nil {
						t.Fatal(err)
					}
					narrowEngineSink = r
				})
				if e.Memory().Used() != 0 {
					t.Fatalf("retained=%d", e.Memory().Used())
				}
				if mode == kernel.ModeScalar {
					scalar = allocs
				} else if allocs > scalar {
					t.Fatalf("allocations=%g scalar=%g", allocs, scalar)
				}
			})
		}
	}
}
