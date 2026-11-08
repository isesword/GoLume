//go:build !race

package physical

import (
	"gocolumn/kernel"
	"testing"
)

// The race runtime intentionally discards some sync.Pool entries, so exact
// allocation-count equality is only meaningful without race instrumentation.
// All result, validity, charged-memory and release oracles still run under race.
func TestNarrowSIMDWholeQueryAllocations(t *testing.T) {
	for _, c := range narrowQueryBenchmarkCases() {
		if c.rows == 7 {
			continue
		}
		var scalar float64
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			t.Run(c.name()+"/mode="+mode.String(), func(t *testing.T) {
				run, _, _ := narrowQueryRunner(t, c, mode)
				run(true)
				allocs := testing.AllocsPerRun(5, func() { run(false) })
				if mode == kernel.ModeScalar {
					scalar = allocs
				} else if allocs > scalar {
					t.Fatalf("allocations=%g scalar=%g", allocs, scalar)
				}
			})
		}
	}
}
