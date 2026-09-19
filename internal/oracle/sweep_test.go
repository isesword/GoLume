package oracle

import "testing"

// TestDifferentialSweep is a wide randomized sweep beyond the fixed
// regression corpus: many more seeds, each producing a table and several
// queries compared across all kernel modes. It is skipped in -short mode;
// the fixed regression corpus in TestDifferentialRegressionSeeds always runs.
func TestDifferentialSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("sweep runs without -short")
	}
	for seed := int64(0); seed < 800; seed++ {
		g := NewGenerator(seed)
		table := g.Table(TableOptions{})
		for qi := 0; qi < queriesPerSeed; qi++ {
			q := g.Query(table)
			runDifferential(t, Describe(seed, qi, q), table, batchShapes[qi%len(batchShapes)], q)
		}
	}
}
