package engine_test

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	parquetsource "gocolumn/source/parquet"
	"os"
	"path/filepath"
	"testing"
)

func testPath(t testing.TB) string {
	p := os.Getenv("GOCOLUMN_TEST_PARQUET")
	if p != "" {
		return p
	}
	p = filepath.Join("..", "testdata", "bench-1m.parquet")
	if _, err := os.Stat(p); err != nil {
		t.Skip("test parquet not available")
	}
	return p
}
func query(force string, workers int) expr.Query {
	return expr.Query{Filter: expr.GT(expr.Col("pred"), expr.I64(32767)), GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("count")}, Hints: expr.Hints{Selectivity: .451, Cardinality: 1 << 18, ForceStrategy: force, ForceWorkerCount: workers}}
}
func TestParquetStrategies(t *testing.T) {
	src, err := parquetsource.Open(testPath(t), parquetsource.Columns{PredicateIndex: 0, GroupIndex: 1, ValueIndex: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, strategy string
		workers        int
	}{{"fused", "direct-fused", 1}, {"bitmap", "bitmap-direct", 1}, {"radix", "radix", 4}} {
		t.Run(tc.name, func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.Workers = 4
			opts.KernelMode = kernel.ModeAuto
			eng := engine.New(opts)
			defer eng.Close()
			r, err := eng.Execute(context.Background(), src, query(tc.strategy, tc.workers), engine.ExecuteOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if r.GroupCount != 219196 || r.Checksum != 236716301 || r.SelectedRows != 472475 {
				t.Fatalf("got groups=%d checksum=%d selected=%d", r.GroupCount, r.Checksum, r.SelectedRows)
			}
		})
	}
}
