package engine_test

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	parquetsource "gocolumn/source/parquet"
	"testing"
)

func checkMaterialized(t *testing.T, r engine.Result) {
	t.Helper()
	if len(r.Rows) != r.GroupCount {
		t.Fatalf("materialized=%d groups=%d", len(r.Rows), r.GroupCount)
	}
	var sum int64
	var count int
	for _, row := range r.Rows {
		sum += row.Sum
		count += int(row.Count)
	}
	if sum != r.Checksum || count != r.SelectedRows {
		t.Fatalf("materialized checksum/count mismatch sum=%d/%d count=%d/%d", sum, r.Checksum, count, r.SelectedRows)
	}
}

func TestMaterialize(t *testing.T) {
	src, err := parquetsource.OpenFile(testPath(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		strategy string
		workers  int
	}{{"direct-fused", 1}, {"bitmap-direct", 1}, {"", 4}} {
		t.Run(tc.strategy, func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.Workers = 4
			eng := engine.New(opts)
			defer eng.Close()
			q := expr.Query{Filter: expr.GT(expr.Col("pred"), expr.I64(32767)), GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("count")}, Hints: expr.Hints{Selectivity: .451, Cardinality: 1 << 18, ForceStrategy: tc.strategy}}
			r, err := eng.Execute(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			checkMaterialized(t, r)
		})
	}
}
