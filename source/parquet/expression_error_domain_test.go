package parquet

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"path/filepath"
	"testing"
)

func TestPushdownDoesNotHideFalliblePredicateBehindFalseConjunct(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checked.parquet")
	writePushdownFixture(t, path)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fallible := expr.GT(expr.CastTo(expr.Col("pred"), memory.TypeInt8), expr.I64(0))
	neverTrue := expr.LT(expr.Col("pred"), expr.I64(-1))
	for _, predicate := range []expr.Expr{expr.And(fallible, neverTrue), expr.And(neverTrue, fallible)} {
		_, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
		if err != nil {
			t.Fatal(err)
		}
		if report.PrunedPartitions != 0 || report.SelectedPartitions != 3 {
			t.Fatalf("fallible predicate was metadata-pruned: %+v", report)
		}
	}
	_, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: neverTrue})
	if err != nil || report.PrunedPartitions != 3 {
		t.Fatalf("safe predicate lost pruning: %+v, %v", report, err)
	}
}
