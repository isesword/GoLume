package optimizer

import (
	"fmt"
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"slices"
	"testing"
)

func TestScanIndependentReviewEffectiveSchema(t *testing.T) {
	source := &testSource{schema: memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64}, memory.Field{Name: "c", Type: memory.TypeInt64})}
	for _, projection := range [][]string{nil, {}, {"b"}, {"c", "a"}, {"c", "b", "a"}} {
		for _, kind := range []string{"scan", "filter", "limit", "replace-all"} {
			t.Run(kind+"/"+fmt.Sprint(projection), func(t *testing.T) {
				scan := &logical.Scan{Source: source, Projection: slices.Clone(projection)}
				original := slices.Clone(scan.Projection)
				var plan logical.Plan = scan
				switch kind {
				case "filter":
					plan = &logical.Filter{Input: scan, Predicate: expr.GT(expr.Col(schemaNamesMustReview(t, scan)[0]), expr.Lit(0))}
				case "limit":
					plan = &logical.Limit{Input: scan, N: 2}
				case "replace-all":
					var named []expr.NamedExpr
					for _, name := range schemaNamesMustReview(t, scan) {
						named = append(named, expr.Named(expr.Lit(7), name))
					}
					plan = &logical.ExprProjection{Input: scan, IncludeInput: true, Exprs: named}
				}
				want, err := logical.InferSchema(plan)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 3; i++ {
					result, err := Optimize(plan)
					if err != nil {
						t.Fatal(err)
					}
					got, err := logical.InferSchema(result.Plan)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("pass %d schema=%v want %v", i, got, want)
					}
					plan = result.Plan
				}
				if !reflect.DeepEqual(scan.Projection, original) {
					t.Fatalf("original scan mutated: %v", scan.Projection)
				}
			})
		}
	}
}
func schemaNamesMustReview(t *testing.T, plan logical.Plan) []string {
	t.Helper()
	s, e := logical.InferSchema(plan)
	if e != nil {
		t.Fatal(e)
	}
	return schemaNames(s)
}

func TestScanIndependentReviewRejectHiddenDependency(t *testing.T) {
	source := &testSource{schema: memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64})}
	scan := &logical.Scan{Source: source, Projection: []string{"b"}}
	var rules []string
	if _, err := pushProjection(scan, map[string]struct{}{"a": {}}, &rules); err == nil {
		t.Fatal("accepted hidden source column as a logical scan dependency")
	}
}

func TestScanIndependentReviewPreservesMetadataAndOwnership(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64})}
	predicate := expr.GT(expr.Col("b"), expr.Lit(0))
	report := source.PushdownReport{Projection: []string{"b"}, Predicate: predicate.String(), TotalRows: 13, SelectedRows: 7, PrunedRows: 6, TotalPartitions: 3, SelectedPartitions: 2, PrunedPartitions: 1}
	scan := &logical.Scan{Source: src, Projection: []string{"b"}, PushedPredicate: predicate, Pushdown: report}
	result, err := Optimize(scan)
	if err != nil {
		t.Fatal(err)
	}
	out := result.Plan.(*logical.Scan)
	if out == scan || out.Source != src || out.PushedPredicate != predicate || !reflect.DeepEqual(out.Pushdown, report) {
		t.Fatalf("scan metadata changed: %+v", out)
	}
	out.Projection[0] = "changed"
	out.Pushdown.Projection[0] = "changed"
	if scan.Projection[0] != "b" || scan.Pushdown.Projection[0] != "b" {
		t.Fatal("output aliases caller-owned slices")
	}
}
