package optimizer

import (
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"testing"
)

func errorDomainScan() *logical.Scan {
	return &logical.Scan{Source: &testSource{schema: memory.NewSchema(
		memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "keep", Type: memory.TypeBool, Nullable: true},
	)}}
}

func narrowingPredicate() expr.Expr {
	return expr.GT(expr.CastTo(expr.Col("x"), memory.TypeInt8), expr.I64(0))
}

func TestFallibleSecondFilterRetainsSelectionBoundary(t *testing.T) {
	plan := &logical.Filter{Input: &logical.Filter{
		Input: errorDomainScan(), Predicate: expr.Col("keep"),
	}, Predicate: narrowingPredicate()}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	outer, ok := got.Plan.(*logical.Filter)
	if !ok {
		t.Fatalf("outer=%T", got.Plan)
	}
	if _, ok := outer.Input.(*logical.Filter); !ok {
		t.Fatal("fallible outer filter lost its TRUE-only row-selection boundary")
	}
	if contains(got.AppliedRules, "filter-merge") {
		t.Fatal("fallible outer filter was merged into SQL AND")
	}
	if pushed := findScan(t, got.Plan).PushedPredicate; pushed == nil || pushed.String() != "keep" {
		t.Fatalf("only the earlier safe filter may prune, got %v", pushed)
	}
}

func TestSafeOuterFilterCannotPruneFallibleInnerFilter(t *testing.T) {
	plan := &logical.Filter{Input: &logical.Filter{
		Input: errorDomainScan(), Predicate: narrowingPredicate(),
	}, Predicate: expr.Col("keep")}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	if pushed := findScan(t, got.Plan).PushedPredicate; pushed != nil {
		t.Fatalf("safe outer predicate could suppress demanded inner errors: %v", pushed)
	}
}

func TestUnusedFallibleProjectionRetainsDeadOutputPruningContract(t *testing.T) {
	plan := &logical.Projection{Input: &logical.ExprProjection{
		Input: errorDomainScan(), IncludeInput: true,
		Exprs: []expr.NamedExpr{expr.Named(expr.CastTo(expr.Col("x"), memory.TypeInt8), "checked")},
	}, Columns: []string{"keep"}}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(got.AppliedRules, "expression-projection-elimination") {
		t.Fatalf("unused output was retained: %v", got.AppliedRules)
	}
	if contains(findScan(t, got.Plan).Projection, "x") {
		t.Fatal("unused expression dependency was retained")
	}
	out, err := logical.InferSchema(got.Plan)
	if err != nil || len(out.Fields) != 1 || out.Fields[0].Name != "keep" {
		t.Fatalf("hidden checked output changed visible schema: %+v, %v", out, err)
	}
}

func TestPredicateDoesNotCrossFallibleProjectionOrOrdering(t *testing.T) {
	for _, input := range []logical.Plan{
		&logical.ExprProjection{Input: errorDomainScan(), IncludeInput: true, Exprs: []expr.NamedExpr{
			expr.Named(expr.CastTo(expr.Col("x"), memory.TypeInt8), "checked"),
		}},
		&logical.Sort{Input: errorDomainScan(), By: []logical.SortKey{{Expr: expr.CastTo(expr.Col("x"), memory.TypeInt8)}}},
	} {
		got, err := Optimize(&logical.Filter{Input: input, Predicate: expr.Col("keep")})
		if err != nil {
			t.Fatal(err)
		}
		if pushed := findScan(t, got.Plan).PushedPredicate; pushed != nil {
			t.Fatalf("predicate crossed fallible %T: %v", input, pushed)
		}
	}
}
