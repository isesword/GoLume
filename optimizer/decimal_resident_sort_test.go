package optimizer

import (
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"testing"
)

func TestDecimalResidentSortLimitUsesTopK(t *testing.T) {
	d, e := memory.Decimal128Field("amount", true, 38, 38)
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"amount", "id"} {
		for _, n := range []int64{0, 3, 1000} {
			src := &testSource{schema: memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, d)}
			plan := &logical.Limit{Input: &logical.Sort{Input: &logical.Scan{Source: src}, By: []logical.SortKey{{Expr: expr.Col(key)}}}, N: n}
			got, e := Optimize(plan)
			if e != nil {
				t.Fatal(e)
			}
			if top, ok := got.Plan.(*logical.TopK); !ok || top.K != int(n) {
				t.Fatalf("key=%s n=%d plan=%T", key, n, got.Plan)
			}
			if !contains(got.AppliedRules, "sort-limit-to-topk") {
				t.Fatal(got.AppliedRules)
			}
		}
	}
}
func TestDecimalResidentSortLimitDerivedAndPruned(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64})}
	derived := &logical.ExprProjection{Input: &logical.Scan{Source: src}, Exprs: []expr.NamedExpr{expr.Col("id").Alias("id"), expr.CastDecimal128(expr.Col("id"), 20, 0).Alias("amount")}}
	plan := &logical.Limit{Input: &logical.Sort{Input: derived, By: []logical.SortKey{{Expr: expr.Col("id")}}}, N: 3}
	got, e := Optimize(plan)
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := got.Plan.(*logical.TopK); !ok {
		t.Fatalf("derived native payload lost: %T", got.Plan)
	}
	// Native field removed BEFORE sort does not block the legacy rewrite.
	plan.Input = &logical.Sort{Input: &logical.Projection{Input: derived, Columns: []string{"id"}}, By: []logical.SortKey{{Expr: expr.Col("id")}}}
	got, e = Optimize(plan)
	if e != nil {
		t.Fatal(e)
	}
	if !contains(got.AppliedRules, "sort-limit-to-topk") {
		t.Fatal("pruned Decimal blocked ordinary rewrite", got.AppliedRules)
	}
}
func TestDecimalResidentSortLimitLegacyScope(t *testing.T) {
	for _, payload := range []memory.Field{{Name: "amount", Type: memory.TypeDecimal, Nullable: true}, memory.ListInt64Field("list", true, true)} {
		src := &testSource{schema: memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, payload)}
		plan := &logical.Limit{Input: &logical.Sort{Input: &logical.Scan{Source: src}, By: []logical.SortKey{{Expr: expr.Col("id")}}}, N: 3}
		got, e := Optimize(plan)
		if e != nil {
			t.Fatal(e)
		}
		if !contains(got.AppliedRules, "sort-limit-to-topk") {
			t.Fatal("legacy rewrite removed", got.AppliedRules)
		}
	}
	bad := &logical.Limit{Input: &logical.Sort{Input: &logical.Scan{Source: &testSource{schema: memory.NewSchema()}}, By: []logical.SortKey{{Expr: expr.Col("missing")}}}, N: 3}
	if _, e := Optimize(bad); e == nil {
		t.Fatal("invalid schema silently accepted")
	}
}

func TestDecimalResidentSortLimitExpressionKey(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64})}
	sorted := &logical.Sort{Input: &logical.Scan{Source: src}, By: []logical.SortKey{{Expr: expr.CastDecimal128(expr.Col("id"), 20, 0)}}}
	got, e := Optimize(&logical.Limit{Input: sorted, N: 3})
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := got.Plan.(*logical.TopK); !ok {
		t.Fatalf("hidden Decimal key not rewritten: %T", got.Plan)
	}
	if sortLimitTopKCapable(&logical.Sort{Input: &logical.Scan{Source: src}, By: []logical.SortKey{{Expr: expr.Col("missing")}}}) {
		t.Fatal("unknown key capability guessed")
	}
}
