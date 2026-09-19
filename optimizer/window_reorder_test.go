package optimizer

import (
	"testing"

	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
)

// TestWindowReplacementPreservesColumnOrder pins the WithWindows contract that
// an expression whose alias matches an input column replaces that column in
// place: pruning must never move the replacement to the end of the schema.
func TestWindowReplacementPreservesColumnOrder(t *testing.T) {
	src := func() *testSource {
		return &testSource{schema: memory.NewSchema(
			memory.Field{Name: "a", Type: memory.TypeInt64},
			memory.Field{Name: "b", Type: memory.TypeInt64},
			memory.Field{Name: "c", Type: memory.TypeInt64},
		)}
	}
	rowNumber := func(alias string) logical.WindowExpr {
		return logical.WindowExpr{Function: logical.WindowRowNumber, Alias: alias}
	}
	orderByC := []logical.SortKey{{Expr: expr.Col("c")}}
	cases := []struct {
		name       string
		plan       func() logical.Plan
		want       []string
		wantRule   string
		wantNoRule bool
	}{
		{
			name: "alias replaces middle column",
			plan: func() logical.Plan {
				return &logical.Window{
					Input: &logical.Scan{Source: src()},
					Spec:  logical.WindowSpec{PartitionBy: []string{"a"}, By: orderByC},
					Exprs: []logical.WindowExpr{rowNumber("b")},
				}
			},
			want:     []string{"a", "b", "c"},
			wantRule: "window-reorder",
		},
		{
			name: "alias replaces first column under downstream narrowing",
			plan: func() logical.Plan {
				window := &logical.Window{
					Input: &logical.Scan{Source: src()},
					Spec:  logical.WindowSpec{PartitionBy: []string{"b"}, By: orderByC},
					Exprs: []logical.WindowExpr{rowNumber("a")},
				}
				// Sort passes the required column set through unchanged, so the
				// window node output order remains observable in the plan schema.
				return &logical.Sort{Input: window, By: orderByC}
			},
			want:     []string{"a", "b", "c"},
			wantRule: "window-reorder",
		},
		{
			name: "new alias appends without reorder",
			plan: func() logical.Plan {
				return &logical.Window{
					Input: &logical.Scan{Source: src()},
					Spec:  logical.WindowSpec{PartitionBy: []string{"a"}, By: orderByC},
					Exprs: []logical.WindowExpr{rowNumber("rn")},
				}
			},
			want:       []string{"a", "b", "c", "rn"},
			wantNoRule: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Optimize(tc.plan())
			if err != nil {
				t.Fatal(err)
			}
			schema, err := logical.InferSchema(got.Plan)
			if err != nil {
				t.Fatal(err)
			}
			if !equalStrings(schemaNames(schema), tc.want) {
				t.Fatalf("schema=%v want=%v: replaced column must keep its original position", schemaNames(schema), tc.want)
			}
			if tc.wantNoRule {
				if contains(got.AppliedRules, "window-reorder") {
					t.Fatalf("rules=%v: stable column order must not insert a reorder", got.AppliedRules)
				}
				return
			}
			if !contains(got.AppliedRules, tc.wantRule) {
				t.Fatalf("rules=%v missing %s", got.AppliedRules, tc.wantRule)
			}
			if !contains(got.AppliedRules, "projection-pushdown") {
				t.Fatalf("rules=%v: replaced input column must stay pruned", got.AppliedRules)
			}
		})
	}
}

// TestWindowReplacementKeepsPrunedExpressionDead ensures the reorder fix never
// resurrects window expressions the caller no longer requires.
func TestWindowReplacementKeepsPrunedExpressionDead(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64},
		memory.Field{Name: "b", Type: memory.TypeInt64},
	)}
	window := &logical.Window{
		Input: &logical.Scan{Source: src},
		Spec:  logical.WindowSpec{PartitionBy: []string{"a"}, By: []logical.SortKey{{Expr: expr.Col("a")}}},
		Exprs: []logical.WindowExpr{
			{Function: logical.WindowRowNumber, Alias: "b"},
			{Function: logical.WindowRank, Alias: "dead"},
		},
	}
	plan := &logical.Projection{Input: window, Columns: []string{"b"}}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := logical.InferSchema(got.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"b"}; !equalStrings(schemaNames(schema), want) {
		t.Fatalf("schema=%v want=%v", schemaNames(schema), want)
	}
	if !contains(got.AppliedRules, "window-expression-pruning") {
		t.Fatalf("rules=%v missing window-expression-pruning", got.AppliedRules)
	}
}
