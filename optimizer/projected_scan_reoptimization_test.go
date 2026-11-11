package optimizer

import (
	"fmt"
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"reflect"
	"slices"
	"testing"
)

func TestProjectedScanReoptimization(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "unused", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "key", Type: memory.TypeInt32})
	for _, columns := range [][]string{{"value"}, {"value", "key"}, {"key", "value"}, {"key", "unused", "value"}, nil, {}} {
		t.Run(fmt.Sprintf("%v", columns), func(t *testing.T) {
			original := &logical.Scan{Source: &testSource{schema: schema}, Projection: slices.Clone(columns)}
			expected, err := logical.InferSchema(original)
			if err != nil {
				t.Fatal(err)
			}
			var plan logical.Plan = original
			for pass := 0; pass < 5; pass++ {
				before := logical.Explain(plan)
				result, err := Optimize(plan)
				if err != nil {
					t.Fatal(err)
				}
				actual, err := logical.InferSchema(result.Plan)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("pass %d: schema=%+v want=%+v", pass, actual, expected)
				}
				if _, ok := result.Plan.(*logical.Scan); !ok {
					t.Fatalf("pass %d: unexpected wrapper %T", pass, result.Plan)
				}
				if logical.Explain(plan) != before {
					t.Fatalf("pass %d mutated input", pass)
				}
				if pass > 0 && logical.Explain(result.Plan) != before {
					t.Fatalf("pass %d changed stable scan", pass)
				}
				plan = result.Plan
			}
			if !equalStrings(original.Projection, columns) {
				t.Fatalf("input projection mutated: %v", original.Projection)
			}
		})
	}
}

func TestRemovedProjectionRemainsRemovedOnReoptimization(t *testing.T) {
	scan := &logical.Scan{Source: &testSource{schema: memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeInt64}, memory.Field{Name: "hidden", Type: memory.TypeInt64})}}
	first, err := Optimize(&logical.Projection{Input: scan, Columns: []string{"value"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := first.Plan.(*logical.Scan); !ok {
		t.Fatalf("projection not removed: %T", first.Plan)
	}
	for pass := 0; pass < 4; pass++ {
		next, err := Optimize(first.Plan)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := logical.InferSchema(next.Plan)
		if err != nil {
			t.Fatal(err)
		}
		if !equalStrings(schemaNames(schema), []string{"value"}) {
			t.Fatalf("pass %d resurrected hidden column: %v", pass, schemaNames(schema))
		}
		first = next
	}
	invalid := &logical.ExprProjection{Input: first.Plan, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.Col("hidden"), "leak")}}
	if _, err := Optimize(invalid); err == nil {
		t.Fatal("hidden field became available to a later expression")
	}
}

func TestProjectedScanParentSchemaIsStable(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "hidden", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "key", Type: memory.TypeInt64})
	wrappers := map[string]func(logical.Plan) logical.Plan{
		"filter": func(p logical.Plan) logical.Plan {
			return &logical.Filter{Input: p, Predicate: expr.GT(expr.Col("value"), expr.I64(0))}
		},
		"limit": func(p logical.Plan) logical.Plan { return &logical.Limit{Input: p, N: 2} },
		"sort": func(p logical.Plan) logical.Plan {
			return &logical.Sort{Input: p, By: []logical.SortKey{{Expr: expr.Col("value")}}}
		},
		"topk": func(p logical.Plan) logical.Plan {
			return &logical.TopK{Input: p, K: 2, By: []logical.SortKey{{Expr: expr.Col("value")}}}
		},
		"with_columns": func(p logical.Plan) logical.Plan {
			return &logical.ExprProjection{Input: p, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.Col("value").Add(expr.I64(1)), "next")}}
		},
		"replace": func(p logical.Plan) logical.Plan {
			return &logical.ExprProjection{Input: p, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.I64(9), "value")}}
		},
		"literal": func(p logical.Plan) logical.Plan {
			return &logical.ExprProjection{Input: p, Exprs: []expr.NamedExpr{expr.Named(expr.I64(9), "literal")}}
		},
		"count": func(p logical.Plan) logical.Plan {
			return &logical.Aggregate{Input: p, Aggregates: []expr.Aggregate{expr.Count("rows")}}
		},
	}
	for name, wrap := range wrappers {
		for _, columns := range [][]string{{"value"}, {"key", "value"}, {"value", "key"}} {
			t.Run(fmt.Sprintf("%s/%v", name, columns), func(t *testing.T) {
				scan := &logical.Scan{Source: &testSource{schema: schema}, Projection: columns}
				plan := wrap(scan)
				expected, err := logical.InferSchema(plan)
				if err != nil {
					t.Fatal(err)
				}
				for pass := 0; pass < 4; pass++ {
					result, err := Optimize(plan)
					if err != nil {
						t.Fatal(err)
					}
					actual, err := logical.InferSchema(result.Plan)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(actual, expected) {
						t.Fatalf("pass %d: schema=%+v want=%+v", pass, actual, expected)
					}
					if contains(findScan(t, result.Plan).Projection, "hidden") {
						t.Fatalf("pass %d resurrected hidden input: %s", pass, logical.Explain(result.Plan))
					}
					plan = result.Plan
				}
			})
		}
	}
}
