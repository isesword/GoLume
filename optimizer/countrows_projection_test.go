package optimizer

import (
	"gocolumn/expr"
	"gocolumn/logical"
	"testing"
)

func TestCountRowsEliminatesDeadSelectExprs(t *testing.T) {
	for _, include := range []bool{false, true} {
		projection := &logical.ExprProjection{Input: errorDomainScan(), IncludeInput: include, Exprs: []expr.NamedExpr{expr.Named(narrowingPredicate(), "unused")}}
		plan := &logical.Aggregate{Input: projection, Aggregates: []expr.Aggregate{expr.Count("n")}}
		got, err := Optimize(plan)
		if err != nil {
			t.Fatal(err)
		}
		if !contains(got.AppliedRules, "expression-projection-elimination") {
			t.Fatal(got.AppliedRules)
		}
		agg, ok := got.Plan.(*logical.Aggregate)
		if !ok {
			t.Fatalf("plan %T", got.Plan)
		}
		if p, ok := agg.Input.(*logical.ExprProjection); ok {
			if include || p.IncludeInput || len(p.Exprs) != 1 || p.Exprs[0].Expr.String() != expr.I64(0).String() {
				t.Fatal("expected safe constant carrier")
			}
			name, _ := p.Exprs[0].Name()
			if name != "unused" {
				t.Fatal("carrier alias changed")
			}
		} else if !include {
			t.Fatal("SelectExprs schema barrier lost")
		}
		// Source columns may carry row cardinality internally, but cannot leak
		// through the aggregate's externally visible schema.
		schema, err := logical.InferSchema(got.Plan)
		if err != nil || len(schema.Fields) != 1 || schema.Fields[0].Name != "n" {
			t.Fatalf("schema %+v err %v", schema, err)
		}
		if len(projection.Exprs) != 1 {
			t.Fatal("input plan mutated")
		}
		again, err := Optimize(got.Plan)
		if err != nil {
			t.Fatal(err)
		}
		if !include {
			p, ok := again.Plan.(*logical.Aggregate).Input.(*logical.ExprProjection)
			if !ok || p.IncludeInput || len(p.Exprs) != 1 {
				t.Fatal("carrier shape changed")
			}
			name, _ := p.Exprs[0].Name()
			if name != "unused" || p.Exprs[0].Expr.String() != expr.I64(0).String() {
				t.Fatal("carrier changed on reoptimization")
			}
		}
		schema, err = logical.InferSchema(again.Plan)
		if err != nil || len(schema.Fields) != 1 || schema.Fields[0].Name != "n" {
			t.Fatal("reoptimization changed schema", err)
		}
	}
}
