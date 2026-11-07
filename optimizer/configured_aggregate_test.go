package optimizer

import (
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"testing"
)

func TestConfiguredAggregateProjectionAndHaving(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeFloat32}, memory.Field{Name: "pred", Type: memory.TypeInt64}, memory.Field{Name: "unused", Type: memory.TypeInt64})}
	node := &logical.ConfiguredAggregate{Input: &logical.Filter{Input: &logical.Scan{Source: src}, Predicate: expr.GT(expr.Col("pred"), expr.Lit(0))}, GroupBy: []string{"k"}, Aggregates: []expr.AggregateSpec{expr.Quantile("v", .25, aggregate.QuantileLinear, "q"), expr.Count("n")}}
	plan := &logical.Projection{Input: &logical.Filter{Input: node, Predicate: expr.GT(expr.Col("q"), expr.Float64Literal{Value: 1})}, Columns: []string{"q"}}
	result, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	filter := result.Plan.(*logical.Projection).Input.(*logical.Filter)
	agg, ok := filter.Input.(*logical.ConfiguredAggregate)
	if !ok {
		t.Fatalf("%T", filter.Input)
	}
	scan := agg.Input.(*logical.Filter).Input.(*logical.Scan)
	if contains(scan.Projection, "unused") || !contains(scan.Projection, "v") || !contains(scan.Projection, "k") || !contains(scan.Projection, "pred") {
		t.Fatal(scan.Projection)
	}
	if scan.PushedPredicate == nil || scan.PushedPredicate.String() != "(pred > 0)" {
		t.Fatal(scan.PushedPredicate)
	}
	if got := agg.Aggregates[0].(expr.QuantileSpec); got.Probability != .25 || got.Mode != aggregate.QuantileLinear {
		t.Fatal(got)
	}
	agg.GroupBy[0] = "changed"
	agg.Aggregates[0] = expr.Count("changed")
	if node.GroupBy[0] != "k" || node.Aggregates[0].(expr.QuantileSpec).Probability != .25 {
		t.Fatal("optimizer aliased input")
	}
}
