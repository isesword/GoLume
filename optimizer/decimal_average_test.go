package optimizer

import (
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"strings"
	"testing"
)

func TestN20AverageConfigurationProjection(t *testing.T) {
	d, _ := memory.Decimal128Field("d", true, 38, 2)
	src := &testSource{schema: memory.NewSchema(d, memory.Field{Name: "pred", Type: memory.TypeInt64}, memory.Field{Name: "unused", Type: memory.TypeInt64}, memory.Field{Name: "third", Type: memory.TypeInt64})}
	spec := expr.AvgDecimal128("d", 15, 4, expr.DecimalRoundHalfEven, "mean")
	n := &logical.ConfiguredAggregate{Input: &logical.Filter{Input: &logical.Scan{Source: src}, Predicate: expr.GT(expr.Col("pred"), expr.Lit(0))}, Aggregates: []expr.AggregateSpec{spec, expr.Count("n")}}
	p := &logical.Projection{Input: n, Columns: []string{"mean"}}
	result, e := Optimize(p)
	if e != nil {
		t.Fatal(e)
	}
	sc, e := logical.InferSchema(result.Plan)
	if e != nil || sc.Fields[0].DecimalPrecision != 15 || sc.Fields[0].DecimalScale != 4 || !sc.Fields[0].Nullable {
		t.Fatal(sc, e)
	}
	agg := result.Plan.(*logical.Projection).Input.(*logical.ConfiguredAggregate)
	scan := agg.Input.(*logical.Filter).Input.(*logical.Scan)
	if contains(scan.Projection, "unused") || !contains(scan.Projection, "d") || !contains(scan.Projection, "pred") {
		t.Fatal(scan.Projection)
	}
	if agg.Aggregates[0].(expr.DecimalAverageSpec) != spec {
		t.Fatal(agg.Aggregates)
	}
	explain := logical.Explain(result.Plan)
	if !strings.Contains(explain, "p=15,s=4,mode=half-even") {
		t.Fatal(explain)
	}
	agg.Aggregates[0] = expr.Count("different")
	if n.Aggregates[0].(expr.DecimalAverageSpec) != spec {
		t.Fatal("aliased options")
	}
	for _, specs := range [][]expr.AggregateSpec{{expr.Avg("d", "a")}, {expr.AvgDecimal128("pred", 3, 0, expr.DecimalRoundHalfEven, "a")}, {spec, expr.Count("mean")}} {
		if _, e := logical.InferSchema(&logical.ConfiguredAggregate{Input: n.Input, Aggregates: specs}); e == nil {
			t.Fatal("invalid accepted", specs)
		}
	}
	grouped := &logical.ConfiguredAggregate{Input: n.Input, GroupBy: []string{"pred"}, Aggregates: []expr.AggregateSpec{spec}}
	groupPlan, e := Optimize(grouped)
	if e != nil {
		t.Fatal(e)
	}
	groupSchema, e := logical.InferSchema(groupPlan.Plan)
	if e != nil || len(groupSchema.Fields) != 2 {
		t.Fatal(groupSchema, e)
	}
	key, mean := groupSchema.Fields[0], groupSchema.Fields[1]
	if key.Name != "pred" || key.Type != memory.TypeInt64 || mean.Name != "mean" || mean.Type != memory.TypeDecimal || !mean.Nullable || mean.DecimalPrecision != 15 || mean.DecimalScale != 4 {
		t.Fatal(groupSchema)
	}
	if got := groupPlan.Plan.(*logical.ConfiguredAggregate).Aggregates[0]; got != spec {
		t.Fatal(got)
	}
	// One or two Int64 keys are a logical shape; execution still requires
	// globally ordered input, verified independently in the Lazy tests.
	if _, e := logical.InferSchema(&logical.ConfiguredAggregate{Input: n.Input, GroupBy: []string{"pred", "unused", "third"}, Aggregates: []expr.AggregateSpec{spec}}); e == nil {
		t.Fatal("third group key accepted")
	}
	if _, e := logical.InferSchema(&logical.ConfiguredAggregate{Input: n.Input, GroupBy: []string{"pred"}, Aggregates: []expr.AggregateSpec{expr.AvgDecimal128("d", 15, 4, expr.DecimalRoundHalfEven, "pred")}}); e == nil {
		t.Fatal("aggregate alias collides with group key")
	}
	if _, e := logical.InferSchema(&logical.Aggregate{Input: n.Input, GroupBy: []string{"pred", "unused"}, Aggregates: []expr.Aggregate{expr.Sum("d", "unused")}}); e == nil {
		t.Fatal("bare SUM alias collides with second key")
	}

}
