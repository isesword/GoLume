package optimizer

import (
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"testing"
)

func TestN17DecimalGroupedSortedPlan(t *testing.T) {
	d, _ := memory.Decimal128Field("d", true, 38, 38)
	scan := &logical.Scan{Source: &testSource{schema: memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64, Nullable: true}, d)}}
	sorted := &logical.Sort{Input: scan, By: []logical.SortKey{{Expr: expr.Col("k")}}}
	plan := &logical.Aggregate{Input: sorted, GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("d", "sum"), expr.Count("rows")}}
	out, e := Optimize(plan)
	if e != nil {
		t.Fatal(e)
	}
	agg, ok := out.Plan.(*logical.Aggregate)
	if !ok {
		t.Fatalf("%T", out.Plan)
	}
	if _, ok := agg.Input.(*logical.Sort); !ok {
		t.Fatalf("sort lost %T", agg.Input)
	}
	sc, e := logical.InferSchema(out.Plan)
	if e != nil || len(sc.Fields) != 3 || sc.Fields[0].Type != memory.TypeInt64 || !sc.Fields[0].Nullable || sc.Fields[1].DecimalPrecision != 38 || sc.Fields[1].DecimalScale != 38 || sc.Fields[2].Type != memory.TypeUint64 {
		t.Fatal(sc, e)
	}
}
