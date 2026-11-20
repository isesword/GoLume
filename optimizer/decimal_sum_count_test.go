package optimizer

import (
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"testing"
)

func TestN15DecimalSumCountMetadataSurvivesOptimizer(t *testing.T) {
	for _, scale := range []int32{0, 38} {
		f, e := memory.Decimal128Field("d", true, 38, scale)
		if e != nil {
			t.Fatal(e)
		}
		scan := &logical.Scan{Source: &testSource{schema: memory.NewSchema(f)}}
		plan := &logical.Projection{Input: &logical.Aggregate{Input: scan, Aggregates: []expr.Aggregate{expr.Sum("d", "sum"), expr.CountColumn("d", "valid"), expr.Count("rows")}}, Columns: []string{"rows", "sum", "valid"}}
		out, e := Optimize(plan)
		if e != nil {
			t.Fatal(e)
		}
		sc, e := logical.InferSchema(out.Plan)
		if e != nil {
			t.Fatal(e)
		}
		if len(sc.Fields) != 3 || sc.Fields[1].DecimalPrecision != 38 || sc.Fields[1].DecimalScale != scale || !sc.Fields[1].Nullable || sc.Fields[0].Type != memory.TypeUint64 || sc.Fields[0].Nullable || sc.Fields[2].Type != memory.TypeUint64 || sc.Fields[2].Nullable {
			t.Fatal(sc)
		}
	}
}
