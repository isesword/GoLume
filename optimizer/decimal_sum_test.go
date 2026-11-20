package optimizer

import (
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"testing"
)

func TestN14DecimalSumMetadataSurvivesOptimizer(t *testing.T) {
	for _, scale := range []int32{0, 38} {
		f, e := memory.Decimal128Field("d", true, 38, scale)
		if e != nil {
			t.Fatal(e)
		}
		scan := &logical.Scan{Source: &testSource{schema: memory.NewSchema(f)}}
		plan := &logical.Projection{Input: &logical.Aggregate{Input: scan, Aggregates: []expr.Aggregate{expr.Sum("d", "sum"), expr.Sum("d", "alias")}}, Columns: []string{"alias"}}
		out, e := Optimize(plan)
		if e != nil {
			t.Fatal(e)
		}
		sc, e := logical.InferSchema(out.Plan)
		if e != nil {
			t.Fatal(e)
		}
		if len(sc.Fields) != 1 || sc.Fields[0].DecimalPrecision != 38 || sc.Fields[0].DecimalScale != scale || !sc.Fields[0].Nullable {
			t.Fatal(sc)
		}
	}
}
