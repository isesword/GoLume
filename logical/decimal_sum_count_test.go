package logical

import (
	"gocolumn/expr"
	"gocolumn/memory"
	"testing"
)

func TestN15DecimalSumCountSchema(t *testing.T) {
	d, _ := memory.Decimal128Field("d", true, 38, 38)
	scan := &Scan{Source: n14SchemaSource{schema: memory.NewSchema(d, memory.Field{Name: "i", Type: memory.TypeInt64})}}
	aggs := []expr.Aggregate{expr.Count("rows"), expr.Sum("d", "total"), expr.CountColumn("d", "valid"), {Kind: expr.AggCountRows, Column: "not_a_column", Alias: "rows2"}, expr.Sum("d", "")}
	sc, e := InferSchema(&Aggregate{Input: scan, Aggregates: aggs})
	if e != nil {
		t.Fatal(e)
	}
	for i, f := range sc.Fields {
		if i == 1 || i == 4 {
			if f.Type != memory.TypeDecimal || f.DecimalPrecision != 38 || f.DecimalScale != 38 || !f.Nullable {
				t.Fatal(f)
			}
		} else if f.Type != memory.TypeUint64 || f.Nullable {
			t.Fatal(f)
		}
	}
	if sc.Fields[4].Name != "agg_4" {
		t.Fatal(sc)
	}
	for _, bad := range [][]expr.Aggregate{{expr.Sum("d", "s"), expr.CountColumn("", "n")}, {expr.Sum("d", "s"), expr.CountColumn("missing", "n")}, {expr.Sum("d", "s"), expr.CountColumn("i", "n")}, {expr.Sum("d", "s"), expr.Avg("d", "a")}, {expr.CountColumn("d", "n")}} {
		if _, e := InferSchema(&Aggregate{Input: scan, Aggregates: bad}); e == nil {
			t.Fatal("unsupported", bad)
		}
	}
}
