package logical

import (
	"gocolumn/expr"
	"gocolumn/memory"
	"testing"
)

func TestN17DecimalGroupedSchema(t *testing.T) {
	d, _ := memory.Decimal128Field("d", true, 38, 38)
	scan := &Scan{Source: n14SchemaSource{schema: memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64, Nullable: true}, d)}}
	aggs := []expr.Aggregate{expr.Count("rows"), expr.Sum("d", "sum"), expr.CountColumn("d", "valid")}
	sc, e := InferSchema(&Aggregate{Input: scan, GroupBy: []string{"k"}, Aggregates: aggs})
	if e != nil {
		t.Fatal(e)
	}
	if len(sc.Fields) != 4 || sc.Fields[0].Name != "k" || sc.Fields[0].Type != memory.TypeInt64 || !sc.Fields[0].Nullable || sc.Fields[1].Type != memory.TypeUint64 || sc.Fields[1].Nullable || sc.Fields[2].DecimalPrecision != 38 || sc.Fields[2].DecimalScale != 38 || !sc.Fields[2].Nullable || sc.Fields[3].Type != memory.TypeUint64 {
		t.Fatal(sc)
	}
	for _, keys := range [][]string{{"d"}, {"missing"}, {"k", "k"}} {
		if _, e := InferSchema(&Aggregate{Input: scan, GroupBy: keys, Aggregates: aggs}); e == nil {
			t.Fatal(keys)
		}
	}
}
