package logical

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
)

type n14SchemaSource struct {
	source.Source
	schema memory.Schema
}

func (s n14SchemaSource) Schema() memory.Schema                       { return s.schema }
func (s n14SchemaSource) Stats(context.Context) (source.Stats, error) { return source.Stats{}, nil }
func TestN14DecimalSumSchema(t *testing.T) {
	d, _ := memory.Decimal128Field("d", true, 1, 1)
	scan := &Scan{Source: n14SchemaSource{schema: memory.NewSchema(d)}}
	sc, e := InferSchema(&Aggregate{Input: scan, Aggregates: []expr.Aggregate{expr.Sum("d", "a"), expr.Sum("d", "b")}})
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range sc.Fields {
		if f.Type != memory.TypeDecimal || f.DecimalPrecision != 38 || f.DecimalScale != 1 || !f.Nullable {
			t.Fatal(f)
		}
	}
	for _, aggs := range [][]expr.Aggregate{{expr.Avg("d", "a")}, {expr.Min("d", "a")}} {
		if _, e := InferSchema(&Aggregate{Input: scan, Aggregates: aggs}); e == nil {
			t.Fatal("unsupported mixture")
		}
	}
	if _, e := InferSchema(&Aggregate{Input: scan, GroupBy: []string{"d"}, Aggregates: []expr.Aggregate{expr.Sum("d", "a")}}); e == nil {
		t.Fatal("grouped expanded")
	}
}
