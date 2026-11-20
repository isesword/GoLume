package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
)

func TestN21DecimalGroupedAverageLateOverflow(t *testing.T) {
	en, _, src := n17Fixture(t, 66, 17, false, false, false)
	ix := 0
	for _, b := range src.batches {
		v := b.Decimals["d"]
		for i := range v.Values {
			coefficient := int64(100)
			if ix/3 == 66 {
				coefficient = 1000
			}
			v.Values[i] = decimal128.FromI64(coefficient)
			ix++
		}
	}
	p, err := en.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{
		GroupBy:    []string{"k"},
		Aggregates: []expr.AggregateSpec{expr.AvgDecimal128("d", 1, 0, expr.DecimalRoundHalfEven, "s")},
	}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	pages := 0
	_, _, err = n17Run(t, p, 64<<20, 0, 64, func(b *memory.Batch) {
		pages++
		if b.Rows != 64 {
			t.Fatal("partial overflowing page emitted", b.Rows)
		}
		v := b.Decimals["s"]
		if v.Precision != 1 || v.Scale != 0 {
			t.Fatal("AVG metadata", v.Precision, v.Scale)
		}
		for row, x := range v.Values {
			if !memory.IsSet(v.Validity, row) || x != decimal128.FromI64(1) {
				t.Fatal("prior page value", row, x)
			}
		}
	})
	if pages != 1 || !errors.Is(err, aggregate.ErrDecimalAverageOverflow) {
		t.Fatal("expected one committed page followed by precise AVG overflow", pages, err)
	}
}
