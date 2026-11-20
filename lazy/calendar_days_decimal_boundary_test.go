package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"strings"
	"testing"
)

// Calendar expressions belong to ordinary Lazy execution, not N34's deliberately
// restricted scan/filter/direct-projection predicate domain.
func TestCalendarDaysDecimalBoundedBoundary(t *testing.T) {
	r := calendarLazyResolver(t)
	ctx := context.Background()
	f := n34PublicFrame(t, n34PublicParquet(t, []int64{2, 1, 2}, nil, []string{"3", "4", "5"}, 0))
	opts := n34PublicOptions(t, engine.DecimalBoundedSumAverageCounts)
	calendar := AddCalendarDays(expr.Timestamp(0), expr.I64(1), r)
	for _, candidate := range []LazyFrame{
		f.Filter(expr.IsNull(calendar)),
		f.Filter(expr.And(expr.GT(Col("k"), expr.I64(0)), expr.IsNull(calendar))),
		f.Filter(expr.And(expr.IsNull(calendar), expr.GT(Col("k"), expr.I64(0)))),
		f.WithColumns(calendar.Alias("next")),
	} {
		result, e := candidate.CollectDecimalBounded(ctx, "k", "d", opts)
		if result != nil {
			result.Close(ctx)
			t.Fatal("unsupported expression acquired result")
		}
		if e == nil || !strings.Contains(e.Error(), "unsupported") {
			t.Fatal("predicate domain widened", e)
		}
		if opts.Budget.Used() != 0 || opts.Store.Used() != 0 || opts.Budget.Snapshot().Closed {
			t.Fatal("preflight ownership")
		}
	}
	// Original admitted filter/order/count contract still works after rejection.
	result, e := f.Filter(expr.GT(Col("k"), expr.I64(0))).CollectDecimalBounded(ctx, "k", "d", opts)
	if e != nil {
		t.Fatal(e)
	}
	defer result.Close(ctx)
	var keys []int64
	var counts []uint64
	for {
		b, ok, e := result.Next(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		for _, row := range b.Records[:b.Rows] {
			keys = append(keys, row.Key)
			counts = append(counts, row.CountRows)
		}
	}
	if len(keys) != 2 || keys[0] != 1 || keys[1] != 2 || counts[0] != 1 || counts[1] != 2 {
		t.Fatal(keys, counts)
	}
	if e := result.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if opts.Budget.Used() != 0 || opts.Store.Used() != 0 || opts.Budget.Snapshot().Closed {
		t.Fatal("postflight ownership")
	}
}
