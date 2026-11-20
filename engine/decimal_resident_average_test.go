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

func TestN22DecimalResidentAveragePreflightOverflow(t *testing.T) {
	en, _, src := n18Fixture(t, 130, 1, false)
	// Insert the overflowing key last in the encounter order and run across
	// more than one output page. Resident preflight must expose no partial page.
	encountered := map[int64]bool{}
	var key int64
	for _, b := range src.batches {
		keys := b.Int64s["k"]
		for i := 0; i < b.Rows; i++ {
			k := keys.Values[i]
			if keys.IsValid(i) && !encountered[k] {
				encountered[k] = true
				key = k
			}
		}
	}
	if len(encountered) < 65 {
		t.Fatal("fixture does not cross a page", len(encountered))
	}
	for _, b := range src.batches {
		for i := 0; i < b.Rows; i++ {
			coefficient := int64(100)
			keys := b.Int64s["k"]
			if keys.IsValid(i) && keys.Values[i] == key {
				coefficient = 1000
			}
			b.Decimals["d"].Values[i] = decimal128.FromI64(coefficient)
		}
	}
	p, e := en.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{
		GroupBy:    []string{"k"},
		Aggregates: []expr.AggregateSpec{expr.AvgDecimal128("d", 1, 0, expr.DecimalRoundHalfEven, "s"), expr.Count("n")},
	}, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	seen := 0
	_, _, e = n18Run(t, p, 64<<20, 0, func(*memory.Batch) { seen++ })
	if seen != 0 || !errors.Is(e, aggregate.ErrDecimalAverageOverflow) {
		t.Fatal(seen, e)
	}
}

func TestN22DecimalResidentAverageFiniteBudget(t *testing.T) {
	en, _, src := n18Fixture(t, 195, 1, false)
	query := expr.ConfiguredQuery{GroupBy: []string{"k"}, Aggregates: []expr.AggregateSpec{expr.AvgDecimal128("d", 4, 2, expr.DecimalRoundHalfEven, "s"), expr.Count("n")}}
	p, e := en.PrepareConfigured(context.Background(), src, query, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	rows := 0
	calls, peak, e := n18Run(t, p, 64<<20, 0, func(b *memory.Batch) { rows += b.Rows })
	if e != nil || peak <= 0 || rows != 196 {
		t.Fatal(rows, peak, e)
	}
	t.Logf("groups%d calls%d peak%d", rows, calls, peak)
	for i := 1; i <= calls; i++ {
		_, _, e = n18Run(t, p, 64<<20, i, nil)
		if !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("ordinal", i, e)
		}
	}
	if _, _, e = n18Run(t, p, peak, 0, nil); e != nil {
		t.Fatal("exact peak", e)
	}
	if _, _, e = n18Run(t, p, peak-1, 0, nil); !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("peak minus one", e)
	}

	unlimited := New(Options{ExecutionPolicy: ExecutionMemoryOnly})
	defer unlimited.Close()
	up, e := unlimited.PrepareConfigured(context.Background(), src, query, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer up.Close()
	q, e := memory.NewEngineBudget(0).BeginQuery(memory.BudgetOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	st, e := up.NewResidentAggregateStream(memory.WithQueryBudget(context.Background(), q), up.resultSchema(), 64)
	if e == nil {
		st.Close()
		t.Fatal("unlimited accepted")
	}
	if q.Used() != 0 {
		t.Fatal("retained", q.Used())
	}
}
