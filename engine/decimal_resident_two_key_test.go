package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/hashagg"
	"gocolumn/memory"
	"gocolumn/planner"
)

func n24Query(precision int32) expr.ConfiguredQuery {
	return expr.ConfiguredQuery{GroupBy: []string{"x", "y"}, Aggregates: []expr.AggregateSpec{
		expr.Sum("d", "s"), expr.Count("n"), expr.CountColumn("d", "valid"),
		expr.AvgDecimal128("d", precision, 0, expr.DecimalRoundHalfEven, "mean"),
	}}
}

func TestN24DecimalResidentTwoKey(t *testing.T) {
	// NewGroupIndex starts with 16 slots here. Force matching low bits well
	// beyond that mask to exercise real bucket collisions in the operator.
	a := n23Key{7, 0, true, true}
	var left, right [18]byte
	left[0], left[1], left[9] = 1, 7, 1
	right = left
	for n := int64(1); ; n++ {
		binary.LittleEndian.PutUint64(right[10:], uint64(n))
		if hashagg.HashBytes(left[:])&63 == hashagg.HashBytes(right[:])&63 {
			if hashagg.HashBytes(left[:]) == hashagg.HashBytes(right[:]) {
				t.Fatal("expected bucket, not full hash collision")
			}
			break
		}
	}
	b := n23Key{7, int64(binary.LittleEndian.Uint64(right[10:])), true, true}
	c, d, e := n23Key{7, 0, true, false}, n23Key{0, 0, false, true}, n23Key{}
	src := n23Data(t, [][]n23Row{
		{{a, 11, true}, {b, 25, true}, {c, 7, false}, {d, -25, true}},
		{{a, 19, true}, {n23Key{x: 999, y: 999}, 100, false}, {n23Key{x: -123, y: 567}, 45, true}, {b, 25, true}},
	}, nil)
	ctx := context.Background()
	en := New(Options{ExecutionPolicy: ExecutionMemoryOnly, MemoryLimitBytes: 64 << 20})
	defer en.Close()
	p, err := en.PrepareConfigured(ctx, src, n24Query(5), PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.strategy.Kind != planner.StrategyBitmapDirect || !p.decimalResident() {
		t.Fatal(p.strategy)
	}
	type result struct {
		sum, avg int64
		n, valid uint64
	}
	want := map[n23Key]result{a: {30, 2, 2, 2}, b: {50, 2, 2, 2}, c: {0, 0, 1, 0}, d: {-25, -2, 1, 1}, e: {45, 4, 2, 1}}
	seen := make(map[n23Key]bool)
	calls, peak, err := n18Run(t, p, 64<<20, 0, func(batch *memory.Batch) {
		x, y, s, av, n, v := batch.Int64s["x"], batch.Int64s["y"], batch.Decimals["s"], batch.Decimals["mean"], batch.Uint64s["n"], batch.Uint64s["valid"]
		if x == nil || y == nil || s == nil || av == nil || n == nil || v == nil {
			t.Fatal("missing output")
		}
		if s.Precision != 38 || s.Scale != 1 || av.Precision != 5 || av.Scale != 0 {
			t.Fatal("wrong decimal metadata")
		}
		for i := 0; i < batch.Rows; i++ {
			var k n23Key
			if k.xv = x.IsValid(i); k.xv {
				k.x = x.Values[i]
			}
			if k.yv = y.IsValid(i); k.yv {
				k.y = y.Values[i]
			}
			w, ok := want[k]
			if !ok || seen[k] || n.Values[i] != w.n || v.Values[i] != w.valid {
				t.Fatal("key/count", k, w)
			}
			seen[k] = true
			has := w.valid != 0
			if s.IsValid(i) != has || av.IsValid(i) != has || has && (s.Values[i] != decimal128.FromI64(w.sum) || av.Values[i] != decimal128.FromI64(w.avg)) {
				t.Fatal("coefficients", k, w, s.Values[i], av.Values[i])
			}
		}
	})
	if err != nil || len(seen) != len(want) || peak <= 0 || calls == 0 {
		t.Fatal(seen, calls, peak, err)
	}
	t.Logf("resident two-key groups=%d peak=%d reserve-calls=%d", len(seen), peak, calls)
	if _, _, err = n18Run(t, p, peak, 0, nil); err != nil {
		t.Fatal("exact peak", err)
	}
	if _, _, err = n18Run(t, p, peak-1, 0, nil); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal("peak minus one", err)
	}
	for i := 1; i <= calls; i++ {
		if _, _, err = n18Run(t, p, 64<<20, i, nil); !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatal("reserve fault", i, err)
		}
	}
	q, err := memory.NewEngineBudget(0).BeginQuery(memory.BudgetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	st, err := p.NewResidentAggregateStream(memory.WithQueryBudget(ctx, q), p.resultSchema(), 64)
	if err == nil {
		st.Close()
		t.Fatal("unlimited budget accepted")
	}
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
}

func TestN24DecimalResidentTwoKeyPreflightAndCancel(t *testing.T) {
	var rows []n23Row
	for i := int64(0); i < 130; i++ {
		rows = append(rows, n23Row{n23Key{1, i, true, true}, 15, true})
	}
	rows[len(rows)-1].d = 1000
	src := n23Data(t, [][]n23Row{rows[:65], rows[65:]}, nil)
	ctx := context.Background()
	en := New(Options{ExecutionPolicy: ExecutionMemoryOnly, MemoryLimitBytes: 64 << 20})
	defer en.Close()
	p, err := en.PrepareConfigured(ctx, src, n24Query(1), PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	pages := 0
	if _, _, err = n18Run(t, p, 64<<20, 0, func(*memory.Batch) { pages++ }); !errors.Is(err, aggregate.ErrDecimalAverageOverflow) || pages != 0 {
		t.Fatal("late overflow exposed a page", pages, err)
	}
	q, err := en.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	base := memory.WithQueryBudget(ctx, q)
	st, err := p.NewResidentAggregateStream(base, p.resultSchema(), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := st.(*decimalResidentStream)
	canceled := &n14CancelContext{Context: base, function: "(*decimalResidentStream).grow", at: 1}
	ok, err := s.Next(canceled, memory.NewBatch())
	if ok || !errors.Is(err, context.Canceled) || q.Used() != 0 || s.index != nil || s.values != nil {
		t.Fatal(ok, err, q.Used())
	}
	if ok, err = s.Next(base, memory.NewBatch()); ok || !errors.Is(err, context.Canceled) {
		t.Fatal("sticky", ok, err)
	}
}
