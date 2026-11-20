package engine

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestN20ConfiguredAverageWideResult(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 38, 0)
	max, _ := decimal128.FromString("99999999999999999999999999999999999999", 38, 0)
	b := memory.NewBatch()
	b.Rows = 2
	b.Decimals["d"] = &memory.Decimal128Vector{Precision: 38, Values: []memory.Decimal128{max, max}}
	src, e := inmemory.New(memory.NewSchema(f), b)
	if e != nil {
		t.Fatal(e)
	}
	en := New(Options{MemoryLimitBytes: 1 << 20})
	defer en.Close()
	p, e := en.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.AvgDecimal128("d", 38, 0, expr.DecimalRoundHalfEven, "avg")}}, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	r, e := p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: true})
	if e != nil {
		t.Fatal(e)
	}
	got := r.Rows[0].Aggregates[0].Value
	if !got.Valid || got.Coefficient != max || got.Precision != 38 || got.Scale != 0 {
		t.Fatal(got)
	}
}

func n20Prepared(t *testing.T, repeat int, filter expr.Expr, specs []expr.AggregateSpec) (*Engine, *PreparedQuery, *n14StreamSource) {
	t.Helper()
	f, _ := memory.Decimal128Field("d", true, 38, 1)
	b := memory.NewBatch()
	b.Rows = 4
	b.Decimals["d"] = &memory.Decimal128Vector{Precision: 38, Scale: 1, Values: []memory.Decimal128{decimal128.FromI64(15), decimal128.FromI64(20), decimal128.FromI64(-5), decimal128.New(math.MaxInt64, math.MaxUint64)}, Validity: memory.Bitmap{7}}
	src := &n14StreamSource{schema: memory.NewSchema(f), batches: []*memory.Batch{b}, repeat: repeat}
	en := New(Options{MemoryLimitBytes: 1 << 20})
	p, e := en.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Filter: filter, Aggregates: specs}, PrepareOptions{MaxConcurrency: 2})
	if e != nil {
		en.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() { p.Close(); en.Close() })
	return en, p, src
}
func n20Specs() []expr.AggregateSpec {
	return []expr.AggregateSpec{expr.AvgDecimal128("d", 4, 2, expr.DecimalRoundHalfEven, "average"), expr.Sum("d", "sum"), expr.Count("rows"), expr.CountColumn("d", "valid")}
}
func TestN20ConfiguredMetadataAndMutation(t *testing.T) {
	specs := n20Specs()
	_, p, _ := n20Prepared(t, 5, nil, specs)
	specs[0] = expr.AvgDecimal128("d", 1, 0, expr.DecimalRoundFloor, "changed")
	for _, materialize := range []bool{false, true} {
		r, e := p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: materialize})
		if e != nil {
			t.Fatal(e)
		}
		if r.Summary.SelectedRows != 20 || r.Summary.GroupCount != 1 {
			t.Fatal(r.Summary)
		}
		if !materialize {
			if len(r.Rows) != 0 {
				t.Fatal(r)
			}
			continue
		}
		a := r.Rows[0].Aggregates
		if len(a) != 4 || a[0].Alias != "average" || a[0].Kind != expr.AggDecimalAverage || a[0].Value.Precision != 4 || a[0].Value.Scale != 2 || a[0].Value.Coefficient != decimal128.FromI64(100) || !a[0].Value.Valid {
			t.Fatal(a)
		}
		if a[1].Value.Coefficient != decimal128.FromI64(150) || a[1].Value.Precision != 38 || a[1].Value.Scale != 1 || a[2].Value.Uint64 != 20 || a[3].Value.Uint64 != 15 {
			t.Fatal(a)
		}
	}
	if _, e := p.Execute(context.Background(), ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("legacy materialized")
	}
	if _, e := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("typed materialized")
	}
}
func TestN20AverageEmptyFilterAndModes(t *testing.T) {
	for _, repeat := range []int{0, 1} {
		for _, filter := range []expr.Expr{nil, expr.Bool(false), expr.IsNotNull(expr.Col("d"))} {
			_, p, _ := n20Prepared(t, repeat, filter, n20Specs())
			r, e := p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: true})
			if e != nil {
				t.Fatal(e)
			}
			empty := repeat == 0 || r.Summary.SelectedRows == 0
			if r.Rows[0].Aggregates[0].Value.Valid == empty {
				t.Fatal(r)
			}
		}
	}
	f, _ := memory.Decimal128Field("d", true, 2, 1)
	for _, n := range []int64{-25, -15, 15, 25} {
		for _, m := range []expr.DecimalRoundingMode{expr.DecimalRoundTowardZero, expr.DecimalRoundAwayFromZero, expr.DecimalRoundFloor, expr.DecimalRoundCeil, expr.DecimalRoundHalfEven, expr.DecimalRoundHalfAwayFromZero} {
			b := memory.NewBatch()
			b.Rows = 1
			b.Decimals["d"] = &memory.Decimal128Vector{Precision: 2, Scale: 1, Values: []memory.Decimal128{decimal128.FromI64(n)}}
			src, _ := inmemory.New(memory.NewSchema(f), b)
			en := New(Options{MemoryLimitBytes: 1 << 20})
			p, e := en.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.AvgDecimal128("d", 1, 0, m, "a")}}, PrepareOptions{})
			if e != nil {
				en.Close()
				t.Fatal(e)
			}
			r, e := p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: true})
			p.Close()
			en.Close()
			if e != nil {
				t.Fatal(e)
			}
			want := n / 10
			rem := n % 10
			negative := n < 0
			up := m == expr.DecimalRoundAwayFromZero || m == expr.DecimalRoundFloor && negative || m == expr.DecimalRoundCeil && !negative || m == expr.DecimalRoundHalfAwayFromZero || (m == expr.DecimalRoundHalfEven && want%2 != 0)
			if rem != 0 && up {
				if negative {
					want--
				} else {
					want++
				}
			}
			if r.Rows[0].Aggregates[0].Value.Coefficient != decimal128.FromI64(want) {
				t.Fatal(n, m, r, want)
			}
		}
	}
}
func TestN20AverageBudgetAndBorrow(t *testing.T) {
	en, p, _ := n20Prepared(t, 100, expr.IsNotNull(expr.Col("d")), n20Specs())
	calls, peak, e := n14BudgetRun(t, p, 1<<20, 0)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("admissions=%d peak=%d", calls, peak)
	for i := 1; i <= calls; i++ {
		if _, _, e = n14BudgetRun(t, p, 1<<20, i); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(i, e)
		}
	}
	if _, _, e = n14BudgetRun(t, p, peak, 0); e != nil {
		t.Fatal(e)
	}
	if _, _, e = n14BudgetRun(t, p, peak-1, 0); !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal(e)
	}
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	out := memory.NewBatch()
	ok, e := st.Next(ctx, out)
	if e != nil || !ok {
		t.Fatal(ok, e)
	}
	v := out.Decimals["average"]
	if v.Precision != 4 || v.Scale != 2 {
		t.Fatal(v)
	}
	if e = st.Close(); e != nil {
		t.Fatal(e)
	}
	if q.Used() != 0 || len(v.Values) != 0 || out.Rows != 0 {
		t.Fatal("lease/header", q.Used(), out.Rows)
	}
}
func TestN20AverageRejectedAndOverflow(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 38, 0)
	src, _ := inmemory.New(memory.NewSchema(f, memory.Field{Name: "k", Type: memory.TypeInt64}))
	en := New(Options{MemoryLimitBytes: 1 << 20})
	defer en.Close()
	valid := expr.AvgDecimal128("d", 4, 2, expr.DecimalRoundHalfEven, "a")
	for _, q := range []expr.ConfiguredQuery{
		{GroupBy: []string{"k"}, Aggregates: []expr.AggregateSpec{valid}},
		{Aggregates: []expr.AggregateSpec{expr.Avg("d", "a")}},
		{Aggregates: []expr.AggregateSpec{expr.Aggregate{Kind: expr.AggDecimalAverage, Column: "d", Alias: "a"}}},
		{Aggregates: []expr.AggregateSpec{valid, expr.Median("k", "m")}},
		{Filter: expr.Bool(false), Aggregates: []expr.AggregateSpec{expr.AvgDecimal128("d", 4, 2, 0, "a")}},
		{Aggregates: []expr.AggregateSpec{expr.AvgDecimal128("k", 4, 2, expr.DecimalRoundHalfEven, "a")}},
		{Aggregates: []expr.AggregateSpec{valid, expr.Count("a")}},
		{Aggregates: []expr.AggregateSpec{valid}, Hints: expr.Hints{ForceStrategy: "bitmap-direct"}},
	} {
		p, e := en.PrepareConfigured(context.Background(), src, q, PrepareOptions{})
		if e == nil {
			p.Close()
			t.Fatal("accepted", q)
		}
	}
	_, p, _ := n20Prepared(t, 1, nil, []expr.AggregateSpec{expr.AvgDecimal128("d", 1, 1, expr.DecimalRoundHalfEven, "a")})
	if r, e := p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: true}); !errors.Is(e, aggregate.ErrDecimalAverageOverflow) || len(r.Rows) != 0 {
		t.Fatal(r, e)
	}
}

type n20CloseSource struct {
	*n14StreamSource
	cancel context.CancelFunc
	err    error
}

func (s *n20CloseSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	st, e := s.n14StreamSource.NewBatchStream(ctx, o)
	return &n20CloseInput{BatchStream: st, cancel: s.cancel, err: s.err}, e
}

type n20CloseInput struct {
	source.BatchStream
	cancel context.CancelFunc
	err    error
}

func (s *n20CloseInput) Close() error {
	e := s.BatchStream.Close()
	if s.cancel != nil {
		s.cancel()
	}
	return errors.Join(e, s.err)
}
func TestN20AverageCancellationAndClose(t *testing.T) {
	for _, fn := range []string{"(*decimalGlobalStream).consume", "(*decimalGlobalStream).Next"} {
		en, p, _ := n20Prepared(t, 100, nil, n20Specs())
		q, _ := en.BeginQueryMemory()
		base := memory.WithQueryBudget(context.Background(), q)
		st, e := p.NewSortedAggregateStream(base, p.resultSchema(), 1)
		if e != nil {
			q.Close()
			t.Fatal(e)
		}
		c := &n14CancelContext{Context: base, function: fn, at: 2}
		_, e = st.Next(c, memory.NewBatch())
		if !errors.Is(e, context.Canceled) {
			st.Close()
			q.Close()
			t.Fatal(fn, e)
		}
		_, e = st.Next(base, memory.NewBatch())
		if !errors.Is(e, context.Canceled) {
			st.Close()
			q.Close()
			t.Fatal("not sticky", e)
		}
		if q.Used() != 0 {
			st.Close()
			q.Close()
			t.Fatal("leak")
		}
		st.Close()
		q.Close()
		if _, e = p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: true}); e != nil {
			t.Fatal("retry", e)
		}
	}
	closeErr := errors.New("n20 source close")
	for _, failure := range []bool{false, true} {
		_, p, src := n20Prepared(t, 0, nil, n20Specs())
		ctx, cancel := context.WithCancel(context.Background())
		custom := &n20CloseSource{n14StreamSource: src, cancel: cancel}
		if failure {
			custom.err = closeErr
		}
		p.src = custom
		r, e := p.ExecuteDecimal(ctx, ExecuteOptions{Materialize: true})
		cancel()
		if len(r.Rows) != 0 || e == nil || !failure && !errors.Is(e, context.Canceled) || failure && !errors.Is(e, closeErr) {
			t.Fatal(r, e)
		}
	}
}
