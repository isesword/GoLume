package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"runtime"
	"strings"
	"testing"
)

func TestN14PrepareDecimalSum(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 38, 0)
	src, e := inmemory.New(memory.NewSchema(f))
	if e != nil {
		t.Fatal(e)
	}
	en := New(Options{MemoryLimitBytes: 1 << 20})
	defer en.Close()
	p, e := en.Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("d", "sum")}}, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
}

func n14Prepared(t *testing.T, rows int, filtered ...bool) (*Engine, *PreparedQuery) {
	t.Helper()
	f, _ := memory.Decimal128Field("d", true, 38, 0)
	b := memory.NewBatch()
	b.Rows = rows
	v := &memory.Decimal128Vector{Precision: 38, Values: make([]memory.Decimal128, rows)}
	for i := range v.Values {
		v.Values[i] = decimal128.FromI64(int64(i%3 - 1))
	}
	b.Decimals["d"] = v
	ids := &memory.Int64Vector{Values: make([]int64, rows)}
	for i := range ids.Values {
		ids.Values[i] = int64(i % 3)
	}
	b.Int64s["id"] = ids
	src, e := inmemory.New(memory.NewSchema(f, memory.Field{Name: "id", Type: memory.TypeInt64}), b)
	if e != nil {
		t.Fatal(e)
	}
	en := New(Options{MemoryLimitBytes: 1 << 20})
	var filter expr.Expr
	if len(filtered) > 0 && filtered[0] {
		filter = expr.GT(expr.Col("id"), expr.I64(0))
	}
	p, e := en.Prepare(context.Background(), src, expr.Query{Filter: filter, Aggregates: []expr.Aggregate{expr.Sum("d", "sum"), expr.Sum("d", "again")}}, PrepareOptions{})
	if e != nil {
		en.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() { p.Close(); en.Close() })
	return en, p
}
func TestN14DecimalStreamLifecycleAndPublicBoundary(t *testing.T) {
	en, p := n14Prepared(t, 3000)
	for _, typed := range []bool{false, true} {
		if typed {
			_, e := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
			if e == nil {
				t.Fatal("typed materialize accepted")
			}
		} else {
			_, e := p.Execute(context.Background(), ExecuteOptions{Materialize: true})
			if e == nil {
				t.Fatal("legacy materialize accepted")
			}
		}
	}
	r, e := p.Execute(context.Background(), ExecuteOptions{})
	if e != nil || r.GroupCount != 1 || r.SelectedRows != 3000 {
		t.Fatal(r, e)
	}
	q, e := en.BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
	if e != nil {
		t.Fatal(e)
	}
	out := memory.NewBatch()
	ok, e := st.Next(ctx, out)
	if !ok || e != nil {
		t.Fatal(ok, e)
	}
	v := out.Decimals["sum"]
	if !v.IsValid(0) || v.Values[0] != decimal128.FromI64(0) || out.Decimals["again"].Values[0] != v.Values[0] {
		t.Fatal("sum result")
	}
	if q.Used() == 0 {
		t.Fatal("output uncharged")
	}
	if e = st.Close(); e != nil {
		t.Fatal(e)
	}
	if q.Used() != 0 || len(v.Values) != 0 || out.Rows != 0 {
		t.Fatal("borrow lifecycle", q.Used(), out.Rows)
	}
	bad := memory.NewSchema(memory.Field{Name: "sum", Type: memory.TypeInt64})
	if _, e = p.NewSortedAggregateStream(ctx, bad, 1); e == nil {
		t.Fatal("schema accepted")
	}
}

type n14Fault struct{ calls, fail int }
type n14Manager struct {
	memory.Manager
	f *n14Fault
}

func (m n14Manager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.f.calls++
	if m.f.calls == m.f.fail {
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}
func n14BudgetRun(t *testing.T, p *PreparedQuery, hard int64, fail int) (int, int64, error) {
	t.Helper()
	q, e := memory.NewEngineBudget(hard).BeginQuery(memory.BudgetOptions{HardLimitBytes: hard})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	f := &n14Fault{fail: fail}
	opts := source.ReadOptions{Kernels: p.e.kernels, BatchRows: 64, BatchBytes: 1 << 16}
	st, e := p.newDecimalGlobalStreamWithManagers(ctx, p.resultSchema(), opts, true, func(m memory.Manager) memory.Manager { return n14Manager{m, f} })
	if e == nil {
		out := memory.NewBatch()
		for {
			ok, err := st.Next(ctx, out)
			if err != nil {
				e = err
				break
			}
			if !ok {
				break
			}
		}
		closeErr := st.Close()
		if e == nil {
			e = closeErr
		}
	}
	if q.Used() != 0 {
		t.Fatalf("leaked %d", q.Used())
	}
	return f.calls, q.Snapshot().PeakBytes, e
}
func TestN14EveryReserveAndExact(t *testing.T) {
	_, p := n14Prepared(t, 3000, true)
	calls, peak, e := n14BudgetRun(t, p, 1<<20, 0)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("calls=%d peak=%d", calls, peak)
	for i := 1; i <= calls; i++ {
		_, _, e = n14BudgetRun(t, p, 1<<20, i)
		if !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatalf("failure %d: %v", i, e)
		}
	}
	_, _, e = n14BudgetRun(t, p, peak, 0)
	if e != nil {
		t.Fatal("exact", e)
	}
	_, _, e = n14BudgetRun(t, p, peak-1, 0)
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("minus one", e)
	}
}

type n14StreamSource struct {
	schema  memory.Schema
	batches []*memory.Batch
	repeat  int
	closed  int
}

func (s *n14StreamSource) Schema() memory.Schema { return s.schema }
func (s *n14StreamSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: -1}, nil
}
func (s *n14StreamSource) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (s *n14StreamSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &n14Input{s: s}, nil
}

type n14Input struct {
	s      *n14StreamSource
	i      int
	closed bool
}

func (s *n14Input) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	if s.i >= s.s.repeat*len(s.s.batches) {
		b.ClearColumns()
		return false, nil
	}
	src := s.s.batches[s.i%len(s.s.batches)]
	*b = memory.Batch{Rows: src.Rows, Decimals: make(map[string]*memory.Decimal128Vector, len(src.Decimals))}
	for name, v := range src.Decimals {
		b.Decimals[name] = v
	}
	s.i++
	return true, ctx.Err()
}
func (s *n14Input) Close() error {
	if !s.closed {
		s.closed = true
		s.s.closed++
	}
	return nil
}
func TestN14LongStreamConstantState(t *testing.T) {
	var peak int64
	for _, repeat := range []int{1, 1000} {
		f, _ := memory.Decimal128Field("d", true, 38, 0)
		b := memory.NewBatch()
		b.Rows = 64
		b.Decimals["d"] = &memory.Decimal128Vector{Precision: 38, Values: make([]memory.Decimal128, 64)}
		src := &n14StreamSource{schema: memory.NewSchema(f), batches: []*memory.Batch{b}, repeat: repeat}
		en := New(Options{MemoryLimitBytes: 1 << 20})
		p, e := en.Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("d", "sum")}}, PrepareOptions{})
		if e != nil {
			t.Fatal(e)
		}
		_, used, e := n14BudgetRun(t, p, 1<<20, 0)
		if e != nil {
			t.Fatal(e)
		}
		if repeat == 1 {
			peak = used
		} else if peak != used {
			t.Fatal("state grows with stream", peak, used)
		}
		if src.closed != 1 {
			t.Fatal(src.closed)
		}
		p.Close()
		en.Close()
	}
}
func TestN14BatchFailureAtomic(t *testing.T) {
	f, _ := memory.Decimal128Field("d", false, 1, 0)
	good := memory.NewBatch()
	good.Rows = 1
	good.Decimals["d"] = &memory.Decimal128Vector{Precision: 1, Values: []memory.Decimal128{decimal128.FromI64(3)}}
	bad := memory.NewBatch()
	bad.Rows = 2
	bad.Decimals["d"] = &memory.Decimal128Vector{Precision: 1, Values: []memory.Decimal128{decimal128.FromI64(4), decimal128.FromI64(10)}}
	src := &n14StreamSource{schema: memory.NewSchema(f), batches: []*memory.Batch{good, bad}, repeat: 1}
	en := New(Options{MemoryLimitBytes: 1 << 20})
	defer en.Close()
	p, e := en.Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("d", "sum")}}, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	raw, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
	if e != nil {
		t.Fatal(e)
	}
	st := raw.(*decimalGlobalStream)
	if e = st.consume(ctx); e == nil {
		t.Fatal("invalid coefficient accepted")
	}
	got, valid, e := st.states[0].Finalize()
	if e != nil || !valid || got != decimal128.FromI64(3) {
		t.Fatal("partial batch committed", got, valid, e)
	}
	st.Close()
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
}

type n14CancelContext struct {
	context.Context
	function  string
	calls, at int
}

func (c *n14CancelContext) Err() error {
	pc, _, _, _ := runtime.Caller(1)
	if strings.HasSuffix(runtime.FuncForPC(pc).Name(), c.function) {
		c.calls++
		if c.calls >= c.at {
			return context.Canceled
		}
	}
	return nil
}
func TestN14WideEmptyCancellation(t *testing.T) {
	for _, phase := range []string{"consume", "finalize", "output"} {
		t.Run(phase, func(t *testing.T) {
			f, _ := memory.Decimal128Field("d", true, 38, 0)
			b := memory.NewBatch()
			b.Decimals["d"] = &memory.Decimal128Vector{Precision: 38}
			repeat := 0
			if phase == "consume" {
				repeat = 1
			}
			src := &n14StreamSource{schema: memory.NewSchema(f), batches: []*memory.Batch{b}, repeat: repeat}
			aggs := make([]expr.Aggregate, 512)
			for i := range aggs {
				aggs[i] = expr.Sum("d", fmt.Sprintf("s%d", i))
			}
			en := New(Options{MemoryLimitBytes: 2 << 20})
			defer en.Close()
			p, e := en.Prepare(context.Background(), src, expr.Query{Aggregates: aggs}, PrepareOptions{})
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			q, _ := en.BeginQueryMemory()
			defer q.Close()
			ctx := memory.WithQueryBudget(context.Background(), q)
			st, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
			if e != nil {
				t.Fatal(e)
			}
			c := &n14CancelContext{Context: ctx, function: "(*decimalGlobalStream).Next", at: 3}
			if phase == "consume" {
				c.function = "(*decimalGlobalStream).consume"
				c.at = 50
			}
			if phase == "output" {
				c.at = 12
			}
			out := memory.NewBatch()
			if ok, e := st.Next(c, out); ok || !errors.Is(e, context.Canceled) {
				t.Fatal(ok, e, c.calls)
			}
			if q.Used() != 0 || out.Rows != 0 {
				t.Fatal("failure cleanup", q.Used(), out.Rows)
			}
			if _, e = st.Next(context.Background(), out); !errors.Is(e, context.Canceled) {
				t.Fatal("nonsticky", e)
			}
			st.Close()
		})
	}
}

func TestN14WrongPublicStrategiesReject(t *testing.T) {
	en, p := n14Prepared(t, 3)
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	if st, e := p.NewResidentAggregateStream(ctx, p.resultSchema(), 1); e == nil {
		st.Close()
		t.Fatal("resident hash accepted")
	}
	if st, e := p.NewSpillableAggregateStream(ctx, p.resultSchema(), 1); e == nil {
		st.Close()
		t.Fatal("spill hash accepted")
	}
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
	r, e := p.ExecuteTyped(ctx, ExecuteOptions{})
	if e != nil || r.Summary.GroupCount != 1 {
		t.Fatal(r, e)
	}
}
func TestN14FixtureBindingMapProtocol(t *testing.T) {
	src := memory.NewBatch()
	src.Rows = 1
	v := &memory.Decimal128Vector{Precision: 1, Values: []memory.Decimal128{decimal128.FromI64(1)}}
	src.Decimals["d"] = v
	bad := *src
	bad.ClearColumns()
	if len(src.Decimals) != 0 {
		t.Fatal("expected shallow map alias demonstration")
	}
	src.Decimals["d"] = v
	input := &n14Input{s: &n14StreamSource{batches: []*memory.Batch{src}, repeat: 2}}
	dst := memory.NewBatch()
	for i := 0; i < 2; i++ {
		ok, e := input.Next(context.Background(), dst)
		if !ok || e != nil {
			t.Fatal(ok, e)
		}
		dst.ClearColumns()
		if src.Decimals["d"] != v || len(v.Values) != 1 || v.Values[0] != decimal128.FromI64(1) {
			t.Fatal("independent binding map changed source vector")
		}
	}
}

func TestN14SelectedMultiLane(t *testing.T) {
	en, p := n14Prepared(t, 3000, true)
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
	if e != nil {
		t.Fatal(e)
	}
	out := memory.NewBatch()
	ok, e := st.Next(ctx, out)
	if !ok || e != nil {
		t.Fatal(ok, e)
	}
	for _, name := range []string{"sum", "again"} {
		if out.Decimals[name].Values[0] != decimal128.FromI64(1000) {
			t.Fatal(name)
		}
	}
	st.Close()
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
}

func TestN14LegacyMixedStillRejected(t *testing.T) {
	d, _ := memory.Decimal128Field("d", true, 38, 0)
	schema := memory.NewSchema(d, memory.Field{Name: "i", Type: memory.TypeInt64})
	for _, a := range []expr.Aggregate{expr.Count("n"), expr.Sum("i", "sum")} {
		if _, e := compileQuery(schema, expr.Query{Aggregates: []expr.Aggregate{a}}); e == nil {
			t.Fatal("old mixed input boundary widened")
		}
	}
}
func TestN14SelectedStatsOverflow(t *testing.T) {
	en, p := n14Prepared(t, 3)
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
	if e != nil {
		t.Fatal(e)
	}
	s := st.(*decimalGlobalStream)
	s.selected = math.MaxInt
	if _, e = s.Next(ctx, memory.NewBatch()); e == nil || !strings.Contains(e.Error(), "selected-row statistics exceed MaxInt") {
		t.Fatal(e)
	}
	s.Close()
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
}
