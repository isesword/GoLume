package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"math"
	"math/big"
	"testing"
)

func TestN18Prepare(t *testing.T) {
	_, _, src := n17Fixture(t, 3, 3, false, false, false)
	src.ordering = nil
	en := New(Options{MemoryLimitBytes: 64 << 20, ExecutionPolicy: ExecutionMemoryOnly})
	defer en.Close()
	p, e := en.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("d", "s")}}, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	p.Close()
}

func n18Fixture(t *testing.T, g, repeat int, filter bool) (*Engine, *PreparedQuery, *n17Source) {
	t.Helper()
	_, _, src := n17Fixture(t, g, 17, false, false, false)
	src.ordering = nil
	for l, r := 0, len(src.batches)-1; l < r; l, r = l+1, r-1 {
		src.batches[l], src.batches[r] = src.batches[r], src.batches[l]
	}
	original := append([]*memory.Batch(nil), src.batches...)
	for i := 1; i < repeat; i++ {
		src.batches = append(src.batches, original...)
	}
	en := New(Options{MemoryLimitBytes: 64 << 20, ExecutionPolicy: ExecutionMemoryOnly})
	var pred expr.Expr
	if filter {
		pred = expr.IsNotNull(expr.Col("d"))
	}
	p, e := en.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"k"}, Filter: pred, Aggregates: []expr.Aggregate{expr.Count("n"), expr.Sum("d", "s"), expr.CountColumn("d", "valid"), expr.Sum("e", "s38")}}, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { p.Close(); en.Close() })
	return en, p, src
}
func n18Run(t *testing.T, p *PreparedQuery, limit int64, fail int, check func(*memory.Batch)) (int, int64, error) {
	t.Helper()
	q, e := memory.NewEngineBudget(limit).BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	f := &n14Fault{fail: fail}
	st, e := p.newDecimalResidentStream(ctx, p.resultSchema(), 64, source.ReadOptions{Kernels: p.e.kernels, BatchRows: 64, BatchBytes: 65536}, true, func(m memory.Manager) memory.Manager { return n14Manager{m, f} })
	if e == nil {
		b := memory.NewBatch()
		var old *memory.Decimal128Vector
		for {
			ok, err := st.Next(ctx, b)
			if old != nil && len(old.Values) != 0 {
				t.Fatal("borrow retained")
			}
			if err != nil {
				e = err
				break
			}
			if !ok {
				break
			}
			if b.Rows < 1 || b.Rows > 64 {
				t.Fatal("page bounds")
			}
			if check != nil {
				check(b)
			}
			old = b.Decimals["s"]
		}
		ce := st.Close()
		if e == nil {
			e = ce
		}
		s := st.(*decimalResidentStream)
		if s.index != nil || s.values != nil || s.countsByGroup != nil || s.truth != nil || s.operationContext != nil {
			t.Fatal("closed references")
		}
	}
	peak := q.Snapshot().PeakBytes
	if q.Used() != 0 {
		t.Fatal("leak", q.Used())
	}
	return f.calls, peak, e
}
func TestN18OracleBudgetAndGrowth(t *testing.T) {
	var peak int64
	for _, repeat := range []int{1, 20} {
		_, p, src := n18Fixture(t, 195, repeat, true)
		seen := map[string]bool{}
		calls, got, e := n18Run(t, p, 64<<20, 0, func(b *memory.Batch) {
			if src.closed != 1 {
				t.Fatal("emitted before source closed")
			}
			for r := 0; r < b.Rows; r++ {
				key := "null"
				if b.Int64s["k"].IsValid(r) {
					key = fmt.Sprint(b.Int64s["k"].Values[r])
				}
				if seen[key] {
					t.Fatal("duplicate", key)
				}
				seen[key] = true
				if b.Decimals["s"].Values[r] != decimal128.FromI64(int64(4*repeat)) || b.Decimals["s38"].Values[r] != decimal128.FromI64(0) || b.Decimals["s38"].Scale != 38 || b.Uint64s["n"].Values[r] != uint64(2*repeat) || b.Uint64s["valid"].Values[r] != uint64(2*repeat) {
					t.Fatal("oracle", key)
				}
			}
		})
		if e != nil || len(seen) != 196 {
			t.Fatal(e, len(seen))
		}
		t.Logf("groups196 repeat%d calls%d peak%d", repeat, calls, got)
		if repeat > 1 && got != peak {
			t.Fatal("rows grow state", peak, got)
		}
		peak = got
		if repeat == 1 {
			for i := 1; i <= calls; i++ {
				_, _, e = n18Run(t, p, 64<<20, i, nil)
				if !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal("ordinal", i, e)
				}
			}
			_, _, e = n18Run(t, p, peak, 0, nil)
			if e != nil {
				t.Fatal("exact", e)
			}
			_, _, e = n18Run(t, p, peak-1, 0, nil)
			if !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatal("minus1", e)
			}
		}
	}
	_, p, _ := n18Fixture(t, 1950, 1, true)
	_, more, e := n18Run(t, p, 64<<20, 0, nil)
	if e != nil || more <= peak {
		t.Fatal("group growth", e, peak, more)
	}
	t.Log("groups1951 peak", more)
}
func TestN18PreflightAndMalformed(t *testing.T) {
	for _, mode := range []string{"overflow", "coefficient", "rows0", "empty"} {
		t.Run(mode, func(t *testing.T) {
			_, p, src := n18Fixture(t, 3, 1, false)
			switch mode {
			case "empty":
				src.batches = nil
			case "rows0":
				src.batches[0].Rows = 0
			case "coefficient":
				src.batches[len(src.batches)-1].Decimals["d"].Values[2] = decimal128.New(math.MaxInt64, math.MaxUint64)
			case "overflow":
				m := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
				for _, b := range src.batches {
					v := b.Decimals["d"]
					v.Validity = nil
					for i := range v.Values {
						v.Values[i] = decimal128.FromBigInt(m)
					}
				}
			}
			pages := 0
			_, _, e := n18Run(t, p, 64<<20, 0, func(*memory.Batch) { pages++ })
			if pages != 0 {
				t.Fatal("partial output", pages)
			}
			if mode == "empty" {
				if e != nil {
					t.Fatal(e)
				}
			} else if e == nil {
				t.Fatal("bad accepted")
			}
		})
	}
}
func TestN18PublicAndPolicy(t *testing.T) {
	en, p, src := n18Fixture(t, 3, 1, false)
	r, e := p.Execute(context.Background(), ExecuteOptions{})
	if e != nil || r.GroupCount != 4 || r.SelectedRows != 12 {
		t.Fatal(r, e)
	}
	if _, e = p.ExecuteTyped(context.Background(), ExecuteOptions{}); e != nil {
		t.Fatal(e)
	}
	for _, mat := range []bool{true, false} {
		if _, e = p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: mat}); e == nil {
			t.Fatal("global API")
		}
	}
	if _, e = p.Execute(context.Background(), ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("legacy")
	}
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	if st, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 64); e == nil {
		st.Close()
		t.Fatal("wrong sorted")
	}
	if st, e := p.NewSpillableAggregateStream(ctx, p.resultSchema(), 64); e == nil {
		st.Close()
		t.Fatal("wrong spill")
	}
	for _, policy := range []ExecutionPolicy{ExecutionAuto, ExecutionBounded} {
		other := New(Options{MemoryLimitBytes: 64 << 20, ExecutionPolicy: policy})
		pp, e := other.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("d", "s")}}, PrepareOptions{})
		if e == nil {
			pp.Close()
			t.Fatal("policy", policy)
		}
		other.Close()
	}
	recorder := &observer.Collector{}
	en.opts.Observer = recorder
	if p.Plan().Strategy.Reason != "unordered Decimal grouping retains budgeted per-group state" || p.Plan().Strategy.Workers != 1 {
		t.Fatal(p.Plan())
	}
	p.Execute(context.Background(), ExecuteOptions{})
	found := false
	for _, event := range recorder.Events() {
		if event.Stage == "prepared-query" {
			found = true
			if event.Attributes["reason"] != p.Plan().Strategy.Reason {
				t.Fatal(event)
			}
		}
	}
	if !found {
		t.Fatal("observer")
	}
}
func TestN18CancelAndRetry(t *testing.T) {
	for _, fn := range []string{"(*decimalResidentStream).consumeAll", "(*decimalResidentStream).grow", "(*decimalResidentStream).loadGroup", "(*decimalGroupedStream).emit", "(*decimalGroupedStream).finishPage.func1"} {
		t.Run(fn, func(t *testing.T) {
			en, p, _ := n18Fixture(t, 195, 1, false)
			q, _ := en.BeginQueryMemory()
			defer q.Close()
			base := memory.WithQueryBudget(context.Background(), q)
			st, e := p.NewResidentAggregateStream(base, p.resultSchema(), 64)
			if e != nil {
				t.Fatal(e)
			}
			s := st.(*decimalResidentStream)
			defer s.Close()
			ctx := &n14CancelContext{Context: base, function: fn, at: 1}
			_, e = s.Next(ctx, memory.NewBatch())
			if !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			if q.Used() != 0 || s.index != nil || s.values != nil || s.truth != nil {
				t.Fatal("cleanup")
			}
			_, e = s.Next(base, memory.NewBatch())
			if !errors.Is(e, context.Canceled) {
				t.Fatal("sticky")
			}
			s.Close()
			if _, _, e = n18Run(t, p, 64<<20, 0, nil); e != nil {
				t.Fatal("retry", e)
			}
		})
	}
}

type n18CloseCancelSource struct {
	*n17Source
	cancel context.CancelFunc
}

func (s *n18CloseCancelSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &n18CloseCancelInput{n17Input: &n17Input{s: s.n17Source}, cancel: s.cancel}, nil
}

type n18CloseCancelInput struct {
	*n17Input
	cancel context.CancelFunc
}

func (s *n18CloseCancelInput) Close() error { e := s.n17Input.Close(); s.cancel(); return e }
func TestN18EmptyCloseCancellation(t *testing.T) {
	en, p, src := n18Fixture(t, 3, 1, false)
	src.batches = nil
	raw, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.src = &n18CloseCancelSource{src, cancel}
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := memory.WithQueryBudget(raw, q)
	st, e := p.NewResidentAggregateStream(ctx, p.resultSchema(), 64)
	if e != nil {
		t.Fatal(e)
	}
	ok, e := st.Next(ctx, memory.NewBatch())
	st.Close()
	if ok || !errors.Is(e, context.Canceled) {
		t.Fatalf("late source-close cancellation lost: ok=%v err=%v", ok, e)
	}
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
}

func TestN18BigIntCancellationOracle(t *testing.T) {
	_, p, src := n18Fixture(t, 195, 1, false)
	m := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	want := map[string]*big.Int{}
	for _, b := range src.batches {
		v := b.Decimals["d"]
		v.Validity = nil
		for i := range v.Values {
			key := "null"
			if b.Int64s["k"].IsValid(i) {
				key = fmt.Sprint(b.Int64s["k"].Values[i])
			}
			x := new(big.Int).Set(m)
			if v.Values[i] == decimal128.FromI64(3) {
				x.Neg(x)
			}
			v.Values[i] = decimal128.FromBigInt(x)
			if want[key] == nil {
				want[key] = new(big.Int)
			}
			want[key].Add(want[key], x)
		}
	}
	seen := 0
	_, _, e := n18Run(t, p, 64<<20, 0, func(b *memory.Batch) {
		for r := 0; r < b.Rows; r++ {
			key := "null"
			if b.Int64s["k"].IsValid(r) {
				key = fmt.Sprint(b.Int64s["k"].Values[r])
			}
			if b.Decimals["s"].Values[r].BigInt().Cmp(want[key]) != 0 {
				t.Fatal(key)
			}
			seen++
		}
	})
	if e != nil || seen != 196 {
		t.Fatal(e, seen)
	}
}
func TestN18FiniteCallerBudgetAndHints(t *testing.T) {
	_, _, src := n18Fixture(t, 3, 1, false)
	en := New(Options{ExecutionPolicy: ExecutionMemoryOnly})
	defer en.Close()
	query := expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("d", "s")}}
	p, e := en.Prepare(context.Background(), src, query, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for _, limit := range []int64{0, 64 << 20} {
		q, _ := memory.NewEngineBudget(limit).BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
		ctx := memory.WithQueryBudget(context.Background(), q)
		st, e := p.NewResidentAggregateStream(ctx, p.resultSchema(), 64)
		if limit == 0 {
			if e == nil {
				st.Close()
				t.Fatal("unlimited accepted")
			}
		} else {
			if e != nil {
				t.Fatal(e)
			}
			b := memory.NewBatch()
			for {
				ok, e := st.Next(ctx, b)
				if e != nil {
					st.Close()
					t.Fatal(e)
				}
				if !ok {
					break
				}
			}
			st.Close()
		}
		if q.Used() != 0 {
			t.Fatal(q.Used())
		}
		q.Close()
	}
	for _, hint := range []string{"bitmap-direct", "sorted", "direct-fused", "partitioned-hash", "radix", "typo"} {
		query.Hints.ForceStrategy = hint
		pp, e := en.Prepare(context.Background(), src, query, PrepareOptions{})
		if hint == "bitmap-direct" {
			if e != nil {
				t.Fatal(e)
			}
			pp.Close()
		} else if e == nil {
			pp.Close()
			t.Fatal(hint)
		}
	}
}
func TestN18EmptyCloseCancellationPublic(t *testing.T) {
	_, p, src := n18Fixture(t, 3, 1, false)
	src.batches = nil
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.src = &n18CloseCancelSource{src, cancel}
	r, e := p.Execute(ctx, ExecuteOptions{})
	if !errors.Is(e, context.Canceled) || r.GroupCount != 0 {
		t.Fatal(r, e)
	}
}
