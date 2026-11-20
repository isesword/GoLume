package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/planner"
	"gocolumn/source"
	"math"
	"math/big"
	"testing"
)

func TestN17Compile(t *testing.T) {
	d, _ := memory.Decimal128Field("d", true, 38, 2)
	_, e := compileQuery(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64, Nullable: true}, d), expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("d", "s")}})
	if e != nil {
		t.Fatal(e)
	}
}

type n17Source struct {
	schema   memory.Schema
	batches  []*memory.Batch
	ordering []source.OrderingColumn
	closed   int
}

func (s *n17Source) Schema() memory.Schema                                  { return s.schema }
func (s *n17Source) Stats(context.Context) (source.Stats, error)            { return source.Stats{Rows: -1}, nil }
func (s *n17Source) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (s *n17Source) Ordering() []source.OrderingColumn                      { return s.ordering }
func (s *n17Source) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &n17Input{s: s}, nil
}

type n17Input struct {
	s      *n17Source
	i      int
	closed bool
}

func (s *n17Input) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	if s.i == len(s.s.batches) {
		return false, ctx.Err()
	}
	src := s.s.batches[s.i]
	*b = memory.Batch{Rows: src.Rows, Decimals: make(map[string]*memory.Decimal128Vector), Int64s: make(map[string]*memory.Int64Vector)}
	for n, v := range src.Decimals {
		b.Decimals[n] = v
	}
	for n, v := range src.Int64s {
		b.Int64s[n] = v
	}
	s.i++
	return true, ctx.Err()
}
func (s *n17Input) Close() error {
	if !s.closed {
		s.closed = true
		s.s.closed++
	}
	return nil
}
func n17Fixture(t *testing.T, groups, batch int, desc, last, filtered bool) (*Engine, *PreparedQuery, *n17Source) {
	t.Helper()
	d, _ := memory.Decimal128Field("d", true, 38, 2)
	e, _ := memory.Decimal128Field("e", true, 38, 38)
	src := &n17Source{schema: memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64, Nullable: true}, d, e), ordering: []source.OrderingColumn{{Name: "k", Descending: desc, NullsLast: last}}}
	rows := (groups + 1) * 3
	for start := 0; start < rows; start += batch {
		n := min(batch, rows-start)
		b := memory.NewBatch()
		b.Rows = n
		k := &memory.Int64Vector{Values: make([]int64, n), Validity: memory.EnsureBitmap(nil, n)}
		v := &memory.Decimal128Vector{Precision: 38, Scale: 2, Values: make([]memory.Decimal128, n), Validity: memory.EnsureBitmap(nil, n)}
		w := &memory.Decimal128Vector{Precision: 38, Scale: 38, Values: make([]memory.Decimal128, n)}
		for i := 0; i < n; i++ {
			g := (start + i) / 3
			null := g == 0
			if last {
				null = g == groups
			}
			key := g
			if !last {
				key--
			}
			if desc {
				key = groups - 1 - key
			}
			k.Values[i] = int64(key)
			if !null {
				memory.Set(k.Validity, i)
			}
			r := (start + i) % 3
			v.Values[i] = decimal128.FromI64(int64(r + 1))
			if r != 1 {
				memory.Set(v.Validity, i)
			}
			w.Values[i] = decimal128.FromI64(int64(r - 1))
		}
		b.Int64s["k"] = k
		b.Decimals["d"] = v
		b.Decimals["e"] = w
		src.batches = append(src.batches, b)
	}
	en := New(Options{MemoryLimitBytes: 64 << 20})
	var pred expr.Expr
	if filtered {
		pred = expr.IsNotNull(expr.Col("d"))
	}
	p, err := en.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"k"}, Filter: pred, Aggregates: []expr.Aggregate{expr.Count("n"), expr.Sum("d", "s"), expr.CountColumn("d", "valid"), expr.Sum("e", "s38")}}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close(); en.Close() })
	return en, p, src
}
func n17Run(t *testing.T, p *PreparedQuery, limit int64, fail, page int, check func(*memory.Batch)) (int, int64, error) {
	t.Helper()
	q, e := memory.NewEngineBudget(limit).BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	f := &n14Fault{fail: fail}
	st, e := p.newDecimalGroupedStreamWithManagers(ctx, p.resultSchema(), source.ReadOptions{Kernels: p.e.kernels, BatchRows: 64, BatchBytes: 65536}, page, true, func(m memory.Manager) memory.Manager { return n14Manager{m, f} })
	if e == nil {
		b := memory.NewBatch()
		var old *memory.Decimal128Vector
		for {
			ok, err := st.Next(ctx, b)
			if old != nil && len(old.Values) != 0 {
				t.Fatal("borrow not expired")
			}
			if err != nil {
				e = err
				break
			}
			if !ok {
				break
			}
			if b.Rows < 1 || b.Rows > min(page, 64) {
				t.Fatal("page bound", b.Rows)
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
	}
	peak := q.Snapshot().PeakBytes
	if q.Used() != 0 {
		t.Fatal("leak", q.Used())
	}
	return f.calls, peak, e
}
func TestN17PagesOracle(t *testing.T) {
	for _, desc := range []bool{false, true} {
		for _, last := range []bool{false, true} {
			for _, batch := range []int{1, 2, 17, 64} {
				for _, page := range []int{1, 2, 63, 64} {
					for _, filter := range []bool{false, true} {
						t.Run(fmt.Sprint(desc, last, batch, page, filter), func(t *testing.T) {
							_, p, src := n17Fixture(t, 195, batch, desc, last, filter)
							seen := 0
							_, _, err := n17Run(t, p, 64<<20, 0, page, func(b *memory.Batch) {
								for r := 0; r < b.Rows; r++ {
									null := seen == 0
									if last {
										null = seen == 195
									}
									k := seen
									if !last {
										k--
									}
									if desc {
										k = 194 - k
									}
									kv := b.Int64s["k"]
									if kv.IsValid(r) == null || !null && kv.Values[r] != int64(k) {
										t.Fatal("key", seen)
									}
									if b.Decimals["s"].Values[r] != decimal128.FromI64(4) || b.Decimals["s38"].Values[r] != decimal128.FromI64(0) || b.Decimals["s38"].Scale != 38 {
										t.Fatal("sum")
									}
									n := uint64(3)
									if filter {
										n = 2
									}
									if b.Uint64s["n"].Values[r] != n || b.Uint64s["valid"].Values[r] != 2 {
										t.Fatal("count")
									}
									seen++
								}
							})
							if err != nil || seen != 196 || src.closed != 1 {
								t.Fatal(err, seen, src.closed)
							}
						})
					}
				}
			}
		}
	}
}
func TestN17BudgetAndConstantState(t *testing.T) {
	var peak int64
	for _, g := range []int{195, 1950} {
		_, p, _ := n17Fixture(t, g, 17, false, false, true)
		calls, got, e := n17Run(t, p, 64<<20, 0, 64, nil)
		if e != nil {
			t.Fatal(e)
		}
		if peak != 0 && got != peak {
			t.Fatal("group growth", peak, got)
		}
		peak = got
		t.Log(g, calls, peak)
		if g == 195 {
			for i := 1; i <= calls; i++ {
				_, _, e = n17Run(t, p, 64<<20, i, 64, nil)
				if !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal(i, e)
				}
			}
			_, _, e = n17Run(t, p, peak, 0, 64, nil)
			if e != nil {
				t.Fatal("exact", e)
			}
			_, _, e = n17Run(t, p, peak-1, 0, 64, nil)
			if !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatal("minus1", e)
			}
		}
	}
}
func TestN17PublicAndBadBatch(t *testing.T) {
	_, p, src := n17Fixture(t, 3, 3, false, false, false)
	r, e := p.Execute(context.Background(), ExecuteOptions{})
	if e != nil || r.GroupCount != 4 || r.SelectedRows != 12 {
		t.Fatal(r, e)
	}
	tr, e := p.ExecuteTyped(context.Background(), ExecuteOptions{})
	if e != nil || tr.Summary.GroupCount != 4 {
		t.Fatal(tr, e)
	}
	for _, materialize := range []bool{false, true} {
		if _, e = p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: materialize}); e == nil {
			t.Fatal("global API grouped")
		}
	}
	if _, e = p.Execute(context.Background(), ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("legacy")
	}
	if _, e = p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("typed")
	}
	// Entire first batch invalid later coefficient must not expose a partial row.
	src.batches[0].Decimals["d"].Values[2] = decimal128.New(math.MaxInt64, math.MaxUint64)
	pages := 0
	_, _, e = n17Run(t, p, 64<<20, 0, 1, func(*memory.Batch) { pages++ })
	if e == nil || pages != 0 {
		t.Fatal("full validation", e, pages)
	}
}
func TestN17OrderViolationAndEmpty(t *testing.T) {
	for _, mode := range []string{"empty", "filter", "keyorder", "nullphase", "rows0"} {
		t.Run(mode, func(t *testing.T) {
			_, p, src := n17Fixture(t, 3, 3, false, false, false)
			switch mode {
			case "empty":
				src.batches = nil
			case "filter":
				p.cq.Predicate, _ = expr.Compile(src.schema, expr.Bool(false))
			case "keyorder":
				src.batches[3].Int64s["k"].Values[0] = -100
			case "nullphase":
				src.batches[3].Int64s["k"].Validity = memory.Bitmap{0}
			case "rows0":
				src.batches[0].Rows = 0
			}
			pages := 0
			_, _, e := n17Run(t, p, 64<<20, 0, 1, func(*memory.Batch) { pages++ })
			if mode == "empty" || mode == "filter" {
				if e != nil || pages != 0 {
					t.Fatal(e, pages)
				}
			} else if e == nil {
				t.Fatal("bad accepted")
			}
		})
	}
}

func TestN17ExactCancellationAndLateOverflow(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	for _, overflow := range []bool{false, true} {
		_, p, src := n17Fixture(t, 5, 2, false, false, false)
		oracle := map[int]*big.Int{}
		absolute := 0
		for _, b := range src.batches {
			v := b.Decimals["d"]
			v.Validity = nil
			for i := range v.Values {
				g := absolute / 3
				r := absolute % 3
				x := new(big.Int).Set(max)
				if r == 2 {
					x.Neg(x)
				}
				if overflow && g == 5 && r == 2 {
					x.SetInt64(0)
				}
				v.Values[i] = decimal128.FromBigInt(x)
				if oracle[g] == nil {
					oracle[g] = new(big.Int)
				}
				oracle[g].Add(oracle[g], x)
				absolute++
			}
		}
		seen := 0
		_, _, e := n17Run(t, p, 64<<20, 0, 2, func(b *memory.Batch) {
			for r := 0; r < b.Rows; r++ {
				if b.Decimals["s"].Values[r].BigInt().Cmp(oracle[seen]) != 0 {
					t.Fatal("oracle", seen)
				}
				seen++
			}
		})
		if overflow {
			if !errors.Is(e, aggregate.ErrDecimalSumOverflow) || seen != 4 {
				t.Fatal("late overflow must keep only prior pages", e, seen)
			}
		} else if e != nil || seen != 6 {
			t.Fatal(e, seen)
		}
	}
}
func TestN17CancelAndReferenceRelease(t *testing.T) {
	for _, fn := range []string{"(*decimalGroupedStream).validateBatch", "(*decimalGroupedStream).addRow", "(*decimalGroupedStream).emit", "(*decimalGroupedStream).Next"} {
		t.Run(fn, func(t *testing.T) {
			en, p, _ := n17Fixture(t, 195, 17, false, false, true)
			q, _ := en.BeginQueryMemory()
			defer q.Close()
			base := memory.WithQueryBudget(context.Background(), q)
			stream, e := p.NewSortedAggregateStream(base, p.resultSchema(), 2)
			if e != nil {
				t.Fatal(e)
			}
			s := stream.(*decimalGroupedStream)
			ctx := &n14CancelContext{Context: base, function: fn, at: 2}
			b := memory.NewBatch()
			_, e = s.Next(ctx, b)
			if !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			if q.Used() != 0 || s.truth != nil || s.states != nil || s.input != nil || s.output != nil {
				t.Fatal("cancel references")
			}
			_, again := s.Next(base, b)
			if !errors.Is(again, context.Canceled) {
				t.Fatal("not sticky", again)
			}
			s.Close()
		})
	}
	en, p, _ := n17Fixture(t, 195, 17, false, false, true)
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, _ := p.NewSortedAggregateStream(ctx, p.resultSchema(), 2)
	s := st.(*decimalGroupedStream)
	b := memory.NewBatch()
	ok, e := s.Next(ctx, b)
	if !ok || e != nil {
		t.Fatal(e)
	}
	if s.truth == nil || s.row == 0 {
		t.Fatal("fixture lacks held predicate")
	}
	key, count, dec := b.Int64s["k"], b.Uint64s["n"], b.Decimals["s"]
	s.Close()
	if q.Used() != 0 || s.truth != nil || s.states != nil || s.input != nil || len(key.Values) != 0 || len(count.Values) != 0 || len(dec.Values) != 0 {
		t.Fatal("close references")
	}
}
func TestN17CandidateRowAtomic(t *testing.T) {
	en, p, _ := n17Fixture(t, 3, 3, false, false, false)
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, _ := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
	s := st.(*decimalGroupedStream)
	defer s.Close()
	s.input.Next(ctx, &s.inputBatch)
	if e := s.validateBatch(ctx); e != nil {
		t.Fatal(e)
	}
	s.counts[2] = math.MaxUint64
	if e := s.addRow(ctx, 0); e == nil {
		t.Fatal("countoverflow")
	}
	if s.states[1].Count() != 0 || s.counts[0] != 0 || s.counts[2] != math.MaxUint64 || s.selected != 0 {
		t.Fatal("partial commit")
	}
	if s.scratch[1].Count() != 1 {
		t.Fatal("did not reach late lane")
	}
}

type n17PartSource struct {
	*n17Source
	rows int64
}

func (s n17PartSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: s.rows}}, nil
}
func TestN17StrategyProofAndUnknownPartitions(t *testing.T) {
	en, _, src := n17Fixture(t, 3, 3, false, false, false)
	q := expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("d", "s")}}
	for _, n := range []int64{-1, -2} {
		p, e := en.Prepare(context.Background(), n17PartSource{src, n}, q, PrepareOptions{})
		if n == -1 {
			if e != nil {
				t.Fatal(e)
			}
			r, e := p.Execute(context.Background(), ExecuteOptions{})
			p.Close()
			if e != nil || r.GroupCount != 4 {
				t.Fatal(r, e)
			}
		} else if e == nil {
			p.Close()
			t.Fatal("negative2")
		}
	}
	for _, force := range []string{"bitmap-direct", "radix", "spill-hash"} {
		q.Hints.ForceStrategy = force
		if p, e := en.Prepare(context.Background(), src, q, PrepareOptions{}); e == nil {
			p.Close()
			t.Fatal("forced", force)
		}
	}
	q.Hints.ForceStrategy = ""
	src.ordering = nil
	if p, e := en.Prepare(context.Background(), src, q, PrepareOptions{}); e == nil {
		p.Close()
		t.Fatal("missing proof")
	}
}

func TestN17AllNullAndReadCaps(t *testing.T) {
	en, p, src := n17Fixture(t, 195, 17, false, false, false)
	for _, b := range src.batches {
		v := b.Decimals["d"]
		v.Validity = memory.EnsureBitmap(nil, b.Rows)
		for i := range v.Values {
			v.Values[i] = decimal128.New(math.MaxInt64, math.MaxUint64)
		}
	}
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, e := p.NewSortedAggregateStreamWithReadOptions(ctx, p.resultSchema(), 64, source.ReadOptions{Kernels: en.kernels, BatchRows: 3, BatchBytes: 65536})
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	seen := 0
	for {
		ok, e := st.Next(ctx, b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		if b.Rows > 3 {
			t.Fatal("read cap")
		}
		for r := 0; r < b.Rows; r++ {
			if b.Decimals["s"].IsValid(r) || b.Decimals["s"].Values[r] != (memory.Decimal128{}) || b.Uint64s["valid"].Values[r] != 0 {
				t.Fatal("null poison")
			}
		}
		seen += b.Rows
	}
	s := st.(*decimalGroupedStream)
	if seen != 196 || s.truth != nil || s.input != nil || q.Used() != 0 {
		t.Fatal("EOF cleanup", seen, q.Used())
	}
	st.Close()
}

func TestN17PlanAndObserverReason(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(fmt.Sprintf("grouped=%v", grouped), func(t *testing.T) {
			var en *Engine
			var p *PreparedQuery
			want := "global aggregation uses one bounded state"
			if grouped {
				en, p, _ = n17Fixture(t, 3, 3, false, false, false)
				want = "ordered Decimal grouping uses one bounded current-group state"
			} else {
				en, p = n14Prepared(t, 3)
			}
			recorder := &observer.Collector{}
			en.opts.Observer = recorder
			plan := p.Plan().Strategy
			if plan.Kind != planner.StrategySorted || plan.Workers != 1 || plan.Reason != want {
				t.Errorf("Plan strategy=%v workers=%d reason=%q want=%q", plan.Kind, plan.Workers, plan.Reason, want)
			}
			if _, e := p.Execute(context.Background(), ExecuteOptions{}); e != nil {
				t.Fatal(e)
			}
			found := 0
			for _, ev := range recorder.Events() {
				if ev.Stage != "prepared-query" {
					continue
				}
				found++
				if ev.Attributes["reason"] != want || ev.Workers != 1 {
					t.Errorf("prepared-query workers=%d reason=%q want=%q", ev.Workers, ev.Attributes["reason"], want)
				}
			}
			if found != 1 {
				t.Fatalf("prepared-query events=%d", found)
			}
		})
	}
}
