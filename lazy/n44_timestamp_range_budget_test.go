package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"strings"
	"testing"
)

type n44BudgetRow struct {
	ts, value int64
	valid     bool
}
type n44BudgetSource struct {
	rows                  []n44BudgetRow
	batchRows             int
	opened, closed, nexts int
	live                  int64
	expectedQuery         *memory.QueryBudget
}

func (s *n44BudgetSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true})
}
func (s *n44BudgetSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(len(s.rows)), Partitions: 1}, nil
}
func (s *n44BudgetSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(len(s.rows))}}, nil
}
func (s *n44BudgetSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "ts"}}
}
func (s *n44BudgetSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Operator: "Scan", Class: source.StreamPipeline}
}
func (s *n44BudgetSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opened++
	return &n44BudgetStream{s: s, m: o.MemoryManager}, nil
}

type n44BudgetStream struct {
	s          *n44BudgetSource
	m          memory.Manager
	lease      memory.Reservation
	ts, values []int64
	bits       memory.Bitmap
	at         int
	closed     bool
}

func (s *n44BudgetStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.s.nexts++
	if s.s.expectedQuery != nil && memory.QueryBudgetFromContext(ctx) != s.s.expectedQuery {
		return false, fmt.Errorf("source received different query budget")
	}
	b.ClearColumns()
	if e := ctx.Err(); e != nil {
		return false, e
	}
	if s.at == len(s.s.rows) {
		return false, nil
	}
	n := s.s.batchRows
	if n < 1 {
		n = 1
	}
	if s.lease == nil {
		charge := int64(4096 + 16*n + 8*memory.WordsFor(n))
		var e error
		s.lease, e = s.m.Reserve(ctx, charge)
		if e != nil {
			return false, e
		}
		s.s.live = charge
		s.ts = make([]int64, n)
		s.values = make([]int64, n)
		s.bits = make(memory.Bitmap, memory.WordsFor(n))
	}
	clear(s.bits)
	n = min(n, len(s.s.rows)-s.at)
	for i := 0; i < n; i++ {
		r := s.s.rows[s.at+i]
		s.ts[i] = r.ts
		s.values[i] = r.value
		if r.valid {
			memory.Set(s.bits, i)
		}
	}
	s.at += n
	b.Rows = n
	b.Int64s["ts"] = &memory.Int64Vector{Values: s.ts[:n]}
	b.Int64s["value"] = &memory.Int64Vector{Values: s.values[:n], Validity: s.bits[:memory.WordsFor(n)]}
	return true, nil
}
func (s *n44BudgetStream) Close() error {
	if !s.closed {
		s.closed = true
		s.s.closed++
		for i := range s.values {
			s.values[i] = math.MinInt64
			s.ts[i] = math.MaxInt64
		}
		clear(s.bits)
		if s.lease != nil {
			s.lease.Release()
		}
		s.s.live = 0
	}
	return nil
}

func n44BudgetSession(limit int64, policy engine.ExecutionPolicy) *Session {
	o := engine.DefaultOptions()
	o.Workers = 1
	o.Pipeline.Workers = 1
	o.ExecutionPolicy = policy
	o.MemoryLimitBytes = limit
	o.PreparedCacheBytes = 0
	return NewSession(o)
}
func n44BudgetRows() []n44BudgetRow {
	r := make([]n44BudgetRow, 110)
	for i := range r {
		r[i] = n44BudgetRow{int64(i / 5), int64(i%5 + 1), i%5 != 2}
	}
	return r
}
func n44BudgetSpec() TimestampRangeSpec {
	return TimestampRangeSpec{OrderBy: "ts", Column: "value", Frame: TimestampRangeFrame{Kind: WindowFrameRange, PrecedingNS: 0}, CountRowsAlias: "rows", CountAlias: "count", SumAlias: "sum", AvgAlias: "avg"}
}
func n44BudgetRun(t *testing.T, limit int64, cancelPage int, want error) (int64, int) {
	t.Helper()
	root := memory.NewEngineBudget(limit)
	q, e := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	base, stop := context.WithCancel(context.Background())
	defer stop()
	ctx := memory.WithQueryBudget(base, q)
	session := n44BudgetSession(8<<20, engine.ExecutionMemoryOnly)
	defer session.Close()
	src := &n44BudgetSource{rows: n44BudgetRows(), batchRows: 3, expectedQuery: q}
	f := session.FromSource(src).WithTimestampRange(n44BudgetSpec())
	r, e := f.CollectBatches(ctx, BatchOptions{})
	pages, seen := 0, 0
	var coexist memory.BudgetSnapshot
	var view BatchView
	if e == nil {
		defer r.Close()
		for r.Next(ctx) {
			pages++
			snap := q.Snapshot()
			if snap.Operators["SourceScan"] > 0 && snap.Operators["TimestampRange"] > 0 && snap.UsedBytes > coexist.UsedBytes {
				coexist = snap
			}
			view = r.Batch()
			for _, name := range []string{"rows", "count", "sum"} {
				v, err := view.Column(name)
				if err != nil {
					t.Fatal(err)
				}
				vals, valid, err := v.CopyInt64()
				if err != nil {
					t.Fatal(err)
				}
				expected := int64(5)
				if name == "count" {
					expected = 4
				}
				if name == "sum" {
					expected = 12
				}
				for i, x := range vals {
					if x != expected || valid != nil && !memory.IsSet(valid, i) {
						t.Fatalf("%s row%d got%d", name, seen+i, x)
					}
				}
			}
			a, _ := view.Column("avg")
			vals, _, err := a.CopyFloat64()
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range vals {
				if v != 3 {
					t.Fatal("average", v)
				}
			}
			seen += int(view.Len())
			if cancelPage == pages {
				stop()
			}
		}
		e = r.Err()
		if ce := r.Close(); ce != nil && e == nil {
			e = ce
		}
		_ = r.Close()
		if view.Valid() {
			t.Fatal("borrow survived EOF/Close")
		}
	}
	if want != nil {
		if !errors.Is(e, want) {
			t.Fatalf("want %v got %v", want, e)
		}
	} else if e != nil {
		t.Fatal(e)
	} else if seen != 110 {
		t.Fatal("rows", seen)
	}
	snap := q.Snapshot()
	if q.Used() != 0 || src.opened != src.closed {
		t.Fatalf("cleanup used%d opens%d closes%d", q.Used(), src.opened, src.closed)
	}
	lease, e := q.Operator("CallerAfterClose", memory.MemoryWorkspace).Reserve(context.Background(), 1)
	if e != nil {
		t.Fatal("caller q closed", e)
	}
	lease.Release()
	if want == nil && coexist.UsedBytes == 0 {
		t.Fatal("no shared source + RANGE output coexistence observed")
	}
	t.Logf("peak%d pages%d sourceBatch3 coexist%d classes%v operators%v", snap.PeakBytes, pages, coexist.UsedBytes, coexist.ByClass, coexist.Operators)
	return snap.PeakBytes, pages
}
func TestN44BudgetShared(t *testing.T) {
	peak, pages := n44BudgetRun(t, 8<<20, 0, nil)
	t.Run("exact", func(t *testing.T) { n44BudgetRun(t, peak, 0, nil) })
	t.Run("minusone", func(t *testing.T) { n44BudgetRun(t, peak-1, 0, memory.ErrBudgetExceeded) })
	for i := 1; i <= pages; i++ {
		t.Run(fmt.Sprintf("cancelpage%d", i), func(t *testing.T) { n44BudgetRun(t, 8<<20, i, context.Canceled) })
	}
}
func TestN44BudgetAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(LazyFrame) LazyFrame
	}{
		{"filtertrue", func(f LazyFrame) LazyFrame { return f.Filter(Bool(true)) }},
		{"positiveLimit", func(f LazyFrame) LazyFrame { return f.Limit(2) }},
		{"computedValue", func(f LazyFrame) LazyFrame {
			return f.SelectExprs(Col("ts").Alias("ts"), Col("value").Add(Lit(0)).Alias("value"))
		}},
		{"hiddenComputed", func(f LazyFrame) LazyFrame {
			return f.WithColumns(Col("value").Add(Lit(0)).Alias("hidden")).Select("ts", "value")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := n44BudgetSession(8<<20, engine.ExecutionMemoryOnly)
			defer s.Close()
			src := &n44BudgetSource{rows: n44BudgetRows(), batchRows: 3}
			f := tc.mut(s.FromSource(src)).WithTimestampRange(n44BudgetSpec())
			if _, e := f.Schema(); e == nil {
				t.Fatal("Schema accepted unsafe lineage")
			}
			if _, e := f.ExplainOptimized(); e == nil {
				t.Fatal("Explain accepted unsafe lineage")
			}
			p, e := f.Prepare(context.Background())
			if e == nil {
				p.Close()
				t.Fatal("Prepare accepted unsafe lineage")
			}
			if src.opened != 0 {
				t.Fatal("admission executed source")
			}
		})
	}
}
func TestN44BudgetOutputNonelision(t *testing.T) {
	for _, mode := range []string{"select", "count", "positiveLimit"} {
		t.Run(mode, func(t *testing.T) {
			s := n44BudgetSession(8<<20, engine.ExecutionMemoryOnly)
			defer s.Close()
			src := &n44BudgetSource{rows: []n44BudgetRow{{0, math.MaxInt64, true}, {0, 1, true}}, batchRows: 1}
			f := s.FromSource(src).WithTimestampRange(n44BudgetSpec())
			switch mode {
			case "select":
				f = f.Select("ts")
			case "count":
				f = f.Select("ts").Agg(Count("n"))
			case "positiveLimit":
				f = f.Limit(1)
			}
			d, e := f.Collect(context.Background())
			if e == nil {
				d.Close()
				t.Fatal("complete peer SUM overflow hidden")
			}
			if !strings.Contains(e.Error(), "complete-window SUM overflows Int64") {
				t.Fatal("wrong error", e)
			}
		})
	}
}

func TestN44BudgetExecutionPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limit  int64
		policy engine.ExecutionPolicy
	}{
		{"auto", 8 << 20, engine.ExecutionAuto}, {"bounded", 8 << 20, engine.ExecutionBounded}, {"unlimited", 0, engine.ExecutionMemoryOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := n44BudgetSession(tc.limit, tc.policy)
			defer s.Close()
			src := &n44BudgetSource{rows: n44BudgetRows(), batchRows: 3}
			f := s.FromSource(src).WithTimestampRange(n44BudgetSpec())
			if _, e := f.Schema(); e == nil {
				t.Fatal("Schema accepted execution policy")
			}
			if _, e := f.ExplainOptimized(); e == nil {
				t.Fatal("Explain accepted execution policy")
			}
			p, e := f.Prepare(context.Background())
			if e == nil {
				p.Close()
				t.Fatal("Prepare accepted execution policy")
			}
			if src.opened != 0 {
				t.Fatal("admission opened source")
			}
		})
	}
}
