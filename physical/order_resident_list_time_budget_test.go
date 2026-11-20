package physical

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
)

const n37BudgetDay int64 = 86400000000000

// Synthetic eight-row/two-batch workload, not the missing historical 16/64MiB
// workload. No Parquet row groups, no filesystem/spill/sink allocations.
type n37BudgetSource struct {
	schema         memory.Schema
	opened, closed int
}

func n37BudgetInput() *n37BudgetSource {
	return &n37BudgetSource{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "id", Type: memory.TypeInt64}, memory.ListTime64Field("times", true, true))}
}
func (s *n37BudgetSource) Schema() memory.Schema { return s.schema }
func (s *n37BudgetSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: 8, Partitions: 1}, nil
}
func (s *n37BudgetSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: 8}}, nil
}
func (s *n37BudgetSource) NewBatchStream(_ context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opened++
	return &n37BudgetStream{src: s, m: o.MemoryManager}, nil
}

type n37BudgetStream struct {
	src    *n37BudgetSource
	m      memory.Manager
	lease  memory.Reservation
	page   int
	b      *memory.Batch
	closed bool
}

func (s *n37BudgetStream) poison() {
	if s.b != nil {
		for i := range s.b.Int64Lists["times"].Values.Values {
			s.b.Int64Lists["times"].Values.Values[i] = -777
		}
	}
}
func (s *n37BudgetStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	dst.ClearColumns()
	s.poison()
	if s.lease != nil {
		s.lease.Release()
		s.lease = nil
	}
	if e := ctx.Err(); e != nil {
		return false, e
	}
	if s.page == 2 {
		return false, nil
	}
	// Keys32 + IDs32 + offsets20 + parentbitmap8 + childvalues40 + childbitmap8.
	var e error
	s.lease, e = s.m.Reserve(ctx, 140)
	if e != nil {
		return false, e
	}
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{2, 1, 0, 1}}
	base := int64(s.page * 4)
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{base, base + 1, base + 2, base + 3}}
	b.Int64Lists["times"] = &memory.Int64ListVector{Offsets: []uint32{0, 2, 2, 3, 5}, Validity: memory.Bitmap{11}, Values: &memory.Int64Vector{Values: []int64{100, -999, n37BudgetDay + 1, n37BudgetDay - 1, 0}, Validity: memory.Bitmap{29}}}
	s.b = b
	s.page++
	dst.Rows = 4
	dst.Int64s["key"] = b.Int64s["key"]
	dst.Int64s["id"] = b.Int64s["id"]
	dst.Int64Lists["times"] = b.Int64Lists["times"]
	return true, nil
}
func (s *n37BudgetStream) Close() error {
	if !s.closed {
		s.closed = true
		s.src.closed++
		s.poison()
		if s.lease != nil {
			s.lease.Release()
			s.lease = nil
		}
	}
	return nil
}

type n37BudgetProbe struct {
	lastRequested, lastUsed int64
	memory.Manager
	calls, failAt, cancelAt int
	peak                    int64
	trace                   []int64
	cancel                  context.CancelFunc
}

func (m *n37BudgetProbe) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	m.lastRequested = n
	m.lastUsed = m.Used()
	m.trace = append(m.trace, n)
	if m.calls == m.failAt {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	if e == nil && m.calls == m.cancelAt {
		m.cancel()
	}
	return r, e
}
func n37BudgetCheck(t *testing.T, b *memory.Batch, start int) {
	t.Helper()
	want := []int64{2, 6, 1, 3, 5, 7, 0, 4}
	v := b.Int64Lists["times"]
	if v == nil {
		t.Fatal("TIME routed away from Int64Lists")
	}
	for i := 0; i < b.Rows; i++ {
		id := want[start+i]
		if b.Int64s["id"].Values[i] != id {
			t.Fatal("unstable order", id, b.Int64s["id"].Values[i])
		}
		row := id % 4
		lo, hi, valid := v.ValueRange(i)
		if row == 2 {
			if valid {
				t.Fatal("parentNULL lost")
			}
			continue
		}
		if !valid {
			t.Fatal("valid parent lost")
		}
		if row == 1 {
			if hi != lo {
				t.Fatal("empty list lost")
			}
			continue
		}
		if hi-lo != 2 {
			t.Fatal("child count")
		}
		if row == 0 {
			if !v.Values.IsValid(lo) || v.Values.Values[lo] != 100 || v.Values.IsValid(lo+1) {
				t.Fatal("childNULL/value lost")
			}
		} else {
			if !v.Values.IsValid(lo) || !v.Values.IsValid(lo+1) || v.Values.Values[lo] != n37BudgetDay-1 || v.Values.Values[lo+1] != 0 {
				t.Fatal("TIME coefficient changed")
			}
		}
	}
}
func n37BudgetRun(t *testing.T, limit int64, fail, cancelAt int, early bool, want error) (int64, int, []int64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &n37BudgetProbe{Manager: memory.NewBudgetManager(limit), failAt: fail, cancelAt: cancelAt, cancel: cancel}
	src := n37BudgetInput()
	op, e := NewSortSource(src, []SortKey{{Column: "key"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(op.Schema(), src.Schema()) {
		t.Fatal("TIME schema metadata changed")
	}
	opts := n27Opts(m)
	opts.BatchRows = 4
	st, e := source.OpenBatchStream(ctx, op, opts)
	rows := 0
	if e == nil {
		if _, ok := st.(*residentSortStream); !ok {
			t.Fatalf("not resident execution: %T", st)
		}
		b := memory.NewBatch()
		for {
			var ok bool
			ok, e = st.Next(ctx, b)
			if e != nil {
				break
			}
			if !ok {
				break
			}
			n37BudgetCheck(t, b, rows)
			rows += b.Rows
			if early {
				break
			}
		}
	}
	if want != nil {
		t.Logf("observed reservation failure/cancel: request=%d usedBefore=%d limit=%d call=%d operator=shared-unclassified probe error=%v", m.lastRequested, m.lastUsed, m.Limit(), m.calls, e)
		if !errors.Is(e, want) {
			t.Fatalf("want=%v got=%v", want, e)
		}
		var resource *memory.ResourceError
		if errors.As(e, &resource) {
			t.Logf("resource requested=%d used=%d limit=%d operator=%s class=%v", resource.RequestedBytes, resource.UsedBytes, resource.LimitBytes, resource.Operator, resource.Class)
		}
	} else if e != nil {
		t.Fatal(e)
	} else if !early && rows != 8 {
		t.Fatal("missing rows", rows)
	}
	peak, calls, trace := m.peak, m.calls, append([]int64(nil), m.trace...)
	m.failAt = 0
	m.cancelAt = 0
	if st != nil {
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if m.Used() != 0 || src.opened != src.closed {
		t.Fatalf("leak memory=%d opens=%d closes=%d", m.Used(), src.opened, src.closed)
	}
	return peak, calls, trace
}
func TestN37ListTimeResidentBudgetExact(t *testing.T) {
	p, c, tr := n37BudgetRun(t, 8<<20, 0, 0, false, nil)
	t.Logf("rows8 childSlots10 (4 hiddenNULL slots), batches2 BatchRows4 workers1 storage=synthetic queryLimit=%d peak=%d calls=%d trace=%v", 8<<20, p, c, tr)
	t.Run("exact", func(t *testing.T) { n37BudgetRun(t, p, 0, 0, false, nil) })
	t.Run("minus_one", func(t *testing.T) { n37BudgetRun(t, p-1, 0, 0, false, memory.ErrBudgetExceeded) })
	t.Run("early_close", func(t *testing.T) { n37BudgetRun(t, 8<<20, 0, 0, true, nil) })
}
func TestN37ListTimeResidentBudgetFailures(t *testing.T) {
	_, calls, _ := n37BudgetRun(t, 8<<20, 0, 0, false, nil)
	for at := 1; at <= calls; at++ {
		t.Run(fmt.Sprintf("reserve_%d", at), func(t *testing.T) { n37BudgetRun(t, 8<<20, at, 0, false, memory.ErrBudgetExceeded) })
	}
}
func TestN37ListTimeResidentBudgetCancel(t *testing.T) {
	_, calls, _ := n37BudgetRun(t, 8<<20, 0, 0, false, nil)
	for at := 1; at <= calls; at++ {
		t.Run(fmt.Sprintf("reserve_%d", at), func(t *testing.T) { n37BudgetRun(t, 8<<20, 0, at, false, context.Canceled) })
	}
}

type n37QueryObserver struct {
	context.Context
	mu                      sync.Mutex
	q                       *memory.QueryBudget
	overlap, retained, scan int64
}

func (c *n37QueryObserver) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.q.Snapshot()
	a, b := s.Operators["SourceScan"], s.Operators["ResidentSort"]
	if a > c.scan {
		c.scan = a
	}
	if b > c.retained {
		c.retained = b
	}
	if a > 0 && b > 0 && a+b > c.overlap {
		c.overlap = a + b
	}
	return c.Context.Err()
}
func TestN37ListTimeResidentSharedQuery(t *testing.T) {
	q, e := memory.NewEngineBudget(8 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 8 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	obs := &n37QueryObserver{Context: context.Background(), q: q}
	ctx := memory.WithQueryBudget(obs, q)
	src := n37BudgetInput()
	op, e := NewSortSource(src, []SortKey{{Column: "key"}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	opts := n27Opts(q)
	opts.BatchRows = 4
	st, e := source.OpenBatchStream(ctx, op, opts)
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	rows := 0
	for {
		ok, e := st.Next(ctx, b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		n37BudgetCheck(t, b, rows)
		rows += b.Rows
	}
	if rows != 8 {
		t.Fatal(rows)
	}
	if e := st.Close(); e != nil {
		t.Fatal(e)
	}
	if q.Used() != 0 || src.closed != 1 {
		t.Fatal("query/source leak")
	}
	if obs.scan != 140 || obs.overlap <= 140 || obs.retained <= 0 {
		t.Fatalf("not shared fullscan+retained: %+v", obs)
	}
	t.Logf("shared q peak=%d source140 retainedMax=%d simultaneous=%d snapshot=%+v", q.Snapshot().PeakBytes, obs.retained, obs.overlap, q.Snapshot())
}
func TestN37ListTimeResidentDoesNotBroadenStrategies(t *testing.T) {
	for _, mode := range []string{"spill", "topk"} {
		t.Run(mode, func(t *testing.T) {
			src := n37BudgetInput()
			m := memory.NewBudgetManager(8 << 20)
			var op source.Source
			var e error
			if mode == "spill" {
				op, e = NewSortSourceWithOptions(src, []SortKey{{Column: "key"}}, m, SortSourceOptions{Spillable: true})
			} else {
				op, e = NewTopKSourceWithManager(src, 2, []SortKey{{Column: "key"}}, m)
			}
			if e == nil {
				st, err := source.OpenBatchStream(context.Background(), op, n27Opts(m))
				e = err
				if st != nil {
					_, e = st.Next(context.Background(), memory.NewBatch())
					st.Close()
				}
			}
			if e == nil {
				t.Fatal("unsupported strategy admitted")
			}
			if m.Used() != 0 {
				t.Fatal("rejected strategy leak")
			}
		})
	}
}
