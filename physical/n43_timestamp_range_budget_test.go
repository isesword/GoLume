package physical

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"testing"
	"unsafe"

	"gocolumn/memory"
	"gocolumn/source"
)

type n43BudgetRow struct {
	ts, value int64
	valid     bool
}
type n43BudgetExpected struct {
	rows, count int64
	sum         *big.Int
	average     float64
	valid       bool
}

// Independent O(N²) mathematical oracle: bound arithmetic is unbounded, and
// every timestamp peer (including future source batches) participates.
func n43BudgetOracle(in []n43BudgetRow, width int64) []n43BudgetExpected {
	out := make([]n43BudgetExpected, len(in))
	for i, r := range in {
		lo := new(big.Int).Sub(big.NewInt(r.ts), big.NewInt(width))
		sum := new(big.Int)
		var count, rows int64
		for _, v := range in {
			if v.ts <= r.ts && big.NewInt(v.ts).Cmp(lo) >= 0 {
				rows++
				if v.valid {
					count++
					sum.Add(sum, big.NewInt(v.value))
				}
			}
		}
		avg := float64(0)
		if count > 0 {
			avg, _ = new(big.Rat).SetFrac(sum, big.NewInt(count)).Float64()
		}
		out[i] = n43BudgetExpected{rows, count, sum, avg, count > 0}
	}
	return out
}

type n43BudgetSource struct {
	rows                  []n43BudgetRow
	batchRows             int
	opened, closed, nexts int
	live                  int64
}

func (s *n43BudgetSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true})
}
func (s *n43BudgetSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(len(s.rows)), Partitions: 1}, nil
}
func (s *n43BudgetSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(len(s.rows))}}, nil
}
func (s *n43BudgetSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "ts"}}
}
func (s *n43BudgetSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Operator: "Scan", Class: source.StreamPipeline}
}
func (s *n43BudgetSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opened++
	return &n43BudgetStream{s: s, m: o.MemoryManager}, nil
}

type n43BudgetStream struct {
	s          *n43BudgetSource
	m          memory.Manager
	lease      memory.Reservation
	ts, values []int64
	bits       memory.Bitmap
	at         int
	closed     bool
}

func (s *n43BudgetStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.s.nexts++
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
func (s *n43BudgetStream) Close() error {
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
func n43BudgetMainRows() []n43BudgetRow {
	out := []n43BudgetRow{}
	for peer := 0; peer < 20; peer++ {
		for j := 0; j < 7; j++ {
			out = append(out, n43BudgetRow{int64(peer * 3), int64(peer*7 + j - 60), j%4 != 0})
		}
	}
	return out
}
func TestN43TimestampRangeOracleFixture(t *testing.T) {
	r := []n43BudgetRow{{math.MinInt64, 1, true}, {math.MinInt64, 3, true}, {math.MinInt64 + 1, 0, false}}
	o := n43BudgetOracle(r, 2)
	if o[0].rows != 2 || o[0].sum.Int64() != 4 || o[2].rows != 3 || o[2].count != 2 {
		t.Fatal("independent oracle fixture")
	}
}

// QueryBudget has no injectable parent Manager. Observe the actual shared
// query and cancel at real context checks rather than claiming Reserve hooks.
type n43BudgetManager struct {
	memory.Manager
	calls, fail, cancel int
	releases, releaseAt int
	stream              *timestampRangeStream
	growthOverlap       int64
	stop                context.CancelFunc
}

func (m *n43BudgetManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if e == nil && m.calls == m.cancel {
		m.stop()
	}
	if e == nil {
		if m.stream != nil && m.stream.queueLease != nil && n == int64(2*len(m.stream.queue))*int64(unsafe.Sizeof(timestampRangeRow{})) {
			overlap := m.stream.queueLease.Bytes() + n
			if overlap > m.growthOverlap {
				m.growthOverlap = overlap
			}
		}
		return &n43BudgetLease{Reservation: r, m: m}, nil
	}
	return r, e
}

type n43BudgetLease struct {
	memory.Reservation
	m    *n43BudgetManager
	done bool
}

func (r *n43BudgetLease) Release() {
	if r.done {
		return
	}
	r.done = true
	r.Reservation.Release()
	r.m.releases++
	if r.m.releases == r.m.releaseAt {
		r.m.stop()
	}
}

type n43BudgetContext struct {
	context.Context
	mu         sync.Mutex
	checks, at int
	stop       context.CancelFunc
	q          *memory.QueryBudget
	peak       memory.BudgetSnapshot
	sourcePeak int64
}

func (c *n43BudgetContext) Err() error {
	snap := c.q.Snapshot()
	c.mu.Lock()
	c.checks++
	hit := c.at > 0 && c.checks >= c.at
	if snap.UsedBytes > c.peak.UsedBytes {
		c.peak = snap
	}
	if snap.Operators["SourceScan"] > 0 && snap.UsedBytes > c.sourcePeak {
		c.sourcePeak = snap.UsedBytes
	}
	c.mu.Unlock()
	if hit {
		c.stop()
	}
	return c.Context.Err()
}
func n43BudgetSpec(width int64) TimestampRangeSpec {
	return TimestampRangeSpec{OrderBy: "ts", Column: "value", Frame: TimestampRangeFrame{Kind: WindowFrameRange, PrecedingNS: width}, CountRowsAlias: "rows", CountAlias: "count", SumAlias: "sum", AvgAlias: "avg"}
}
func n43BudgetRun(t *testing.T, rows []n43BudgetRow, width int64, batchRows int, limit int64, fail, cancelAt int, want error, requestedRows ...int) (int64, int) {
	t.Helper()
	root := memory.NewEngineBudget(limit)
	q, e := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	base, stop := context.WithCancel(context.Background())
	defer stop()
	observer := &n43BudgetContext{Context: base, q: q, at: cancelAt, stop: stop}
	ctx := memory.WithQueryBudget(observer, q)
	src := &n43BudgetSource{rows: rows, batchRows: batchRows}

	op, e := NewTimestampRangeSource(src, n43BudgetSpec(width))
	if e != nil {
		t.Fatal(e)
	}
	opts := n27Opts(q.Operator("N43Caller", memory.MemoryWorkspace))
	opts.BatchRows = 4
	early := len(requestedRows) > 0 && requestedRows[0] == -1
	if len(requestedRows) > 0 && requestedRows[0] > 0 {
		opts.BatchRows = requestedRows[0]
	}
	pages := []int{}
	st, e := source.OpenBatchStream(ctx, op, opts)
	var hook *n43BudgetManager
	if e == nil {
		raw := st.(*timestampRangeStream)
		hook = &n43BudgetManager{Manager: raw.manager, stop: stop, stream: raw}
		if fail > 100000 {
			hook.releaseAt = fail - 100000
		} else if fail > 0 {
			hook.fail = fail
		}
		if fail < 0 {
			hook.cancel = -fail
		}
		raw.manager = hook
	}
	seen := 0
	expected := n43BudgetOracle(rows, width)
	if e == nil {
		defer st.Close()
		b := memory.NewBatch()
		for {
			var ok bool
			ok, e = st.Next(ctx, b)
			if e != nil {
				if ok || len(b.Int64s) != 0 || len(b.Float64s) != 0 {
					t.Fatal("partial error page")
				}
				break
			}
			if !ok {
				break
			}
			pages = append(pages, b.Rows)
			if b.Rows > 1024 {
				t.Fatal("hard output page bound", b.Rows)
			}
			for i := 0; i < b.Rows; i++ {
				r, w := rows[seen+i], expected[seen+i]
				if b.Int64s["ts"].Values[i] != r.ts || b.Int64s["value"].IsValid(i) != r.valid || r.valid && b.Int64s["value"].Values[i] != r.value {
					t.Fatal("source reuse lost input")
				}
				if b.Int64s["rows"].Values[i] != w.rows || b.Int64s["count"].Values[i] != w.count {
					t.Fatalf("peer/frame counts row%d", seen+i)
				}
				if b.Int64s["sum"].IsValid(i) != w.valid || b.Float64s["avg"].IsValid(i) != w.valid {
					t.Fatal("nullable aggregate")
				}
				if w.valid {
					if !w.sum.IsInt64() {
						t.Fatal("overflow was published")
					}
					if b.Int64s["sum"].Values[i] != w.sum.Int64() || math.Float64bits(b.Float64s["avg"].Values[i]) != math.Float64bits(w.average) {
						t.Fatalf("exact aggregate row%d", seen+i)
					}
				}
			}
			seen += b.Rows
			if early {
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				if b.Rows != 0 || len(b.Int64s) != 0 {
					t.Fatal("early Close left borrowed output")
				}
				break
			}
		}
	}
	if want != nil {
		if !errors.Is(e, want) {
			t.Fatalf("expected%v got%v", want, e)
		}
	} else if e != nil {
		t.Fatal(e)
	} else if !early && seen != len(rows) {
		t.Fatal("rowcount", seen)
	}
	peak := q.Snapshot().PeakBytes
	observer.mu.Lock()
	calls := observer.checks
	observedPeak := observer.peak
	sourcePeak := observer.sourcePeak
	observer.at = 0
	observer.mu.Unlock()
	if st != nil {
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if q.Used() != 0 || src.opened != src.closed {
		t.Fatalf("cleanup RAM%d opens%d closes%d", q.Used(), src.opened, src.closed)
	}
	if want == nil {
		if opts.BatchRows == 2048 && (len(pages) != 3 || pages[0] != 1024 || pages[1] != 76 || pages[2] != 1) {
			t.Fatalf("hard pages %v", pages)
		}
		t.Logf("output pages=%v", pages)
		t.Logf("rows%d sourceBatch%d width%d peak%d contextChecks%d fullSourceOverlap%d classes%v operators%v", len(rows), batchRows, width, peak, calls, sourcePeak, observedPeak.ByClass, observedPeak.Operators)
	}
	if hook != nil {
		t.Logf("internal queue/page Reserve calls=%d releases=%d old+new ring=%d", hook.calls, hook.releases, hook.growthOverlap)
		if cancelAt == -2 {
			calls = hook.releases
		}
		if cancelAt == -1 {
			calls = hook.calls
		}
	}
	return peak, calls
}
func TestN43TimestampRangeBudget(t *testing.T) {
	rows := n43BudgetMainRows()
	peak, calls := n43BudgetRun(t, rows, 8, 5, 8<<20, 0, 0, nil)
	t.Run("exact", func(t *testing.T) { n43BudgetRun(t, rows, 8, 5, peak, 0, 0, nil) })
	t.Run("minusone", func(t *testing.T) { n43BudgetRun(t, rows, 8, 5, peak-1, 0, 0, memory.ErrBudgetExceeded) })
	_, reserves := n43BudgetRun(t, rows, 8, 5, 8<<20, 0, -1, nil)
	for at := 1; at <= reserves; at++ {
		t.Run(fmt.Sprintf("reservefail%d", at), func(t *testing.T) { n43BudgetRun(t, rows, 8, 5, 8<<20, at, 0, memory.ErrBudgetExceeded) })
		t.Run(fmt.Sprintf("reservecancel%d", at), func(t *testing.T) { n43BudgetRun(t, rows, 8, 5, 8<<20, -at, 0, context.Canceled) })
	}
	_, releases := n43BudgetRun(t, rows, 8, 5, 8<<20, 0, -2, nil)
	for at := 1; at <= releases; at++ {
		t.Run(fmt.Sprintf("releasecancel%d", at), func(t *testing.T) { n43BudgetRun(t, rows, 8, 5, 8<<20, 100000+at, 0, context.Canceled) })
	}
	for at := 1; at <= calls; at++ {

		t.Run(fmt.Sprintf("cancel%d", at), func(t *testing.T) { n43BudgetRun(t, rows, 8, 5, 8<<20, 0, at, context.Canceled) })
	}
}
func TestN43TimestampRangeEdges(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rows  []n43BudgetRow
		width int64
	}{{"zero_future_peers", []n43BudgetRow{{0, 1, true}, {0, 2, true}, {0, 0, false}, {1, 7, true}}, 0}, {"underflow", []n43BudgetRow{{math.MinInt64, 1, true}, {math.MinInt64, 2, true}, {math.MinInt64 + 1, 4, true}, {0, 8, true}}, math.MaxInt64}, {"wide_intermediate", []n43BudgetRow{{0, math.MaxInt64, true}, {0, math.MaxInt64, true}, {0, -math.MaxInt64, true}, {1, 0, false}}, 0}} {
		t.Run(tc.name, func(t *testing.T) { n43BudgetRun(t, tc.rows, tc.width, 1, 8<<20, 0, 0, nil) })
	}
}

func TestN43TimestampRangeLargePeerBudget(t *testing.T) {
	rows := make([]n43BudgetRow, 601)
	for i := 0; i < 600; i++ {
		rows[i] = n43BudgetRow{ts: 9, value: int64(i%17 - 8), valid: i%5 != 0}
	}
	rows[600] = n43BudgetRow{ts: 10, value: 7, valid: true}
	peak, _ := n43BudgetRun(t, rows, 0, 17, 8<<20, 0, 0, nil)
	t.Run("exact", func(t *testing.T) { n43BudgetRun(t, rows, 0, 17, peak, 0, 0, nil) })
	t.Run("minusone", func(t *testing.T) { n43BudgetRun(t, rows, 0, 17, peak-1, 0, 0, memory.ErrBudgetExceeded) })
}

func TestN43TimestampRangePageLifecycle(t *testing.T) {
	t.Run("earlyclose", func(t *testing.T) { n43BudgetRun(t, n43BudgetMainRows(), 8, 5, 8<<20, 0, 0, nil, -1) })
	t.Run("hard1024", func(t *testing.T) {
		rows := make([]n43BudgetRow, 1101)
		for i := 0; i < 1100; i++ {
			rows[i] = n43BudgetRow{9, int64(i%11 - 5), i%7 != 0}
		}
		rows[1100] = n43BudgetRow{10, 3, true}
		n43BudgetRun(t, rows, 0, 17, 8<<20, 0, 0, nil, 2048)
	})
}
