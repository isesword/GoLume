package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestN16PublicEndpointExists(t *testing.T) {
	if _, ok := reflect.TypeOf((*PreparedQuery)(nil)).MethodByName("ExecuteDecimal"); !ok {
		t.Fatal("missing exact Decimal public materialization endpoint")
	}
}

func TestN16ExactPublicAndLifetime(t *testing.T) {
	en, p, src := n15Prepared(t, 3, false)
	r, e := p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: true})
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Rows) != 1 || r.Summary.GroupCount != 1 || len(r.Summary.Rows) != 0 {
		t.Fatal(r)
	}
	a := r.Rows[0].Aggregates
	if len(a) != 4 {
		t.Fatal(a)
	}
	n, ok := a[0].Value.AsUint64()
	if !ok || n != 12 {
		t.Fatal(n, ok)
	}
	v, pr, sc, ok := a[1].Value.AsDecimal128()
	if !ok || v != decimal128.FromI64(0) || pr != 38 || sc != 0 {
		t.Fatal(v, pr, sc, ok)
	}
	if _, ok = a[1].Value.AsUint64(); ok {
		t.Fatal("Decimal truncated to uint64")
	}
	if _, _, _, ok = a[0].Value.AsDecimal128(); ok {
		t.Fatal("count misread")
	}
	second, e := en.ExecuteDecimal(context.Background(), src, expr.Query{Aggregates: p.cq.Aggregates}, ExecuteOptions{Materialize: true})
	if e != nil {
		t.Fatal(e)
	}
	a[0].Value.Uint64 = 99
	if second.Rows[0].Aggregates[0].Value.Uint64 != 12 {
		t.Fatal("results aliased")
	}
	summary, e := p.ExecuteDecimal(context.Background(), ExecuteOptions{})
	if e != nil || len(summary.Rows) != 0 || summary.Summary.SelectedRows != 12 {
		t.Fatal(summary, e)
	}
	if _, e = p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("old typed expanded")
	}
	p.Close()
	en.Close()
	if second.Rows[0].Aggregates[0].Alias != "rows" || second.Rows[0].Aggregates[0].Value.Uint64 != 12 {
		t.Fatal("closed owner invalidated result")
	}
}
func n16RunBudget(t *testing.T, p *PreparedQuery, limit int64) (int64, error) {
	t.Helper()
	q, e := memory.NewEngineBudget(limit).BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	r, e := p.ExecuteDecimal(memory.WithQueryBudget(context.Background(), q), ExecuteOptions{Materialize: true})
	if e != nil && len(r.Rows) != 0 {
		t.Fatal("partial result escaped")
	}
	if e == nil && len(r.Rows) != 1 {
		t.Fatal("missing row")
	}
	if q.Used() != 0 {
		t.Fatal("retained lease", q.Used())
	}
	return q.Snapshot().PeakBytes, e
}
func TestN16QueryExactBudget(t *testing.T) {
	_, p, _ := n15Prepared(t, 100, true)
	peak, e := n16RunBudget(t, p, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("materialized peak=%d", peak)
	if _, e = n16RunBudget(t, p, peak); e != nil {
		t.Fatal("exact", e)
	}
	if _, e = n16RunBudget(t, p, peak-1); !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("minus one", e)
	}
	if _, e = n16RunBudget(t, p, 1<<20); e != nil {
		t.Fatal("failed execute poisoned prepared", e)
	}
}
func TestN16CollectorReserveFailure(t *testing.T) {
	en, p, _ := n15Prepared(t, 1, false)
	for _, fail := range []int{0, 1} {
		q, _ := en.BeginQueryMemory()
		ctx := memory.WithQueryBudget(context.Background(), q)
		st, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		if ok, e := st.Next(ctx, b); !ok || e != nil {
			t.Fatal(ok, e)
		}
		f := &n14Fault{fail: fail}
		result := DecimalResult{}
		collector := &decimalResultCollector{query: p, result: &result, manager: n14Manager{q.Operator("test-result", memory.MemoryRetainedState), f}}
		streamUsed := q.Used()
		e = collector.append(ctx, b)
		if fail == 1 {
			if !errors.Is(e, memory.ErrBudgetExceeded) || len(result.Rows) != 0 {
				t.Fatal(e, result)
			}
		} else if e != nil || len(result.Rows) != 1 {
			t.Fatal(e, result)
		}
		if fail == 0 {
			if q.Used() != streamUsed+collector.lease.Bytes() {
				t.Fatal("collector/output overlap not charged", q.Used(), streamUsed, collector.lease.Bytes())
			}
			for i, a := range result.Rows[0].Aggregates {
				if unsafe.StringData(a.Alias) == unsafe.StringData(p.executionSchema.Fields[i].Name) {
					t.Fatal("alias backing borrowed")
				}
			}
		}
		collector.close()
		st.Close()
		if q.Used() != 0 {
			t.Fatal(q.Used())
		}
		q.Close()
	}
}
func TestN16CancelAfterCopyRevokesResult(t *testing.T) {
	_, p, _ := n15Prepared(t, 1, false)
	ctx := &n14CancelContext{Context: context.Background(), function: "collectDecimalStream", at: 1}
	r, e := p.ExecuteDecimal(ctx, ExecuteOptions{Materialize: true})
	if !errors.Is(e, context.Canceled) || len(r.Rows) != 0 {
		t.Fatal(r, e, ctx.calls)
	}
	if _, e = p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: true}); e != nil {
		t.Fatal("lifecycle not released", e)
	}
}

type n16BlockingSource struct {
	*n14StreamSource
	entered  chan struct{}
	release  chan struct{}
	closeErr error
}

func (s *n16BlockingSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &n16BlockingInput{n14Input: n14Input{s: s.n14StreamSource}, owner: s}, nil
}

type n16BlockingInput struct {
	n14Input
	owner   *n16BlockingSource
	started bool
}

func (s *n16BlockingInput) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	if !s.started {
		s.started = true
		if s.owner.entered != nil {
			s.owner.entered <- struct{}{}
			select {
			case <-s.owner.release:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
	}
	return s.n14Input.Next(ctx, b)
}
func (s *n16BlockingInput) Close() error { _ = s.n14Input.Close(); return s.owner.closeErr }

type n16QueuedContext struct {
	context.Context
	once   sync.Once
	queued chan struct{}
}

func (c *n16QueuedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.queued) })
	return c.Context.Done()
}
func TestN16ConcurrentAdmissionAndPreparedClose(t *testing.T) {
	en, _, base := n15Prepared(t, 1, false)
	src := &n16BlockingSource{n14StreamSource: base, entered: make(chan struct{}, 1), release: make(chan struct{})}
	p, e := en.Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("d", "sum")}}, PrepareOptions{MaxConcurrency: 1})
	if e != nil {
		t.Fatal(e)
	}
	first := make(chan error, 1)
	go func() { _, e := p.ExecuteDecimal(context.Background(), ExecuteOptions{Materialize: true}); first <- e }()
	select {
	case <-src.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first did not open")
	}
	ctx, cancel := context.WithCancel(context.Background())
	queued := &n16QueuedContext{Context: ctx, queued: make(chan struct{})}
	second := make(chan error, 1)
	go func() {
		r, e := p.ExecuteDecimal(queued, ExecuteOptions{Materialize: true})
		if len(r.Rows) != 0 {
			second <- fmt.Errorf("queued result escaped")
			return
		}
		second <- e
	}()
	select {
	case <-queued.queued:
	case <-time.After(5 * time.Second):
		t.Fatal("second did not queue")
	}
	cancel()
	if e = <-second; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case e := <-closed:
		t.Fatal("closed with active execution", e)
	default:
	}
	close(src.release)
	if e = <-first; e != nil {
		t.Fatal(e)
	}
	if e = <-closed; e != nil {
		t.Fatal(e)
	}
	if _, e = p.ExecuteDecimal(context.Background(), ExecuteOptions{}); !errors.Is(e, ErrPreparedClosed) {
		t.Fatal(e)
	}
}
func TestN16SourceCloseErrorRevokes(t *testing.T) {
	en, _, base := n15Prepared(t, 1, false)
	want := errors.New("source-close-sentinel")
	src := &n16BlockingSource{n14StreamSource: base, closeErr: want}
	r, e := en.ExecuteDecimal(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("d", "sum")}}, ExecuteOptions{Materialize: true})
	if !errors.Is(e, want) || len(r.Rows) != 0 {
		t.Fatal(r, e)
	}
}

func TestN16CopyMidCancel(t *testing.T) {
	en, _, src := n15Prepared(t, 1, false)
	aggs := make([]expr.Aggregate, 512)
	for i := range aggs {
		aggs[i] = expr.Sum("d", fmt.Sprintf("sum_%d", i))
	}
	p, e := en.Prepare(context.Background(), src, expr.Query{Aggregates: aggs}, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	q, _ := en.BeginQueryMemory()
	defer q.Close()
	ctx := &n14CancelContext{Context: memory.WithQueryBudget(context.Background(), q), function: "(*decimalResultCollector).append", at: 12}
	r, e := p.ExecuteDecimal(ctx, ExecuteOptions{Materialize: true})
	if !errors.Is(e, context.Canceled) || len(r.Rows) != 0 || q.Used() != 0 {
		t.Fatal(r, e, q.Used(), ctx.calls)
	}
	if _, e = p.ExecuteDecimal(memory.WithQueryBudget(context.Background(), q), ExecuteOptions{Materialize: true}); e != nil {
		t.Fatal("copy failure retained execution", e)
	}
}
