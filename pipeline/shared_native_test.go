package pipeline

import (
	"context"
	"gocolumn/memory"
	"testing"
	"time"
	"unsafe"
)

func sharedTestMux(t *testing.T, manager memory.Manager, ctx context.Context, capacity int64) *boundedMultiplexer {
	t.Helper()
	ctx, cancel := context.WithCancelCause(ctx)
	m := &boundedMultiplexer{ctx: ctx, cancel: cancel, manager: manager, opts: Options{Workers: 1, TargetMorselBytes: capacity, MaxMorselBytes: capacity, MaxInFlightBytes: capacity, MinMorselRows: 1, MaxMorselRows: 4096}}
	m.sharedImmediate = sharedImmediateManager(ctx, manager)
	m.active.Store(2)
	for i := 0; i < 2; i++ {
		e, err := NewExchange(manager, capacity, 2, 1)
		if err != nil {
			t.Fatal(err)
		}
		c, cc := context.WithCancel(ctx)
		b := &fanOutBranch{owner: m, id: i, exchange: e, ctx: c, cancel: cc}
		b.active.Store(true)
		m.branches = append(m.branches, b)
	}
	t.Cleanup(func() {
		for _, b := range m.branches {
			_ = b.Close()
		}
		cancel(context.Canceled)
	})
	return m
}
func sharedTestBatch(n int) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = n
	v := make([]int64, n)
	for i := range v {
		v[i] = int64(i)
	}
	b.Int64s["x"] = &memory.Int64Vector{Values: v}
	return b
}
func sharedAssertEmpty(t *testing.T, m *boundedMultiplexer) {
	t.Helper()
	for _, b := range m.branches {
		e := b.exchange
		if e.Stats().CurrentInFlightBytes != 0 {
			t.Fatal("live credit")
		}
		if !e.bytes.TryAcquire(e.maxBytes) {
			t.Fatal("global leak")
		}
		e.bytes.Release(e.maxBytes)
		if !e.groupBytes[0].TryAcquire(e.groupMax) {
			t.Fatal("group leak")
		}
		e.groupBytes[0].Release(e.groupMax)
	}
}
func TestSharedNativeNodeAllowances(t *testing.T) {
	if int64(unsafe.Sizeof(sharedNativeOwner{})) > memory.NativeSharedOwnerAllowance || int64(unsafe.Sizeof(preparedSharedItem{})) > memory.NativeSharedBindingOverheadAllowance {
		t.Fatal("bookkeeping exceeds reserved allowance")
	}
}
func TestSharedNativeLastBindingMatchesGuardCharge(t *testing.T) {
	src := sharedTestBatch(1024)
	cost, err := memory.EstimateNativeSharedBatchRange(src, 0, src.Rows)
	if err != nil {
		t.Fatal(err)
	}
	engine := memory.NewEngineBudget(1 << 20)
	q, _ := engine.BeginQuery(memory.BudgetOptions{})
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	m := sharedTestMux(t, q, ctx, 1<<18)
	ok, n, err := m.trySharedRange(src, MorselRange{Rows: src.Rows, Bytes: 1}, Sequence{})
	if err != nil || !ok || n != 2 {
		t.Fatalf("shared: %v %d %v", ok, n, err)
	}
	a, b := <-m.branches[0].exchange.items, <-m.branches[1].exchange.items
	if a.shared.owner != b.shared.owner || a.batch.Int64s["x"] == b.batch.Int64s["x"] || &a.batch.Int64s["x"].Values[0] != &b.batch.Int64s["x"].Values[0] {
		t.Fatal("expected independent headers and shared payload")
	}
	if q.Used() != cost.OwnerBytes+2*cost.BindingBytes {
		t.Fatal("wrong aggregate charge")
	}
	a.batch.ClearColumns()
	m.branches[0].exchange.release(a)
	if q.Used() != cost.CloneBytes || b.batch.Int64s["x"].Values[100] != 100 {
		t.Fatal("first ACK released live payload or retained dead binding")
	}
	m.branches[1].exchange.release(b)
	if q.Used() != 0 || engine.Used() != 0 {
		t.Fatal("owner leak")
	}
	sharedAssertEmpty(t, m)
}
func TestSharedNativeAdmissionMissFullyRollsBack(t *testing.T) {
	src := sharedTestBatch(1024)
	c, _ := memory.EstimateNativeSharedBatchRange(src, 0, src.Rows)
	manager := memory.NewBudgetManager(c.OwnerBytes + 2*c.BindingBytes - 1)
	m := sharedTestMux(t, manager, context.Background(), 1<<18)
	ok, _, err := m.trySharedRange(src, MorselRange{Rows: src.Rows, Bytes: c.CloneBytes}, Sequence{})
	if ok || err != nil {
		t.Fatalf("want clean guard fallback: %v %v", ok, err)
	}
	if manager.Used() != 0 {
		t.Fatal("rollback reservation leak")
	}
	sharedAssertEmpty(t, m)
	// The same manager can admit each original guard item after rollback.
	for _, b := range m.branches {
		if err := b.exchange.enqueueRange(b.ctx, Sequence{}, src, MorselRange{Rows: src.Rows, Bytes: 1}); err != nil {
			t.Fatal(err)
		}
		b.exchange.release(<-b.exchange.items)
	}
	sharedAssertEmpty(t, m)
}
func TestSharedNativeCloseDoesNotWaitOnOtherBranchCredit(t *testing.T) {
	src := sharedTestBatch(1024)
	c, _ := memory.EstimateNativeSharedBatchRange(src, 0, src.Rows)
	manager := memory.NewBudgetManager(1 << 20)
	m := sharedTestMux(t, manager, context.Background(), c.CloneBytes)
	blocked := m.branches[1].exchange
	if err := blocked.enqueueRange(m.ctx, Sequence{}, src, MorselRange{Rows: src.Rows, Bytes: c.CloneBytes}); err != nil {
		t.Fatal(err)
	}
	old := <-blocked.items
	done := make(chan error, 1)
	go func() {
		_, _, err := m.trySharedRange(src, MorselRange{Rows: src.Rows, Bytes: c.CloneBytes}, Sequence{Index: 1})
		done <- err
	}()
	deadline := time.After(time.Second)
	for m.branches[0].exchange.Stats().CurrentInFlightBytes == 0 {
		select {
		case <-deadline:
			t.Fatal("preparation did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	closed := make(chan struct{})
	go func() { _ = m.branches[0].Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close waited for another branch credit")
	}
	blocked.release(old)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("preparation stuck")
	}
	blocked.release(<-blocked.items)
	if manager.Used() != 0 {
		t.Fatal("close rollback leak")
	}
	sharedAssertEmpty(t, m)
}
