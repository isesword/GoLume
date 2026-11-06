package expr

import (
	"context"
	"errors"
	"gocolumn/memory"
	"testing"
)

type predicateRecordingManager struct {
	base           *memory.BudgetManager
	deny           bool
	requests, live []int64
}

func (m *predicateRecordingManager) Used() int64  { return m.base.Used() }
func (m *predicateRecordingManager) Limit() int64 { return m.base.Limit() }
func (m *predicateRecordingManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.requests = append(m.requests, n)
	m.live = append(m.live, m.Used())
	if m.deny {
		return nil, memory.ErrBudgetExceeded
	}
	return m.base.Reserve(ctx, n)
}
func TestPredicateWorkspaceReserveBeforeAllocationAndGrowthRollback(t *testing.T) {
	ctx := context.Background()
	p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), GT(Col("x"), I64(0)))
	if err != nil {
		t.Fatal(err)
	}
	manager := &predicateRecordingManager{base: memory.NewBudgetManager(0), deny: true}
	w, err := NewPredicateWorkspace(ctx, p, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1}}
	if _, err = w.Evaluate(ctx, batch, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if w.MemoryBytes() != 0 || manager.Used() != 0 {
		t.Fatal("denied admission allocated storage")
	}
	manager.deny = false
	if _, err = w.Evaluate(ctx, batch, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	old := manager.Used()
	pointer := &w.workspace.regs[0].Truth[0]
	if old != w.MemoryBytes() {
		t.Fatalf("charge=%d capacity=%d", old, w.MemoryBytes())
	}
	batch.Rows = 129
	batch.Int64s["x"].Values = make([]int64, 129)
	manager.deny = true
	if _, err = w.Evaluate(ctx, batch, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if manager.Used() != old || &w.workspace.regs[0].Truth[0] != pointer {
		t.Fatal("failed growth changed old storage")
	}
	expected, _ := p.WorkspaceBytes(129)
	if manager.requests[len(manager.requests)-1] != expected || manager.live[len(manager.live)-1] != old {
		t.Fatal("growth failed to reserve full replacement with old live")
	}
	manager.deny = false
	if _, err = w.Evaluate(ctx, batch, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != expected || manager.Used() != w.MemoryBytes() {
		t.Fatal("growth capacity accounting mismatch")
	}
	w.Release()
	w.Release()
	if manager.Used() != 0 {
		t.Fatal("release leaked")
	}
	if _, err = w.Evaluate(ctx, batch, testKernelSet()); err == nil {
		t.Fatal("released workspace reused")
	}
}
func TestPredicateWorkspaceDictionaryGrowthAndQueryOwner(t *testing.T) {
	p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), Contains(Col("s"), Str("a")))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m := &predicateRecordingManager{base: memory.NewBudgetManager(0)}
	w, err := NewPredicateWorkspace(ctx, p, m)
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte("a"), DictionaryDomain: "one"}
	if _, err = w.Evaluate(ctx, batch, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	old := m.Used()
	if old != w.MemoryBytes() {
		t.Fatalf("charge=%d actual=%d", old, w.MemoryBytes())
	}
	batch.DictStrings["s"].Offsets = []uint32{0, 1, 2, 3}
	batch.DictStrings["s"].Data = []byte("abc")
	batch.DictStrings["s"].DictionaryDomain = "three"
	m.deny = true
	if _, err = w.Evaluate(ctx, batch, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if m.Used() != old || m.live[len(m.live)-1] != old || m.requests[len(m.requests)-1] != 3 {
		t.Fatal("dictionary rollback or overlap failed")
	}
	m.deny = false
	if _, err = w.Evaluate(ctx, batch, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	if m.Used() != old+2 || m.Used() != w.MemoryBytes() {
		t.Fatal("dynamic buffers not accounted")
	}
	w.Release()
	if m.Used() != 0 {
		t.Fatal("dictionary token leaked")
	}
	q, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer q.Close()
	queryCtx := memory.WithQueryBudget(ctx, q)
	fallback := memory.NewBudgetManager(1)
	w, err = NewPredicateWorkspace(queryCtx, p, fallback, "CustomPredicate")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	if _, err = w.Evaluate(queryCtx, batch, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	if fallback.Used() != 0 || q.Used() != w.MemoryBytes() || q.Snapshot().Operators["CustomPredicate"] != q.Used() {
		t.Fatal("external budget/classification not honored")
	}
	if _, err = w.Evaluate(ctx, batch, testKernelSet()); err != nil {
		t.Fatalf("plain context lost bound query: %v", err)
	}
	other, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer other.Close()
	if _, err = w.Evaluate(memory.WithQueryBudget(ctx, other), batch, testKernelSet()); err == nil {
		t.Fatal("explicit query ownership drift allowed")
	}
	cancelled, cancel := context.WithCancel(queryCtx)
	cancel()
	if _, err = w.Evaluate(cancelled, batch, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	w.Release()
	if q.Used() != 0 {
		t.Fatal("query leaked")
	}
}

func TestPredicateWorkspacePlainContextKeepsOpeningOwner(t *testing.T) {
	plain := context.Background()
	q, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer q.Close()
	owned := memory.WithQueryBudget(plain, q)
	p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), GT(Col("x"), I64(0)))
	if err != nil {
		t.Fatal(err)
	}
	fallback := memory.NewBudgetManager(1)
	w, err := NewPredicateWorkspace(owned, p, fallback)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1}}
	for _, ctx := range []context.Context{plain, owned, plain} {
		if _, err := w.Evaluate(ctx, batch, testKernelSet()); err != nil {
			t.Fatal(err)
		}
		if memory.QueryBudgetFromContext(w.ctx) != q || q.Used() != w.MemoryBytes() || fallback.Used() != 0 {
			t.Fatal("opening owner not retained")
		}
	}
	w.Release()
	if q.Used() != 0 {
		t.Fatal("bound owner leaked")
	}
	unowned, err := NewPredicateWorkspace(plain, p, memory.NewBudgetManager(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer unowned.Release()
	if _, err := unowned.Evaluate(plain, batch, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	before := unowned.MemoryBytes()
	if _, err := unowned.Evaluate(owned, batch, testKernelSet()); err == nil {
		t.Fatal("unowned workspace adopted query")
	}
	if unowned.MemoryBytes() != before || q.Used() != 0 {
		t.Fatal("rejected owner changed allocation")
	}
	if _, err := unowned.Evaluate(plain, batch, testKernelSet()); err != nil {
		t.Fatal(err)
	}
}
