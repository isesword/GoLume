package memory

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func reusableQuery(t *testing.T, e *EngineBudget, hard int64, name string, class MemoryClass) (*QueryBudget, *OperatorBudget) {
	t.Helper()
	q, err := e.BeginQuery(BudgetOptions{HardLimitBytes: hard, SoftLimitBytes: hard})
	if err != nil {
		t.Fatal(err)
	}
	return q, q.Operator(name, class)
}

func reusableAccount(t *testing.T, e *EngineBudget, op *OperatorBudget, class MemoryClass) *ReusableAccount {
	t.Helper()
	a, err := e.NewReusableAccount()
	if err != nil {
		t.Fatal(err)
	}
	if op != nil {
		if err := a.Attach(context.Background(), op, class); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

func checkReusableUsage(t *testing.T, e *EngineBudget, a *ReusableAccount, q *QueryBudget, op *OperatorBudget, class MemoryClass, root, account, query, operator int64) {
	t.Helper()
	snapshot := q.Snapshot()
	if e.Used() != root || a.Used() != account || q.Used() != query || op.Used() != operator || snapshot.ByClass[class] != query {
		t.Fatalf("usage root=%d account=%d query=%d operator=%d class=%d, want %d/%d/%d/%d/%d", e.Used(), a.Used(), q.Used(), op.Used(), snapshot.ByClass[class], root, account, query, operator, query)
	}
}

func TestReusableAccountRetainsRootAcrossQueryLifetimes(t *testing.T) {
	e := NewEngineBudget(100)
	q1, op1 := reusableQuery(t, e, 100, "first", MemoryRetainedState)
	q2, op2 := reusableQuery(t, e, 100, "second", MemoryWorkspace)
	a := reusableAccount(t, e, op1, MemoryRetainedState)
	lease, err := a.Reserve(WithQueryBudget(context.Background(), q1), 100)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Bytes() != 100 || a.Limit() != 100 {
		t.Fatal("wrong token/limit")
	}
	checkReusableUsage(t, e, a, q1, op1, MemoryRetainedState, 100, 100, 100, 100)
	if err := a.Attach(context.Background(), op1, MemoryRetainedState); err != nil {
		t.Fatal(err)
	}
	if q1.Snapshot().PeakBytes != 100 {
		t.Fatal("same-owner attach doubled peak")
	}
	if err := q1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Attach(context.Background(), op1, MemoryRetainedState); !errors.Is(err, ErrQueryBudgetClosed) {
		t.Fatalf("closed no-op attach=%v", err)
	}
	if err := a.Detach(); err != nil {
		t.Fatal(err)
	}
	if err := a.Detach(); err != nil {
		t.Fatal(err)
	}
	if a.owner != nil {
		t.Fatal("idle account retained query owner")
	}
	checkReusableUsage(t, e, a, q1, op1, MemoryRetainedState, 100, 100, 0, 0)
	if _, err := a.Reserve(context.Background(), 1); !errors.Is(err, ErrReusableAccountDetached) {
		t.Fatalf("detached reserve=%v", err)
	}
	if _, err := e.Reserve(context.Background(), 1); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("uncharged idle storage: %v", err)
	}
	if err := a.Attach(WithQueryBudget(context.Background(), q2), op2, MemoryWorkspace); err != nil {
		t.Fatal(err)
	}
	checkReusableUsage(t, e, a, q2, op2, MemoryWorkspace, 100, 100, 100, 100)
	lease.Release() // The original table-held token releases its current owner.
	lease.Release()
	checkReusableUsage(t, e, a, q2, op2, MemoryWorkspace, 0, 0, 0, 0)
	if q1.Snapshot().PeakBytes != 100 || q2.Snapshot().PeakBytes != 100 {
		t.Fatal("historical peak changed")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if a.owner != nil {
		t.Fatal("closed account retained query")
	}
}

func TestReusableAccountAttachFailuresPreserveRootAndSource(t *testing.T) {
	e := NewEngineBudget(100)
	q1, op1 := reusableQuery(t, e, 100, "first", MemoryRetainedState)
	q2, op2 := reusableQuery(t, e, 59, "second", MemoryWorkspace)
	foreign := NewEngineBudget(100)
	_, foreignOp := reusableQuery(t, foreign, 100, "foreign", MemoryWorkspace)
	a := reusableAccount(t, e, op1, MemoryRetainedState)
	r, err := a.Reserve(context.Background(), 60)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	for _, target := range []*OperatorBudget{op2, q1.Operator("other", MemoryWorkspace)} {
		if err := a.Attach(context.Background(), target, MemoryWorkspace); !errors.Is(err, ErrReusableAccountAttached) {
			t.Fatalf("active attach=%v", err)
		}
		checkReusableUsage(t, e, a, q1, op1, MemoryRetainedState, 60, 60, 60, 60)
	}
	if err := a.Detach(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	closedQ, closedOp := reusableQuery(t, e, 100, "closed", MemoryWorkspace)
	_ = closedQ.Close()
	cases := []struct {
		name  string
		ctx   context.Context
		op    *OperatorBudget
		class MemoryClass
		want  error
	}{
		{"hard", context.Background(), op2, MemoryWorkspace, ErrBudgetExceeded},
		{"foreign", context.Background(), foreignOp, MemoryWorkspace, nil},
		{"class", context.Background(), op1, memoryClassCount, nil},
		{"cancel", ctx, op1, MemoryRetainedState, context.Canceled},
		{"closed", context.Background(), closedOp, MemoryWorkspace, ErrQueryBudgetClosed},
		{"context owner", WithQueryBudget(context.Background(), q2), op1, MemoryRetainedState, nil},
		{"nil context", nil, op1, MemoryRetainedState, nil},
		{"nil destination", context.Background(), nil, MemoryRetainedState, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := a.Attach(tc.ctx, tc.op, tc.class)
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err=%v want %v", err, tc.want)
			}
			if a.owner != nil || e.Used() != 60 || a.Used() != 60 || q1.Used() != 0 || q2.Used() != 0 || foreign.Used() != 0 {
				t.Fatal("failed attach mutated ownership")
			}
		})
	}
	q3, op3 := reusableQuery(t, e, 60, "exact", MemoryWorkspace)
	if err := a.Attach(context.Background(), op3, MemoryWorkspace); err != nil {
		t.Fatal(err)
	}
	checkReusableUsage(t, e, a, q3, op3, MemoryWorkspace, 60, 60, 60, 60)
}

func TestReusableAccountCloseKeepsLiveCharges(t *testing.T) {
	for _, detach := range []bool{false, true} {
		t.Run(map[bool]string{false: "attached", true: "detached"}[detach], func(t *testing.T) {
			e := NewEngineBudget(100)
			q, op := reusableQuery(t, e, 100, "state", MemoryWorkspace)
			a := reusableAccount(t, e, op, MemoryWorkspace)
			r, err := a.Reserve(context.Background(), 70)
			if err != nil {
				t.Fatal(err)
			}
			_ = a.Close()
			_ = a.Close()
			if e.Used() != 70 || q.Used() != 70 {
				t.Fatal("Close uncharged live backing")
			}
			if _, err := a.Reserve(context.Background(), 1); !errors.Is(err, ErrReusableAccountClosed) {
				t.Fatal(err)
			}
			if err := a.Attach(context.Background(), op, MemoryWorkspace); !errors.Is(err, ErrReusableAccountClosed) {
				t.Fatal(err)
			}
			if detach {
				if err := a.Detach(); err != nil {
					t.Fatal(err)
				}
				if e.Used() != 70 || q.Used() != 0 || a.owner != nil {
					t.Fatal("closed detach changed root")
				}
			}
			r.Release()
			r.Release()
			checkReusableUsage(t, e, a, q, op, MemoryWorkspace, 0, 0, 0, 0)
			if a.owner != nil {
				t.Fatal("final release retained owner")
			}
		})
	}
}

func TestReusableAccountGrowthDoesNotInvokeSpill(t *testing.T) {
	e := NewEngineBudget(100)
	q, err := e.BeginQuery(BudgetOptions{SoftLimitBytes: 20, HardLimitBytes: 60})
	if err != nil {
		t.Fatal(err)
	}
	op := q.Operator("state", MemoryRetainedState)
	a := reusableAccount(t, e, op, MemoryRetainedState)
	spiller := &releasingSpiller{bytes: 100}
	unregister, err := op.RegisterSpiller(spiller)
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	r, err := a.Reserve(context.Background(), 50)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if _, err := a.Reserve(context.Background(), 11); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if spiller.calls.Load() != 0 || a.Used() != 50 || q.Used() != 50 || e.Used() != 50 {
		t.Fatal("growth invoked spill or changed failed reservation")
	}
}

func TestReusableAccountCanceledAndMismatchedReserve(t *testing.T) {
	e := NewEngineBudget(100)
	q, op := reusableQuery(t, e, 100, "state", MemoryWorkspace)
	q2, _ := reusableQuery(t, e, 100, "other", MemoryWorkspace)
	a := reusableAccount(t, e, op, MemoryWorkspace)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{nil, ctx, WithQueryBudget(context.Background(), q2)} {
		if _, err := a.Reserve(ctx, 1); err == nil {
			t.Fatal("invalid reserve accepted")
		}
	}
	if e.Used() != 0 || a.Used() != 0 || q.Used() != 0 {
		t.Fatal("failed reserve charged")
	}
	for _, n := range []int64{0, -1} {
		r, err := a.Reserve(context.Background(), n)
		if err != nil {
			t.Fatal(err)
		}
		r.Release()
		if r.Bytes() != 0 {
			t.Fatal("zero token bytes")
		}
	}
}

func TestReusableAccountConcurrentReleaseAttachDetachAndClose(t *testing.T) {
	for iteration := 0; iteration < 200; iteration++ {
		e := NewEngineBudget(128)
		q1, op1 := reusableQuery(t, e, 128, "first", MemoryRetainedState)
		q2, op2 := reusableQuery(t, e, 128, "second", MemoryWorkspace)
		a := reusableAccount(t, e, op1, MemoryRetainedState)
		r, err := a.Reserve(context.Background(), 64)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(4)
		go func() { defer wg.Done(); <-start; r.Release(); r.Release() }()
		go func() {
			defer wg.Done()
			<-start
			_ = a.Detach()
			_ = a.Attach(context.Background(), op2, MemoryWorkspace)
		}()
		go func() { defer wg.Done(); <-start; _ = a.Close() }()
		go func() { defer wg.Done(); <-start; _ = q2.Close() }()
		close(start)
		wg.Wait()
		if e.Used() != 0 || a.Used() != 0 || q1.Used() != 0 || q2.Used() != 0 || op1.Used() != 0 || op2.Used() != 0 || a.owner != nil {
			t.Fatalf("iteration %d leaked root/account/query/operator ownership", iteration)
		}
	}
}

func TestReusableAccountConcurrentReserveAndQueryClose(t *testing.T) {
	for iteration := 0; iteration < 200; iteration++ {
		e := NewEngineBudget(128)
		q, op := reusableQuery(t, e, 128, "state", MemoryWorkspace)
		a := reusableAccount(t, e, op, MemoryWorkspace)
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 8; i++ {
				r, err := a.Reserve(context.Background(), 8)
				if err == nil {
					r.Release()
				} else if !errors.Is(err, ErrQueryBudgetClosed) && !errors.Is(err, ErrReusableAccountClosed) {
					t.Errorf("reserve=%v", err)
				}
			}
		}()
		go func() { defer wg.Done(); <-start; _ = q.Close() }()
		go func() { defer wg.Done(); <-start; _ = a.Close() }()
		close(start)
		wg.Wait()
		if e.Used() != 0 || a.Used() != 0 || q.Used() != 0 || op.Used() != 0 {
			t.Fatalf("iteration %d leaked", iteration)
		}
	}
}

func TestReusableAccountCrossQueryAttachHasNoLockCycle(t *testing.T) {
	e := NewEngineBudget(128)
	q1, op1 := reusableQuery(t, e, 128, "first", MemoryWorkspace)
	q2, op2 := reusableQuery(t, e, 128, "second", MemoryWorkspace)
	a1 := reusableAccount(t, e, op1, MemoryWorkspace)
	a2 := reusableAccount(t, e, op2, MemoryWorkspace)
	r1, err := a1.Reserve(context.Background(), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Release()
	r2, err := a2.Reserve(context.Background(), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Release()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = a1.Detach()
			if err := a1.Attach(context.Background(), op2, MemoryWorkspace); err != nil {
				t.Error(err)
			}
			_ = a1.Detach()
			if err := a1.Attach(context.Background(), op1, MemoryWorkspace); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = a2.Detach()
			if err := a2.Attach(context.Background(), op1, MemoryWorkspace); err != nil {
				t.Error(err)
			}
			_ = a2.Detach()
			if err := a2.Attach(context.Background(), op2, MemoryWorkspace); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cross-query attach deadlocked")
	}
	if e.Used() != 64 || q1.Used() != 32 || q2.Used() != 32 {
		t.Fatal("crossing ownership changed totals")
	}
}

func TestMemoryBudgetAccountingRejectsOverflow(t *testing.T) {
	e := NewEngineBudget(0)
	q, op := reusableQuery(t, e, math.MaxInt64, "state", MemoryWorkspace)
	a := reusableAccount(t, e, op, MemoryWorkspace)
	r, err := a.Reserve(context.Background(), math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Reserve(context.Background(), 1); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("root overflow=%v", err)
	}
	if _, err := a.Reserve(context.Background(), 1); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("query overflow=%v", err)
	}
	_ = a.Detach()
	if err := a.Attach(context.Background(), op, MemoryWorkspace); err != nil {
		t.Fatal(err)
	}
	if e.Used() != math.MaxInt64 || q.Used() != math.MaxInt64 {
		t.Fatal("overflow changed counters")
	}
	r.Release()
	if e.Used() != 0 || q.Used() != 0 {
		t.Fatal("large token release failed")
	}
}

func TestReusableAccountCapacityIsStableAcrossRepeatedQueries(t *testing.T) {
	e := NewEngineBudget(64)
	a := reusableAccount(t, e, nil, MemoryWorkspace)
	var r Reservation
	for iteration := 0; iteration < 100; iteration++ {
		q, op := reusableQuery(t, e, 64, "state", MemoryWorkspace)
		if err := a.Attach(context.Background(), op, MemoryWorkspace); err != nil {
			t.Fatal(err)
		}
		if r == nil {
			var err error
			r, err = a.Reserve(context.Background(), 64)
			if err != nil {
				t.Fatal(err)
			}
		}
		if q.Used() != 64 || op.Used() != 64 || q.Snapshot().PeakBytes != 64 {
			t.Fatal("reattached token not charged to current owner")
		}
		_ = q.Close()
		_ = a.Detach()
		if e.Used() != 64 || q.Used() != 0 || op.Used() != 0 || a.owner != nil {
			t.Fatal("idle capacity or old owner changed")
		}
	}
	r.Release()
	_ = a.Close()
	if e.Used() != 0 {
		t.Fatal("final release leaked")
	}
}

func TestReusableAccountNoNewTokensDuringAttach(t *testing.T) {
	e := NewEngineBudget(64)
	q, op := reusableQuery(t, e, 64, "state", MemoryWorkspace)
	a := reusableAccount(t, e, op, MemoryWorkspace)
	r, err := a.Reserve(context.Background(), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	ctx := context.Background()
	allocs := testing.AllocsPerRun(100, func() {
		_ = a.Detach()
		if err := a.Attach(ctx, op, MemoryWorkspace); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("Attach/Detach allocated %g times", allocs)
	}
	if q.Used() != 64 || e.Used() != 64 {
		t.Fatal("allocation test changed accounting")
	}
}

func TestReusableAccountPeakTracksActualConcurrentOwnership(t *testing.T) {
	e := NewEngineBudget(100)
	q, op := reusableQuery(t, e, 100, "state", MemoryWorkspace)
	a := reusableAccount(t, e, op, MemoryWorkspace)
	var maxRoot atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r, err := a.Reserve(context.Background(), 5)
				if err != nil {
					t.Error(err)
					return
				}
				used := e.Used()
				for old := maxRoot.Load(); used > old && !maxRoot.CompareAndSwap(old, used); old = maxRoot.Load() {
				}
				r.Release()
			}
		}()
	}
	wg.Wait()
	if e.Used() != 0 || q.Used() != 0 || a.Used() != 0 || q.Snapshot().PeakBytes > 40 || maxRoot.Load() > 40 {
		t.Fatal("concurrent counters escaped live token bound")
	}
}

func TestReusableAccountReclassifiesSameQueryWithoutExtraHeadroom(t *testing.T) {
	e := NewEngineBudget(100)
	q, old := reusableQuery(t, e, 100, "old", MemoryRetainedState)
	next := q.Operator("next", MemoryWorkspace)
	a := reusableAccount(t, e, old, MemoryRetainedState)
	r, err := a.Reserve(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	var wrongRoot atomic.Bool
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			default:
				if e.Used() != 100 {
					wrongRoot.Store(true)
				}
			}
		}
	}()
	for i := 0; i < 100; i++ {
		if err := a.Detach(); err != nil {
			t.Fatal(err)
		}
		if err := a.Attach(context.Background(), next, MemoryWorkspace); err != nil {
			t.Fatal(err)
		}
		if err := a.Attach(context.Background(), next, MemoryWorkspace); err != nil {
			t.Fatal(err)
		}
	}
	close(done)
	<-stopped
	snapshot := q.Snapshot()
	if wrongRoot.Load() || q.Used() != 100 || old.Used() != 0 || next.Used() != 100 || snapshot.ByClass[MemoryRetainedState] != 0 || snapshot.ByClass[MemoryWorkspace] != 100 || snapshot.PeakBytes != 100 {
		t.Fatalf("same-query accounting=%+v", snapshot)
	}
}

func TestReusableAccountAttachCountsExistingDestinationUsage(t *testing.T) {
	e := NewEngineBudget(200)
	q1, op1 := reusableQuery(t, e, 100, "old", MemoryRetainedState)
	q2, op2 := reusableQuery(t, e, 100, "new", MemoryWorkspace)
	a := reusableAccount(t, e, op1, MemoryRetainedState)
	r, err := a.Reserve(context.Background(), 60)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	_ = a.Detach()
	existing, err := q2.Reserve(context.Background(), 41)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Attach(context.Background(), op2, MemoryWorkspace); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("remaining-cap attach=%v", err)
	}
	if a.owner != nil || q1.Used() != 0 || q2.Used() != 41 || e.Used() != 101 {
		t.Fatal("rejected aggregate transfer changed charge")
	}
	existing.Release()
	existing, err = q2.Reserve(context.Background(), 40)
	if err != nil {
		t.Fatal(err)
	}
	defer existing.Release()
	if err := a.Attach(context.Background(), op2, MemoryWorkspace); err != nil {
		t.Fatal(err)
	}
	if q2.Used() != 100 || op2.Used() != 60 || e.Used() != 100 || q2.Snapshot().PeakBytes != 100 {
		t.Fatal("exact residual capacity not respected")
	}
}
