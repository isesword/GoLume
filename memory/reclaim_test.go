package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRootPressureReclaimsIdleAccountForEveryReservePath(t *testing.T) {
	for _, path := range []string{"direct", "query", "growth", "account"} {
		t.Run(path, func(t *testing.T) {
			e := NewEngineBudget(100)
			old, oldOp := reusableQuery(t, e, 100, "old", MemoryRetainedState)
			idle := reusableAccount(t, e, oldOp, MemoryRetainedState)
			held, err := idle.Reserve(context.Background(), 80)
			if err != nil {
				t.Fatal(err)
			}
			backing := make([]byte, 80)
			_ = old.Close()
			_ = idle.Detach()
			q, op := reusableQuery(t, e, 100, "new", MemoryWorkspace)
			active := reusableAccount(t, e, op, MemoryWorkspace)
			calls := 0
			unregister, err := e.RegisterReclaimer(func(ctx context.Context, shortfall int64) {
				calls++
				if shortfall != 10 {
					t.Errorf("shortfall=%d want10", shortfall)
				}
				if !q.reserveMu.TryLock() {
					t.Error("callback ran under query admission lock")
				} else {
					q.reserveMu.Unlock()
				}
				if !active.mu.TryLock() {
					t.Error("callback ran under account lock")
				} else {
					active.mu.Unlock()
				}
				if e.Used() != 80 || len(backing) != 80 {
					t.Error("idle backing became uncharged before eviction")
				}
				backing = nil // Drop every retained reference before releasing its charge.
				held.Release()
			})
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			ctx := WithQueryBudget(context.Background(), q)
			var r Reservation
			switch path {
			case "direct":
				r, err = e.Reserve(context.Background(), 30)
			case "query":
				r, err = q.Reserve(ctx, 30)
			case "growth":
				r, err = op.ReserveGrowth(ctx, 30)
			case "account":
				r, err = active.Reserve(ctx, 30)
			}
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 || backing != nil || e.Used() != 30 || idle.Used() != 0 || old.Used() != 0 {
				t.Fatal("pressure reclaim failed exact ownership")
			}
			r.Release()
			if e.Used() != 0 || q.Used() != 0 {
				t.Fatal("reclaimed allocation leaked")
			}
		})
	}
}

func TestRootReclaimerCanReenterAndUnregisterWithoutLocks(t *testing.T) {
	e := NewEngineBudget(100)
	q, op := reusableQuery(t, e, 100, "state", MemoryWorkspace)
	held, err := e.Reserve(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	var unregister func()
	unregister, err = e.RegisterReclaimer(func(ctx context.Context, shortfall int64) {
		if calls.Add(1) != 1 {
			t.Fatal("recursive reclaim")
		}
		if !q.reserveMu.TryLock() {
			t.Error("query mutex held")
		} else {
			q.reserveMu.Unlock()
		}
		// Both recursion barriers are inherited by this nested query reserve.
		if _, err := op.Reserve(ctx, 1); !errors.Is(err, ErrBudgetExceeded) {
			t.Errorf("nested reserve=%v", err)
		}
		unregister()
		unregister()
		held.Release()
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var r Reservation
	go func() { r, err = op.Reserve(context.Background(), 10); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reclaim callback deadlocked")
	}
	if err != nil {
		t.Fatal(err)
	}
	r.Release()
	if calls.Load() != 1 || e.Used() != 0 || q.Used() != 0 {
		t.Fatal("reentrant callback accounting")
	}
}

func TestRootReclaimPassAndRetryAreBounded(t *testing.T) {
	for _, path := range []string{"direct", "query", "growth", "account"} {
		t.Run(path, func(t *testing.T) {
			e := NewEngineBudget(1)
			q, op := reusableQuery(t, e, 1, "state", MemoryWorkspace)
			a := reusableAccount(t, e, op, MemoryWorkspace)
			held, err := e.Reserve(context.Background(), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Release()
			calls := 0
			unregister, err := e.RegisterReclaimer(func(context.Context, int64) { calls++ })
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			switch path {
			case "direct":
				_, err = e.Reserve(context.Background(), 1)
			case "query":
				_, err = q.Reserve(context.Background(), 1)
			case "growth":
				_, err = op.ReserveGrowth(context.Background(), 1)
			case "account":
				_, err = a.Reserve(context.Background(), 1)
			}
			if !errors.Is(err, ErrBudgetExceeded) || calls != 1 || e.Used() != 1 || q.Used() != 0 {
				t.Fatalf("err=%v calls=%d root=%d query=%d", err, calls, e.Used(), q.Used())
			}
		})
	}
}

func TestReusableReserveRejectsOwnershipChangeDuringReclaim(t *testing.T) {
	for _, change := range []string{"detach", "reattach", "close", "query close", "cancel"} {
		t.Run(change, func(t *testing.T) {
			e := NewEngineBudget(100)
			q, op := reusableQuery(t, e, 100, "old", MemoryWorkspace)
			q2, op2 := reusableQuery(t, e, 100, "new", MemoryWorkspace)
			a := reusableAccount(t, e, op, MemoryWorkspace)
			held, err := e.Reserve(context.Background(), 100)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			unregister, err := e.RegisterReclaimer(func(context.Context, int64) {
				switch change {
				case "detach":
					_ = a.Detach()
				case "reattach":
					_ = a.Detach()
					if err := a.Attach(context.Background(), op2, MemoryWorkspace); err != nil {
						t.Error(err)
					}
				case "close":
					_ = a.Close()
				case "query close":
					_ = q.Close()
				case "cancel":
					cancel()
				}
				held.Release()
			})
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			_, err = a.Reserve(ctx, 10)
			want := ErrReusableAccountChanged
			switch change {
			case "close":
				want = ErrReusableAccountClosed
			case "query close":
				want = ErrQueryBudgetClosed
			case "cancel":
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("err=%v want%v", err, want)
			}
			if e.Used() != 0 || q.Used() != 0 || q2.Used() != 0 || a.Used() != 0 {
				t.Fatal("retry charged changed ownership")
			}
		})
	}
}

func TestRootReclaimCancellationDoesNotAdmitNewCharge(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before", false: "during"}[before], func(t *testing.T) {
			e := NewEngineBudget(100)
			q, _ := reusableQuery(t, e, 100, "state", MemoryWorkspace)
			held, err := e.Reserve(context.Background(), 100)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			unregister, err := e.RegisterReclaimer(func(context.Context, int64) { calls++; cancel(); held.Release() })
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			if before {
				cancel()
			}
			if _, err := q.Reserve(ctx, 10); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if q.Used() != 0 {
				t.Fatal("canceled retry admitted bytes")
			}
			if before && calls != 0 || !before && calls != 1 {
				t.Fatalf("calls=%d", calls)
			}
		})
	}
}

func TestRootReclaimerUnregisterDoesNotWaitForRunningCallback(t *testing.T) {
	e := NewEngineBudget(1)
	held, err := e.Reserve(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	started, finish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unregister, err := e.RegisterReclaimer(func(context.Context, int64) { close(started); <-finish })
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = e.Reserve(context.Background(), 1); close(done) }()
	<-started
	unregistered := make(chan struct{})
	go func() { unregister(); close(unregistered) }()
	select {
	case <-unregistered:
	case <-time.After(5 * time.Second):
		t.Fatal("unregister waited for callback")
	}
	close(finish)
	<-done
	if _, err := e.Reserve(context.Background(), 1); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal(err)
	}
}

func TestRootReclaimConcurrentCallbacksReleaseIdleExactlyOnce(t *testing.T) {
	e := NewEngineBudget(128)
	old, oldOp := reusableQuery(t, e, 128, "old", MemoryWorkspace)
	idle := reusableAccount(t, e, oldOp, MemoryWorkspace)
	held, err := idle.Reserve(context.Background(), 128)
	if err != nil {
		t.Fatal(err)
	}
	_ = idle.Detach()
	var cacheMu sync.Mutex
	backing := make([]byte, 128)
	var evictions atomic.Int64
	unregister, err := e.RegisterReclaimer(func(context.Context, int64) {
		cacheMu.Lock()
		defer cacheMu.Unlock()
		if backing != nil {
			backing = nil
			held.Release()
			evictions.Add(1)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q, err := e.BeginQuery(BudgetOptions{HardLimitBytes: 128})
			if err != nil {
				t.Error(err)
				return
			}
			r, err := q.Reserve(context.Background(), 1)
			if err != nil {
				t.Error(err)
				return
			}
			r.Release()
			_ = q.Close()
		}()
	}
	wg.Wait()
	if evictions.Load() != 1 || e.Used() != 0 || idle.Used() != 0 || old.Used() != 0 {
		t.Fatal("concurrent eviction leaked or double released")
	}
}

func TestRootReclaimConcurrentRegistrationAndReserve(t *testing.T) {
	e := NewEngineBudget(1)
	held, err := e.Reserve(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				unregister, err := e.RegisterReclaimer(func(context.Context, int64) {})
				if err != nil {
					t.Error(err)
					return
				}
				_, _ = e.Reserve(context.Background(), 1)
				unregister()
				unregister()
			}
		}()
	}
	wg.Wait()
	e.reclaimMu.Lock()
	defer e.reclaimMu.Unlock()
	if len(e.reclaimers) != 0 {
		t.Fatal("registry retained unregistered callbacks")
	}
}

type reclaimScratchSpiller struct {
	op         *OperatorBudget
	account    *ReusableAccount
	reserveErr error
}

// SpillableBytes reports retained state available to the test arbiter.
func (s *reclaimScratchSpiller) SpillableBytes() int64 { return 60 }

// Spill requests root-pressured scratch while the arbiter mutex is held.
func (s *reclaimScratchSpiller) Spill(ctx context.Context, _ SpillRequest) (int64, error) {
	var r Reservation
	if s.account != nil {
		r, s.reserveErr = s.account.Reserve(ctx, 20)
	} else {
		r, s.reserveErr = s.op.ReserveGrowth(ctx, 20)
	}
	if r != nil {
		r.Release()
	}
	return 0, nil
}

func TestRootReclaimNeverRunsUnderActualSpillArbiterLock(t *testing.T) {
	for _, accountPath := range []bool{false, true} {
		t.Run(map[bool]string{false: "operator growth", true: "account growth"}[accountPath], func(t *testing.T) {
			e := NewEngineBudget(100)
			q, op := reusableQuery(t, e, 100, "state", MemoryWorkspace)
			held, err := op.Reserve(context.Background(), 60)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Release()
			idle, err := e.Reserve(context.Background(), 40)
			if err != nil {
				t.Fatal(err)
			}
			defer idle.Release()
			a := reusableAccount(t, e, op, MemoryWorkspace)
			spiller := &reclaimScratchSpiller{op: op}
			if accountPath {
				spiller.account = a
			}
			unregisterSpill, err := op.RegisterSpiller(spiller)
			if err != nil {
				t.Fatal(err)
			}
			defer unregisterSpill()
			calls := 0
			unregister, err := e.RegisterReclaimer(func(ctx context.Context, _ int64) {
				calls++
				if !q.arbiter.mu.TryLock() {
					t.Error("root callback inherited held spill arbiter mutex")
					return
				}
				q.arbiter.mu.Unlock()
				temporary := q.Operator("temporary", MemoryWorkspace)
				remove, registerErr := temporary.RegisterSpiller(&releasingSpiller{})
				if registerErr != nil {
					t.Error(registerErr)
				} else {
					remove()
				}
				if _, spillErr := op.RequestHardSpill(ctx, 1); spillErr != nil {
					t.Error(spillErr)
				}
				idle.Release()
			})
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			if _, err := op.RequestHardSpill(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(spiller.reserveErr, ErrBudgetExceeded) || calls != 0 || e.Used() != 100 {
				t.Fatalf("spill scratch err=%v calls=%d root=%d", spiller.reserveErr, calls, e.Used())
			}
			// The same standalone growth operation is outside the arbiter and must
			// still reclaim idle root state instead of losing the supported path.
			var r Reservation
			if accountPath {
				r, err = a.Reserve(context.Background(), 20)
			} else {
				r, err = op.ReserveGrowth(context.Background(), 20)
			}
			if err != nil {
				t.Fatal(err)
			}
			r.Release()
			if calls != 1 || e.Used() != 60 {
				t.Fatalf("standalone growth calls=%d root=%d", calls, e.Used())
			}
		})
	}
}
