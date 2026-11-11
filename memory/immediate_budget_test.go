package memory

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestImmediateBudgetAccountsEveryClassAndReleasesExactlyOnce(t *testing.T) {
	e := NewEngineBudget(100)
	q, err := e.BeginQuery(BudgetOptions{SoftLimitBytes: 100, HardLimitBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithQueryBudget(context.Background(), q)
	var reservations []Reservation
	for class := MemoryClass(0); class < memoryClassCount; class++ {
		op := q.Operator(class.String(), class)
		r, ok, err := op.TryReserveImmediate(ctx, 20)
		if err != nil || !ok || r == nil || r.Bytes() != 20 {
			t.Fatalf("class=%s reservation=%v ok=%v err=%v", class, r, ok, err)
		}
		reservations = append(reservations, r)
		s := q.Snapshot()
		if s.ByClass[class] != 20 || s.Operators[class.String()] != 20 || op.Used() != 20 {
			t.Fatalf("class=%s snapshot=%+v operator=%d", class, s, op.Used())
		}
	}
	if q.Used() != 100 || e.Used() != 100 || q.Snapshot().PeakBytes != 100 {
		t.Fatalf("query=%+v engine=%d", q.Snapshot(), e.Used())
	}
	var wg sync.WaitGroup
	for _, r := range reservations {
		for range 8 {
			wg.Add(1)
			go func() { defer wg.Done(); r.Release() }()
		}
	}
	wg.Wait()
	s := q.Snapshot()
	if e.Used() != 0 || s.UsedBytes != 0 || s.PeakBytes != 100 {
		t.Fatalf("release query=%+v engine=%d", s, e.Used())
	}
	for class, bytes := range s.ByClass {
		if bytes != 0 || s.Operators[class.String()] != 0 {
			t.Fatalf("release class=%s snapshot=%+v", class, s)
		}
	}
}

func TestImmediateBudgetMissNeverCallsSpillOrReclaimer(t *testing.T) {
	for _, tc := range []struct {
		name       string
		soft, hard int64
		held, want int64
		rootHeld   int64
	}{
		{name: "soft", soft: 70, hard: 100, held: 60, want: 11},
		{name: "hard", soft: 70, hard: 70, held: 60, want: 11},
		{name: "engine", soft: 100, hard: 100, held: 20, rootHeld: 70, want: 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewEngineBudget(100)
			q, err := e.BeginQuery(BudgetOptions{SoftLimitBytes: tc.soft, HardLimitBytes: tc.hard})
			if err != nil {
				t.Fatal(err)
			}
			ctx := WithQueryBudget(context.Background(), q)
			op := q.Operator("PipelineExchange", MemoryInFlightBatch)
			held, err := op.Reserve(ctx, tc.held)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Release()
			rootHeld, err := e.Reserve(context.Background(), tc.rootHeld)
			if err != nil {
				t.Fatal(err)
			}
			defer rootHeld.Release()
			spiller := &releasingSpiller{bytes: tc.held, reservations: []Reservation{held}}
			unregister, err := op.RegisterSpiller(spiller)
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			var reclaims atomic.Int64
			unregisterRoot, err := e.RegisterReclaimer(func(context.Context, int64) {
				reclaims.Add(1)
				rootHeld.Release()
			})
			if err != nil {
				t.Fatal(err)
			}
			defer unregisterRoot()
			before := q.Snapshot()
			rootBefore := e.Used()
			// A growth/spill context must not bypass the immediate soft boundary.
			for _, callCtx := range []context.Context{ctx, withSpillArbitration(ctx), withSpillCallback(ctx)} {
				r, ok, err := op.TryReserveImmediate(callCtx, tc.want)
				if r != nil || ok || err != nil {
					t.Fatalf("reservation=%v ok=%v err=%v", r, ok, err)
				}
			}
			if reclaims.Load() != 0 || spiller.calls.Load() != 0 || e.Used() != rootBefore || !reflect.DeepEqual(q.Snapshot(), before) {
				t.Fatalf("miss changed state: reclaims=%d spills=%d root=%d before=%+v after=%+v", reclaims.Load(), spiller.calls.Load(), e.Used(), before, q.Snapshot())
			}
		})
	}
}

func TestImmediateEngineMissNeverReclaims(t *testing.T) {
	e := NewEngineBudget(10)
	held, err := e.Reserve(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	var calls atomic.Int64
	unregister, err := e.RegisterReclaimer(func(context.Context, int64) { calls.Add(1); held.Release() })
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	r, ok, err := e.TryReserveImmediate(context.Background(), 1)
	if r != nil || ok || err != nil || calls.Load() != 0 || e.Used() != 10 {
		t.Fatalf("reservation=%v ok=%v err=%v callbacks=%d used=%d", r, ok, err, calls.Load(), e.Used())
	}
}

func TestImmediateBudgetReceiverRouting(t *testing.T) {
	e := NewEngineBudget(100)
	q, _ := e.BeginQuery(BudgetOptions{SoftLimitBytes: 100, HardLimitBytes: 100})
	otherEngine := NewEngineBudget(100)
	other, _ := otherEngine.BeginQuery(BudgetOptions{SoftLimitBytes: 100, HardLimitBytes: 100})
	ctx := WithQueryBudget(context.Background(), q)
	r, ok, err := e.TryReserveImmediate(ctx, 20)
	if err != nil || !ok || q.Used() != 20 || e.Used() != 20 || q.Snapshot().ByClass[MemoryWorkspace] != 20 {
		t.Fatalf("same-engine routing query=%+v engine=%d ok=%v err=%v", q.Snapshot(), e.Used(), ok, err)
	}
	r.Release()
	// Engine routing deliberately mirrors Reserve. The exchange chooses the
	// active query before using an injected manager from another engine.
	r, ok, err = e.TryReserveImmediate(WithQueryBudget(context.Background(), other), 20)
	if err != nil || !ok || e.Used() != 20 || q.Used() != 0 || other.Used() != 0 || otherEngine.Used() != 0 {
		t.Fatalf("different-engine routing engine=%d query=%d other=%d ok=%v err=%v", e.Used(), q.Used(), other.Used(), ok, err)
	}
	r.Release()
	op := q.Operator("own-query", MemoryInFlightBatch)
	for _, m := range []ImmediateReservationManager{q, op} {
		r, ok, err := m.TryReserveImmediate(WithQueryBudget(context.Background(), other), 20)
		if err != nil || !ok || q.Used() != 20 || other.Used() != 0 {
			t.Fatalf("receiver routing query=%d other=%d ok=%v err=%v", q.Used(), other.Used(), ok, err)
		}
		r.Release()
	}
	plain := NewBudgetManager(100)
	r, ok, err = plain.TryReserveImmediate(ctx, 20)
	if err != nil || !ok || plain.Used() != 20 || q.Used() != 0 || e.Used() != 0 {
		t.Fatalf("plain routing used=%d query=%d engine=%d ok=%v err=%v", plain.Used(), q.Used(), e.Used(), ok, err)
	}
	r.Release()
}

func TestImmediateBudgetErrorsAndNonPositiveSizes(t *testing.T) {
	e := NewEngineBudget(10)
	q, _ := e.BeginQuery(BudgetOptions{SoftLimitBytes: 10, HardLimitBytes: 10})
	managers := []ImmediateReservationManager{NewBudgetManager(10), e, q, q.Operator("op", MemoryInFlightBatch)}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, expire := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer expire()
	for _, m := range managers {
		for _, size := range []int64{math.MinInt64, -1, 0, 1} {
			if r, ok, err := m.TryReserveImmediate(nil, size); r != nil || ok || err == nil {
				t.Fatalf("%T nil ctx size=%d reservation=%v ok=%v err=%v", m, size, r, ok, err)
			}
			if r, ok, err := m.TryReserveImmediate(cancelled, size); r != nil || ok || !errors.Is(err, context.Canceled) {
				t.Fatalf("%T cancelled size=%d reservation=%v ok=%v err=%v", m, size, r, ok, err)
			}
			if r, ok, err := m.TryReserveImmediate(expired, size); r != nil || ok || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%T expired size=%d reservation=%v ok=%v err=%v", m, size, r, ok, err)
			}
			if size <= 0 {
				r, ok, err := m.TryReserveImmediate(context.Background(), size)
				if r == nil || !ok || err != nil || r.Bytes() != 0 {
					t.Fatalf("%T no-op size=%d reservation=%v ok=%v err=%v", m, size, r, ok, err)
				}
				r.Release()
				r.Release()
			}
		}
	}
	if e.Used() != 0 || q.Used() != 0 || q.Snapshot().PeakBytes != 0 {
		t.Fatalf("no-op/error leaked query=%+v root=%d", q.Snapshot(), e.Used())
	}
	var nilPlain *BudgetManager
	var nilEngine *EngineBudget
	var nilQuery *QueryBudget
	var nilOperator *OperatorBudget
	for _, m := range []ImmediateReservationManager{nilPlain, nilEngine, nilQuery, nilOperator, &EngineBudget{}, &QueryBudget{}, &OperatorBudget{}} {
		if r, ok, err := m.TryReserveImmediate(context.Background(), 1); r != nil || ok || err == nil {
			t.Fatalf("invalid %T reservation=%v ok=%v err=%v", m, r, ok, err)
		}
	}
	if r, ok, err := q.Operator("invalid", memoryClassCount).TryReserveImmediate(context.Background(), 0); r != nil || ok || err == nil {
		t.Fatalf("invalid class reservation=%v ok=%v err=%v", r, ok, err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	for _, m := range []ImmediateReservationManager{q, q.Operator("op", MemoryWorkspace), e} {
		for _, size := range []int64{-1, 0, 1} {
			r, ok, err := m.TryReserveImmediate(WithQueryBudget(context.Background(), q), size)
			if r != nil || ok || !errors.Is(err, ErrQueryBudgetClosed) {
				t.Fatalf("%T closed size=%d reservation=%v ok=%v err=%v", m, size, r, ok, err)
			}
		}
	}
}

func TestImmediateBudgetAdmissionContentionDoesNotWait(t *testing.T) {
	e := NewEngineBudget(100)
	q, _ := e.BeginQuery(BudgetOptions{SoftLimitBytes: 100, HardLimitBytes: 100})
	op := q.Operator("op", MemoryInFlightBatch)
	before := q.Snapshot()
	q.reserveMu.Lock()
	defer q.reserveMu.Unlock()
	// Calling in the lock holder would deadlock if this used Lock or Reserve.
	r, ok, err := op.TryReserveImmediate(context.Background(), 10)
	if r != nil || ok || err != nil || e.Used() != 0 || !reflect.DeepEqual(q.Snapshot(), before) {
		t.Fatalf("contended reservation=%v ok=%v err=%v snapshot=%+v", r, ok, err, q.Snapshot())
	}
}

func TestImmediateBudgetUnlimitedOverflowIsAnUnchangedMiss(t *testing.T) {
	for _, hierarchical := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "query"}[hierarchical], func(t *testing.T) {
			e := NewEngineBudget(0)
			q, _ := e.BeginQuery(BudgetOptions{})
			var m ImmediateReservationManager = e.root
			if hierarchical {
				m = q.Operator("op", MemoryInFlightBatch)
			}
			r, ok, err := m.TryReserveImmediate(context.Background(), math.MaxInt64)
			if !ok || err != nil {
				t.Fatalf("max reservation ok=%v err=%v", ok, err)
			}
			before := q.Snapshot()
			if extra, ok, err := m.TryReserveImmediate(context.Background(), 1); extra != nil || ok || err != nil {
				t.Fatalf("overflow reservation=%v ok=%v err=%v", extra, ok, err)
			}
			if e.Used() != math.MaxInt64 || !reflect.DeepEqual(q.Snapshot(), before) {
				t.Fatalf("overflow changed state engine=%d query=%+v", e.Used(), q.Snapshot())
			}
			r.Release()
			if e.Used() != 0 || q.Used() != 0 {
				t.Fatalf("overflow release engine=%d query=%d", e.Used(), q.Used())
			}
		})
	}
}

func TestImmediateBudgetConcurrentQueryAndEngineLimits(t *testing.T) {
	for _, sharedEngine := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "engine"}[sharedEngine], func(t *testing.T) {
			e := NewEngineBudget(71)
			q, _ := e.BeginQuery(BudgetOptions{SoftLimitBytes: 37, HardLimitBytes: 37})
			var queries []*QueryBudget
			start := make(chan struct{})
			reservations := make(chan Reservation, 256)
			var wg sync.WaitGroup
			for range 256 {
				owner := q
				if sharedEngine {
					owner, _ = e.BeginQuery(BudgetOptions{SoftLimitBytes: 37, HardLimitBytes: 37})
					queries = append(queries, owner)
				}
				op := owner.Operator("op", MemoryInFlightBatch)
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					r, ok, err := op.TryReserveImmediate(context.Background(), 3)
					if err != nil {
						t.Errorf("concurrent reservation: %v", err)
					} else if ok {
						reservations <- r
					} else if r != nil {
						t.Error("miss returned a reservation")
					}
				}()
			}
			close(start)
			wg.Wait()
			close(reservations)
			limit := int64(36)
			if sharedEngine {
				limit = 69
			}
			if e.Used() <= 0 || e.Used() > limit || e.Used() != int64(len(reservations))*3 {
				t.Fatalf("engine=%d reservation count=%d limit=%d", e.Used(), len(reservations), limit)
			}
			for r := range reservations {
				r.Release()
			}
			if e.Used() != 0 || q.Used() != 0 {
				t.Fatalf("release engine=%d query=%d", e.Used(), q.Used())
			}
			for _, owner := range queries {
				if owner.Used() != 0 || owner.Snapshot().ByClass[MemoryInFlightBatch] != 0 || owner.Operator("op", MemoryInFlightBatch).Used() != 0 {
					t.Fatalf("release query=%+v", owner.Snapshot())
				}
			}
		})
	}
}

func TestImmediateBudgetCloseRaceKeepsLiveTokensReleasable(t *testing.T) {
	for range 100 {
		e := NewEngineBudget(100)
		q, _ := e.BeginQuery(BudgetOptions{SoftLimitBytes: 100, HardLimitBytes: 100})
		op := q.Operator("op", MemoryInFlightBatch)
		held, ok, err := op.TryReserveImmediate(context.Background(), 10)
		if err != nil || !ok {
			t.Fatalf("initial reservation ok=%v err=%v", ok, err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				r, ok, err := op.TryReserveImmediate(context.Background(), 10)
				if err != nil && !errors.Is(err, ErrQueryBudgetClosed) {
					t.Errorf("close race: %v", err)
				}
				if ok {
					r.Release()
				}
			}()
		}
		close(start)
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if r, ok, err := op.TryReserveImmediate(context.Background(), 1); r != nil || ok || !errors.Is(err, ErrQueryBudgetClosed) {
			t.Fatalf("after close reservation=%v ok=%v err=%v", r, ok, err)
		}
		held.Release()
		held.Release()
		if e.Used() != 0 || q.Used() != 0 || op.Used() != 0 || q.Snapshot().ByClass[MemoryInFlightBatch] != 0 {
			t.Fatalf("close race leaked engine=%d query=%+v", e.Used(), q.Snapshot())
		}
	}
}
