package pipeline

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"testing"
	"time"
)

type reviewImmediateHook struct {
	*memory.BudgetManager
	calls  int
	before func(int)
	after  func(int)
	deny   int
}

func (h *reviewImmediateHook) TryReserveImmediate(ctx context.Context, n int64) (memory.Reservation, bool, error) {
	h.calls++
	i := h.calls
	if h.before != nil {
		h.before(i)
	}
	if i == h.deny {
		return nil, false, nil
	}
	r, ok, e := h.BudgetManager.TryReserveImmediate(ctx, n)
	if h.after != nil {
		h.after(i)
	}
	return r, ok, e
}
func reviewSharedMux(t *testing.T, manager memory.Manager, immediate memory.ImmediateReservationManager) (*boundedMultiplexer, context.CancelCauseFunc) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	m := &boundedMultiplexer{ctx: ctx, cancel: cancel, manager: manager, sharedImmediate: immediate, opts: Options{MaxMorselBytes: 65536}}
	m.active.Store(2)
	for i := 0; i < 2; i++ {
		e, err := NewExchange(manager, 131072, 2, 1)
		if err != nil {
			t.Fatal(err)
		}
		bc, cc := context.WithCancel(ctx)
		b := &fanOutBranch{owner: m, id: i, exchange: e, ctx: bc, cancel: cc}
		b.active.Store(true)
		m.branches = append(m.branches, b)
	}
	return m, cancel
}
func reviewSharedSource(rows int) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = rows
	b.Int64s["v"] = &memory.Int64Vector{Values: make([]int64, rows)}
	for i := range b.Int64s["v"].Values {
		b.Int64s["v"].Values[i] = int64(i + 1)
	}
	return b
}
func reviewSharedClean(t *testing.T, m *boundedMultiplexer, manager memory.Manager) {
	t.Helper()
	for _, b := range m.branches {
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if manager.Used() != 0 {
		t.Fatalf("retained query bytes %d", manager.Used())
	}
	for _, b := range m.branches {
		e := b.exchange
		if e.Stats().CurrentInFlightBytes != 0 {
			t.Fatal("inflight leaked")
		}
		if !e.bytes.TryAcquire(e.maxBytes) {
			t.Fatal("global credit leaked")
		}
		e.bytes.Release(e.maxBytes)
		if !e.groupBytes[0].TryAcquire(e.groupMax) {
			t.Fatal("group credit leaked")
		}
		e.groupBytes[0].Release(e.groupMax)
	}
}
func TestSharedReviewPreparationRollback(t *testing.T) {
	for _, mode := range []string{"deny_owner", "deny_first", "deny_second", "cancel_owner", "cancel_first", "cancel_second", "close_second", "close_first"} {
		t.Run(mode, func(t *testing.T) {
			h := &reviewImmediateHook{BudgetManager: memory.NewBudgetManager(1 << 20)}
			m, cancel := reviewSharedMux(t, h, h)
			defer cancel(context.Canceled)
			switch mode {
			case "deny_owner":
				h.deny = 1
			case "deny_first":
				h.deny = 2
			case "deny_second":
				h.deny = 3
			}
			h.before = func(i int) {
				if mode == fmt.Sprintf("cancel_%s", []string{"", "owner", "first", "second"}[i]) {
					cancel(context.Canceled)
				}
				if i == 3 && mode == "close_second" {
					_ = m.branches[1].Close()
				}
				if i == 3 && mode == "close_first" {
					_ = m.branches[0].Close()
				}
			}
			handled, _, err := m.trySharedRange(reviewSharedSource(64), MorselRange{Rows: 64}, Sequence{})
			if h.deny != 0 && (handled || err != nil) {
				t.Fatalf("denial must rollback to guard: %v %v", handled, err)
			}
			if len(mode) >= 6 && mode[:6] == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel: %v", err)
			}
			if h.deny != 0 || err != nil {
				for _, b := range m.branches {
					if len(b.exchange.items) != 0 {
						t.Fatal("partial publication on preparation failure")
					}
				}
			}
			reviewSharedClean(t, m, h)
		})
	}
}
func TestSharedReviewEarlyAckAndRootMutation(t *testing.T) {
	h := &reviewImmediateHook{BudgetManager: memory.NewBudgetManager(1 << 20)}
	m, cancel := reviewSharedMux(t, h, h)
	defer cancel(context.Canceled)
	src := reviewSharedSource(64)
	child := reviewSharedSource(64)
	src.Structs["s"] = &memory.StructVector{Rows: 64, Fields: []memory.Field{{Name: "v", Type: memory.TypeInt64}}, Values: child}
	done := make(chan error, 1)
	// An unbuffered first branch makes its delivery/early ACK precede the
	// producer's second publication; no scheduler sleep is used.
	m.branches[0].exchange.items = make(chan exchangeItem)
	go func() { _, _, err := m.trySharedRange(src, MorselRange{Rows: 64}, Sequence{}); done <- err }()
	first := <-m.branches[0].exchange.items
	secondDone := make(chan struct{})
	dst := memory.NewBatch()
	m.branches[0].bindCurrent(dst, first)
	dst.ClearColumns() // legal root mutation must not clear another binding graph
	m.branches[0].releaseCurrent()
	if h.Used() == 0 {
		t.Fatal("early first ACK released shared storage")
	}
	go func() {
		defer close(secondDone)
		item := <-m.branches[1].exchange.items
		if item.batch.Structs["s"].Values.Int64s["v"].Values[63] != 64 {
			t.Error("other branch corrupted")
		}
		m.branches[1].exchange.release(item)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("producer stalled")
	}
	<-secondDone
	reviewSharedClean(t, m, h)
}
func TestSharedReviewGuardSizedTightBudgets(t *testing.T) {
	for _, limit := range []int64{12000, 20000, 512 << 10} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			h := &reviewImmediateHook{BudgetManager: memory.NewBudgetManager(limit)}
			m, cancel := reviewSharedMux(t, h, h)
			defer cancel(context.Canceled)
			src := reviewSharedSource(1024)
			opts := Options{Workers: 1, TargetMorselBytes: 8192, MaxMorselBytes: 32768, MaxInFlightBytes: 65536, MinMorselRows: 1, MaxMorselRows: 1024}
			s, err := newOwnedByteSizer(memory.Schema{}, opts)
			if err != nil {
				t.Fatal(err)
			}
			rs, err := s.Split(src)
			if err != nil {
				t.Fatal(err)
			}
			shared := 0
			for _, r := range rs {
				handled, _, err := m.trySharedRange(src, r, Sequence{})
				if err != nil {
					t.Fatal(err)
				}
				if handled {
					shared++
					for _, b := range m.branches {
						item := <-b.exchange.items
						if item.batch.Rows != r.Rows {
							t.Fatal("rows changed")
						}
						b.exchange.release(item)
					}
				} else {
					for _, b := range m.branches {
						if err = b.exchange.enqueueRange(b.ctx, Sequence{}, src, r); err != nil {
							t.Fatal(err)
						}
						b.exchange.release(<-b.exchange.items)
					}
				}
				if h.Used() != 0 {
					t.Fatal("range release leak")
				}
			}
			t.Logf("limit=%d ranges=%d shared=%d", limit, len(rs), shared)
			reviewSharedClean(t, m, h)
		})
	}
}

type reviewUnknownManager struct {
	base  *memory.BudgetManager
	calls int
}

func (m *reviewUnknownManager) Reserve(c context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	return m.base.Reserve(c, n)
}
func (m *reviewUnknownManager) Used() int64  { return m.base.Used() }
func (m *reviewUnknownManager) Limit() int64 { return m.base.Limit() }
func TestSharedReviewFallbackBoundaries(t *testing.T) {
	unknown := &reviewUnknownManager{base: memory.NewBudgetManager(1 << 20)}
	if sharedImmediateManager(context.Background(), unknown) != nil {
		t.Fatal("unknown manager acquired speculative capability")
	}
	m, cancel := reviewSharedMux(t, unknown, nil)
	defer cancel(context.Canceled)
	if handled, _, err := m.trySharedRange(reviewSharedSource(4), MorselRange{Rows: 4}, Sequence{}); handled || err != nil || unknown.calls != 0 {
		t.Fatal("unknown manager was speculatively probed")
	}
	reviewSharedClean(t, m, unknown)
	for _, nested := range []bool{false, true} {
		h := &reviewImmediateHook{BudgetManager: memory.NewBudgetManager(1 << 20)}
		m, cancel := reviewSharedMux(t, h, h)
		src := reviewSharedSource(4)
		opaque := src
		if nested {
			opaque = reviewSharedSource(4)
			src.Structs["undeclared"] = &memory.StructVector{Rows: 4, Values: opaque}
		}
		// Even an undeclared runtime Opaque must decline without touching Value.
		opaque.Opaque["legacy"] = &memory.OpaqueVector{Rows: 4, Value: struct{}{}}
		if handled, _, err := m.trySharedRange(src, MorselRange{Rows: 4}, Sequence{}); handled || err != nil || h.calls != 0 {
			t.Fatalf("opaque speculative access nested=%v handled=%v calls=%d err=%v", nested, handled, h.calls, err)
		}
		reviewSharedClean(t, m, h)
		cancel(context.Canceled)
	}
}
func TestSharedReviewCloseDuringSecondPublication(t *testing.T) {
	h := &reviewImmediateHook{BudgetManager: memory.NewBudgetManager(1 << 20)}
	m, cancel := reviewSharedMux(t, h, h)
	defer cancel(context.Canceled)
	for _, b := range m.branches {
		b.exchange.items = make(chan exchangeItem)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := m.trySharedRange(reviewSharedSource(64), MorselRange{Rows: 64}, Sequence{})
		done <- err
	}()
	first := <-m.branches[0].exchange.items
	closed := make(chan struct{})
	go func() { _ = m.branches[1].Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("second Close blocked behind producer")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("producer failed to leave closed branch")
	}
	if h.Used() == 0 {
		t.Fatal("first delivered view lost charge")
	}
	m.branches[0].exchange.release(first)
	first = exchangeItem{}
	reviewSharedClean(t, m, h)
}
