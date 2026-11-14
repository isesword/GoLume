package pipeline

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"strconv"
	"testing"
	"time"
)

type customSharingManager struct {
	base   *memory.BudgetManager
	calls  int
	failAt int
	fatal  bool
}

func (m *customSharingManager) Reserve(c context.Context, n int64) (memory.Reservation, error) {
	return m.base.Reserve(c, n)
}
func (m *customSharingManager) Used() int64  { return m.base.Used() }
func (m *customSharingManager) Limit() int64 { return m.base.Limit() }
func (m *customSharingManager) TryReserveImmediate(c context.Context, n int64) (memory.Reservation, bool, error) {
	m.calls++
	if m.calls == m.failAt {
		if m.fatal {
			return nil, false, errors.New("policy denied")
		}
		return nil, false, nil
	}
	return m.base.TryReserveImmediate(c, n)
}

type inheritedSharingManager struct{ *memory.BudgetManager }

func (m *inheritedSharingManager) Reserve(context.Context, int64) (memory.Reservation, error) {
	return nil, errors.New("custom policy")
}

type wrappedSharingManager struct{ *memory.NativeSharingManager }

func TestCustomManagerSharingExplicitOptIn(t *testing.T) {
	ctx := context.Background()
	inherited := &inheritedSharingManager{memory.NewBudgetManager(1 << 20)}
	if sharedImmediateManager(ctx, inherited) != nil {
		t.Fatal("inherited method bypassed custom policy")
	}
	raw := &customSharingManager{base: memory.NewBudgetManager(1 << 20)}
	if sharedImmediateManager(ctx, raw) != nil {
		t.Fatal("implicit custom opt-in")
	}
	opted, err := memory.NewNativeSharingManager(raw)
	if err != nil {
		t.Fatal(err)
	}
	if sharedImmediateManager(ctx, &wrappedSharingManager{opted}) != nil {
		t.Fatal("wrapper implicitly opted in")
	}
	src := sharedTestBatch(130)
	cost, _ := memory.EstimateNativeSharedBatchRange(src, 0, src.Rows)
	mux := sharedTestMux(t, opted, ctx, 1<<18)
	ok, n, err := mux.trySharedRange(src, MorselRange{Rows: src.Rows, Bytes: cost.CloneBytes}, Sequence{})
	if err != nil || !ok || n != 2 {
		t.Fatalf("share %v %d %v", ok, n, err)
	}
	a, b := <-mux.branches[0].exchange.items, <-mux.branches[1].exchange.items
	if a.shared.owner != b.shared.owner || &a.batch.Int64s["x"].Values[0] != &b.batch.Int64s["x"].Values[0] {
		t.Fatal("not shared")
	}
	if raw.Used() != cost.OwnerBytes+2*cost.BindingBytes {
		t.Fatal("wrong charge")
	}
	mux.branches[0].exchange.release(a)
	if raw.Used() != cost.CloneBytes || b.batch.Int64s["x"].Values[129] != 129 {
		t.Fatal("early release")
	}
	mux.branches[1].exchange.release(b)
	if raw.Used() != 0 {
		t.Fatal("leak")
	}
	sharedAssertEmpty(t, mux)
}
func TestCustomManagerSharingRollback(t *testing.T) {
	for _, fatal := range []bool{false, true} {
		for _, at := range []int{1, 2, 3} {
			t.Run(string(rune('0'+at))+map[bool]string{false: "miss", true: "error"}[fatal], func(t *testing.T) {
				raw := &customSharingManager{base: memory.NewBudgetManager(1 << 20), failAt: at, fatal: fatal}
				opted, _ := memory.NewNativeSharingManager(raw)
				mux := sharedTestMux(t, opted, context.Background(), 1<<18)
				src := sharedTestBatch(130)
				handled, _, err := mux.trySharedRange(src, MorselRange{Rows: 130}, Sequence{})
				if fatal {
					if !handled || err == nil {
						t.Fatal("lost policy error")
					}
				} else if handled || err != nil {
					t.Fatal("not fallback")
				}
				if raw.Used() != 0 {
					t.Fatal("reservation leak")
				}
				sharedAssertEmpty(t, mux)
				for _, b := range mux.branches {
					if len(b.exchange.items) != 0 {
						t.Fatal("partial publication")
					}
				}
			})
		}
	}
}

func TestCustomManagerSharingOpaqueAndCancel(t *testing.T) {
	raw := &customSharingManager{base: memory.NewBudgetManager(512 << 10)}
	opted, _ := memory.NewNativeSharingManager(raw)
	mux := sharedTestMux(t, opted, context.Background(), 1<<18)
	src := sharedTestBatch(130)
	src.Opaque["hidden"] = &memory.OpaqueVector{Rows: 130, Value: struct{}{}}
	if handled, _, err := mux.trySharedRange(src, MorselRange{Rows: 130}, Sequence{}); handled || err != nil || raw.calls != 0 {
		t.Fatal("opaque must fall back before admission")
	}
	delete(src.Opaque, "hidden")
	mux.cancel(context.Canceled)
	if handled, _, err := mux.trySharedRange(src, MorselRange{Rows: 130}, Sequence{}); !handled || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v %v", handled, err)
	}
	if raw.Used() != 0 {
		t.Fatal("leak")
	}
	sharedAssertEmpty(t, mux)
}

func TestCustomManagerCloseDuringSecondPublication(t *testing.T) {
	h := &reviewImmediateHook{BudgetManager: memory.NewBudgetManager(1 << 20)}
	adapter, err := memory.NewNativeSharingManager(h)
	if err != nil {
		t.Fatal(err)
	}
	m, cancel := reviewSharedMux(t, adapter, sharedImmediateManager(context.Background(), adapter))
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

func TestCustomManagerOpenFanOut512KiB(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "guard", true: "shared"}[enabled], func(t *testing.T) {
			raw := &customSharingManager{base: memory.NewBudgetManager(512 << 10)}
			var manager memory.Manager = raw
			if enabled {
				var err error
				manager, err = memory.NewNativeSharingManager(raw)
				if err != nil {
					t.Fatal(err)
				}
			}
			collector := &observer.Collector{}
			opts := Options{Workers: 1, TargetMorselBytes: 32768, MaxMorselBytes: 65536, MaxInFlightBytes: 262144, MinMorselRows: 1, MaxMorselRows: 64}
			streams, err := OpenFanOut(context.Background(), &finalReviewSource{reviewSharedSource(1024)}, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)}, nil, manager, opts, 2, collector)
			if err != nil {
				t.Fatal(err)
			}
			errs := make(chan error, 2)
			for _, stream := range streams {
				go func(s source.BatchStream) {
					dst := memory.NewBatch()
					rows := 0
					for {
						ok, e := s.Next(context.Background(), dst)
						if e != nil {
							_ = s.Close()
							errs <- e
							return
						}
						if !ok {
							break
						}
						for _, v := range dst.Int64s["v"].Values[:dst.Rows] {
							rows++
							if v != int64(rows) {
								_ = s.Close()
								errs <- fmt.Errorf("value order %d %d", rows, v)
								return
							}
						}
					}
					e := s.Close()
					if e == nil && rows != 1024 {
						e = fmt.Errorf("rows=%d", rows)
					}
					errs <- e
				}(stream)
			}
			for range streams {
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			}
			for _, stream := range streams {
				_ = stream.Close()
			}
			if raw.Used() != 0 {
				t.Fatalf("leak %d", raw.Used())
			}
			events := collector.Events()
			if len(events) != 1 {
				t.Fatalf("events %v", events)
			}
			shared, _ := strconv.Atoi(events[0].Attributes["shared_native_ranges"])
			if enabled && shared == 0 {
				t.Fatal("opt-in never shared")
			}
			if !enabled && shared != 0 {
				t.Fatal("unexpected opt-in")
			}
		})
	}
}

func TestCustomManagerQueryContextPriority(t *testing.T) {
	raw := &customSharingManager{base: memory.NewBudgetManager(1 << 20)}
	opted, _ := memory.NewNativeSharingManager(raw)
	engine := memory.NewEngineBudget(1 << 20)
	q, err := engine.BeginQuery(memory.BudgetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	immediate := sharedImmediateManager(ctx, opted)
	r, ok, err := immediate.TryReserveImmediate(ctx, 100)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if raw.calls != 0 || raw.Used() != 0 || q.Used() != 100 {
		t.Fatal("query routing changed")
	}
	r.Release()
	if q.Used() != 0 || engine.Used() != 0 {
		t.Fatal("query leak")
	}
}
