package memory

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func TestHierarchicalBudgetAccountsClassesOperatorsAndIdempotentRelease(t *testing.T) {
	engine := NewEngineBudget(1024)
	query, err := engine.BeginQuery(BudgetOptions{SoftLimitBytes: 800, HardLimitBytes: 900})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithQueryBudget(context.Background(), query)
	exchange := query.Operator("Exchange", MemoryInFlightBatch)
	reservation, err := exchange.Reserve(ctx, 256)
	if err != nil {
		t.Fatal(err)
	}
	if engine.Used() != 256 || query.Used() != 256 || exchange.Used() != 256 {
		t.Fatalf("engine=%d query=%d operator=%d", engine.Used(), query.Used(), exchange.Used())
	}
	snapshot := query.Snapshot()
	if snapshot.ByClass[MemoryInFlightBatch] != 256 || snapshot.Operators["Exchange"] != 256 || snapshot.PeakBytes != 256 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	reservation.Release()
	reservation.Release()
	if engine.Used() != 0 || query.Used() != 0 || exchange.Used() != 0 {
		t.Fatalf("release engine=%d query=%d operator=%d", engine.Used(), query.Used(), exchange.Used())
	}
	if peak := query.Snapshot().PeakBytes; peak != 256 {
		t.Fatalf("peak changed after release: %d", peak)
	}
}

func TestQueryBudgetSoftPressureRequestsSpillBeforeAllocation(t *testing.T) {
	engine := NewEngineBudget(100)
	query, err := engine.BeginQuery(BudgetOptions{SoftLimitBytes: 80, HardLimitBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithQueryBudget(context.Background(), query)
	retained := query.Operator("HashState", MemoryRetainedState)
	held, err := retained.Reserve(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	spiller := &releasingSpiller{bytes: 60, reservations: []Reservation{held}}
	unregister, err := retained.RegisterSpiller(spiller)
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()

	output := query.Operator("Collect", MemoryOutput)
	result, err := output.Reserve(ctx, 40)
	if err != nil {
		t.Fatal(err)
	}
	if spiller.calls.Load() != 1 || query.Used() != 40 || engine.Used() != 40 {
		t.Fatalf("spill calls=%d query=%d engine=%d", spiller.calls.Load(), query.Used(), engine.Used())
	}
	snapshot := query.Snapshot()
	if snapshot.SpillRequests != 1 || snapshot.SpilledBytes != 60 || snapshot.ByClass[MemoryOutput] != 40 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	result.Release()
}

func TestQueryBudgetHardFailureIsTypedAndOutputHintIsActionable(t *testing.T) {
	engine := NewEngineBudget(100)
	query, _ := engine.BeginQuery(BudgetOptions{SoftLimitBytes: 80, HardLimitBytes: 100})
	ctx := WithQueryBudget(context.Background(), query)
	first, err := query.Operator("Collect", MemoryOutput).Reserve(ctx, 90)
	if err != nil {
		t.Fatal(err)
	}
	_, err = query.Operator("Collect", MemoryOutput).Reserve(ctx, 20)
	var resourceErr *ResourceError
	if !errors.Is(err, ErrBudgetExceeded) || !errors.As(err, &resourceErr) {
		t.Fatalf("error=%T %v", err, err)
	}
	if resourceErr.Operator != "Collect" || resourceErr.Class != MemoryOutput || resourceErr.UsedBytes != 90 || resourceErr.LimitBytes != 100 || resourceErr.Hint == "" {
		t.Fatalf("resource error=%+v", resourceErr)
	}
	first.Release()
	if engine.Used() != 0 || query.Used() != 0 {
		t.Fatalf("failure leaked engine=%d query=%d", engine.Used(), query.Used())
	}
}

func TestQueryBudgetSerializesConcurrentSpillPressure(t *testing.T) {
	engine := NewEngineBudget(100)
	query, _ := engine.BeginQuery(BudgetOptions{SoftLimitBytes: 80, HardLimitBytes: 100})
	ctx := WithQueryBudget(context.Background(), query)
	op := query.Operator("State", MemoryRetainedState)
	held, err := op.Reserve(ctx, 70)
	if err != nil {
		t.Fatal(err)
	}
	spiller := &releasingSpiller{bytes: 70, reservations: []Reservation{held}}
	unregister, err := op.RegisterSpiller(spiller)
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()

	start := make(chan struct{})
	reservations := make(chan Reservation, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reservation, err := query.Operator("Output", MemoryOutput).Reserve(ctx, 30)
			if err != nil {
				errs <- err
				return
			}
			reservations <- reservation
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	close(reservations)
	for reservation := range reservations {
		reservation.Release()
	}
	if spiller.calls.Load() != 1 {
		t.Fatalf("concurrent spill calls=%d want=1", spiller.calls.Load())
	}
	if engine.Used() != 0 || query.Used() != 0 {
		t.Fatalf("concurrent release engine=%d query=%d", engine.Used(), query.Used())
	}
}

func TestClosedQueryRejectsNewReservationsButAllowsOutputRelease(t *testing.T) {
	engine := NewEngineBudget(100)
	query, _ := engine.BeginQuery(BudgetOptions{HardLimitBytes: 100})
	reservation, err := query.Reserve(context.Background(), 40)
	if err != nil {
		t.Fatal(err)
	}
	if err := query.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := query.Reserve(context.Background(), 1); !errors.Is(err, ErrQueryBudgetClosed) {
		t.Fatalf("closed reserve error=%v", err)
	}
	reservation.Release()
	if engine.Used() != 0 || query.Used() != 0 {
		t.Fatalf("closed release engine=%d query=%d", engine.Used(), query.Used())
	}
}

func TestQuerySpillStoreIsLazyQueryScopedAndClosedWithBudget(t *testing.T) {
	base := t.TempDir()
	engine := NewEngineBudget(1 << 20)
	query, err := engine.BeginQuery(BudgetOptions{
		HardLimitBytes: 1 << 20,
		Spill:          SpillOptions{Directory: base, MaxBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !query.SpillConfigured() {
		t.Fatal("configured spill was not reported")
	}
	if entries, err := os.ReadDir(base); err != nil || len(entries) != 0 {
		t.Fatalf("spill directory created eagerly entries=%v err=%v", entries, err)
	}
	first, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	second, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("query returned different spill stores")
	}
	dir := first.(*QuerySpillStore).Directory()
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("lazy spill directory stat: %v", err)
	}
	if err := query.Close(); err != nil {
		t.Fatal(err)
	}
	if err := query.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("query spill directory remains: %v", err)
	}
	if _, err := query.SpillManager(); !errors.Is(err, ErrQueryBudgetClosed) {
		t.Fatalf("closed spill manager error=%v", err)
	}
}

func TestDisabledSpillReturnsNoSpillAndInvalidOptionalConfigFails(t *testing.T) {
	engine := NewEngineBudget(1 << 20)
	query, err := engine.BeginQuery(BudgetOptions{HardLimitBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	if query.SpillConfigured() {
		t.Fatal("zero spill configuration reported enabled")
	}
	if _, err := manager.Spill(context.Background(), SpillObject{}); !errors.Is(err, ErrSpillUnsupported) {
		t.Fatalf("disabled spill error=%v", err)
	}
	if _, err := engine.BeginQuery(BudgetOptions{Spill: SpillOptions{Directory: t.TempDir()}}); err == nil {
		t.Fatal("spill directory without quota was accepted")
	}
}

type releasingSpiller struct {
	bytes        int64
	reservations []Reservation
	calls        atomic.Int64
	once         sync.Once
}

func (s *releasingSpiller) SpillableBytes() int64 { return s.bytes }
func (s *releasingSpiller) Spill(context.Context, SpillRequest) (int64, error) {
	s.calls.Add(1)
	released := int64(0)
	s.once.Do(func() {
		for _, reservation := range s.reservations {
			released += reservation.Bytes()
			reservation.Release()
		}
	})
	return released, nil
}

var _ SpillableConsumer = (*releasingSpiller)(nil)
