package spillmeta

import (
	"context"
	"errors"
	"gocolumn/memory"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

func testBase(t *testing.T) *memory.QuerySpillStore {
	t.Helper()
	base, err := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := base.Close(); err != nil {
			t.Error(err)
		}
	})
	return base
}
func testObject() memory.SpillObject {
	return memory.SpillObject{Kind: memory.SpillAggregateState, Codec: "metadata-test-v1", Payload: []byte("row")}
}

func TestStoreAdmissionAndPrivateOwnership(t *testing.T) {
	ctx := context.Background()
	base := testBase(t)
	manager := memory.NewBudgetManager(1 << 20)
	a, b := NewStore(base, manager), NewStore(base, manager)
	ah, err := a.Spill(ctx, testObject())
	if err != nil {
		t.Fatal(err)
	}
	bh, err := b.Spill(ctx, testObject())
	if err != nil {
		t.Fatal(err)
	}
	if manager.Used() < 2048 {
		t.Fatalf("unaccounted metadata: %d", manager.Used())
	}
	if err = a.Delete(ctx, bh); !errors.Is(err, memory.ErrInvalidSpillHandle) {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if base.Used() != bh.Bytes() || manager.Used() == 0 {
		t.Fatal("one operator closed shared store")
	}
	if ah.(*handle).base != nil {
		t.Fatal("deleted wrapper retains uncharged path")
	}
	if ah.ID() == "" || ah.Bytes() == 0 {
		t.Fatal("immutable diagnostics lost")
	}
	restored, err := b.Restore(ctx, bh, [32]byte{}, manager)
	if err != nil {
		t.Fatal(err)
	}
	restored.Close()
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	if base.Used() != 0 || manager.Used() != 0 {
		t.Fatalf("leak disk=%d memory=%d", base.Used(), manager.Used())
	}
	if err = b.Delete(ctx, bh); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRejectsBeforeWrite(t *testing.T) {
	for _, limit := range []int64{1, 256, 512, 1024} {
		t.Run(strconv.FormatInt(limit, 10), func(t *testing.T) {
			base := testBase(t)
			manager := memory.NewBudgetManager(limit)
			s := NewStore(base, manager)
			_, err := s.Spill(context.Background(), testObject())
			if !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatal(err)
			}
			files, err := os.ReadDir(base.Directory())
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 0 || base.Used() != 0 || manager.Used() != 0 {
				t.Fatal("rejected admission retained files or bytes")
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type legacyStore struct{ memory.SpillManager }

func TestStoreRequiresBudgetedProvider(t *testing.T) {
	base := testBase(t)
	manager := memory.NewBudgetManager(1 << 20)
	s := NewStore(legacyStore{base}, manager)
	if _, err := s.Spill(context.Background(), testObject()); !errors.Is(err, memory.ErrSpillUnsupported) {
		t.Fatal(err)
	}
	if base.Used() != 0 || manager.Used() != 0 {
		t.Fatal("legacy provider allocated metadata")
	}
}

type deleteFailureStore struct {
	*memory.QuerySpillStore
	fail bool
}

func (s *deleteFailureStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	if s.fail {
		return os.ErrPermission
	}
	return s.QuerySpillStore.Delete(ctx, h)
}
func TestStoreFailedDeleteRetainsChargeThenQueryCleanup(t *testing.T) {
	base := testBase(t)
	failing := &deleteFailureStore{base, true}
	manager := memory.NewBudgetManager(1 << 20)
	s := NewStore(failing, manager)
	h, err := s.Spill(context.Background(), testObject())
	if err != nil {
		t.Fatal(err)
	}
	before := manager.Used()
	if err = s.Delete(context.Background(), h); !errors.Is(err, os.ErrPermission) {
		t.Fatal(err)
	}
	if manager.Used() != before || s.first == nil {
		t.Fatal("failed delete lost owner or reservation")
	}
	if err = s.Close(); !errors.Is(err, os.ErrPermission) {
		t.Fatal(err)
	}
	if s.first != nil || manager.Used() == 0 || base.Used() == 0 {
		t.Fatal("query ownership must survive private cleanup")
	}
	if h.(*handle).base != nil {
		t.Fatal("private handle retained after close")
	}
	if err = base.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatal(manager.Used())
	}
}

func TestStoreManyRunsRespectHardBudget(t *testing.T) {
	base := testBase(t)
	manager := memory.NewBudgetManager(32 << 10)
	s := NewStore(base, manager)
	count := 0
	for ; count < 10000; count++ {
		_, err := s.Spill(context.Background(), testObject())
		if errors.Is(err, memory.ErrBudgetExceeded) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if manager.Used() > manager.Limit() {
			t.Fatal("hard budget exceeded")
		}
	}
	if count == 0 || count == 10000 {
		t.Fatalf("unexpected spill count %d", count)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 || base.Used() != 0 {
		t.Fatal("budget failure leaked metadata")
	}
}

func TestStoreConcurrentSpillDeleteClose(t *testing.T) {
	base := testBase(t)
	manager := memory.NewBudgetManager(8 << 20)
	s := NewStore(base, manager)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 32; j++ {
				h, err := s.Spill(context.Background(), testObject())
				if errors.Is(err, memory.ErrSpillClosed) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				if err = s.Delete(context.Background(), h); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 || base.Used() != 0 {
		t.Fatal("concurrent close leaked metadata")
	}
}

type rejectGrowth struct {
	memory.Manager
	reject int64
	during int64
}

func (m *rejectGrowth) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if n == m.reject {
		m.during = m.Used()
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}
func TestSliceOverlapRollbackAndRetainedCapacity(t *testing.T) {
	ctx := context.Background()
	manager := &rejectGrowth{Manager: memory.NewBudgetManager(1 << 20)}
	var s Slice[int]
	if err := s.Append(ctx, manager, 1); err != nil {
		t.Fatal(err)
	}
	before := manager.Used()
	manager.reject = 128 + 2*(2*8+256)
	if err := s.Append(ctx, manager, 2); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if manager.during != before || manager.Used() != before || len(s.Values) != 1 || s.Values[0] != 1 {
		t.Fatal("growth denial changed old allocation")
	}
	manager.reject = 0
	if err := s.Append(ctx, manager, 2); err != nil {
		t.Fatal(err)
	}
	grown := manager.Used()
	s.Clear()
	if len(s.Values) != 0 || cap(s.Values) != 2 || manager.Used() != grown {
		t.Fatal("Clear must retain capacity admission")
	}
	if err := s.Append(ctx, manager, 3); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != grown {
		t.Fatal("reusing capacity recharged")
	}
	s.Release()
	s.Release()
	if s.Values != nil || manager.Used() != 0 {
		t.Fatal("Release leaked")
	}
}

func TestSliceCanceledAppendHasNoEffect(t *testing.T) {
	manager := memory.NewBudgetManager(1024)
	var s Slice[int]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Append(ctx, manager, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.Values != nil || manager.Used() != 0 {
		t.Fatal("canceled admission allocated")
	}
}

type countingSpiller struct{ calls int }

func (s *countingSpiller) SpillableBytes() int64 { return 1024 }
func (s *countingSpiller) Spill(context.Context, memory.SpillRequest) (int64, error) {
	s.calls++
	return 0, nil
}
func TestGrowthManagerDoesNotReenterSpill(t *testing.T) {
	engine := memory.NewEngineBudget(2048)
	query, err := engine.BeginQuery(memory.BudgetOptions{SoftLimitBytes: 512, HardLimitBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	defer query.Close()
	op := query.Operator("mutating", memory.MemoryRetainedState)
	spiller := &countingSpiller{}
	unregister, err := op.RegisterSpiller(spiller)
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	manager := GrowthManager(op)
	r, err := manager.Reserve(context.Background(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if spiller.calls != 0 || manager.Used() != 1024 {
		t.Fatal("metadata admission reentered mutation")
	}
	if _, err := manager.Reserve(context.Background(), 2048); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if spiller.calls != 0 {
		t.Fatal("hard denial reentered mutation")
	}
	r.Release()
	if query.Used() != 0 {
		t.Fatal(query.Used())
	}
}

type blockedMetadataStore struct {
	*memory.QuerySpillStore
	entered, proceed chan struct{}
}

func (s *blockedMetadataStore) SpillWithMetadata(ctx context.Context, obj memory.SpillObject, manager memory.Manager) (memory.SpillHandle, error) {
	close(s.entered)
	<-s.proceed
	return s.QuerySpillStore.SpillWithMetadata(ctx, obj, manager)
}
func TestStoreCloseWaitsForAdmittedWriter(t *testing.T) {
	base := testBase(t)
	manager := memory.NewBudgetManager(1 << 20)
	blocked := &blockedMetadataStore{base, make(chan struct{}), make(chan struct{})}
	s := NewStore(blocked, manager)
	wrote := make(chan error, 1)
	go func() { _, err := s.Spill(context.Background(), testObject()); wrote <- err }()
	<-blocked.entered
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	// Observe the closed bit to ensure Close has actually started, without
	// relying on a sleep to arrange the in-flight-write race.
	for {
		s.mu.Lock()
		started := s.closed
		s.mu.Unlock()
		if started {
			break
		}
		runtime.Gosched()
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before admitted writer: %v", err)
	default:
	}
	close(blocked.proceed)
	if err := <-wrote; !errors.Is(err, memory.ErrSpillClosed) {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if base.Used() != 0 || manager.Used() != 0 {
		t.Fatal("Close returned with in-flight allocation")
	}
}

func TestStoreCloseFailureDoesNotBuildRunSizedErrorList(t *testing.T) {
	base := testBase(t)
	manager := memory.NewBudgetManager(1 << 20)
	failing := &deleteFailureStore{base, true}
	s := NewStore(failing, manager)
	for i := 0; i < 64; i++ {
		if _, err := s.Spill(context.Background(), testObject()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != os.ErrPermission {
		t.Fatalf("Close must preserve only first error, got %T", err)
	}
	if s.first != nil || base.Used() == 0 || manager.Used() == 0 {
		t.Fatal("failed files must remain in query store")
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatal(manager.Used())
	}
}
