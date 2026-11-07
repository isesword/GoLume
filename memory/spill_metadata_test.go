package memory

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestSpillMetadataAdmissionAndLifetime(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir() + "/" + strings.Repeat("d", 180)
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	store, e := NewQuerySpillStore(SpillOptions{Directory: dir, MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	mgr := NewBudgetManager(1 << 20)
	object := SpillObject{Kind: SpillAggregateState, Codec: "test.v1", Payload: []byte("value")}
	h, e := store.SpillWithMetadata(ctx, object, mgr)
	if e != nil {
		t.Fatal(e)
	}
	if mgr.Used() < int64(2*len(store.dir))+512+1152 {
		t.Fatalf("metadata not fully admitted: %d", mgr.Used())
	}
	if e = store.Delete(ctx, h); e != nil {
		t.Fatal(e)
	}
	if mgr.Used() != 0 {
		t.Fatal(mgr.Used())
	}
	h, e = store.SpillWithMetadata(ctx, object, mgr)
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Close(); e != nil {
		t.Fatal(e)
	}
	if mgr.Used() != 0 {
		t.Fatal(mgr.Used())
	}
}
func TestSpillMetadataRejectsBeforeFileWrite(t *testing.T) {
	store, e := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	mgr := NewBudgetManager(1)
	_, e = store.SpillWithMetadata(context.Background(), SpillObject{Kind: SpillAggregateState, Codec: "v1"}, mgr)
	if !errors.Is(e, ErrBudgetExceeded) {
		t.Fatal(e)
	}
	files, e := os.ReadDir(store.dir)
	if e != nil {
		t.Fatal(e)
	}
	if len(files) != 0 || store.Used() != 0 || mgr.Used() != 0 {
		t.Fatalf("failed admission left files or reservations")
	}
}

type mapRejectManager struct {
	Manager
	reject      int64
	oldAtReject int64
}

func (m *mapRejectManager) Reserve(ctx context.Context, n int64) (Reservation, error) {
	if n == m.reject {
		m.oldAtReject = m.Used()
		return nil, ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}
func TestSpillMapGrowthOverlapAndRollback(t *testing.T) {
	ctx := context.Background()
	store, e := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	mgr := &mapRejectManager{Manager: NewBudgetManager(1 << 20)}
	object := SpillObject{Kind: SpillAggregateState, Codec: "v1"}
	for i := 0; i < 8; i++ {
		if _, e = store.SpillWithMetadata(ctx, object, mgr); e != nil {
			t.Fatal(e)
		}
	}
	before, bytes := mgr.Used(), store.Used()
	mgr.reject = 128 + 16*128
	if _, e = store.SpillWithMetadata(ctx, object, mgr); !errors.Is(e, ErrBudgetExceeded) {
		t.Fatal(e)
	}
	if mgr.oldAtReject <= before {
		t.Fatal("old map and new handle must remain charged during replacement admission")
	}
	if mgr.Used() != before || store.Used() != bytes || len(store.handles) != 8 || store.handleCapacity != 8 {
		t.Fatal("growth failure did not roll back")
	}
	mgr.reject = 0
	if _, e = store.SpillWithMetadata(ctx, object, mgr); e != nil {
		t.Fatal(e)
	}
	if store.handleCapacity != 16 {
		t.Fatal(store.handleCapacity)
	}
	if e = store.Close(); e != nil {
		t.Fatal(e)
	}
	if mgr.Used() != 0 {
		t.Fatal(mgr.Used())
	}
}

func TestMetadataFailedDurabilityCleanupRetainsAccounting(t *testing.T) {
	s, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m := NewBudgetManager(1 << 20)
	originalSync, originalRemove := s.ops.syncDir, s.ops.remove
	s.ops.syncDir = func(string) error { return os.ErrPermission }
	s.ops.remove = func(string) error { return os.ErrPermission }
	_, err = s.SpillWithMetadata(context.Background(), SpillObject{Kind: SpillAggregateState, Codec: "v1", Payload: []byte("payload")}, m)
	s.ops.syncDir, s.ops.remove = originalSync, originalRemove
	if !errors.Is(err, os.ErrPermission) {
		t.Fatal(err)
	}
	if len(s.handles) != 1 || s.Used() == 0 || m.Used() == 0 {
		t.Fatalf("lost ownership handles%d disk%d memory%d", len(s.handles), s.Used(), m.Used())
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.Used() != 0 || m.Used() != 0 {
		t.Fatalf("cleanup disk%d memory%d", s.Used(), m.Used())
	}
}

// Admission may reenter the store and invalidate the pre-reservation size.
type metadataReenterManager struct {
	Manager
	callback func() error
}

func (m *metadataReenterManager) Reserve(ctx context.Context, n int64) (Reservation, error) {
	if n == 128+8*128 && m.callback != nil {
		callback := m.callback
		m.callback = nil
		if err := callback(); err != nil {
			return nil, err
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestSpillMetadataReentrantGrowthRechecksCapacity(t *testing.T) {
	ctx := context.Background()
	s, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	object := SpillObject{Kind: SpillAggregateState, Codec: "v1"}
	m := &metadataReenterManager{Manager: NewBudgetManager(1 << 20)}
	m.callback = func() error {
		for i := 0; i < 9; i++ {
			if _, err := s.Spill(ctx, object); err != nil {
				return err
			}
		}
		return nil
	}
	if _, err = s.SpillWithMetadata(ctx, object, m); err != nil {
		t.Fatal(err)
	}
	if len(s.handles) != 10 || s.handleCapacity < 10 {
		t.Fatalf("handles%d capacity%d", len(s.handles), s.handleCapacity)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 || s.Used() != 0 {
		t.Fatal(m.Used(), s.Used())
	}
}
func TestSpillMetadataReentrantCloseReleasesReservation(t *testing.T) {
	ctx := context.Background()
	s, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	m := &metadataReenterManager{Manager: NewBudgetManager(1 << 20), callback: s.Close}
	_, err = s.SpillWithMetadata(ctx, SpillObject{Kind: SpillAggregateState, Codec: "v1"}, m)
	if !errors.Is(err, ErrSpillClosed) {
		t.Fatal(err)
	}
	if m.Used() != 0 || s.Used() != 0 {
		t.Fatal(m.Used(), s.Used())
	}
}
