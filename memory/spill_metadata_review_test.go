package memory

import (
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestReviewMetadataMapFailureDoesNotOrphanCommittedFile(t *testing.T) {
	store, e := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	mgr := &mapRejectManager{Manager: NewBudgetManager(1 << 20), reject: 128 + 8*128}
	original := store.ops.remove
	store.ops.remove = func(string) error { return syscall.EACCES }
	_, e = store.SpillWithMetadata(context.Background(), SpillObject{Kind: SpillAggregateState, Codec: "v1"}, mgr)
	store.ops.remove = original
	if !errors.Is(e, ErrBudgetExceeded) {
		t.Fatal(e)
	}
	files, e := os.ReadDir(store.dir)
	if e != nil {
		t.Fatal(e)
	}
	if len(files) != 0 {
		t.Fatalf("map admission failure orphaned %d committed files with disk accounting=%d and handles=%d", len(files), store.Used(), len(store.handles))
	}
	if mgr.Used() != 0 || store.Used() != 0 {
		t.Fatal(mgr.Used(), store.Used())
	}
}

type reviewReclaimManager struct {
	Manager
	reclaim  func() error
	timedOut bool
}

func (m *reviewReclaimManager) Reserve(ctx context.Context, n int64) (Reservation, error) {
	if n == 128+8*128 && m.reclaim != nil {
		result := make(chan error, 1)
		go func() { result <- m.reclaim() }()
		select {
		case e := <-result:
			if e != nil {
				return nil, e
			}
		case <-time.After(200 * time.Millisecond):
			m.timedOut = true
			return nil, ErrBudgetExceeded
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestReviewSpillMetadataAdmissionAllowsReclamation(t *testing.T) {
	s, e := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	object := SpillObject{Kind: SpillAggregateState, Codec: "v1"}
	h, e := s.Spill(ctx, object)
	if e != nil {
		t.Fatal(e)
	}
	mgr := &reviewReclaimManager{Manager: NewBudgetManager(1 << 20)}
	mgr.reclaim = func() error { return s.Delete(ctx, h) }
	_, e = s.SpillWithMetadata(ctx, object, mgr)
	if mgr.timedOut {
		t.Fatal("metadata Reserve under store mutex blocks reclamation Delete callback")
	}
	if e != nil {
		t.Fatal(e)
	}
}

func TestReviewConcurrentMetadataGrowthAndDeletion(t *testing.T) {
	s, e := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	mgr := NewBudgetManager(8 << 20)
	ctx := context.Background()
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 12)
	for worker := 0; worker < 12; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var handles []SpillHandle
			for i := 0; i < 20; i++ {
				h, e := s.SpillWithMetadata(ctx, SpillObject{Kind: SpillAggregateState, Codec: "v1", Payload: []byte("x")}, mgr)
				if e != nil {
					errs <- e
					return
				}
				handles = append(handles, h)
			}
			for _, h := range handles {
				if e := s.Delete(ctx, h); e != nil {
					errs <- e
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if s.Used() != 0 || mgr.Used() != 0 {
		t.Fatal(s.Used(), mgr.Used())
	}
}
