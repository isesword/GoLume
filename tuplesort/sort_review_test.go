package tuplesort

import (
	"context"
	"errors"
	"gocolumn/memory"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestReviewRandomizedRunBoundaries(t *testing.T) {
	for _, keep := range []bool{false, true} {
		for _, spilled := range []bool{false, true} {
			ctx := context.Background()
			mgr := memory.NewBudgetManager(16 << 20)
			var store memory.SpillManager
			if spilled {
				st, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 32 << 20})
				if e != nil {
					t.Fatal(e)
				}
				defer st.Close()
				store = st
			}
			s, e := New(Config{Manager: mgr, Spill: store, BlockBytes: 71, KeepDuplicates: keep})
			if e != nil {
				t.Fatal(e)
			}
			r := rand.New(rand.NewSource(9102))
			var want []string
			for i := 0; i < 1500; i++ {
				b := make([]byte, r.Intn(130))
				for j := range b {
					b[j] = byte(r.Intn(4))
				}
				if i%3 == 0 {
					b = []byte(strings.Repeat("x", i%5))
				}
				want = append(want, string(b))
				if e = s.Add(ctx, b); e != nil {
					t.Fatal(e)
				}
				for j := range b {
					b[j] = 255
				}
			}
			sort.Strings(want)
			if !keep {
				d := want[:0]
				for _, x := range want {
					if len(d) == 0 || x != d[len(d)-1] {
						d = append(d, x)
					}
				}
				want = d
			}
			if e = s.Finish(ctx); e != nil {
				t.Fatal(e)
			}
			var got []string
			for {
				b, ok, e := s.Next(ctx)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				got = append(got, string(b))
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("keep=%v spill=%v got=%d want=%d", keep, spilled, len(got), len(want))
			}
			if mgr.Used() != 0 {
				t.Fatal(mgr.Used())
			}
		}
	}
}

type reviewDeleteStore struct {
	memory.SpillManager
	fail bool
}

var reviewDeleteError = errors.New("injected transient delete failure")

func (d *reviewDeleteStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	if d.fail {
		return reviewDeleteError
	}
	return d.SpillManager.Delete(ctx, h)
}
func TestReviewCloseRetryAfterDeleteFailure(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewBudgetManager(1 << 20)
	st, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	wrapped := &reviewDeleteStore{SpillManager: st}
	s, _ := New(Config{Manager: mgr, Spill: wrapped, BlockBytes: 32})
	for i := 0; i < 20; i++ {
		if e = s.Add(ctx, []byte{byte(i)}); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Finish(ctx); e != nil {
		t.Fatal(e)
	}
	wrapped.fail = true
	if e = s.Close(); !errors.Is(e, reviewDeleteError) {
		t.Fatalf("close error=%v", e)
	}
	wrapped.fail = false
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if st.Used() != 0 {
		t.Fatalf("Close retry silently succeeded but %d spill bytes remain", st.Used())
	}
	if mgr.Used() != 0 {
		t.Fatal(mgr.Used())
	}
}

func TestReviewSegmentMetadataAdmission(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewBudgetManager(4 << 20)
	// A caller-controlled valid directory length must not disappear from retained metadata.
	dir := t.TempDir() + "/" + strings.Repeat("d", 180)
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	st, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: dir, MaxBytes: 4 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	s, _ := New(Config{Manager: mgr, Spill: st, BlockBytes: 32})
	defer s.Close()
	for i := 0; i < 250; i++ {
		if e = s.Add(ctx, []byte{byte(i)}); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Finish(ctx); e != nil {
		t.Fatal(e)
	}
	var lowerBound int64
	for x := s.final.first; x != nil; x = x.next {
		lowerBound += int64(reflect.TypeOf(*x).Size())
		h := reflect.ValueOf(x.handle).Elem()
		lowerBound += int64(h.Type().Size())
		for _, f := range []string{"id", "path"} {
			lowerBound += int64(h.FieldByName(f).Len())
		}
	}
	if mgr.Used() < lowerBound {
		t.Fatalf("charged %d but retained segment/handle/string minimum is %d (excluding map)", mgr.Used(), lowerBound)
	}
}

type reviewFailManager struct {
	memory.Manager
	calls, failAt int
}

func (m *reviewFailManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls == m.failAt {
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}
func TestReviewAdmissionFailureAtEveryBoundary(t *testing.T) {
	for failAt := 1; failAt < 110; failAt++ {
		ctx := context.Background()
		mgr := &reviewFailManager{Manager: memory.NewBudgetManager(4 << 20), failAt: failAt}
		st, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20})
		if e != nil {
			t.Fatal(e)
		}
		s, _ := New(Config{Manager: mgr, Spill: st, BlockBytes: 32})
		for i := 0; i < 70; i++ {
			key := []byte{byte(i % 13)}
			if i%11 == 0 {
				key = []byte(strings.Repeat("L", 200))
			}
			e = s.Add(ctx, key)
			if e != nil {
				break
			}
		}
		if e == nil {
			e = s.Finish(ctx)
		}
		if e == nil {
			for {
				_, ok, err := s.Next(ctx)
				if err != nil {
					e = err
					break
				}
				if !ok {
					break
				}
			}
		}
		if e != nil && !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatalf("failAt=%d err=%v", failAt, e)
		}
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
		if mgr.Used() != 0 || st.Used() != 0 {
			t.Fatalf("failAt=%d memory=%d disk=%d", failAt, mgr.Used(), st.Used())
		}
		if e = st.Close(); e != nil {
			t.Fatal(e)
		}
	}
}

type reviewSelectiveDeleteStore struct {
	memory.SpillManager
	first   string
	enabled bool
}

func (d *reviewSelectiveDeleteStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	if d.enabled {
		if d.first == "" {
			d.first = h.ID()
		}
		if h.ID() == d.first {
			return reviewDeleteError
		}
	}
	return d.SpillManager.Delete(ctx, h)
}
func TestReviewPartialDeleteDetachesReleasedSegments(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewBudgetManager(1 << 20)
	st, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	wrapped := &reviewSelectiveDeleteStore{SpillManager: st}
	s, _ := New(Config{Manager: mgr, Spill: wrapped, BlockBytes: 32})
	for i := 0; i < 50; i++ {
		if e = s.Add(ctx, []byte{byte(i)}); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Finish(ctx); e != nil {
		t.Fatal(e)
	}
	wrapped.first = s.final.first.handle.ID()
	wrapped.enabled = true
	if e = s.Close(); !errors.Is(e, reviewDeleteError) {
		t.Fatal(e)
	}
	for x := s.owned; x != nil; x = x.ownerNext {
		if x.next != nil {
			t.Error("failed handle retains already-deleted run segment chain")
		}
	}
	wrapped.enabled = false
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if mgr.Used() != 0 || st.Used() != 0 {
		t.Fatal(mgr.Used(), st.Used())
	}
}

func (d *reviewDeleteStore) SpillWithMetadata(ctx context.Context, obj memory.SpillObject, m memory.Manager) (memory.SpillHandle, error) {
	return d.SpillManager.(interface {
		SpillWithMetadata(context.Context, memory.SpillObject, memory.Manager) (memory.SpillHandle, error)
	}).SpillWithMetadata(ctx, obj, m)
}
func (d *reviewSelectiveDeleteStore) SpillWithMetadata(ctx context.Context, obj memory.SpillObject, m memory.Manager) (memory.SpillHandle, error) {
	return d.SpillManager.(interface {
		SpillWithMetadata(context.Context, memory.SpillObject, memory.Manager) (memory.SpillHandle, error)
	}).SpillWithMetadata(ctx, obj, m)
}

func TestReviewCancellationAfterFinish(t *testing.T) {
	for _, spill := range []bool{false, true} {
		ctx := context.Background()
		mgr := memory.NewBudgetManager(1 << 20)
		var st *memory.QuerySpillStore
		var backend memory.SpillManager
		if spill {
			var e error
			st, e = memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			backend = st
		}
		s, _ := New(Config{Manager: mgr, Spill: backend, BlockBytes: 32})
		for i := 0; i < 30; i++ {
			if e := s.Add(ctx, []byte{byte(i)}); e != nil {
				t.Fatal(e)
			}
		}
		if e := s.Finish(ctx); e != nil {
			t.Fatal(e)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, _, e := s.Next(canceled); !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
		if mgr.Used() != 0 {
			t.Fatal(mgr.Used())
		}
		if st != nil && st.Used() != 0 {
			t.Fatal(st.Used())
		}
	}
}
