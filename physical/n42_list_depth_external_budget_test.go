package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var n42IO = errors.New("n42 injected store failure")

type n42Fault struct {
	q                     *memory.QueryBudget
	src                   *n40Source
	peakSnapshot          memory.BudgetSnapshot
	sourceOverlap         int64
	calls, fail, cancelAt int
	stop                  context.CancelFunc
}
type n42Manager struct {
	memory.Manager
	f *n42Fault
}

func (m n42Manager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.f.calls++
	if m.f.calls == m.f.fail {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if e == nil && m.f.q != nil {
		snap := m.f.q.Snapshot()
		if snap.UsedBytes > m.f.peakSnapshot.UsedBytes {
			m.f.peakSnapshot = snap
		}
		if m.f.src.live > 0 && snap.UsedBytes > m.f.sourceOverlap {
			m.f.sourceOverlap = snap.UsedBytes
		}
	}
	if e == nil && m.f.calls == m.f.cancelAt {
		m.f.stop()
	}
	return r, e
}

type n42Store struct {
	*memory.QuerySpillStore
	t                                     *testing.T
	peak                                  int64
	files, writes, writeFail, cancelWrite int
	stop                                  context.CancelFunc
	deleteFail                            bool
}

func (s *n42Store) observe() {
	entries, e := os.ReadDir(s.Directory())
	if e != nil {
		s.t.Fatal(e)
	}
	var bytes int64
	files := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			st, e := entry.Info()
			if e != nil {
				s.t.Fatal(e)
			}
			bytes += st.Size()
			files++
		}
	}
	if bytes != s.Used() {
		s.t.Fatalf("real file bytes%d != quota Used%d", bytes, s.Used())
	}
	if bytes > s.peak {
		s.peak = bytes
	}
	if files > s.files {
		s.files = files
	}
}
func (s *n42Store) SpillWithMetadata(ctx context.Context, obj memory.SpillObject, m memory.Manager) (memory.SpillHandle, error) {
	s.writes++
	if s.writeFail == s.writes {
		return nil, n42IO
	}
	h, e := s.QuerySpillStore.SpillWithMetadata(ctx, obj, m)
	if e == nil {
		s.observe()
		if s.cancelWrite == s.writes {
			s.stop()
		}
	}
	return h, e
}
func (s *n42Store) Delete(ctx context.Context, h memory.SpillHandle) error {
	if s.deleteFail {
		return n42IO
	}
	return s.QuerySpillStore.Delete(ctx, h)
}

type n42Measure struct {
	ram, disk                    int64
	calls, writes, files, levels int
	runs                         int64
}

func n42Run(t *testing.T, dir string, text, many bool, ram, disk int64, fail, cancelAt, writeFail, cancelWrite int, deleteFail bool, want error) n42Measure {
	t.Helper()
	root := memory.NewEngineBudget(ram)
	var q *memory.QueryBudget
	var store *memory.QuerySpillStore
	// Same parent and exact 10-digit random suffix width: path metadata costs are identical.
	for {
		var e error
		q, e = root.BeginQuery(memory.BudgetOptions{HardLimitBytes: ram, Spill: memory.SpillOptions{Directory: dir, MaxBytes: disk}})
		if e != nil {
			t.Fatal(e)
		}
		base, e := q.SpillManager()
		if e != nil {
			t.Fatal(e)
		}
		store = base.(*memory.QuerySpillStore)
		if len(filepath.Base(store.Directory())) == len("golume-q-")+10 {
			break
		}
		q.Close()
	}
	defer q.Close() // Real cleanup also runs if a later test assertion fails.
	base, stop := context.WithCancel(context.Background())
	defer stop()
	ctx := memory.WithQueryBudget(base, q)
	f := &n42Fault{fail: fail, cancelAt: cancelAt, stop: stop}
	src0 := n40Input(text)
	f.q = q
	f.src = src0
	var src source.Source = src0
	if many {
		src = &n41ManySource{src0}
	}
	owner, e := NewSortSourceWithOptions(src, []SortKey{{Column: "key"}}, root, SortSourceOptions{Spillable: true})
	if e != nil {
		q.Close()
		t.Fatal(e)
	}
	opts := n27Opts(q.Operator("N42Source", memory.MemoryWorkspace))
	opts.MemoryManager = n42Manager{opts.MemoryManager, f}
	opts.BatchRows = 4
	s, e := newExternalSortStream(ctx, owner.(*SortSource), opts)
	if e != nil {
		q.Close()
		if !errors.Is(e, want) {
			t.Fatal(e)
		}
		return n42Measure{}
	}
	defer s.Close()
	spy := &n42Store{QuerySpillStore: store, t: t, writeFail: writeFail, cancelWrite: cancelWrite, stop: stop, deleteFail: deleteFail}
	s.store.Close()
	s.chunkRows = 4
	s.runBytes = 1024
	s.workspace.parent = n42Manager{s.workspace.parent, f}
	s.sortBudget = n42Manager{s.sortBudget, f}
	s.encode = n42Manager{s.encode, f}
	s.restore = n42Manager{s.restore, f}
	s.metadata = n42Manager{s.metadata, f}
	s.store = spillmeta.NewStore(spy, s.metadata)
	expected := []int64{2, 6, 1, 3, 5, 7, 0, 4}
	if many {
		expected = nil
		for key := int64(0); key < 5; key++ {
			for id := key; id < 80; id += 5 {
				expected = append(expected, id)
			}
		}
	}
	b := memory.NewBatch()
	seen := 0
	var runErr error
	for {
		ok, e := s.Next(ctx, b)
		if e != nil {
			runErr = e
			break
		}
		if !ok {
			break
		}
		for i := 0; i < b.Rows; i++ {
			if seen+i >= len(expected) {
				t.Fatal("excess rows")
			}
			id := expected[seen+i]
			if b.Int64s["id"].Values[i] != id || !reflect.DeepEqual(n40Read(b.Arrays["items"], i), n40Want(text, id)) {
				t.Fatalf("independent stable oracle row%d", seen+i)
			}
		}
		seen += b.Rows
		if want == nil {
			t.Logf("published rows%d queryUsed%d classes%v operators%v", b.Rows, q.Used(), q.Snapshot().ByClass, q.Snapshot().Operators)
		}
	}
	result := n42Measure{q.Snapshot().PeakBytes, spy.peak, f.calls, spy.writes, spy.files, s.mergeLevels, s.spillRuns}
	closeErr := s.Close()
	if runErr == nil {
		runErr = closeErr
	}
	_ = s.Close()
	if want != nil {
		if !errors.Is(runErr, want) {
			t.Fatalf("expected%v got%v measure%+v", want, runErr, result)
		}
	} else {
		if runErr != nil {
			t.Fatal(runErr)
		}
		if seen != len(expected) || spy.writes == 0 || spy.files == 0 {
			t.Fatalf("missing actual disk/rows %+v rows%d", result, seen)
		}
		if many && s.mergeLevels < 2 {
			t.Fatalf("not multilevel %+v", result)
		}
		t.Logf("text%v many%v measured%+v querypathlen%d peakClasses%v peakOperators%v fullSourceOverlap%d", text, many, result, len(store.Directory()), f.peakSnapshot.ByClass, f.peakSnapshot.Operators, f.sourceOverlap)
	}
	if !deleteFail && (q.Used() != 0 || store.Used() != 0) {
		t.Fatalf("stream cleanup RAM%d disk%d", q.Used(), store.Used())
	}
	if deleteFail {
		t.Logf("delete fault before queryClose retained disk=%d RAM=%d streamCloseError=%v", store.Used(), q.Used(), closeErr)
	}
	spy.deleteFail = false
	if e := q.Close(); e != nil {
		t.Fatal(e)
	}
	if q.Used() != 0 || root.Used() != 0 || src0.opened != src0.closed {
		t.Fatalf("query cleanup RAM%d root%d opens%d closes%d", q.Used(), root.Used(), src0.opened, src0.closed)
	}
	entries, e := os.ReadDir(dir)
	if e != nil || len(entries) != 0 {
		t.Fatalf("disk cleanup entries%d err%v", len(entries), e)
	}
	return result
}
func TestN42ListDepthExternalExact(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("string%v", text), func(t *testing.T) {
			dir := t.TempDir()
			r := n42Run(t, dir, text, true, 16<<20, 64<<20, 0, 0, 0, 0, false, nil)
			t.Run("memoryexact", func(t *testing.T) { n42Run(t, dir, text, true, r.ram, 64<<20, 0, 0, 0, 0, false, nil) })
			t.Run("memoryminusone", func(t *testing.T) {
				n42Run(t, dir, text, true, r.ram-1, 64<<20, 0, 0, 0, 0, false, memory.ErrBudgetExceeded)
			})
			t.Run("diskexact", func(t *testing.T) { n42Run(t, dir, text, true, 16<<20, r.disk, 0, 0, 0, 0, false, nil) })
			t.Run("diskminusone", func(t *testing.T) {
				n42Run(t, dir, text, true, 16<<20, r.disk-1, 0, 0, 0, 0, false, memory.ErrSpillQuotaExceeded)
			})
		})
	}
}
func TestN42ListDepthExternalFaults(t *testing.T) {
	dir := t.TempDir()
	r := n42Run(t, dir, false, true, 16<<20, 64<<20, 0, 0, 0, 0, false, nil)
	for at := 1; at <= r.calls; at++ {
		t.Run(fmt.Sprintf("reserve%d", at), func(t *testing.T) {
			n42Run(t, dir, false, true, 16<<20, 64<<20, at, 0, 0, 0, false, memory.ErrBudgetExceeded)
		})
		t.Run(fmt.Sprintf("cancel%d", at), func(t *testing.T) { n42Run(t, dir, false, true, 16<<20, 64<<20, 0, at, 0, 0, false, context.Canceled) })
	}
	for _, mode := range []string{"write", "cancelwrite", "delete"} {
		t.Run(mode, func(t *testing.T) {
			w, c := 0, 0
			d := false
			want := n42IO
			if mode == "write" {
				w = 25
			} else if mode == "cancelwrite" {
				c = 25
				want = context.Canceled
			} else {
				d = true
			}
			n42Run(t, t.TempDir(), false, !d, 16<<20, 64<<20, 0, 0, w, c, d, want)
		})
	}
}
