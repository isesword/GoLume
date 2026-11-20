package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"testing"
	"unsafe"
)

// Reuse N40's fully charged, genuinely reused source buffers; advertise replay
// to prove List3 still chooses the one-pass snapshot route and skips keyInput.
type n41Source struct {
	*n40Source
	badLoser bool
}

func (s *n41Source) Replayability() source.Replayability { return source.ReplayStable }
func (s *n41Source) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	st, e := s.n40Source.NewBatchStream(ctx, o)
	if e != nil {
		return nil, e
	}
	return &n41Stream{BatchStream: st, src: s}, nil
}

type n41Stream struct {
	source.BatchStream
	src   *n41Source
	pages int
}

func (s *n41Stream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	ok, e := s.BatchStream.Next(ctx, b)
	if ok && s.src.badLoser {
		s.pages++
		leaf := b.Arrays["items"].Child.Child.Child
		bits := uint64(13)
		if s.pages == 2 {
			bits = 5
		}
		if leaf.Int64 != nil {
			leaf.Int64.Validity[0] = bits
		} else {
			leaf.Strings.Validity[0] = bits
		}
	}
	return ok, e
}

type n41Probe struct {
	completedReplacementPeak int64
	memory.Manager
	src                                        *n40Source
	stream                                     *topKContainerSnapshotStream
	calls, fail, cancelAt, releases, releaseAt int
	stop                                       context.CancelFunc
	peak, atomicPeak, pagePeak, sourcePeak     int64
	atomicBuilding                             bool
	atomicSeen, pageSeen                       bool
	trace                                      []string
}

func n41Credits(w *sortWorkspace) int64 {
	if w == nil {
		return 0
	}
	var n int64
	for c := w.credits; c != nil; c = c.next {
		n += c.reservation.Bytes()
	}
	return n
}
func (m *n41Probe) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	before := m.Used()
	if m.fail == m.calls {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if e != nil {
		return nil, e
	}
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	if m.src.live > 0 && m.Used() > m.sourcePeak {
		m.sourcePeak = m.Used()
	}
	if s := m.stream; s != nil {
		var winners int64
		live := 0
		for i := range s.heap.items {
			c := &s.heap.items[i]
			if c.owner != nil {
				live++
				winners += n41Credits(c.workspace)
				if c.lease != nil {
					winners += c.lease.Bytes()
				}
			}
		}
		if !s.prepared && live == s.owner.k && s.page == nil && n == 2048+int64(len(s.owner.keys))*int64(unsafe.Sizeof(sortCell{})) {
			m.atomicBuilding = true
		}
		if !s.prepared && live == s.owner.k && s.page == nil && m.atomicBuilding {
			m.atomicSeen = true
			if m.Used() > m.atomicPeak {
				m.atomicPeak = m.Used()
			}
		}
		if s.prepared && live > 0 && s.pageWorkspace != nil {
			m.pageSeen = true
			if m.Used() > m.pagePeak {
				m.pagePeak = m.Used()
			}
		}
		m.trace = append(m.trace, fmt.Sprintf("call%d before%d new%d after%d source%d winners%d live%d prepared%v page%d", m.calls, before, n, m.Used(), m.src.live, winners, live, s.prepared, n41Credits(s.pageWorkspace)))
	}
	if m.calls == m.cancelAt {
		m.stop()
	}
	return &n41Lease{Reservation: r, m: m}, nil
}

type n41Lease struct {
	memory.Reservation
	m    *n41Probe
	done bool
}

func (r *n41Lease) Release() {
	if !r.done {
		r.done = true
		if st := r.m.stream; st != nil && !st.prepared && len(st.heap.items) == st.owner.k {
			for i := range st.heap.items {
				if st.heap.items[i].owner != nil && st.heap.items[i].key.ordinal == st.rows {
					if r.m.Used() > r.m.completedReplacementPeak {
						r.m.completedReplacementPeak = r.m.Used()
					}
					break
				}
			}
		}
		r.Reservation.Release()
		r.m.releases++
		r.m.atomicBuilding = false
		if r.m.releases == r.m.releaseAt {
			r.m.stop()
		}
	}
}
func n41Run(t *testing.T, text bool, limit int64, fail, cancelAt, releaseAt int, preparedCancel, firstOnly bool, want error) (peak int64, calls, releases int) {
	t.Helper()
	base, stop := context.WithCancel(context.Background())
	defer stop()
	ctx := &n40Checks{Context: base}
	src := &n41Source{n40Source: n40Input(text)}
	keys := &n41Source{n40Source: n40Input(text)}
	m := &n41Probe{Manager: memory.NewBudgetManager(limit), src: src.n40Source, fail: fail, cancelAt: cancelAt, releaseAt: releaseAt, stop: stop}
	op, e := NewTopKSourceWithKeyInput(src, keys, 3, []SortKey{{Column: "key"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	opts := n27Opts(m)
	opts.BatchRows = 1
	st, e := source.OpenBatchStream(ctx, op, opts)
	rows := 0
	if e == nil {
		var ok bool
		m.stream, ok = st.(*topKContainerSnapshotStream)
		if !ok {
			t.Fatalf("replay List3 not forced snapshot %T", st)
		}
		if preparedCancel {
			ctx.ready = func() bool { return m.stream.prepared }
		}
		b := memory.NewBatch()
		order := []int64{2, 6, 1}
		for {
			ok, e = st.Next(ctx, b)
			if e != nil {
				if ok || len(b.Arrays) > 0 || len(b.Int64s) > 0 {
					t.Fatal("partial cancelled/failed publication")
				}
				break
			}
			if !ok {
				break
			}
			if b.Rows != 1 {
				t.Fatal("bounded pageRows1 violated")
			}
			id := order[rows]
			if b.Int64s["id"].Values[0] != id || !reflect.DeepEqual(n40Read(b.Arrays["items"], 0), n40Want(text, id)) {
				t.Fatal("stable independent winner oracle")
			}
			rows++
			if firstOnly {
				break
			}
		}
	}
	peak, calls, releases = m.peak, m.calls, m.releases
	if want != nil {
		if !errors.Is(e, want) {
			t.Fatalf("expected%v got%v calls%d releases%d", want, e, calls, releases)
		}
	} else {
		if e != nil {
			t.Fatal(e)
		}
		if !firstOnly && rows != 3 {
			t.Fatal("row count", rows)
		}
		if !m.atomicSeen || !m.pageSeen {
			t.Fatalf("missing K+1/page overlap atomic%v page%v", m.atomicSeen, m.pageSeen)
		}
		if m.completedReplacementPeak == 0 {
			t.Fatal("no completed candidate replacement observed before old-credit release")
		}
		t.Logf("completed replacement charged peak=%d", m.completedReplacementPeak)
		t.Logf("text%v peak%d sourcePeak%d atomicKplus1Peak%d pageWinnerPeak%d calls%d releasepoints%d trace%v", text, peak, m.sourcePeak, m.atomicPeak, m.pagePeak, calls, releases, m.trace)
	}
	m.releaseAt = 0
	m.cancelAt = 0
	m.fail = 0
	ctx.ready = nil
	if st != nil {
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if m.Used() != 0 || src.opened != src.closed || keys.opened != 0 || src.opened > 1 {
		t.Fatalf("cleanup used%d fullopen%d close%d keyopens%d", m.Used(), src.opened, src.closed, keys.opened)
	}
	return
}
func TestN41ListDepthTopKBudget(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("string%v", text), func(t *testing.T) {
			peak, calls, _ := n41Run(t, text, 8<<20, 0, 0, 0, false, false, nil)
			t.Run("exact", func(t *testing.T) { n41Run(t, text, peak, 0, 0, 0, false, false, nil) })
			t.Run("minusone", func(t *testing.T) { n41Run(t, text, peak-1, 0, 0, 0, false, false, memory.ErrBudgetExceeded) })
			for at := 1; at <= calls; at++ {
				t.Run(fmt.Sprintf("fail%d", at), func(t *testing.T) { n41Run(t, text, 8<<20, at, 0, 0, false, false, memory.ErrBudgetExceeded) })
				t.Run(fmt.Sprintf("cancel%d", at), func(t *testing.T) { n41Run(t, text, 8<<20, 0, at, 0, false, false, context.Canceled) })
			}
		})
	}
}
func TestN41ListDepthTopKPublication(t *testing.T) {
	_, _, releases := n41Run(t, true, 8<<20, 0, 0, 0, false, true, nil)
	for at := 1; at <= releases; at++ {
		t.Run(fmt.Sprintf("release%d", at), func(t *testing.T) { n41Run(t, true, 8<<20, 0, 0, at, false, true, context.Canceled) })
	}
	t.Run("prepared", func(t *testing.T) { n41Run(t, true, 8<<20, 0, 0, 0, true, true, context.Canceled) })
}
func TestN41ListDepthTopKInvalidLoser(t *testing.T) {
	src := &n41Source{n40Source: n40Input(false), badLoser: true}
	src.schema.Fields[2].Children[0].Children[0].Children[0].Nullable = false
	m := memory.NewBudgetManager(8 << 20)
	op, e := NewTopKSourceWithManager(src, 3, []SortKey{{Column: "key"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	st, e := source.OpenBatchStream(context.Background(), op, n27Opts(m))
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if ok || e == nil || len(b.Arrays) > 0 {
		t.Fatal("invalid losing row escaped strict validation")
	}
	st.Close()
	st.Close()
	if m.Used() != 0 || src.opened != src.closed {
		t.Fatal("invalid cleanup")
	}
}

// 80 rows in 20 reused four-row source pages, not an 80-row source allocation.
type n41ManySource struct{ *n40Source }

func (s *n41ManySource) Replayability() source.Replayability { return source.ReplayStable }
func (s *n41ManySource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: 80, Partitions: 1}, nil
}
func (s *n41ManySource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: 80}}, nil
}
func (s *n41ManySource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	st, e := s.n40Source.NewBatchStream(ctx, o)
	if e != nil {
		return nil, e
	}
	return &n41ManyStream{inner: st.(*n40Stream)}, nil
}

type n41ManyStream struct {
	inner *n40Stream
	pages int
}

func (s *n41ManyStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	if s.pages == 20 {
		b.ClearColumns()
		return false, ctx.Err()
	}
	s.inner.page = s.pages % 2
	ok, e := s.inner.Next(ctx, b)
	if !ok || e != nil {
		return ok, e
	}
	leaf := b.Arrays["items"].Child.Child.Child
	for i := 0; i < 4; i++ {
		id := int64(s.pages*4 + i)
		b.Int64s["key"].Values[i] = id % 5
		b.Int64s["id"].Values[i] = id
		if leaf.Int64 != nil {
			leaf.Int64.Values[i] = n40Leaf(false, id).(int64)
		} else {
			copy(leaf.Strings.Data[i*512:(i+1)*512], n40Leaf(true, id).(string))
		}
	}
	s.pages++
	return true, nil
}
func (s *n41ManyStream) Close() error { return s.inner.Close() }
func TestN41ListDepthTopKHard64Pages(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("string%v", text), func(t *testing.T) {
			ctx := context.Background()
			src := &n41ManySource{n40Input(text)}
			m := &n41Probe{Manager: memory.NewBudgetManager(8 << 20), src: src.n40Source, stop: func() {}}
			op, e := NewTopKSourceWithManager(src, 65, []SortKey{{Column: "key"}}, m)
			if e != nil {
				t.Fatal(e)
			}
			opts := n27Opts(m)
			opts.BatchRows = 128
			st, e := source.OpenBatchStream(ctx, op, opts)
			if e != nil {
				t.Fatal(e)
			}
			var ok bool
			m.stream, ok = st.(*topKContainerSnapshotStream)
			if !ok {
				t.Fatal("not snapshot")
			}
			expected := []int64{}
			for key := int64(0); key < 5; key++ {
				for id := key; id < 80; id += 5 {
					expected = append(expected, id)
				}
			}
			b := memory.NewBatch()
			at := 0
			for page, wantRows := range []int{64, 1} {
				ok, e := st.Next(ctx, b)
				if !ok || e != nil || b.Rows != wantRows {
					t.Fatalf("page%d rows%d expected%d ok%v err%v", page, b.Rows, wantRows, ok, e)
				}
				for i := 0; i < b.Rows; i++ {
					id := expected[at+i]
					if b.Int64s["id"].Values[i] != id || !reflect.DeepEqual(n40Read(b.Arrays["items"], i), n40Want(text, id)) {
						t.Fatalf("page%d row%d stable oracle", page, i)
					}
				}
				at += b.Rows
				live := 0
				for i := range m.stream.heap.items {
					if m.stream.heap.items[i].owner != nil {
						live++
					}
				}
				if live != 65-at {
					t.Fatalf("winner release live%d remaining%d", live, 65-at)
				}
				t.Logf("page%d rows%d liveWinners%d used%d peak%d", page, b.Rows, live, m.Used(), m.peak)
			}
			ok, e = st.Next(ctx, b)
			if ok || e != nil {
				t.Fatal("EOF", ok, e)
			}
			st.Close()
			st.Close()
			if m.Used() != 0 || src.opened != 1 || src.closed != 1 {
				t.Fatal("hard64 cleanup")
			}
		})
	}
}
