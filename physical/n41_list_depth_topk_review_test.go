package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source"
	"strings"
	"testing"
)

type n41ReviewSource struct {
	*n40ReviewSource
	opens, stats, parts int
	forbidden           bool
}

func (s *n41ReviewSource) Replayability() source.Replayability { return source.ReplayStable }
func (s *n41ReviewSource) Stats(context.Context) (source.Stats, error) {
	s.stats++
	return source.Stats{}, errors.New("must not ask Stats")
}
func (s *n41ReviewSource) Partitions(context.Context) ([]source.Partition, error) {
	s.parts++
	return nil, errors.New("must not ask Partitions")
}
func (s *n41ReviewSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	if s.forbidden {
		return nil, errors.New("key source opened")
	}
	return s.n40ReviewSource.NewBatchStream(ctx, o)
}
func TestN41ReviewForcedOnePassAndZero(t *testing.T) {
	for _, k := range []int{0, 1, 2, 8} {
		for _, text := range []bool{false, true} {
			input := &n41ReviewSource{n40ReviewSource: n40ReviewFixture(text)}
			input.poison = true
			key := &n41ReviewSource{n40ReviewSource: n40ReviewFixture(text), forbidden: true}
			key.schema = memory.NewSchema(key.schema.Fields[0])
			m := memory.NewBudgetManager(4 << 20)
			top, e := NewTopKSourceWithKeyInput(input, key, k, []SortKey{{Column: "k"}}, m)
			if e != nil {
				t.Fatal(e)
			}
			if !strings.Contains(source.StreamPlanOf(top).Detail, "strategy=one-pass-snapshots") {
				t.Fatal("plan strategy")
			}
			st, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
			if e != nil {
				t.Fatal(e)
			}
			d := st.(source.BatchStreamDiagnostics).StreamDiagnostics()
			if d["strategy"] != "one-pass-snapshots" {
				t.Fatal("execution strategy", d)
			}
			out := memory.NewBatch()
			n := 0
			for {
				ok, e := st.Next(context.Background(), out)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				leaf := out.Arrays["v"].Child.Child.Child
				for r := 0; r < out.Rows; r++ {
					if text {
						if string(leaf.Strings.ValueBytes(r)) != []string{"a", "b", "c"}[n] {
							t.Fatal("string tie/alias", n)
						}
					} else if leaf.Int64.Values[r] != []int64{10, 11, 20}[n] {
						t.Fatal("int tie/alias", n)
					}
					n++
				}
			}
			st.Close()
			want := k
			if want > 3 {
				want = 3
			}
			if n != want || key.opens != 0 || key.stats != 0 || key.parts != 0 || m.Used() != 0 {
				t.Fatal("route/cleanup", k, n, key.opens, key.stats, key.parts, m.Used())
			}
			if k == 0 && input.opens != 0 {
				t.Fatal("K0 opened full source")
			}
			if k > 0 && (input.opens != 1 || input.closes != 1) {
				t.Fatal("replay happened")
			}
		}
	}
}
func TestN41ReviewInvalidLoserAndStrictOwner(t *testing.T) {
	for _, hidden := range []bool{false, true} {
		s := n40ReviewFixture(false)
		s.batch.Int64s["k"].Values = []int64{1, 2, 3}
		if hidden {
			s.batch.Arrays["v"].Validity = memory.Bitmap{3}
			s.batch.Arrays["v"].Child.Offsets[3] = 4
		} else {
			s.schema.Fields[1].Children[0].Children[0].Children[0].Nullable = false
			s.batch.Arrays["v"].Child.Child.Child.Int64.Validity = memory.Bitmap{3}
		}
		m := memory.NewBudgetManager(4 << 20)
		top, e := NewTopKSourceWithManager(&n41ReviewSource{n40ReviewSource: s}, 1, []SortKey{{Column: "k"}}, m)
		if e != nil {
			t.Fatal(e)
		}
		st, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		out := memory.NewBatch()
		ok, e := st.Next(context.Background(), out)
		if ok || e == nil || out.Rows != 0 {
			t.Fatal("invalid loser ignored", hidden, ok, e)
		}
		st.Close()
		if m.Used() != 0 || s.closes != 1 {
			t.Fatal("loser cleanup")
		}
	}
}

type n41ReviewReleaseManager struct {
	*memory.BudgetManager
	armed  bool
	cancel context.CancelFunc
	hits   int
}
type n41ReviewLease struct {
	memory.Reservation
	m *n41ReviewReleaseManager
}

func (m *n41ReviewReleaseManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	r, e := m.BudgetManager.Reserve(ctx, n)
	if e != nil {
		return nil, e
	}
	return &n41ReviewLease{r, m}, nil
}
func (r *n41ReviewLease) Release() {
	r.Reservation.Release()
	if r.m.armed {
		r.m.hits++
		r.m.cancel()
	}
}
func TestN41ReviewWinnerReleaseCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &n41ReviewReleaseManager{BudgetManager: memory.NewBudgetManager(4 << 20), cancel: cancel}
	s := n40ReviewFixture(false)
	s.cancel = func() { m.armed = true }
	top, e := NewTopKSourceWithManager(&n41ReviewSource{n40ReviewSource: s}, 2, []SortKey{{Column: "k"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	st, e := source.OpenBatchStream(ctx, top, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	out := memory.NewBatch()
	ok, e := st.Next(ctx, out)
	if ok || !errors.Is(e, context.Canceled) || out.Rows != 0 || m.hits == 0 {
		t.Fatal("winner release cancellation escaped", ok, e, m.hits)
	}
	st.Close()
	if m.Used() != 0 || s.closes != 1 {
		t.Fatal("release cleanup")
	}
}
