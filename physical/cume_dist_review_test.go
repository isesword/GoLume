package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func cumeReviewWindow(t *testing.T, input source.Source, q *memory.QueryBudget, mode WindowExecutionMode, desc bool) source.Source {
	t.Helper()
	win, err := NewWindowSourceWithOptions(input, []string{"g"}, []SortKey{{Column: "key", Descending: desc, NullsLast: desc}}, []WindowExpression{
		{Function: WindowCumeDist, Alias: "key"}, {Function: WindowCumeDist, Alias: "cd"}, {Function: WindowPercentRank, Alias: "pr"}, {Function: WindowRank, Alias: "rank"},
	}, []memory.Field{{Name: "key", Type: memory.TypeFloat64}, {Name: "cd", Type: memory.TypeFloat64}, {Name: "pr", Type: memory.TypeFloat64}, {Name: "rank", Type: memory.TypeInt64}}, q, WindowSourceOptions{Mode: mode})
	if err != nil {
		t.Fatal(err)
	}
	return win
}

func TestCumeDistReviewSpecialPeers(t *testing.T) {
	for _, desc := range []bool{false, true} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprintf("desc=%v/mode=%d", desc, mode), func(t *testing.T) {
				order := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
				ranks := []int64{1, 1, 3, 4, 4, 6, 7, 7, 1, 1, 1, 1}
				ends := []int64{2, 2, 3, 5, 5, 6, 8, 8, 1, 3, 3, 3}
				if desc {
					order = []int{6, 7, 5, 3, 4, 2, 0, 1, 8, 9, 10, 11}
					ranks = []int64{7, 7, 6, 4, 4, 3, 1, 1, 1, 1, 1, 1}
					ends = []int64{8, 8, 6, 5, 5, 3, 2, 2, 1, 3, 3, 3}
				}
				if mode != WindowExecutionOrdered {
					order = []int{7, 9, 2, 4, 8, 0, 11, 5, 1, 6, 10, 3}
				}
				input := percentRankReviewInput(t, order)
				ctx, q := percentRankReviewQuery(t, 8<<20, true)
				win := cumeReviewWindow(t, input, q, mode, desc)
				st, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				out := memory.NewBatch()
				seen := 0
				for {
					ok, err := st.Next(ctx, out)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					for row, id64 := range out.Int64s["id"].Values {
						id := int(id64)
						if seen >= len(order) || id != order[seen] {
							t.Fatal("original order", seen, id)
						}
						seen++
						n := int64(8)
						if id == 8 {
							n = 1
						} else if id >= 9 {
							n = 3
						}
						cd := float64(ends[id]) / float64(n)
						pr := 0.0
						if n > 1 {
							pr = float64(ranks[id]-1) / float64(n-1)
						}
						for name, want := range map[string]float64{"key": cd, "cd": cd, "pr": pr} {
							v := out.Float64s[name]
							if v == nil || !v.IsValid(row) || v.Values[row] != want {
								t.Fatalf("%s id%d got %v want %g", name, id, v, want)
							}
						}
						if out.Int64s["rank"].Values[row] != ranks[id] {
							t.Fatal("rank alias isolation", id)
						}
					}
				}
				if seen != len(order) {
					t.Fatal("missing", seen)
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
				if input.opens != 1 || input.closes != 1 || q.Used() != 0 {
					t.Fatal("lifecycle", input.opens, input.closes, q.Used())
				}
			})
		}
	}
}

func cumeReviewLongInput(t *testing.T, reverse bool) (*percentRankReviewSource, []int) {
	t.Helper()
	const total = 287
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeInt64})
	order := make([]int, total)
	var batches []*memory.Batch
	for i := 0; i < total; i++ {
		id := i
		if reverse {
			id = total - 1 - i
		}
		order[i] = id
		g, key := int64(0), int64(0)
		if id >= 193 {
			key = 1
		}
		if id >= 257 {
			g = 1
			key = int64(id - 257)
		}
		if id >= 268 {
			g = 2
			key = 0
		}
		b := memory.NewBatch()
		b.Reset(1)
		b.Int64s["id"] = &memory.Int64Vector{Values: []int64{int64(id)}}
		b.Int64s["g"] = &memory.Int64Vector{Values: []int64{g}}
		b.Int64s["key"] = &memory.Int64Vector{Values: []int64{key}}
		batches = append(batches, b)
	}
	input, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	return &percentRankReviewSource{Source: input}, order
}

func TestCumeDistReviewLongPeersAndDistinct(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			input, order := cumeReviewLongInput(t, mode != WindowExecutionOrdered)
			ctx, q := percentRankReviewQuery(t, 8<<20, true)
			st, err := source.OpenBatchStream(ctx, cumeReviewWindow(t, input, q, mode, false), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 4})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			out := memory.NewBatch()
			seen := 0
			for {
				ok, err := st.Next(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				for row, id64 := range out.Int64s["id"].Values {
					id := int(id64)
					if seen >= len(order) || id != order[seen] {
						t.Fatal("original order")
					}
					seen++
					cd, pr, rank := 193.0/257, 0.0, int64(1)
					if id >= 193 {
						cd = 1
						pr = 193.0 / 256
						rank = 194
					}
					if id >= 257 {
						cd = float64(id-256) / 11
						pr = float64(id-257) / 10
						rank = int64(id - 256)
					}
					if id >= 268 {
						cd = 1
						pr = 0
						rank = 1
					}
					for name, want := range map[string]float64{"key": cd, "cd": cd, "pr": pr} {
						v := out.Float64s[name]
						if !v.IsValid(row) || v.Values[row] != want {
							t.Fatalf("%s id%d got%g want%g", name, id, v.Values[row], want)
						}
					}
					if out.Int64s["rank"].Values[row] != rank {
						t.Fatal("rank", id)
					}
				}
				if seen == len(order) && q.Used() == 0 {
					t.Fatal("last borrowed page released early")
				}
			}
			if seen != len(order) {
				t.Fatal("missing rows", seen)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			if input.opens != 1 || input.closes != 1 || q.Used() != 0 {
				t.Fatal("cleanup", input.opens, input.closes, q.Used())
			}
			store, err := q.SpillManager()
			if err != nil {
				t.Fatal(err)
			}
			if store.(*memory.QuerySpillStore).Used() != 0 {
				t.Fatal("disk leak")
			}
		})
	}
}

func TestCumeDistReviewCancelOrCloseAfterFirstPage(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, cancel := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode=%d/cancel=%v", mode, cancel), func(t *testing.T) {
				input, _ := cumeReviewLongInput(t, false)
				ctx, q := percentRankReviewQuery(t, 8<<20, true)
				st, err := source.OpenBatchStream(ctx, cumeReviewWindow(t, input, q, mode, false), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 4})
				if err != nil {
					t.Fatal(err)
				}
				out := memory.NewBatch()
				ok, err := st.Next(ctx, out)
				if err != nil || !ok {
					t.Fatal(ok, err)
				}
				if cancel {
					c, stop := context.WithCancel(ctx)
					stop()
					ok, err = st.Next(c, memory.NewBatch())
					if ok || !errors.Is(err, context.Canceled) {
						t.Fatal("cancel", ok, err)
					}
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
				if input.opens != 1 || input.closes != 1 || q.Used() != 0 {
					t.Fatal("cleanup", input.opens, input.closes, q.Used())
				}
				store, err := q.SpillManager()
				if err != nil {
					t.Fatal(err)
				}
				if store.(*memory.QuerySpillStore).Used() != 0 {
					t.Fatal("disk leak")
				}
			})
		}
	}
}

// A first output inside a long peer leaves two independently owned decoded
// pages alive. Peeking must not retire the future page needed by output replay.
func TestCumeDistReviewTwoOwnersRetirementAndCancellation(t *testing.T) {
	for _, disk := range []bool{false, true} {
		for _, cancel := range []bool{false, true} {
			t.Run(fmt.Sprintf("disk=%v/cancel=%v", disk, cancel), func(t *testing.T) {
				input, _ := cumeReviewLongInput(t, false)
				ctx, q := percentRankReviewQuery(t, 8<<20, disk)
				owner := cumeSpoolOwner(input, q, true)
				owner.order = []SortKey{{Column: "key"}}
				stream, err := newPercentRankPartitionStream(ctx, owner, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 4})
				if err != nil {
					t.Fatal(err)
				}
				s := stream.(*percentRankPartitionStream)
				defer s.Close()
				out := memory.NewBatch()
				ok, err := s.Next(ctx, out)
				if err != nil || !ok {
					t.Fatal(ok, err)
				}
				if s.decoded == nil || s.peekDecoded == nil || s.decoded == s.peekDecoded {
					t.Fatal("expected independent live output and peek owners")
				}
				if s.peerEnd != 193 || s.peekPosition != 193 || s.position != 1 {
					t.Fatal("cursor positions", s.peerEnd, s.peekPosition, s.position)
				}
				if s.peekPage <= 0 || s.peekPage >= len(s.pages.Values) {
					t.Fatal("peek did not cross pages")
				}
				if s.pages.Values[0].handle != nil || s.pages.Values[0].encoded != nil {
					t.Fatal("output-decoded page was not retired")
				}
				future := s.pages.Values[s.peekPage]
				if disk && future.handle == nil || !disk && future.encoded == nil {
					t.Fatal("peek prematurely retired future output page")
				}
				if out.Float64s["cume"].Values[0] != 193.0/257 || q.Used() == 0 {
					t.Fatal("output or owner charge")
				}
				if cancel {
					c, stop := context.WithCancel(ctx)
					stop()
					ok, err = s.Next(c, memory.NewBatch())
					if ok || !errors.Is(err, context.Canceled) {
						t.Fatal("cancel", ok, err)
					}
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				if s.decoded != nil || s.peekDecoded != nil || s.peerLease != nil || s.outputLease != nil || s.owner != nil || q.Used() != 0 {
					t.Fatal("two-owner cleanup", q.Used())
				}
				if input.opens != 1 || input.closes != 1 {
					t.Fatal("source lifecycle", input.opens, input.closes)
				}
				d := s.StreamDiagnostics()
				if d["partition_replay_pages"] != "1" || d["peer_lookahead_pages"] == "0" || d["peer_lookahead_pages"] == "" {
					t.Fatal("cold cursor counters", d)
				}
				if disk {
					store, e := q.SpillManager()
					if e != nil {
						t.Fatal(e)
					}
					if store.(*memory.QuerySpillStore).Used() != 0 {
						t.Fatal("disk leak")
					}
				}
			})
		}
	}
}
