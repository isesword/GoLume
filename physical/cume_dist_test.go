package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"strconv"
	"testing"
)

func cumeDistFixture(t *testing.T, n int, reverse bool, peerSize int) source.Source {
	t.Helper()
	b := memory.NewBatch()
	b.Reset(n)
	for _, name := range []string{"g", "k", "id", "v"} {
		b.Int64s[name] = &memory.Int64Vector{Values: make([]int64, n)}
	}
	for i := 0; i < n; i++ {
		id := i
		if reverse {
			id = n - 1 - i
		}
		b.Int64s["id"].Values[i] = int64(id)
		b.Int64s["k"].Values[i] = int64(id / peerSize)
		b.Int64s["v"].Values[i] = int64(id * 2)
	}
	sc := memory.NewSchema(memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "k", Type: memory.TypeInt64}, memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64})
	src, e := inmemory.New(sc, b)
	if e != nil {
		t.Fatal(e)
	}
	return src
}
func cumeDistWindow(t *testing.T, src source.Source, q memory.Manager, mode WindowExecutionMode, mixed bool) source.Source {
	t.Helper()
	ex := []WindowExpression{{Function: WindowCumeDist, Alias: "cd"}}
	fields := []memory.Field{{Name: "cd", Type: memory.TypeFloat64}}
	if mixed {
		ex = append(ex, WindowExpression{Function: WindowPercentRank, Alias: "pr"})
		fields = append(fields, memory.Field{Name: "pr", Type: memory.TypeFloat64})
	}
	win, e := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "k"}}, ex, fields, q, WindowSourceOptions{Mode: mode})
	if e != nil {
		t.Fatal(e)
	}
	return win
}
func TestCumeDistThreePathsPeers(t *testing.T) {
	for _, n := range []int{0, 1, 65, 2051} {
		for _, peer := range []int{1, 3, 4096} {
			for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
				t.Run(fmt.Sprintf("n%d/peer%d/mode%d", n, peer, mode), func(t *testing.T) {
					ctx, q := percentRankQuery(t, 8<<20, true)
					reverse := mode != WindowExecutionOrdered
					win := cumeDistWindow(t, cumeDistFixture(t, n, reverse, peer), q, mode, true)
					st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 7})
					if e != nil {
						t.Fatal(e)
					}
					defer st.Close()
					out := memory.NewBatch()
					seen := 0
					for {
						ok, e := st.Next(ctx, out)
						if e != nil {
							t.Fatal(e)
						}
						if !ok {
							break
						}
						for i := 0; i < out.Rows; i++ {
							id := seen + i
							if reverse {
								id = n - 1 - seen - i
							}
							cd := float64(min((id/peer+1)*peer, n)) / float64(n)
							pr := 0.0
							if n > 1 {
								pr = float64(id/peer*peer) / float64(n-1)
							}
							if out.Int64s["id"].Values[i] != int64(id) || !out.Float64s["cd"].IsValid(i) || out.Float64s["cd"].Values[i] != cd || out.Float64s["pr"].Values[i] != pr {
								t.Fatal("peer/order oracle", id, cd, pr)
							}
						}
						seen += out.Rows
					}
					if seen != n {
						t.Fatal("rows", seen)
					}
					if e := st.Close(); e != nil {
						t.Fatal(e)
					}
					if q.Used() != 0 {
						t.Fatal("leak", q.Used())
					}
				})
			}
		}
	}
}

// The original physical fixed resource case is unchanged: 262144 rows, four
// Int64 inputs, batch1024, hard4MiB and disk256MiB. Both modes must show actual
// peek and replay reads, not inferred query aggregate spill traffic.
func TestCumeDistFixedStreamingResources(t *testing.T) {
	const n = 262144
	for _, mixed := range []bool{false, true} {
		for _, mode := range []WindowExecutionMode{WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprintf("mixed%t/mode%d", mixed, mode), func(t *testing.T) {
				ctx, q := percentRankQuery(t, 4<<20, true)
				reverse := mode == WindowExecutionExternalSort
				win := cumeDistWindow(t, cumeDistFixture(t, n, reverse, 3), q, mode, mixed)
				st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1024})
				if e != nil {
					t.Fatal(e)
				}
				defer st.Close()
				out := memory.NewBatch()
				seen := 0
				for {
					ok, e := st.Next(ctx, out)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					for i := 0; i < out.Rows; i++ {
						id := seen + i
						if reverse {
							id = n - 1 - seen - i
						}
						if out.Int64s["id"].Values[i] != int64(id) || out.Float64s["cd"].Values[i] != float64(min((id/3+1)*3, n))/float64(n) {
							t.Fatal("fixed cume oracle", id)
						}
						if mixed && out.Float64s["pr"].Values[i] != float64(id/3*3)/float64(n-1) {
							t.Fatal("fixed rank oracle", id)
						}
					}
					seen += out.Rows
				}
				if seen != n {
					t.Fatal("fixed rows", seen)
				}
				diag := st.(source.BatchStreamDiagnostics).StreamDiagnostics()
				keys := []string{"partition_spool_pages", "partition_spool_bytes", "partition_spool_partitions", "peer_lookahead_pages", "partition_replay_pages"}
				if mode == WindowExecutionExternalSort {
					keys = append(keys, "front_sort_spill_runs", "front_sort_spill_bytes", "restore_sort_spill_runs", "restore_sort_spill_bytes")
				}
				for _, key := range keys {
					v, e := strconv.ParseInt(diag[key], 10, 64)
					if e != nil || v <= 0 {
						t.Fatal("missing actual stage", key, diag)
					}
				}
				if e := st.Close(); e != nil {
					t.Fatal(e)
				}
				if q.Used() != 0 {
					t.Fatal("fixed leak", q.Used())
				}
			})
		}
	}
}
func TestCumeDistFixedNoDiskFailure(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		ctx, q := percentRankQuery(t, 4<<20, false)
		win := cumeDistWindow(t, cumeDistFixture(t, 262144, false, 3), q, WindowExecutionOrdered, mixed)
		st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1024})
		if e != nil {
			t.Fatal(e)
		}
		_, e = st.Next(ctx, memory.NewBatch())
		if !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("fixed no-disk expected resource failure", e)
		}
		if e = st.Close(); e != nil {
			t.Fatal(e)
		}
		if q.Used() != 0 {
			t.Fatal("failure leak", q.Used())
		}
	}
}
func TestCumeDistDiagnosticColdCounters(t *testing.T) {
	m := memory.NewBudgetManager(0)
	d, e := newPercentRankDiagnostics(context.Background(), m)
	if e != nil {
		t.Fatal(e)
	}
	d.capture(1, map[string]string{"partition_spool_pages": "3", "partition_spool_bytes": "300", "partition_spool_partitions": "1", "peer_lookahead_pages": "3", "partition_replay_pages": "2"})
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	got := d.Snapshot()
	if got["peer_lookahead_pages"] != "3" || got["partition_replay_pages"] != "2" || m.Used() != 0 {
		t.Fatal("cold", got, m.Used())
	}
}
