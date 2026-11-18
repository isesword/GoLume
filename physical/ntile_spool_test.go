package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"strconv"
	"testing"
)

func ntileTestOwner(src source.Source, m memory.Manager, mixed bool) *percentRankSource {
	o := cumeSpoolOwner(src, m, mixed)
	if !mixed {
		o.cumeHidden = ""
	}
	o.ntilePlans = []ntileSpoolPlan{{1, "tile1"}, {2, "tile2"}, {4, "tile4"}, {math.MaxInt64, "tilemax"}}
	fields := append([]memory.Field(nil), src.Schema().Fields...)
	if mixed {
		fields = append(fields, memory.Field{Name: o.hidden, Type: memory.TypeFloat64}, memory.Field{Name: o.cumeHidden, Type: memory.TypeFloat64})
	}
	for _, p := range o.ntilePlans {
		fields = append(fields, memory.Field{Name: p.hidden, Type: memory.TypeInt64})
	}
	o.schema = memory.NewSchema(fields...)
	return o
}
func TestNTileSpoolMultipleBucketsAndMixed(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		for _, disk := range []bool{false, true} {
			t.Run(fmt.Sprint(mixed, disk), func(t *testing.T) {
				ctx, q := percentRankReviewQuery(t, 8<<20, disk)
				src := cumeSafeByteInput(t, memory.TypeBinary, true)
				st, err := newPercentRankPartitionStream(ctx, ntileTestOwner(src, q, mixed), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
				if err != nil {
					t.Fatal(err)
				}
				s := st.(*percentRankPartitionStream)
				b := memory.NewBatch()
				count := 0
				want2 := []int64{1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 2}
				want4 := []int64{1, 1, 2, 2, 3, 4, 1, 1, 2, 3, 4}
				for {
					ok, err := s.Next(ctx, b)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					for row := 0; row < b.Rows; row++ {
						pos := count
						if count >= 6 {
							pos -= 6
						}
						want := []int64{1, want2[count], want4[count], int64(pos + 1)}
						for i, p := range s.owner.ntilePlans {
							v := b.Int64s[p.hidden]
							if !v.IsValid(row) || v.Values[row] != want[i] {
								t.Fatal(count, p.buckets, v.Values[row], want[i])
							}
						}
						count++
					}
					if !mixed && (s.peekDecoded != nil || s.previousLease != nil || s.metrics.peekPages.Load() != 0 || s.ratio != nil || s.cume != nil) {
						t.Fatal("NTILE-only performed peer work")
					}
				}
				if count != 11 {
					t.Fatal(count)
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				d := s.StreamDiagnostics()
				replay, e := strconv.ParseInt(d["partition_replay_pages"], 10, 64)
				if e != nil || replay <= 0 {
					t.Fatal("cold replay", d)
				}
				if !mixed {
					if _, ok := d["peer_lookahead_pages"]; ok {
						t.Fatal("fabricated cold peer", d)
					}
				}
				if q.Used() != 0 || src.opens != 1 || src.closes != 1 || s.tiles != nil {
					t.Fatal("leak", q.Used(), src.opens, src.closes)
				}
			})
		}
	}
}
func ntileAdmissionRun(t *testing.T, fail int) (int, error) {
	t.Helper()
	m := &rankSpoolFailManager{Manager: memory.NewBudgetManager(8 << 20), fail: fail}
	src := cumeSafeByteInput(t, memory.TypeString, true)
	st, err := newPercentRankPartitionStream(context.Background(), ntileTestOwner(src, m, true), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2, MemoryManager: m})
	if err == nil {
		b := memory.NewBatch()
		for {
			ok, e := st.Next(context.Background(), b)
			if e != nil {
				err = e
				break
			}
			if !ok {
				break
			}
		}
		err = errors.Join(err, st.Close())
	}
	if m.Used() != 0 || src.opens > 1 || src.closes != src.opens {
		t.Fatal("leak", fail, m.Used(), src.opens, src.closes)
	}
	return m.calls, err
}
func TestNTileSpoolEveryActualAdmission(t *testing.T) {
	calls, err := ntileAdmissionRun(t, 0)
	if err != nil {
		t.Fatal(err)
	}
	for fail := 1; fail <= calls; fail++ {
		_, err = ntileAdmissionRun(t, fail)
		if !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatal(fail, err)
		}
	}
	t.Log("actual admissions", calls)
}
func TestNTileSpoolCancellationAndCorruption(t *testing.T) {
	for _, mode := range []string{"cancel", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			ctx, q := percentRankReviewQuery(t, 8<<20, true)
			src := cumeSafeByteInput(t, memory.TypeBinary, true)
			st, err := newPercentRankPartitionStream(ctx, ntileTestOwner(src, q, false), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
			if err != nil {
				t.Fatal(err)
			}
			s := st.(*percentRankPartitionStream)
			b := memory.NewBatch()
			if mode == "corrupt" {
				ok, e := s.capturePartition(ctx)
				if e != nil || !ok {
					t.Fatal(ok, e)
				}
				s.pages.Values[0].rows = 0
				_, err = s.Next(ctx, b)
				if !errors.Is(err, memory.ErrSpillCorrupt) {
					t.Fatal(err)
				}
			} else {
				ok, e := s.Next(ctx, b)
				if e != nil || !ok {
					t.Fatal(ok, e)
				}
				c, cancel := context.WithCancel(ctx)
				cancel()
				_, err = s.Next(c, b)
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			if err = q.Close(); err != nil {
				t.Fatal(err)
			}
			if q.Used() != 0 || src.closes != 1 || s.tiles != nil || s.owner != nil {
				t.Fatal("cleanup", q.Used())
			}
		})
	}
}

func ntileFixedWindow(t *testing.T, src source.Source, m memory.Manager, mode WindowExecutionMode, mixed bool) source.Source {
	t.Helper()
	expressions := []ConfiguredWindowExpression{{Expr: WindowExpression{Function: WindowNTile, Alias: "tile"}, Buckets: 7}}
	fields := []memory.Field{{Name: "tile", Type: memory.TypeInt64}}
	if mixed {
		expressions = append(expressions, ConfiguredWindowExpression{Expr: WindowExpression{Function: WindowPercentRank, Alias: "pr"}}, ConfiguredWindowExpression{Expr: WindowExpression{Function: WindowCumeDist, Alias: "cd"}})
		fields = append(fields, memory.Field{Name: "pr", Type: memory.TypeFloat64}, memory.Field{Name: "cd", Type: memory.TypeFloat64})
	}
	w, err := NewConfiguredWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "id"}}, expressions, fields, m, WindowSourceOptions{Mode: mode})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// ntileSevenOracle independently enumerates the seven bucket sizes, avoiding
// the production quotient/cut helper and any position*buckets multiplication.
func ntileSevenOracle(position, n int) int64 {
	end := 0
	for bucket := 1; bucket <= 7; bucket++ {
		size := n / 7
		if bucket <= n%7 {
			size++
		}
		end += size
		if position < end {
			return int64(bucket)
		}
	}
	return 0
}

// Fixed physical resources remain 262144 rows, four Int64 inputs, batch1024,
// hard4MiB and disk256MiB. Original input order must survive external restore.
func TestNTileFixedStreamingResources(t *testing.T) {
	const n = 262144
	for _, mixed := range []bool{false, true} {
		for _, mode := range []WindowExecutionMode{WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprint(mixed, mode), func(t *testing.T) {
				ctx, q := percentRankQuery(t, 4<<20, true)
				reverse := mode == WindowExecutionExternalSort
				w := ntileFixedWindow(t, cumeDistFixture(t, n, reverse, 3), q, mode, mixed)
				st, err := source.OpenBatchStream(ctx, w, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1024})
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				b := memory.NewBatch()
				count := 0
				for {
					ok, err := st.Next(ctx, b)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					for row := 0; row < b.Rows; row++ {
						id := count + row
						if reverse {
							id = n - 1 - id
						}
						if b.Int64s["id"].Values[row] != int64(id) || !b.Int64s["tile"].IsValid(row) || b.Int64s["tile"].Values[row] != ntileSevenOracle(id, n) {
							t.Fatal("fixed tile oracle", id)
						}
						if mixed && (b.Float64s["pr"].Values[row] != float64(id)/float64(n-1) || b.Float64s["cd"].Values[row] != float64(id+1)/float64(n)) {
							t.Fatal("fixed distributions", id)
						}
					}
					count += b.Rows
				}
				if count != n {
					t.Fatal(count)
				}
				diag := st.(source.BatchStreamDiagnostics).StreamDiagnostics()
				keys := []string{"partition_spool_pages", "partition_spool_bytes", "partition_spool_partitions", "partition_replay_pages"}
				if mixed {
					keys = append(keys, "peer_lookahead_pages", "partition_replay_pages")
				} else {
					if _, ok := diag["peer_lookahead_pages"]; ok {
						t.Fatal("NTILE-only reports peer work", diag)
					}
				}
				if reverse {
					keys = append(keys, "front_sort_spill_runs", "front_sort_spill_bytes", "restore_sort_spill_runs", "restore_sort_spill_bytes")
				}
				for _, k := range keys {
					v, e := strconv.ParseInt(diag[k], 10, 64)
					if e != nil || v <= 0 {
						t.Fatal("missing real stage", k, diag)
					}
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
				if q.Used() != 0 {
					t.Fatal("leak", q.Used())
				}
			})
		}
	}
}
func TestNTileFixedNoDiskFailure(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		for _, mode := range []WindowExecutionMode{WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprint(mixed, mode), func(t *testing.T) {
				ctx, q := percentRankQuery(t, 4<<20, false)
				src := &percentRankReviewSource{Source: cumeDistFixture(t, 262144, mode == WindowExecutionExternalSort, 3)}
				w := ntileFixedWindow(t, src, q, mode, mixed)
				st, err := source.OpenBatchStream(ctx, w, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1024})
				if mode == WindowExecutionExternalSort {
					if st != nil || err == nil || err.Error() != "external window requires finite memory and spill quotas" {
						t.Fatal("expected exact external no-spill admission rejection", st, err)
					}
					if q.Used() != 0 || src.opens != 0 || src.closes != 0 {
						t.Fatal("external rejection consumed source or retained budget", q.Used(), src.opens, src.closes)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				_, err = st.Next(ctx, memory.NewBatch())
				if !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatal("expected fixed resource rejection", err)
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
				if q.Used() != 0 {
					t.Fatal(q.Used())
				}
			})
		}
	}
}

func TestNTileReplayOnlyObserverCold(t *testing.T) {
	m := memory.NewBudgetManager(8 << 20)
	d, err := newPercentRankDiagnostics(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	d.capture(1, map[string]string{"partition_spool_pages": "3", "partition_spool_bytes": "128", "partition_spool_partitions": "1", "partition_replay_pages": "3"})
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	v := d.Snapshot()
	if v["partition_replay_pages"] != "3" || v["partition_spool_observed"] != "true" {
		t.Fatal(v)
	}
	if _, ok := v["peer_lookahead_pages"]; ok {
		t.Fatal("fake peer metric", v)
	}
	if m.Used() != 0 {
		t.Fatal(m.Used())
	}
	// An old PR-only snapshot has neither newly introduced metric.
	d, err = newPercentRankDiagnostics(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	d.capture(1, map[string]string{"partition_spool_pages": "3", "partition_spool_bytes": "128", "partition_spool_partitions": "1"})
	_ = d.Close()
	v = d.Snapshot()
	if _, ok := v["partition_replay_pages"]; ok {
		t.Fatal("PR-only diagnostics changed", v)
	}
	if m.Used() != 0 {
		t.Fatal(m.Used())
	}
}
