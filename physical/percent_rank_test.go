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

func percentRankFixture(t *testing.T, n int, reverse bool) source.Source {
	t.Helper()
	b := memory.NewBatch()
	b.Reset(n)
	for _, name := range []string{"g", "k", "id", "v"} {
		b.Int64s[name] = &memory.Int64Vector{Values: make([]int64, n)}
	}
	for i := 0; i < n; i++ {
		id := int64(i)
		if reverse {
			id = int64(n - 1 - i)
		}
		b.Int64s["id"].Values[i] = id
		b.Int64s["k"].Values[i] = id / 3
		b.Int64s["v"].Values[i] = id * 2
	}
	sc := memory.NewSchema(memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "k", Type: memory.TypeInt64}, memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64})
	src, e := inmemory.New(sc, b)
	if e != nil {
		t.Fatal(e)
	}
	return src
}
func percentRankQuery(t *testing.T, limit int64, disk bool) (context.Context, *memory.QueryBudget) {
	t.Helper()
	opts := memory.BudgetOptions{HardLimitBytes: limit}
	if disk {
		opts.Spill = memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 256 << 20}
	}
	q, e := memory.NewEngineBudget(limit).BeginQuery(opts)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := q.Close(); e != nil {
			t.Error(e)
		}
	})
	return memory.WithQueryBudget(context.Background(), q), q
}
func percentRankWindow(t *testing.T, src source.Source, q *memory.QueryBudget, mode WindowExecutionMode) source.Source {
	t.Helper()
	win, e := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "k"}}, []WindowExpression{{Function: WindowPercentRank, Alias: "pr"}}, []memory.Field{{Name: "pr", Type: memory.TypeFloat64}}, q, WindowSourceOptions{Mode: mode})
	if e != nil {
		t.Fatal(e)
	}
	return win
}
func TestPercentRankResidentAndThreePaths(t *testing.T) {
	for _, n := range []int{0, 1, 2, 7, 65, 1025} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprintf("n%d/mode%d", n, mode), func(t *testing.T) {
				ctx, q := percentRankQuery(t, 8<<20, true)
				reverse := mode != WindowExecutionOrdered
				win := percentRankWindow(t, percentRankFixture(t, n, reverse), q, mode)
				st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 64})
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
						id := int64(seen + i)
						if reverse {
							id = int64(n - 1 - seen - i)
						}
						want := 0.0
						if n > 1 {
							want = float64(id/3*3) / float64(n-1)
						}
						if out.Int64s["id"].Values[i] != id || !out.Float64s["pr"].IsValid(i) || out.Float64s["pr"].Values[i] != want {
							t.Fatal("value/order", seen, i, want)
						}
					}
					seen += out.Rows
				}
				if seen != n {
					t.Fatal("rows", seen, n)
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

// Frozen before first execution: 262144 rows, four Int64 columns, input batches
// 1024, hard memory 4MiB, disk quota 256MiB. No tuning after failure.
func TestPercentRankFixedStreamingResources(t *testing.T) {
	const n = 262144
	for _, tc := range []struct {
		mode       WindowExecutionMode
		disk       bool
		wantBudget bool
	}{
		{WindowExecutionOrdered, true, false}, {WindowExecutionExternalSort, true, false},
		{WindowExecutionOrdered, false, true}, {WindowExecutionResident, true, true},
	} {
		t.Run(fmt.Sprintf("mode%d/disk%t", tc.mode, tc.disk), func(t *testing.T) {
			ctx, q := percentRankQuery(t, 4<<20, tc.disk)
			reverse := tc.mode == WindowExecutionExternalSort
			win := percentRankWindow(t, percentRankFixture(t, n, reverse), q, tc.mode)
			st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1024})
			if e != nil {
				if tc.wantBudget && errors.Is(e, memory.ErrBudgetExceeded) {
					return
				}
				t.Fatal(e)
			}
			defer st.Close()
			out := memory.NewBatch()
			seen := 0
			for {
				var ok bool
				ok, e = st.Next(ctx, out)
				if e != nil || !ok {
					break
				}
				for i := 0; i < out.Rows; i++ {
					id := int64(seen + i)
					if reverse {
						id = int64(n - 1 - seen - i)
					}
					if out.Int64s["id"].Values[i] != id || out.Float64s["pr"].Values[i] != float64(id/3*3)/float64(n-1) {
						t.Fatal("fixed oracle", seen, i)
					}
				}
				seen += out.Rows
			}
			if tc.wantBudget {
				if !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal("expected fixed budget failure", e, seen)
				}
			} else {
				if e != nil || seen != n {
					t.Fatal("fixed stream", e, seen)
				}
				if q.Snapshot().SpilledBytes == 0 {
					t.Fatal("no real spill")
				}
				diagnostic, ok := st.(source.BatchStreamDiagnostics)
				if !ok {
					t.Fatal("missing stage diagnostics")
				}
				keys := []string{"partition_spool_pages", "partition_spool_bytes", "partition_spool_partitions"}
				if tc.mode == WindowExecutionExternalSort {
					keys = append(keys, "front_sort_spill_runs", "front_sort_spill_bytes", "restore_sort_spill_runs", "restore_sort_spill_bytes")
				}
				report := diagnostic.StreamDiagnostics()
				for _, key := range keys {
					value, parseErr := strconv.ParseInt(report[key], 10, 64)
					if parseErr != nil || value <= 0 {
						t.Fatal("stage evidence", key, report)
					}
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

func TestPercentRankPhysicalValidation(t *testing.T) {
	src := percentRankFixture(t, 1, false)
	for _, tc := range []struct {
		order []SortKey
		ex    WindowExpression
		field memory.Field
	}{
		{nil, WindowExpression{Function: WindowPercentRank, Alias: "p"}, memory.Field{Name: "p", Type: memory.TypeFloat64}},
		{[]SortKey{{Column: "k"}}, WindowExpression{Function: WindowPercentRank, Alias: "p"}, memory.Field{Name: "different", Type: memory.TypeFloat64}},
		{[]SortKey{{Column: "k"}}, WindowExpression{Function: WindowPercentRank, Column: "v", Alias: "p"}, memory.Field{Name: "p", Type: memory.TypeFloat64}},
		{[]SortKey{{Column: "k"}}, WindowExpression{Function: WindowPercentRank, Alias: "p"}, memory.Field{Name: "p", Type: memory.TypeInt64}},
		{[]SortKey{{Column: "k"}}, WindowExpression{Function: WindowPercentRank, Alias: "p"}, memory.Field{Name: "p", Type: memory.TypeFloat64, Nullable: true}},
	} {
		if _, e := NewWindowSource(src, nil, tc.order, []WindowExpression{tc.ex}, []memory.Field{tc.field}, memory.NewBudgetManager(0)); e == nil {
			t.Fatal("invalid PR accepted", tc)
		}
	}
}

// Copied stream output intentionally has a distinct 64MiB budget; this is not
// engine Collect validation. The test-owned copied
// Float64 coefficient buffer is reserved before allocation and survives stream
// Close, proving values are not aliases of a reused execution batch.
func TestPercentRankFixedCopiedStreamOutput(t *testing.T) {
	const n = 262144
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			ctx, q := percentRankQuery(t, 64<<20, true)
			lease, e := q.Reserve(ctx, n*8)
			if e != nil {
				t.Fatal(e)
			}
			defer lease.Release()
			values := make([]float64, n)
			reverse := mode == WindowExecutionExternalSort
			win := percentRankWindow(t, percentRankFixture(t, n, reverse), q, mode)
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
				copy(values[seen:], out.Float64s["pr"].Values[:out.Rows])
				seen += out.Rows
			}
			if e := st.Close(); e != nil {
				t.Fatal(e)
			}
			if seen != n {
				t.Fatal("owned count", seen)
			}
			for i, v := range values {
				id := i
				if reverse {
					id = n - 1 - i
				}
				if v != float64(id/3*3)/float64(n-1) {
					t.Fatal("owned alias", i)
				}
			}
			values = nil
			lease.Release()
			if q.Used() != 0 {
				t.Fatal("owned leak", q.Used())
			}
		})
	}
}
