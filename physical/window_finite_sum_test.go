package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestFiniteSumStateAdmissionAndSkippedRows(t *testing.T) {
	frame := WindowFrame{Start: reviewFiniteBound(-1), End: reviewFiniteBound(1)}
	count, countBytes, err := newFiniteRowsState(WindowExpression{Function: WindowCountValues, Frame: frame}, memory.TypeFloat64, memory.TypeInt64, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = count
	for _, fn := range []WindowFunction{WindowSum, WindowAvg} {
		st, n, err := newFiniteRowsState(WindowExpression{Function: fn, Frame: frame}, memory.TypeFloat64, memory.TypeFloat64, nil)
		if err != nil || n != countBytes+windowExactSumBytes || st.aggregate.exactSum != nil {
			t.Fatal("preallocation admission", n, err)
		}
		st.initialize(nil)
		for _, v := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
			if err := st.advance(windowNumericCell{valid: true, f64: v}, 1); err != nil {
				t.Fatal(err)
			}
		}
		if !math.IsNaN(st.aggregate.exactSum.result(1)) {
			t.Fatal("special membership")
		}
		if err := st.advance(windowNumericCell{valid: true, f64: 7}, 100); err != nil {
			t.Fatal(err)
		}
		if st.aggregate.validCount != 1 || st.aggregate.exactSum.result(1) != 7 {
			t.Fatal("skipped expiry")
		}
		st.reset()
		if st.aggregate.validCount != 0 || st.aggregate.exactSum.result(1) != 0 {
			t.Fatal("partition reset")
		}
		st.close()
		if st.aggregate.exactSum != nil || st.aggregate.queue.values != nil {
			t.Fatal("retained state")
		}
	}
	st, n, err := newFiniteRowsState(WindowExpression{Function: WindowSum, Frame: frame}, memory.TypeInt64, memory.TypeInt64, nil)
	if err != nil || n != countBytes+windowFiniteIntegerSumBytes {
		t.Fatal(n, err)
	}
	st.initialize(nil)
	// Intermediate sums exceed Int64, but no result is requested until cancellation.
	for _, v := range []int64{math.MaxInt64, math.MaxInt64, -math.MaxInt64} {
		if err := st.advance(windowNumericCell{valid: true, i64: v}, 1); err != nil {
			t.Fatal(err)
		}
	}
	if !st.aggregate.signedSum.IsInt64() || st.aggregate.signedSum.Int64() != math.MaxInt64 {
		t.Fatal("premature overflow")
	}
	if err := st.advance(windowNumericCell{}, 100); err != nil || st.aggregate.validCount != 0 || st.aggregate.signedSum.Sign() != 0 {
		t.Fatal("integer expiry", err)
	}
	st.close()
}

func finiteSumResourceWindow(t *testing.T, manager memory.Manager) *WindowSource {
	t.Helper()
	future := WindowFrame{Start: reviewFiniteBound(-1), End: reviewFiniteBound(2)}
	trailing := WindowFrame{Start: reviewFiniteBound(-1), End: reviewFiniteBound(0)}
	ex := []WindowExpression{{Function: WindowAvg, Column: "v", Alias: "future", Frame: future}, {Function: WindowAvg, Column: "v", Alias: "trailing", Frame: trailing}, {Function: WindowSum, Column: "v", Alias: "v", Frame: future}}
	fs := []memory.Field{{Name: "future", Type: memory.TypeFloat64, Nullable: true}, {Name: "trailing", Type: memory.TypeFloat64, Nullable: true}, {Name: "v", Type: memory.TypeInt64, Nullable: true}}
	w, err := NewWindowSourceWithOptions(futureTestSource(t, 130), nil, []SortKey{{Column: "id"}}, ex, fs, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
	if err != nil {
		t.Fatal(err)
	}
	return w.(*WindowSource)
}

func TestFiniteSumResourceLifecycleAndImmutablePlans(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	owner := finiteSumResourceWindow(t, manager)
	plan := owner.exactPlans[0]
	for _, cancelRun := range []bool{false, true} {
		st, err := source.OpenBatchStream(context.Background(), owner, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 3})
		if err != nil {
			t.Fatal(err)
		}
		b := memory.NewBatch()
		if ok, err := st.Next(context.Background(), b); err != nil || !ok {
			t.Fatal(ok, err)
		}
		if b.Float64s["future"].Values[0] != 11 || b.Float64s["trailing"].Values[0] != 10 || b.Int64s["v"].Values[0] != 33 {
			t.Fatal("moved exact plan or alias corrupted")
		}
		if owner.exactPlans[0] != plan || plan == nil {
			t.Fatal("owner plan mutated")
		}
		if cancelRun {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := st.Next(ctx, b); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
		st.Close()
		if manager.Used() != 0 {
			t.Fatal("lease leak", manager.Used())
		}
	}
	tiny := memory.NewBudgetManager(1)
	owner = finiteSumResourceWindow(t, tiny)
	if st, err := source.OpenBatchStream(context.Background(), owner, source.ReadOptions{Kernels: offsetTestKernels()}); !errors.Is(err, memory.ErrBudgetExceeded) {
		if st != nil {
			st.Close()
		}
		t.Fatal("budget admission", err)
	}
	if tiny.Used() != 0 {
		t.Fatal("denial leak")
	}
}

func TestFiniteSumFrameCapacityRejection(t *testing.T) {
	for _, fn := range []WindowFunction{WindowSum, WindowAvg} {
		for _, frame := range []WindowFrame{{Start: reviewFiniteBound(-math.MaxInt64), End: reviewFiniteBound(math.MaxInt64)}, {Start: reviewFiniteBound(0), End: reviewFiniteBound(math.MaxInt64)}} {
			if _, _, err := newFiniteRowsState(WindowExpression{Function: fn, Frame: frame}, memory.TypeFloat64, memory.TypeFloat64, nil); err == nil {
				t.Fatal("impossible capacity accepted")
			}
		}
	}
}

func TestFiniteSumActualExternalSpill(t *testing.T) {
	engine := memory.NewEngineBudget(4 << 20)
	query, err := engine.BeginQuery(memory.BudgetOptions{SoftLimitBytes: 48 << 10, HardLimitBytes: 4 << 20, Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer query.Close()
	ctx := memory.WithQueryBudget(context.Background(), query)
	const n = 8192
	b := memory.NewBatch()
	b.Rows = n
	for _, name := range []string{"id", "g", "v"} {
		b.Int64s[name] = &memory.Int64Vector{Values: make([]int64, n)}
	}
	for i := 0; i < n; i++ {
		id := int64(n - 1 - i)
		b.Int64s["id"].Values[i] = id
		b.Int64s["g"].Values[i] = id / 57
		b.Int64s["v"].Values[i] = id * 3
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	starts := []int64{-3, -5, 1}
	ends := []int64{2, -1, 4}
	ex := make([]WindowExpression, 3)
	fs := make([]memory.Field, 3)
	for i, fn := range []WindowFunction{WindowSum, WindowAvg, WindowSum} {
		name := fmt.Sprint("o", i)
		ex[i] = WindowExpression{Function: fn, Column: "v", Alias: name, Frame: WindowFrame{Start: reviewFiniteBound(starts[i]), End: reviewFiniteBound(ends[i])}}

		fs[i] = memory.Field{Name: name, Type: memory.TypeInt64, Nullable: true}
		if i == 1 {
			fs[i].Type = memory.TypeFloat64
		}
	}
	win, err := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "id"}}, ex, fs, query, WindowSourceOptions{Mode: WindowExecutionExternalSort})
	if err != nil {
		t.Fatal(err)
	}
	st, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	restored := st.(*externalWindowStream).child.(*dropColumnsBatchStream).state.child.(*externalSortStream)
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
		for j := 0; j < out.Rows; j++ {
			r := int64(n - 1 - seen - j)
			if out.Int64s["id"].Values[j] != r {
				t.Fatal("original order not restored")
			}
			for e := range ex {
				var count, want int64
				for k := max(int64(0), r+starts[e]); k <= min(int64(n-1), r+ends[e]); k++ {
					if k/57 != r/57 {
						continue
					}
					v := k * 3
					want += v
					count++
				}
				valid := count > 0
				if e == 1 {
					got := out.Float64s[ex[e].Alias]
					if got.IsValid(j) != valid || valid && got.Values[j] != float64(want)/float64(count) {
						t.Fatalf("avg row%d: %v want %d/%d", r, got, want, count)
					}
				} else {
					got := out.Int64s[ex[e].Alias]
					if got.IsValid(j) != valid || valid && got.Values[j] != want {
						t.Fatalf("sum row%d: %v want %d", r, got, want)
					}
				}
			}
		}
		seen += out.Rows
	}
	if seen != n || query.Snapshot().SpilledBytes == 0 || restored.spillRuns < 2 {
		t.Fatalf("not a real multi-run spill: rows=%d runs=%d snapshot=%+v", seen, restored.spillRuns, query.Snapshot())
	}
	t.Logf("restore spill runs=%d merge levels=%d spill bytes=%d", restored.spillRuns, restored.mergeLevels, query.Snapshot().SpilledBytes)
	checkSpillOutputCleanup(t, st, query)
}

func TestFiniteSumEveryAdmissionFailure(t *testing.T) {
	completed := false
	for fail := 1; fail <= 4096; fail++ {
		mgr := &finiteReviewFailManager{Manager: memory.NewBudgetManager(1 << 20), fail: fail}
		owner := finiteSumResourceWindow(t, mgr)
		st, err := source.OpenBatchStream(context.Background(), owner, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
		if err == nil {
			b := memory.NewBatch()
			for {
				ok, nextErr := st.Next(context.Background(), b)
				if nextErr != nil {
					if !errors.Is(nextErr, memory.ErrBudgetExceeded) {
						t.Fatal(nextErr)
					}
					break
				}
				if !ok {
					break
				}
			}
			st.Close()
			st.Close()
		} else if !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatal(err)
		}
		if mgr.Used() != 0 {
			t.Fatalf("failure %d leaked %d", fail, mgr.Used())
		}
		if mgr.calls < fail {
			completed = true
			break
		}
	}
	if !completed {
		t.Fatal("admission sweep did not reach a successful run")
	}
}

func TestFiniteSumReusedChildBuffers(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeString})
	empty, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	src := finiteReviewReuseSource{Source: empty}
	starts := []int64{-3, -2, 3}
	ends := []int64{-1, 4, 5}
	ex := make([]WindowExpression, 3)
	fs := make([]memory.Field, 3)
	for i, fn := range []WindowFunction{WindowSum, WindowAvg, WindowSum} {
		name := fmt.Sprint("o", i)
		ex[i] = WindowExpression{Function: fn, Column: "v", Alias: name, Frame: WindowFrame{Start: reviewFiniteBound(starts[i]), End: reviewFiniteBound(ends[i])}}

		fs[i] = memory.Field{Name: name, Type: memory.TypeInt64, Nullable: true}
		if i == 1 {
			fs[i].Type = memory.TypeFloat64
		}
	}
	mgr := memory.NewBudgetManager(1 << 20)
	win, err := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "id"}}, ex, fs, mgr, WindowSourceOptions{Mode: WindowExecutionOrdered})
	if err != nil {
		t.Fatal(err)
	}
	st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels()})
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	seen := 0
	for {
		ok, err := st.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for j := 0; j < out.Rows; j++ {
			r := seen + j
			for e := range ex {
				var count, want int64
				for k := 0; k < 30; k++ {
					if k/7 != r/7 || int64(k-r) < starts[e] || int64(k-r) > ends[e] {
						continue
					}
					v := int64(k * 3)
					want += v
					count++
				}
				valid := count > 0
				if e == 1 {
					got := out.Float64s[ex[e].Alias]
					if got.IsValid(j) != valid || valid && got.Values[j] != float64(want)/float64(count) {
						t.Fatalf("avg row%d: %v want %d/%d", r, got, want, count)
					}
				} else {
					got := out.Int64s[ex[e].Alias]
					if got.IsValid(j) != valid || valid && got.Values[j] != want {
						t.Fatalf("sum row%d: %v want %d", r, got, want)
					}
				}
			}
		}
		seen += out.Rows
	}
	st.Close()
	if seen != 30 || mgr.Used() != 0 {
		t.Fatal(seen, mgr.Used())
	}
}
