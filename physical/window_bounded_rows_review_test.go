package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func reviewFiniteBound(n int64) WindowBound {
	if n < 0 {
		return WindowBound{Kind: WindowPreceding, Offset: -n}
	}
	if n > 0 {
		return WindowBound{Kind: WindowFollowing, Offset: n}
	}
	return WindowBound{Kind: WindowCurrentRow}
}

// The oracle directly enumerates rows, independently of the ring, deque,
// resident evaluator, and production frame-bound conversion.
func TestFiniteRowsReviewExhaustiveOracle(t *testing.T) {
	vals := []float64{math.Copysign(0, -1), 0, math.Float64frombits(0x7ff8000000000001), -7, math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000000002), 3, 3, 0, math.Copysign(0, -1), 9, -4, 2, math.NaN(), 1, 5, 8, 0, -2, math.Inf(1), 6, 0, -1, math.NaN(), 4, 2, 1, 0}
	n := len(vals)
	b := memory.NewBatch()
	b.Rows = n
	b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, n)}
	b.Int64s["g"] = &memory.Int64Vector{Values: make([]int64, n)}
	b.Int64s["__golume_window_future_0"] = &memory.Int64Vector{Values: make([]int64, n)}
	valid := memory.EnsureBitmap(nil, n)
	for i := 0; i < n; i++ {
		b.Int64s["id"].Values[i] = int64(i)
		b.Int64s["g"].Values[i] = int64(i / 11)
		b.Int64s["__golume_window_future_0"].Values[i] = 99
		if i%5 != 3 {
			memory.Set(valid, i)
		}
	}
	b.Float64s["v"] = &memory.Float64Vector{Values: vals, Validity: valid}
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "__golume_window_future_0", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true})
	src, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	for _, batchRows := range []int{1, 4, 19} {
		for start := int64(-12); start <= 12; start++ {
			for end := start; end <= 12; end++ {
				frame := WindowFrame{Start: reviewFiniteBound(start), End: reviewFiniteBound(end)}
				ex := []WindowExpression{{Function: WindowCountRows, Alias: "rows", Frame: frame}, {Function: WindowCountValues, Column: "v", Alias: "count", Frame: frame}, {Function: WindowMin, Column: "v", Alias: "v", Frame: frame}, {Function: WindowMax, Column: "v", Alias: "__golume_window_future_3", Frame: frame}}
				fs := []memory.Field{{Name: "rows", Type: memory.TypeInt64}, {Name: "count", Type: memory.TypeInt64}, {Name: "v", Type: memory.TypeFloat64, Nullable: true}, {Name: "__golume_window_future_3", Type: memory.TypeFloat64, Nullable: true}}
				mgr := memory.NewBudgetManager(8 << 20)
				win, err := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "id"}}, ex, fs, mgr, WindowSourceOptions{Mode: WindowExecutionOrdered})
				if err != nil {
					t.Fatal(err)
				}
				st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: batchRows})
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
						var rows, count int64
						var lo, hi float64
						for k := 0; k < n; k++ {
							if k/11 != r/11 || int64(k-r) < start || int64(k-r) > end {
								continue
							}
							rows++
							if !b.Float64s["v"].IsValid(k) {
								continue
							}
							v := vals[k]
							if count == 0 {
								lo = v
								hi = v
							} else {
								if (!math.IsNaN(v) && math.IsNaN(lo)) || v < lo {
									lo = v
								}
								if (math.IsNaN(v) && !math.IsNaN(hi)) || v > hi {
									hi = v
								}
							}
							count++
						}
						if out.Int64s["rows"].Values[j] != rows || out.Int64s["count"].Values[j] != count {
							t.Fatalf("batch%d frame[%d,%d] row%d count got %d/%d want %d/%d", batchRows, start, end, r, out.Int64s["rows"].Values[j], out.Int64s["count"].Values[j], rows, count)
						}
						for name, want := range map[string]float64{"v": lo, "__golume_window_future_3": hi} {
							v := out.Float64s[name]
							if v.IsValid(j) != (count > 0) || count > 0 && math.Float64bits(v.Values[j]) != math.Float64bits(want) {
								t.Fatalf("batch%d frame[%d,%d] row%d %s got %x/%t want %x/%t", batchRows, start, end, r, name, math.Float64bits(v.Values[j]), v.IsValid(j), math.Float64bits(want), count > 0)
							}
						}
						if out.Int64s["__golume_window_future_0"].Values[j] != 99 {
							t.Fatal("hidden-name collision")
						}
					}
					seen += out.Rows
				}
				st.Close()
				st.Close()
				if seen != n || mgr.Used() != 0 {
					t.Fatalf("rows=%d leaked=%d", seen, mgr.Used())
				}
			}
		}
	}
}

func TestFiniteRowsReviewMaxInt64(t *testing.T) {
	for _, fn := range []WindowFunction{WindowCountRows, WindowCountValues, WindowMin, WindowMax} {
		t.Run(fmt.Sprint(fn), func(t *testing.T) {
			src := futureTestSource(t, 3)
			bound := WindowBound{Kind: WindowFollowing, Offset: math.MaxInt64}
			ex := WindowExpression{Function: fn, Column: "v", Alias: "out", Frame: WindowFrame{Start: bound, End: bound}}
			if fn == WindowCountRows {
				ex.Column = ""
			}
			nullable := fn == WindowMin || fn == WindowMax
			mgr := memory.NewBudgetManager(1 << 20)
			win, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{ex}, []memory.Field{{Name: "out", Type: memory.TypeInt64, Nullable: nullable}}, mgr, WindowSourceOptions{Mode: WindowExecutionOrdered})
			if err != nil {
				t.Fatal(err)
			}
			st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
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
				for i := 0; i < out.Rows; i++ {
					v := out.Int64s["out"]
					if v.IsValid(i) == nullable || v.Values[i] != 0 {
						t.Fatal("nonempty huge following frame")
					}
					seen++
				}
			}
			st.Close()
			if mgr.Used() != 0 || seen != 3 {
				t.Fatal(mgr.Used(), seen)
			}
		})
	}
}

type finiteReviewFailManager struct {
	memory.Manager
	calls, fail int
}

func (m *finiteReviewFailManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}

func TestFiniteRowsReviewEveryAdmissionFailure(t *testing.T) {
	// Sweep every reservation point, including partial state setup, queued input,
	// output batches, and cleanup after a successful prefix.
	for fail := 1; fail <= 120; fail++ {
		mgr := &finiteReviewFailManager{Manager: memory.NewBudgetManager(1 << 20), fail: fail}
		src := futureTestSource(t, 17)
		ex := []WindowExpression{{Function: WindowMin, Column: "v", Alias: "lo", Frame: WindowFrame{Start: reviewFiniteBound(-4), End: reviewFiniteBound(3)}}, {Function: WindowCountRows, Alias: "cnt", Frame: WindowFrame{Start: reviewFiniteBound(-5), End: reviewFiniteBound(-2)}}, {Function: WindowMax, Column: "v", Alias: "hi", Frame: WindowFrame{Start: reviewFiniteBound(2), End: reviewFiniteBound(5)}}}
		fs := []memory.Field{{Name: "lo", Type: memory.TypeInt64, Nullable: true}, {Name: "cnt", Type: memory.TypeInt64}, {Name: "hi", Type: memory.TypeInt64, Nullable: true}}
		w, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, ex, fs, mgr, WindowSourceOptions{Mode: WindowExecutionOrdered})
		if err != nil {
			t.Fatal(err)
		}
		st, err := source.OpenBatchStream(context.Background(), w, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
		if err == nil {
			out := memory.NewBatch()
			for {
				ok, nextErr := st.Next(context.Background(), out)
				if nextErr != nil || !ok {
					break
				}
			}
			st.Close()
			st.Close()
		}
		if mgr.Used() != 0 {
			t.Fatalf("reservation failure %d leaked %d bytes", fail, mgr.Used())
		}
		if mgr.calls < fail {
			break
		}
	}
}

type finiteReviewReuseSource struct{ source.Source }

func (s finiteReviewReuseSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &finiteReviewReuseStream{}, nil
}

type finiteReviewReuseStream struct {
	next        int
	ids, values [2]int64
	key         [2]byte
}

func (s *finiteReviewReuseStream) Close() error {
	s.ids = [2]int64{-999, -999}
	s.values = s.ids
	s.key = [2]byte{'!', '!'}
	return nil
}
func (s *finiteReviewReuseStream) Next(_ context.Context, b *memory.Batch) (bool, error) {
	s.Close()
	b.ClearColumns()
	if s.next == 30 {
		return false, nil
	}
	for i := 0; i < 2; i++ {
		r := s.next + i
		s.ids[i] = int64(r)
		s.values[i] = int64(r * 3)
		s.key[i] = byte('a' + r/7)
	}
	s.next += 2
	b.Rows = 2
	b.Int64s["id"] = &memory.Int64Vector{Values: s.ids[:]}
	b.Int64s["v"] = &memory.Int64Vector{Values: s.values[:]}
	b.Strings["g"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2}, Data: s.key[:]}
	return true, nil
}
func TestFiniteRowsReviewReusedChildBuffers(t *testing.T) {
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
	for i, fn := range []WindowFunction{WindowCountRows, WindowMin, WindowMax} {
		name := fmt.Sprint("o", i)
		ex[i] = WindowExpression{Function: fn, Column: "v", Alias: name, Frame: WindowFrame{Start: reviewFiniteBound(starts[i]), End: reviewFiniteBound(ends[i])}}
		if i == 0 {
			ex[i].Column = ""
		}
		fs[i] = memory.Field{Name: name, Type: memory.TypeInt64, Nullable: i != 0}
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
					if count == 0 || e == 1 && v < want || e == 2 && v > want {
						want = v
					}
					count++
				}
				valid := count > 0
				if e == 0 {
					want = count
					valid = true
				}
				got := out.Int64s[ex[e].Alias]
				if got.IsValid(j) != valid || valid && got.Values[j] != want {
					t.Fatalf("row%d expr%d got %d/%t want %d/%t", r, e, got.Values[j], got.IsValid(j), want, valid)
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

func TestFiniteRowsReviewActualExternalSpill(t *testing.T) {
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
	for i, fn := range []WindowFunction{WindowCountRows, WindowMin, WindowMax} {
		name := fmt.Sprint("o", i)
		ex[i] = WindowExpression{Function: fn, Column: "v", Alias: name, Frame: WindowFrame{Start: reviewFiniteBound(starts[i]), End: reviewFiniteBound(ends[i])}}
		if i == 0 {
			ex[i].Column = ""
		}
		fs[i] = memory.Field{Name: name, Type: memory.TypeInt64, Nullable: i != 0}
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
					if count == 0 || e == 1 && v < want || e == 2 && v > want {
						want = v
					}
					count++
				}
				valid := count > 0
				if e == 0 {
					want = count
					valid = true
				}
				got := out.Int64s[ex[e].Alias]
				if got.IsValid(j) != valid || valid && got.Values[j] != want {
					t.Fatalf("row%d expr%d got %d/%t want %d/%t", r, e, got.Values[j], got.IsValid(j), want, valid)
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

func TestFiniteRowsReviewCloseDropsReferences(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeString})
	empty, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	ex := WindowExpression{Function: WindowMin, Column: "v", Alias: "out", Frame: WindowFrame{Start: reviewFiniteBound(-4), End: reviewFiniteBound(-1)}}
	mgr := memory.NewBudgetManager(1 << 20)
	win, err := NewWindowSourceWithOptions(finiteReviewReuseSource{Source: empty}, []string{"g"}, []SortKey{{Column: "id"}}, []WindowExpression{ex}, []memory.Field{{Name: "out", Type: memory.TypeInt64, Nullable: true}}, mgr, WindowSourceOptions{Mode: WindowExecutionOrdered})
	if err != nil {
		t.Fatal(err)
	}
	st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels()})
	if err != nil {
		t.Fatal(err)
	}
	ordered := st.(*orderedWindowStream)
	future := ordered.child.(*futureWindowStream)
	state := future.finiteRows[0]
	if _, err := st.Next(context.Background(), memory.NewBatch()); err != nil {
		t.Fatal(err)
	}
	if state.keyLease == nil || len(state.previous.cells) == 0 || state.delay.values == nil || state.aggregate.extrema.values == nil {
		t.Fatal("test did not populate retained state")
	}
	st.Close()
	st.Close()
	if state.previous.cells != nil || state.keys != nil || state.keyLease != nil || state.delay.values != nil || state.aggregate.queue.values != nil || state.aggregate.extrema.values != nil || future.finiteRows != nil || future.owner != nil || future.child != nil || future.scratch != nil || future.head != nil || future.current != nil || mgr.Used() != 0 {
		t.Fatal("closed finite stream retains execution state")
	}
}
