package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func futureTestSource(t *testing.T, rows int) source.Source {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = rows
	b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, rows)}
	b.Int64s["v"] = &memory.Int64Vector{Values: make([]int64, rows)}
	b.Int64s["__golume_window_future_0"] = &memory.Int64Vector{Values: make([]int64, rows)}
	for i := 0; i < rows; i++ {
		b.Int64s["id"].Values[i] = int64(i)
		b.Int64s["v"].Values[i] = int64(i + 10)
		b.Int64s["__golume_window_future_0"].Values[i] = 99
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "__golume_window_future_0", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	return src
}
func futureTestWindow(t *testing.T, input source.Source, manager memory.Manager, lead int64) source.Source {
	t.Helper()
	bound := WindowBound{Kind: WindowFollowing, Offset: lead}
	past := WindowBound{Kind: WindowPreceding, Offset: 1}
	ex := []WindowExpression{{Function: WindowShift, Column: "v", Alias: "v", Frame: WindowFrame{Start: bound, End: bound}}, {Function: WindowDiff, Column: "v", Alias: "__golume_window_future_1", Frame: WindowFrame{Start: bound, End: bound}}, {Function: WindowShift, Column: "v", Alias: "past", Frame: WindowFrame{Start: past, End: past}}}
	fs := []memory.Field{{Name: "v", Type: memory.TypeInt64, Nullable: true}, {Name: "__golume_window_future_1", Type: memory.TypeInt64, Nullable: true}, {Name: "past", Type: memory.TypeInt64, Nullable: true}}
	w, err := NewWindowSourceWithOptions(input, nil, []SortKey{{Column: "id"}}, ex, fs, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func TestFutureWindowOwnedQueueNamesAliasesAndCleanup(t *testing.T) {
	for _, rows := range []int{0, 1, 65, 513} {
		manager := memory.NewBudgetManager(64 << 10)
		win := futureTestWindow(t, futureTestSource(t, rows), manager, 3)
		st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
		if err != nil {
			t.Fatal(err)
		}
		dst := memory.NewBatch()
		seen := 0
		for {
			ok, err := st.Next(context.Background(), dst)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if len(dst.Int64s) != 5 {
				t.Fatalf("hidden column leaked: %v", dst.Int64s)
			}
			for i := 0; i < dst.Rows; i++ {
				n := seen + i
				if dst.Int64s["__golume_window_future_0"].Values[i] != 99 {
					t.Fatal("input hidden-like name overwritten")
				}
				valid := n+3 < rows
				if dst.Int64s["v"].IsValid(i) != valid || dst.Int64s["__golume_window_future_1"].IsValid(i) != valid {
					t.Fatal("tail validity")
				}
				if valid && (dst.Int64s["v"].Values[i] != int64(n+13) || dst.Int64s["__golume_window_future_1"].Values[i] != -3) {
					t.Fatal("future alias changed another expression")
				}
				if n > 0 && dst.Int64s["past"].Values[i] != int64(n+9) {
					t.Fatal("past did not read original value")
				}
			}
			seen += dst.Rows
		}
		if seen != rows {
			t.Fatal(seen, rows)
		}
		st.Close()
		st.Close()
		if manager.Used() != 0 {
			t.Fatal("leak", manager.Used())
		}
	}
}
func TestFutureWindowAdmissionAndCancellationCleanup(t *testing.T) {
	for _, limit := range []int64{4096, 64 << 10} {
		manager := memory.NewBudgetManager(limit)
		win := futureTestWindow(t, futureTestSource(t, 100), manager, 3)
		st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
		if err != nil {
			if limit == 4096 && errors.Is(err, memory.ErrBudgetExceeded) && manager.Used() == 0 {
				continue
			}
			t.Fatal(err)
		}
		out := memory.NewBatch()
		_, err = st.Next(context.Background(), out)
		if limit == 4096 {
			if !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatal(err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = st.Next(ctx, out)
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
		st.Close()
		if manager.Used() != 0 {
			t.Fatal("error leak", manager.Used())
		}
	}
}

type failingFutureChild struct {
	source.BatchStream
	calls int
}

func (s *failingFutureChild) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.calls++
	if s.calls == 3 {
		return false, errors.New("injected future input failure")
	}
	return s.BatchStream.Next(ctx, b)
}
func TestFutureWindowInputErrorReleasesRetainedRows(t *testing.T) {
	manager := memory.NewBudgetManager(64 << 10)
	owner := futureTestWindow(t, futureTestSource(t, 100), manager, 20).(*WindowSource)
	_, input, _ := prepareFutureWindow(owner, owner.input)
	future := input.(*futureWindowSource)
	stream, err := future.NewBatchStream(context.Background(), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	st := stream.(*futureWindowStream)
	st.child = &failingFutureChild{BatchStream: st.child}
	_, err = st.Next(context.Background(), memory.NewBatch())
	if err == nil {
		t.Fatal("missing injected error")
	}
	st.Close()
	if manager.Used() != 0 {
		t.Fatal("input failure leak", manager.Used())
	}
}
