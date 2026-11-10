package physical

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
)

// Reuse both numeric and validity buffers. The child has never had an element,
// so its nil offsets are a valid zero-row representation, not malformed input.
func emptyListCloneConsumerInput(t *testing.T) *topKOneShotSource {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0}}
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{10}}
	b.StringLists["list"] = &memory.StringListVector{Offsets: []uint32{0, 0}, Values: &memory.StringViewVector{}, Validity: memory.Bitmap{1}}
	if err := b.StringLists["list"].Validate(1); err != nil {
		t.Fatal(err)
	}
	return &topKOneShotSource{
		schema: memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "list", Type: memory.TypeList, Nullable: true}),
		batch:  b, count: 4,
		fill: func(i int, b *memory.Batch) {
			b.Int64s["id"].Values[0] = int64(i)
			b.Int64s["v"].Values[0] = int64(10 + i)
			b.StringLists["list"].Validity[0] = uint64(1 - i%2)
			if b.StringLists["list"].Values.Offsets != nil {
				t.Fatal("input nil child offsets were mutated")
			}
		},
	}
}

type emptyListCloneConsumerManager struct {
	*memory.BudgetManager
	calls, failAt int
}

func (m *emptyListCloneConsumerManager) Reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	m.calls++
	if m.failAt != 0 && m.calls == m.failAt {
		return nil, memory.ErrBudgetExceeded
	}
	return m.BudgetManager.Reserve(ctx, bytes)
}

func openEmptyListCloneConsumer(t *testing.T, kind string, input source.Source, manager memory.Manager) (source.BatchStream, error) {
	t.Helper()
	var owner source.Source
	if kind == "topk" {
		top, err := NewTopKSourceWithManager(input, 4, []SortKey{{Column: "id"}}, manager)
		if err != nil {
			t.Fatal(err)
		}
		owner = top
	} else {
		bound := WindowBound{Kind: WindowFollowing, Offset: 1}
		win, err := NewWindowSourceWithOptions(input, nil, []SortKey{{Column: "id"}}, []WindowExpression{{Function: WindowShift, Column: "v", Alias: "lead", Frame: WindowFrame{Start: bound, End: bound}}}, []memory.Field{{Name: "lead", Type: memory.TypeInt64, Nullable: true}}, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
		if err != nil {
			t.Fatal(err)
		}
		owner = win
	}
	st, err := source.OpenBatchStream(context.Background(), owner, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
	if err == nil && kind == "topk" {
		if _, ok := st.(*topKSnapshotStream); !ok {
			t.Fatalf("expected production snapshot caller, got %T", st)
		}
	}
	return st, err
}

func checkEmptyListCloneConsumerRow(t *testing.T, kind string, out *memory.Batch, row int) {
	t.Helper()
	if out.Rows != 1 || out.Int64s["id"].Values[0] != int64(row) {
		t.Fatalf("unexpected output row %d: %+v", row, out)
	}
	v := out.StringLists["list"]
	if v == nil {
		t.Fatal("list payload was dropped")
	}
	if err := v.Validate(1); err != nil {
		t.Fatal(err)
	}
	lo, hi, valid := v.ValueRange(0)
	if valid != (row%2 == 0) || lo != 0 || hi != 0 || v.Values.Len() != 0 || len(v.Values.Data) != 0 {
		t.Fatalf("row %d lost empty/NULL semantics: range=(%d,%d,%v), child=%+v", row, lo, hi, valid, v.Values)
	}
	if kind == "window" {
		lead := out.Int64s["lead"]
		if lead == nil || lead.IsValid(0) != (row < 3) || (row < 3 && lead.Values[0] != int64(row+11)) {
			t.Fatalf("row %d: wrong lead %v", row, lead)
		}
	}
}

func TestEmptyListCloneConsumersSuccessAndCleanup(t *testing.T) {
	for _, kind := range []string{"window", "topk"} {
		for _, ending := range []string{"eof", "early-close", "cancel", "input-error"} {
			t.Run(kind+"/"+ending, func(t *testing.T) {
				input := emptyListCloneConsumerInput(t)
				inputErr := errors.New("empty-list input failure")
				if ending == "input-error" {
					input.nextErr = inputErr
				}
				manager := &emptyListCloneConsumerManager{BudgetManager: memory.NewBudgetManager(1 << 20)}
				st, err := openEmptyListCloneConsumer(t, kind, input, manager)
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				out := memory.NewBatch()
				seen := 0
				for {
					ctx := context.Background()
					if ending == "cancel" && seen == 1 {
						canceled, cancel := context.WithCancel(ctx)
						cancel()
						ctx = canceled
					}
					ok, err := st.Next(ctx, out)
					if err != nil {
						want := inputErr
						if ending == "cancel" {
							want = context.Canceled
						}
						if (ending != "cancel" && ending != "input-error") || !errors.Is(err, want) || ok || (kind == "topk" && out.Rows != 0) {
							t.Fatalf("Next=(%v,%v), rows=%d", ok, err, out.Rows)
						}
						// TopK closes on terminal errors; the ordered-window wrapper
						// releases its own state on the explicit Close below.
						if kind == "topk" && (manager.Used() != 0 || input.closes != 1) {
							t.Fatalf("error did not release ownership: used=%d closes=%d", manager.Used(), input.closes)
						}
						break
					}
					if !ok {
						if ending != "eof" || seen != 4 {
							t.Fatalf("unexpected EOF after %d rows", seen)
						}
						break
					}
					checkEmptyListCloneConsumerRow(t, kind, out, seen)
					seen++
					if ending == "early-close" {
						break
					}
				}
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 || input.opens != 1 || input.closes != 1 {
					t.Fatalf("cleanup: used=%d opens=%d closes=%d", manager.Used(), input.opens, input.closes)
				}
			})
		}
	}
}

func TestEmptyListCloneConsumersBudgetDenial(t *testing.T) {
	for _, kind := range []string{"window", "topk"} {
		t.Run(kind, func(t *testing.T) {
			// Discover every reservation in the actual successful caller flow, then
			// deny each in turn, including admissions after earlier clones succeeded.
			manager := &emptyListCloneConsumerManager{BudgetManager: memory.NewBudgetManager(1 << 20)}
			st, err := openEmptyListCloneConsumer(t, kind, emptyListCloneConsumerInput(t), manager)
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			for {
				ok, err := st.Next(context.Background(), out)
				if err != nil {
					st.Close()
					t.Fatal(err)
				}
				if !ok {
					break
				}
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			calls := manager.calls
			if calls < 2 || manager.Used() != 0 {
				t.Fatalf("invalid control: calls=%d used=%d", calls, manager.Used())
			}
			for failAt := 1; failAt <= calls; failAt++ {
				t.Run(fmt.Sprintf("reservation-%d", failAt), func(t *testing.T) {
					input := emptyListCloneConsumerInput(t)
					denied := &emptyListCloneConsumerManager{BudgetManager: memory.NewBudgetManager(1 << 20), failAt: failAt}
					stream, err := openEmptyListCloneConsumer(t, kind, input, denied)
					if err == nil {
						defer stream.Close()
						for {
							ok, nextErr := stream.Next(context.Background(), out)
							err = nextErr
							if err != nil || !ok {
								break
							}
						}
						if kind == "topk" && out.Rows != 0 {
							t.Fatal("failed Next exposed output")
						}
					}
					if !errors.Is(err, memory.ErrBudgetExceeded) {
						t.Fatalf("denial returned %v", err)
					}
					if kind == "topk" && (denied.Used() != 0 || input.closes != input.opens) {
						t.Fatalf("denial leaked: used=%d opens=%d closes=%d", denied.Used(), input.opens, input.closes)
					}
					if stream != nil {
						stream.Close()
						stream.Close()
					}
					if denied.Used() != 0 || input.closes != input.opens {
						t.Fatal("close after denial changed ownership")
					}
				})
			}
		})
	}
}
