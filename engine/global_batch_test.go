package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"math"
	"testing"
)

func TestGlobalBatchSelectionWords(t *testing.T) {
	for _, rows := range []int{0, 1, 63, 64, 65, 127, 128, 129, 257} {
		for _, pattern := range []string{"dense", "sparse", "empty"} {
			t.Run(fmt.Sprintf("rows=%d/%s", rows, pattern), func(t *testing.T) {
				s, budget := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat32, Nullable: true}), expr.Query{Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.CountColumn("v", "nonnull"), expr.Count("rows")}})
				b := memory.NewBatch()
				b.Rows = rows
				v := &memory.Float32Vector{Values: make([]float32, rows), Validity: make(memory.Bitmap, (rows+63)/64)}
				b.Float32s["v"] = v
				// Include a fully poisoned extra word as well as dirty tail bits.
				selection := make(memory.Bitmap, (rows+63)/64+1)
				var sum float32
				selected, nonnull := 0, uint64(0)
				values := [...]float32{1 << 24, 1, -(1 << 24), .1, -.3, float32(math.Copysign(0, -1))}
				for row := 0; row < rows; row++ {
					v.Values[row] = values[row%len(values)]
					valid := row%11 != 0
					if valid {
						v.Validity[row/64] |= 1 << uint(row%64)
					}
					yes := pattern == "dense" || (pattern == "sparse" && (row == 0 || row == 128 || row == rows-1))
					if yes {
						selection[row/64] |= 1 << uint(row%64)
						selected++
						if valid {
							sum += v.Values[row]
							nonnull++
						}
					}
				}
				for row := rows; row < len(selection)*64; row++ {
					selection[row/64] |= 1 << uint(row%64)
				}
				binding, err := s.query.bindSortedBatch(context.Background(), b, &s.predicate)
				if err != nil {
					t.Fatal(err)
				}
				s.inputBatch, s.binding, s.selection = b, binding, selection
				s.bindGlobalAggregateInputs()
				charged := budget.Used()
				if err := s.consumeGlobalAggregateBatch(context.Background()); err != nil {
					t.Fatal(err)
				}
				got := s.numeric.Result(0, 0)
				if s.selectedRows != selected || s.nextRow != rows || got.CountRows != uint64(selected) || got.CountValues != nonnull || got.SumValid != (nonnull != 0) || got.SumBits != uint64(math.Float32bits(sum)) {
					t.Fatalf("selected=%d next=%d state=%+v want selected=%d count=%d sum=%x", s.selectedRows, s.nextRow, got, selected, nonnull, math.Float32bits(sum))
				}
				if allocs := testing.AllocsPerRun(10, func() {
					if err := s.consumeGlobalAggregateBatch(context.Background()); err != nil {
						panic(err)
					}
				}); allocs != 0 {
					t.Fatalf("batch consumer allocated %v times", allocs)
				}
				if budget.Used() != charged {
					t.Fatalf("batch consumer changed retained charge: %d -> %d", charged, budget.Used())
				}
			})
		}
	}
}

type globalBatchCancelContext struct {
	context.Context
	err func() error
}

func (c globalBatchCancelContext) Err() error { return c.err() }

func TestGlobalBatchCancellationDuringConsumption(t *testing.T) {
	for _, mode := range []string{"dense", "empty-words", "last-row", "eof"} {
		t.Run(mode, func(t *testing.T) {
			q := expr.Query{Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.Count("rows")}}
			if mode == "empty-words" {
				q.Filter = expr.Bool(false)
			}
			s, budget := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64}), q)
			s.input.Close()
			rows := 257
			if mode == "last-row" {
				rows = 64
			}
			calls, wordChecks := 0, 0
			canceled := false
			input := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
				assertGlobalBindingsCleared(t, s)
				b.ClearColumns()
				calls++
				if calls > 1 {
					canceled = mode == "eof"
					return false, nil
				}
				b.Rows = rows
				b.Float64s["v"] = &memory.Float64Vector{Values: make([]float64, rows)}
				return true, nil
			}}
			s.input = input
			ctx := globalBatchCancelContext{Context: context.Background(), err: func() error {
				if s.selection != nil {
					wordChecks++
				}
				if canceled || ((mode == "dense" || mode == "last-row") && s.selectedRows >= 64) || (mode == "empty-words" && wordChecks == 2) {
					canceled = true
					return context.Canceled
				}
				return nil
			}}
			out := memory.NewBatch()
			ok, err := s.Next(ctx, out)
			if ok || !errors.Is(err, context.Canceled) {
				t.Fatalf("next=%v err=%v", ok, err)
			}
			wantRows := 64
			if mode == "empty-words" {
				wantRows = 0
			} else if mode == "eof" {
				wantRows = rows
			}
			if s.selectedRows != wantRows || out.Rows != 0 || budget.Used() != 0 || s.inputBatch != nil || s.globalBindings != nil || input.closed != 1 {
				t.Fatalf("cancel state: selected=%d want=%d output=%d bytes=%d closes=%d", s.selectedRows, wantRows, out.Rows, budget.Used(), input.closed)
			}
			if ok, again := s.Next(context.Background(), memory.NewBatch()); ok || again != err {
				t.Fatalf("terminal error changed: %v %v", ok, again)
			}
		})
	}
}

func TestGlobalBatchOutputAndRecordLifecycle(t *testing.T) {
	for _, finish := range []string{"eof", "close", "cancel-after-output", "record", "record-error"} {
		t.Run(finish, func(t *testing.T) {
			s, budget := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64}), expr.Query{Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.Count("rows")}})
			s.input.Close()
			done := false
			input := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
				b.ClearColumns()
				if done {
					return false, nil
				}
				done = true
				b.Rows = 3
				b.Float64s["v"] = &memory.Float64Vector{Values: []float64{1e16, 1, -1e16}}
				return true, nil
			}}
			s.input = input
			out := memory.NewBatch()
			injected := errors.New("record sink failure")
			calls := 0
			if finish == "record" || finish == "record-error" {
				out = nil
				s.recordSink = func(record aggregateRecord) error {
					calls++
					got := record.numericBundle.Result(record.groupID, 0)
					if !got.SumValid || got.SumBits != 0 || got.CountRows != 3 {
						t.Fatalf("record=%+v", got)
					}
					if finish == "record-error" {
						return injected
					}
					return nil
				}
			}
			ok, err := s.next(context.Background(), out)
			if finish == "record-error" {
				if ok || !errors.Is(err, injected) || budget.Used() != 0 || input.closed != 1 || calls != 1 {
					t.Fatalf("failed sink: ok=%v err=%v bytes=%d closes=%d calls=%d", ok, err, budget.Used(), input.closed, calls)
				}
				return
			}
			if !ok || err != nil || budget.Used() == 0 || input.closed != 1 || (out == nil && calls != 1) {
				t.Fatalf("output: ok=%v err=%v bytes=%d closes=%d", ok, err, budget.Used(), input.closed)
			}
			if out != nil && (out.Rows != 1 || math.Float64bits(out.Float64s["sum"].Values[0]) != 0 || out.Uint64s["rows"].Values[0] != 3) {
				t.Fatal("incorrect final row")
			}
			if finish == "close" {
				err = s.Close()
			} else {
				ctx := context.Background()
				if finish == "cancel-after-output" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				ok, err = s.next(ctx, nil)
				if ok {
					t.Fatal("second global result")
				}
			}
			if err != nil || budget.Used() != 0 || input.closed != 1 || (out != nil && out.Rows != 0) {
				t.Fatalf("cleanup: err=%v bytes=%d closes=%d", err, budget.Used(), input.closed)
			}
		})
	}
}
