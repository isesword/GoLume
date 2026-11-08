package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"reflect"
	"testing"
)

// Batch-local state views must be rebound, not retained with the input
// descriptor. Force the bundle to replace its backing between batches, and
// compare every lane with the original row-at-a-time state update. Different
// validity masks and layouts expose accidentally reused count/sum pointers.
func TestGlobalBatchBindingReviewStateLifetime(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		for _, layout := range []string{"sum", "count", "sum-count"} {
			t.Run(mode.String()+"/"+layout, func(t *testing.T) {
				var fields []memory.Field
				q := expr.Query{Aggregates: []expr.Aggregate{expr.Count("rows")}}
				for lane, typ := range types {
					name := fmt.Sprintf("v%d", lane)
					fields = append(fields, memory.Field{Name: name, Type: typ, Nullable: true})
					if layout != "count" {
						q.Aggregates = append(q.Aggregates, expr.Sum(name, name+"_sum"))
					}
					if layout != "sum" {
						q.Aggregates = append(q.Aggregates, expr.CountColumn(name, name+"_count"))
					}
				}
				s, budget := globalBindingTestStream(t, memory.NewSchema(fields...), q)
				s.query.e.kernels = kernel.New(mode)
				m := s.query.cq.MultiAggregate
				refs := make([]aggregate.NumericVector, len(types))
				for lane := range refs {
					var err error
					refs[lane], err = aggregate.NewNumericVector(1, m.Types[lane], m.Layouts[lane])
					if err != nil {
						t.Fatal(err)
					}
				}
				wantSelected := 0
				// An empty batch and an all-null batch must preserve a prior sum; a
				// subsequent dense batch must not reuse the previous validity bitmap.
				for batch, rows := range []int{129, 0, 65, 257, 63} {
					b := memory.NewBatch()
					b.Rows = rows
					selection := make(memory.Bitmap, memory.WordsFor(rows)+1)
					for row := 0; row < rows; row++ {
						if batch != 3 || row < 64 || row >= 192 {
							memory.Set(selection, row)
						}
					}
					// Poison unused bits and an entire extra word. Neither counts nor
					// typed reducers may read them, including for a zero-row batch.
					for row := rows; row < len(selection)*64; row++ {
						memory.Set(selection, row)
					}
					for lane, typ := range types {
						raw := make([]uint64, rows)
						var valid memory.Bitmap
						if batch != 4 {
							valid = make(memory.Bitmap, memory.WordsFor(rows))
						}
						for row := range raw {
							raw[row] = uint64(int64(row%7 - 3))
							switch typ {
							case memory.TypeFloat32:
								values := [...]float32{1 << 24, 1, -(1 << 24), .1, -.3, 7}
								raw[row] = uint64(math.Float32bits(values[(row+batch)%len(values)]))
							case memory.TypeFloat64:
								values := [...]float64{1e16, 1, -1e16, .1, -.3, 7}
								raw[row] = math.Float64bits(values[(row+batch)%len(values)])
							}
							yes := batch == 4 || (batch != 2 && (row+lane)%5 != 0)
							if yes && valid != nil {
								memory.Set(valid, row)
							}
							if memory.IsSet(selection, row) {
								refs[lane].AddBits(0, raw[row], yes)
								if lane == 0 {
									wantSelected++
								}
							}
						}
						fillNativeTestColumn(b, nativeTestColumn{name: m.Columns[lane], typ: typ, bits: raw, valid: valid})
					}
					clear(s.globalBindings)
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
					for lane := range refs {
						if got, want := s.numeric.Result(0, lane), refs[lane].Result(0); !reflect.DeepEqual(got, want) {
							t.Fatalf("batch=%d lane=%d got=%+v want=%+v", batch, lane, got, want)
						}
					}
					if s.selectedRows != wantSelected || s.nextRow != rows || budget.Used() != charged {
						t.Fatalf("batch=%d selected=%d want=%d next=%d rows=%d charged=%d want charge=%d", batch, s.selectedRows, wantSelected, s.nextRow, rows, budget.Used(), charged)
					}
					if batch == 0 || batch == 2 {
						if err := s.numeric.EnsureGroups(s.numeric.Capacity() + 1); err != nil {
							t.Fatal(err)
						}
					}
				}
				charged := budget.Used()
				if allocs := testing.AllocsPerRun(20, func() {
					if err := s.consumeGlobalAggregateBatch(context.Background()); err != nil {
						panic(err)
					}
				}); allocs != 0 {
					t.Fatalf("batch consumer allocated %v times", allocs)
				}
				if budget.Used() != charged {
					t.Fatal("batch consumer retained extra state")
				}
			})
		}
	}
}

// A cancellation after one completed lane must not publish a partially
// computed global row or retain the stack view's owner. Inspect progress
// inside Err, before the terminal path closes and clears the numeric bundle.
func TestGlobalBatchBindingReviewLaterLaneCancellation(t *testing.T) {
	s, budget := globalBindingTestStream(t, memory.NewSchema(
		memory.Field{Name: "i", Type: memory.TypeInt64},
		memory.Field{Name: "f", Type: memory.TypeFloat64},
	), expr.Query{Aggregates: []expr.Aggregate{expr.Sum("i", "i_sum"), expr.Count("rows"), expr.Sum("f", "f_sum")}})
	if err := s.input.Close(); err != nil {
		t.Fatal(err)
	}
	calls := 0
	input := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
		calls++
		b.ClearColumns()
		if calls > 1 {
			return false, nil
		}
		b.Rows = 129
		ints, floats := make([]int64, b.Rows), make([]float64, b.Rows)
		for i := range ints {
			ints[i], floats[i] = 1, 2
		}
		b.Int64s["i"] = &memory.Int64Vector{Values: ints}
		b.Float64s["f"] = &memory.Float64Vector{Values: floats}
		return true, nil
	}}
	s.input = input
	records := 0
	s.recordSink = func(aggregateRecord) error { records++; return nil }
	canceled := false
	ctx := globalBatchCancelContext{Context: context.Background(), err: func() error {
		if canceled {
			return context.Canceled
		}
		if s.numeric != nil && s.numeric.Result(0, 0).CountRows == 129 {
			if got := s.numeric.Result(0, 1); got.SumValid {
				t.Fatal("later lane completed before cancellation check")
			}
			canceled = true
			return context.Canceled
		}
		return nil
	}}
	out := memory.NewBatch()
	ok, err := s.Next(ctx, out)
	if ok || !errors.Is(err, context.Canceled) || !canceled {
		t.Fatalf("next=%v err=%v canceled=%v", ok, err, canceled)
	}
	if out.Rows != 0 || records != 0 || budget.Used() != 0 || s.numeric != nil || s.globalBindings != nil || s.inputBatch != nil || input.closed != 1 || calls != 1 {
		t.Fatalf("partial result or retained owner: rows=%d bytes=%d closed=%d calls=%d", out.Rows, budget.Used(), input.closed, calls)
	}
	stats, diagnostics := s.Stats(), s.StreamDiagnostics()
	if stats.SelectedRows != 129 || stats.Checksum != 129 || stats.Groups != 0 || diagnostics["selected_rows"] != "129" || diagnostics["checksum"] != "129" || diagnostics["groups"] != "0" {
		t.Fatalf("partial progress inconsistent: stats=%+v diagnostics=%v", stats, diagnostics)
	}
	if ok, again := s.Next(context.Background(), memory.NewBatch()); ok || again != err {
		t.Fatalf("terminal error changed: %v %v", ok, again)
	}
	if err := s.Close(); err != nil || input.closed != 1 {
		t.Fatalf("repeat close=%v calls=%d", err, input.closed)
	}
}
