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
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

func extremaReviewRaw(typ memory.DataType, row, phase int) uint64 {
	if typ == memory.TypeFloat32 || typ == memory.TypeFloat64 {
		if phase == 0 || phase == 4 {
			if typ == memory.TypeFloat32 {
				return uint64(0x7f800001 + uint32(row))
			}
			return 0x7ff0000000000001 + uint64(row)
		}
		v := []float64{0, math.Copysign(0, -1), 1, -1, math.Inf(-1), math.Inf(1), math.NaN()}[row%7]
		if phase == 3 {
			v = []float64{0, math.Copysign(0, -1)}[row%2]
		}
		if typ == memory.TypeFloat32 {
			return uint64(math.Float32bits(float32(v)))
		}
		return math.Float64bits(v)
	}
	return []uint64{0, 1, math.MaxUint64, 1 << 63, math.MaxInt64, 1 << 31, math.MaxUint32}[row%7]
}

// The original row update is the oracle. State survives empty/null batches and
// bundle growth; poisoned bitmap tails and source buffers expose stale reads.
func TestGlobalExtremaReviewStateBitsAndLifetime(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		for _, typ := range types {
			for _, kind := range []string{"min", "max", "minmax", "mixed"} {
				t.Run(fmt.Sprintf("%s/%s/%s", mode, typ, kind), func(t *testing.T) {
					q := expr.Query{Aggregates: []expr.Aggregate{expr.Count("rows"), expr.CountColumn("v", "nonnull")}}
					if kind != "max" {
						q.Aggregates = append(q.Aggregates, expr.Min("v", "lo"))
					}
					if kind != "min" {
						q.Aggregates = append(q.Aggregates, expr.Max("v", "hi"))
					}
					if kind == "mixed" {
						q.Aggregates = append(q.Aggregates, expr.Sum("v", "sum"))
					}
					fields := []memory.Field{{Name: "v", Type: typ, Nullable: true}}
					if typ == memory.TypeInt64 {
						// Pure Int64 plans intentionally retain their legacy path.
						fields = append(fields, memory.Field{Name: "opaque", Type: memory.TypeArrow})
						q.Aggregates = append(q.Aggregates, expr.CountColumn("opaque", "opaque_count"))
					}
					s, budget := globalBindingTestStream(t, memory.NewSchema(fields...), q)
					s.query.e.kernels = kernel.New(mode)
					if len(s.globalBindings) == 0 {
						t.Fatal("extrema path not selected")
					}
					m := s.query.cq.MultiAggregate
					ref, err := aggregate.NewNumericVector(1, typ, m.Layouts[0])
					if err != nil {
						t.Fatal(err)
					}
					for phase, rows := range []int{129, 0, 65, 257, 63, 1} {
						b := memory.NewBatch()
						b.Rows = rows
						if typ == memory.TypeInt64 {
							b.Opaque["opaque"] = &memory.OpaqueVector{Rows: rows}
						}
						raw := make([]uint64, rows)
						selection := make(memory.Bitmap, memory.WordsFor(rows)+1)
						var valid memory.Bitmap
						if phase != 5 {
							valid = make(memory.Bitmap, memory.WordsFor(rows))
						}
						for row := range raw {
							raw[row] = extremaReviewRaw(typ, row, phase)
							selected := phase != 3 || row < 64 || row >= 192
							isValid := phase == 5 || phase != 2 && row%11 != 0
							if selected {
								memory.Set(selection, row)
							}
							if isValid && valid != nil {
								memory.Set(valid, row)
							}
							if selected {
								ref.AddBits(0, raw[row], isValid)
							}
						}
						for row := rows; row < len(selection)*64; row++ {
							memory.Set(selection, row)
						}
						fillNativeTestColumn(b, nativeTestColumn{name: "v", typ: typ, bits: raw, valid: valid})
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
						if got, want := s.numeric.Result(0, 0), ref.Result(0); !reflect.DeepEqual(got, want) {
							t.Fatalf("phase %d got %+v want %+v", phase, got, want)
						}
						if budget.Used() != charged || s.nextRow != rows {
							t.Fatal("consumer retained memory or lost batch position")
						}
						if phase == 0 {
							state := ref.Result(0)
							state.CountRows, state.CountValues = math.MaxUint64-2, math.MaxUint64-3
							if err := ref.InitResult(0, state); err != nil {
								t.Fatal(err)
							}
							if err := s.numeric.InitResult(0, 0, state); err != nil {
								t.Fatal(err)
							}
						}
						if phase == 0 || phase == 2 {
							if err := s.numeric.EnsureGroups(s.numeric.Capacity() + 1); err != nil {
								t.Fatal(err)
							}
						}
					}
					charged := budget.Used()
					if allocs := testing.AllocsPerRun(25, func() {
						if err := s.consumeGlobalAggregateBatch(context.Background()); err != nil {
							panic(err)
						}
					}); allocs != 0 {
						t.Fatalf("consumer allocations=%v", allocs)
					}
					if budget.Used() != charged {
						t.Fatal("consumer added a charge")
					}
				})
			}
		}
	}
}

func TestGlobalExtremaReviewPriorFloatSum(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			layout := aggregate.Layout{Sum: true, Min: true, Max: true, CountRows: true, CountValues: true}
			ref, err := aggregate.NewNumericVector(1, typ, layout)
			if err != nil {
				t.Fatal(err)
			}
			one, large, negLarge := math.Float64bits(1), math.Float64bits(1e16), math.Float64bits(-1e16)
			if typ == memory.TypeFloat32 {
				one, large, negLarge = uint64(math.Float32bits(1)), uint64(math.Float32bits(1<<24)), uint64(math.Float32bits(-(1 << 24)))
			}
			ref.AddBits(0, one, true)
			state := ref.Result(0)
			ref.AddBits(0, large, true)
			ref.AddBits(0, negLarge, true)
			bound := globalAggregateLaneBinding{typ: typ, float32s: &memory.Float32Vector{Values: []float32{math.Float32frombits(uint32(large)), math.Float32frombits(uint32(negLarge))}}, float64s: &memory.Float64Vector{Values: []float64{math.Float64frombits(large), math.Float64frombits(negLarge)}}}
			if err := bound.consumeExtremaBatch(context.Background(), kernel.New(mode), memory.Bitmap{3}, 2, 2, &state); err != nil {
				t.Fatal(err)
			}
			if want := ref.Result(0); !reflect.DeepEqual(state, want) {
				t.Fatalf("%s/%s reassociated prior sum: got %+v want %+v", typ, mode, state, want)
			}
		}
	}
}

func TestGlobalExtremaReviewEligibilityLimits(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration, memory.TypeString, memory.TypeArrow} {
		m := &CompiledMultiAggregate{Native: true, Columns: []string{"wide", "other"}, Types: []memory.DataType{memory.TypeFloat64, typ}, Layouts: []aggregate.Layout{{Min: true}, {Max: true}}}
		p := &PreparedQuery{cq: CompiledQuery{GroupShape: GroupShapeGlobal, MultiAggregate: m}}
		if p.globalAggregateBindingsEligible() {
			t.Fatalf("accepted out-of-scope extrema type %s", typ)
		}
		m.Layouts[1] = aggregate.Layout{CountValues: true}
		if !p.globalAggregateBindingsEligible() {
			t.Fatalf("rejected validity-only %s beside extrema", typ)
		}
		m.Layouts[0].Avg = true
		if p.globalAggregateBindingsEligible() {
			t.Fatal("accepted AVG beside extrema")
		}
	}
}

// Raw payload comparison deliberately includes signaling NaNs and random bit
// patterns. The oracle is the existing scalar aggregate state, not the kernel.
func TestGlobalExtremaReviewRandomFloatBits(t *testing.T) {
	rng := rand.New(rand.NewSource(913573))
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		special := []uint64{0, 1, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff0000000000011, 0x7ff8000000000022, 0xfff8000000000033, math.Float64bits(1e16), math.Float64bits(-1e16), math.Float64bits(1)}
		if typ == memory.TypeFloat32 {
			special = []uint64{0, 1, 1 << 31, 0x7f800000, 0xff800000, 0x7f800011, 0x7fc00022, 0xffc00033, uint64(math.Float32bits(1 << 24)), uint64(math.Float32bits(-(1 << 24))), uint64(math.Float32bits(1))}
		}
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			for trial := 0; trial < 80; trial++ {
				layout := aggregate.Layout{Sum: true, Min: true, Max: true, CountRows: true, CountValues: true}
				ref, err := aggregate.NewNumericVector(1, typ, layout)
				if err != nil {
					t.Fatal(err)
				}
				state := ref.Result(0)
				for batch := 0; batch < 4; batch++ {
					rows := []int{1, 63, 64, 65, 129}[rng.Intn(5)]
					values32, values64 := make([]float32, rows), make([]float64, rows)
					selection, validity := make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
					var selected uint64
					for row := 0; row < rows; row++ {
						raw := special[rng.Intn(len(special))]
						if trial%5 == 0 {
							raw = rng.Uint64()
						}
						values32[row], values64[row] = math.Float32frombits(uint32(raw)), math.Float64frombits(raw)
						valid, keep := rng.Intn(5) != 0, rng.Intn(3) != 0
						if trial%7 == 0 {
							valid, keep = true, true
						}
						if valid {
							memory.Set(validity, row)
						}
						if keep {
							memory.Set(selection, row)
							selected++
							ref.AddBits(0, raw, valid)
						}
					}
					// Unused bits do not correspond to values, even if both masks agree.
					selection[len(selection)-1] |= ^memory.TailMask(rows - (len(selection)-1)*64)
					validity[len(validity)-1] |= ^memory.TailMask(rows - (len(validity)-1)*64)
					bound := globalAggregateLaneBinding{typ: typ, validity: validity, float32s: &memory.Float32Vector{Values: values32}, float64s: &memory.Float64Vector{Values: values64}}
					if err := bound.consumeExtremaBatch(context.Background(), kernel.New(mode), selection, rows, selected, &state); err != nil {
						t.Fatal(err)
					}
					if want := ref.Result(0); !reflect.DeepEqual(state, want) {
						t.Fatalf("type=%s mode=%s trial=%d batch=%d got %+v want %+v", typ, mode, trial, batch, state, want)
					}
				}
			}
		}
	}
}

func TestGlobalExtremaReviewCanceledLaneNotCommitted(t *testing.T) {
	s, budget := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64}), expr.Query{Aggregates: []expr.Aggregate{expr.Min("v", "lo"), expr.Max("v", "hi"), expr.Count("rows")}})
	b := memory.NewBatch()
	b.Rows = 129
	b.Float64s["v"] = &memory.Float64Vector{Values: make([]float64, b.Rows)}
	for row := range b.Float64s["v"].Values {
		b.Float64s["v"].Values[row] = float64(row)
	}
	binding, err := s.query.bindSortedBatch(context.Background(), b, &s.predicate)
	if err != nil {
		t.Fatal(err)
	}
	s.inputBatch, s.binding, s.selection = b, binding, memory.Bitmap{math.MaxUint64, math.MaxUint64, 1}
	s.bindGlobalAggregateInputs()
	before, charged := s.numeric.Result(0, 0), budget.Used()
	calls := 0
	ctx := globalBatchCancelContext{Context: context.Background(), err: func() error {
		calls++
		if calls >= 6 {
			return context.Canceled
		}
		return nil
	}}
	if err := s.consumeGlobalAggregateBatch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wanted in-lane cancellation, got %v after %d checks", err, calls)
	}
	if calls != 6 || !reflect.DeepEqual(s.numeric.Result(0, 0), before) || budget.Used() != charged {
		t.Fatal("failed lane committed a partial snapshot or changed its budget")
	}
}

func TestGlobalExtremaReviewCancellationAndBudget(t *testing.T) {
	s, budget := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "f", Type: memory.TypeFloat64}), expr.Query{Aggregates: []expr.Aggregate{expr.Min("i", "lo"), expr.Count("rows"), expr.Max("f", "hi")}})
	if s.globalBindingLease.Bytes() != int64(len(s.globalBindings))*int64(unsafe.Sizeof(globalAggregateLaneBinding{})) {
		t.Fatal("binding metadata not fully charged")
	}
	limit := budget.Used() - s.globalBindingLease.Bytes()
	small, err := memory.NewEngineBudget(0).BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
	if err != nil {
		t.Fatal(err)
	}
	defer small.Close()
	failed, err := s.query.NewSortedAggregateStream(memory.WithQueryBudget(context.Background(), small), s.query.resultSchema(), 1)
	if failed != nil || !errors.Is(err, memory.ErrBudgetExceeded) || !strings.Contains(err.Error(), "reserve global aggregate bindings") || small.Used() != 0 {
		t.Fatalf("constructor rollback: stream=%v err=%v bytes=%d", failed, err, small.Used())
	}
	if err := s.input.Close(); err != nil {
		t.Fatal(err)
	}
	input := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
		b.ClearColumns()
		b.Rows = 129
		ints, floats := make([]int64, b.Rows), make([]float64, b.Rows)
		for i := range ints {
			ints[i], floats[i] = 1, 2
		}
		b.Int64s["i"], b.Float64s["f"] = &memory.Int64Vector{Values: ints}, &memory.Float64Vector{Values: floats}
		return true, nil
	}}
	s.input = input
	records, canceled := 0, false
	s.recordSink = func(aggregateRecord) error { records++; return nil }
	ctx := globalBatchCancelContext{Context: context.Background(), err: func() error {
		if canceled {
			return context.Canceled
		}
		if s.numeric != nil && s.numeric.Result(0, 0).MinValid {
			if s.numeric.Result(0, 1).MaxValid {
				t.Fatal("later lane committed before cancellation")
			}
			canceled = true
			return context.Canceled
		}
		return nil
	}}
	out := memory.NewBatch()
	ok, err := s.Next(ctx, out)
	if ok || !errors.Is(err, context.Canceled) || !canceled {
		t.Fatalf("cancellation: %v %v %v", ok, err, canceled)
	}
	if records != 0 || out.Rows != 0 || budget.Used() != 0 || s.numeric != nil || s.globalBindings != nil || s.inputBatch != nil || input.closed != 1 {
		t.Fatal("cancellation published a row or retained an owner")
	}
	if ok, again := s.Next(context.Background(), out); ok || again != err {
		t.Fatal("terminal cancellation changed")
	}
	if err := s.Close(); err != nil || input.closed != 1 {
		t.Fatal("cleanup was not idempotent")
	}
}

func TestGlobalExtremaReviewRecycledInputAndValidation(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprintf("malformed=%v", malformed), func(t *testing.T) {
			q := expr.Query{Aggregates: []expr.Aggregate{expr.Min("v", "lo"), expr.Max("v", "hi"), expr.CountColumn("v", "n")}}
			if malformed {
				q.Filter = expr.Bool(false)
			}
			s, budget := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true}), q)
			if err := s.input.Close(); err != nil {
				t.Fatal(err)
			}
			v, index := &memory.Float64Vector{}, 0
			input := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
				assertGlobalBindingsCleared(t, s)
				b.ClearColumns()
				if malformed {
					b.Rows = 65
					b.Float64s["v"] = &memory.Float64Vector{Values: make([]float64, 65), Validity: memory.Bitmap{1}}
					return true, nil
				}
				switch index {
				case 0:
					v.Values, v.Validity = []float64{math.Float64frombits(0x7ff0000000000011), 999}, memory.Bitmap{1}
				case 1:
					v.Values, v.Validity = nil, nil
				case 2:
					v.Values, v.Validity = []float64{0, math.Copysign(0, -1)}, nil
				case 3:
					v.Values, v.Validity = []float64{-999, 999}, memory.Bitmap{0}
				default:
					v.Values, v.Validity = nil, nil
					return false, nil
				}
				index++
				b.Float64s["v"], b.Rows = v, len(v.Values)
				return true, nil
			}}
			s.input = input
			out := memory.NewBatch()
			ok, err := s.Next(context.Background(), out)
			if malformed {
				if ok || err == nil || !strings.Contains(err.Error(), "shorter than batch") || budget.Used() != 0 {
					t.Fatalf("zero-selection validation: %v %v bytes=%d", ok, err, budget.Used())
				}
				return
			}
			if !ok || err != nil {
				t.Fatalf("Next: %v %v", ok, err)
			}
			if math.Float64bits(out.Float64s["lo"].Values[0]) != 1<<63 || math.Float64bits(out.Float64s["hi"].Values[0]) != 0 || out.Uint64s["n"].Values[0] != 3 {
				t.Fatal("recycled header changed extrema/count")
			}
			assertGlobalBindingsCleared(t, s)
			if input.closed != 1 {
				t.Fatal("source not closed at EOF")
			}
			if ok, err := s.Next(context.Background(), memory.NewBatch()); ok || err != nil || budget.Used() != 0 || out.Rows != 0 {
				t.Fatal("result owner not released at EOF")
			}
		})
	}
}
