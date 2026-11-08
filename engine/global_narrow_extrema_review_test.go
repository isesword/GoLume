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
	"math/bits"
	"reflect"
	"testing"
)

func TestGlobalNarrowExtremaPriorStateAndMasks(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
			t.Run(fmt.Sprintf("%s/%s", mode, typ), func(t *testing.T) {
				a, b, c, d := make([]int8, 66), make([]int16, 66), make([]uint8, 66), make([]uint16, 66)
				for i := range a {
					a[i] = int8(i*83 - 128)
					b[i] = int16(i*19001 - 32768)
					c[i] = uint8(i*83 + 255)
					d[i] = uint16(i*19001 + 65535)
				}
				bound := globalAggregateLaneBinding{typ: typ, int8s: &memory.Int8Vector{Values: a[1:]}, int16s: &memory.Int16Vector{Values: b[1:]}, uint8s: &memory.Uint8Vector{Values: c[1:]}, uint16s: &memory.Uint16Vector{Values: d[1:]}}
				for n := 0; n <= 64; n++ {
					for _, mask := range []uint64{0, 1, 1 << 63, 0xaaaaaaaaaaaaaaaa, 0x5555555555555555, ^uint64(0)} {
						for _, valid := range []memory.Bitmap{nil, {0}, {0x5555555555555555}, {^uint64(0)}} {
							for _, prior := range []uint64{0, 1 << 32, 1<<63 - 1, 1 << 63, ^uint64(0)} {
								ref, err := aggregate.NewNumericVector(1, typ, aggregate.Layout{Sum: true, Min: true, Max: true, CountRows: true, CountValues: true})
								if err != nil {
									t.Fatal(err)
								}
								state := ref.Result(0)
								state.SumBits, state.SumValid = prior, true
								state.CountRows, state.CountValues = math.MaxUint64-2, math.MaxUint64-3
								// Alternate a fresh extrema lane and an existing endpoint snapshot.
								if prior != 0 {
									state.MinBits, state.MaxBits, state.MinValid, state.MaxValid = 0, 1, true, true
								}
								if err := ref.InitResult(0, state); err != nil {
									t.Fatal(err)
								}
								selected := mask & memory.TailMask(n)
								for i := 0; i < n; i++ {
									if selected&(uint64(1)<<i) == 0 {
										continue
									}
									var raw uint64
									switch typ {
									case memory.TypeInt8:
										raw = uint64(int64(a[i+1]))
									case memory.TypeInt16:
										raw = uint64(int64(b[i+1]))
									case memory.TypeUint8:
										raw = uint64(c[i+1])
									case memory.TypeUint16:
										raw = uint64(d[i+1])
									}
									ref.AddBits(0, raw, valid == nil || valid[0]&(uint64(1)<<i) != 0)
								}
								bound.validity = valid
								if err := bound.consumeExtremaBatch(context.Background(), kernel.New(mode), memory.Bitmap{mask}, n, uint64(bits.OnesCount64(selected)), &state); err != nil {
									t.Fatal(err)
								}
								if want := ref.Result(0); !reflect.DeepEqual(state, want) {
									t.Fatalf("n=%d mask=%x valid=%x prior=%x got=%+v want=%+v", n, mask, valid, prior, state, want)
								}
							}
						}
					}
				}
			})
		}
	}
}

func TestGlobalNarrowExtremaCanceledLaneNotCommitted(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
		t.Run(typ.String(), func(t *testing.T) {
			s, budget := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "v", Type: typ}), expr.Query{Aggregates: []expr.Aggregate{expr.Min("v", "lo"), expr.Max("v", "hi"), expr.Sum("v", "sum"), expr.Count("rows")}})
			b := memory.NewBatch()
			b.Rows = 129
			raw := make([]uint64, b.Rows)
			for i := range raw {
				raw[i] = uint64(i)
			}
			fillNativeTestColumn(b, nativeTestColumn{name: "v", typ: typ, bits: raw})
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
				t.Fatalf("wanted cancellation, got %v", err)
			}
			if calls != 6 || !reflect.DeepEqual(s.numeric.Result(0, 0), before) || budget.Used() != charged {
				t.Fatal("partial canceled state committed or budget changed")
			}
		})
	}
}
