package aggregate

import (
	"context"
	"gocolumn/memory"
	"math"
	"testing"
)

func TestNumericSumCountStateTransitions(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64}
	for _, typ := range types {
		for mask := 1; mask < 8; mask++ {
			layout := Layout{Sum: mask&1 != 0, CountRows: mask&2 != 0, CountValues: mask&4 != 0}
			t.Run(typ.String()+string(rune('0'+mask)), func(t *testing.T) {
				got, err := NewNumericVector(2, typ, layout)
				if err != nil {
					t.Fatal(err)
				}
				want, _ := NewNumericVector(2, typ, layout)
				seed := NumericResult{Type: typ, Layout: layout, CountRows: math.MaxUint64 - 2, CountValues: math.MaxUint64 - 1}
				if err := got.InitResult(1, seed); err != nil {
					t.Fatal(err)
				}
				_ = want.InitResult(1, seed)
				var view NumericSumCountState
				if !got.BorrowSumCountState(1, &view) {
					t.Fatal("bind failed")
				}
				view.Update(math.MaxUint64, 0, 0)
				if got.Result(1) != want.Result(1) {
					t.Fatal("empty update changed state")
				}
				inputs := []uint64{math.MaxUint64, 1, 1 << 31, 1 << 32, 1 << 63, 65535, 127, 1}
				if typ == memory.TypeFloat32 {
					inputs = []uint64{uint64(math.Float32bits(1 << 24)), uint64(math.Float32bits(1)), uint64(math.Float32bits(-(1 << 24))), uint64(math.Float32bits(0.5))}
				}
				if typ == memory.TypeFloat64 {
					inputs = []uint64{math.Float64bits(1 << 53), math.Float64bits(1), math.Float64bits(-(1 << 53)), math.Float64bits(0.5)}
				}
				for _, input := range inputs {
					// Compute exactly the running sum contract, including the existing state.
					sum := view.SumBits()
					if layout.Sum {
						sum = numericSum(typ, sum, normalizeNumericBits(typ, input))
					}
					view.Update(sum, 2, 1)
					want.AddBits(1, input, true)
					want.AddBits(1, 123, false)
					if got.Result(1) != want.Result(1) {
						t.Fatalf("got %+v want %+v", got.Result(1), want.Result(1))
					}
					before := got.Result(1)
					view.Update(999, 0, 0)
					if got.Result(1) != before {
						t.Fatal("null update changed valid sum")
					}
				}
				if got.Result(0).SumValid || got.Result(0).CountRows != 0 {
					t.Fatal("neighbor modified")
				}
				if typ == memory.TypeInt64 && layout.Sum && (view.legacySum != &got.legacy.sums[1] || view.sum != nil) {
					t.Fatal("not legacy backing")
				}
			})
		}
	}
}

func TestNumericSumCountStateCompleteSumRepresentation(t *testing.T) {
	cases := []struct {
		typ         memory.DataType
		input, want uint64
	}{
		{memory.TypeInt8, 300, 300}, {memory.TypeInt16, 100000, 100000},
		{memory.TypeUint8, 1000, 1000}, {memory.TypeUint16, 100000, 100000},
		{memory.TypeInt32, 0xffffffff, math.MaxUint64}, {memory.TypeUint32, 0x100000001, 1},
		{memory.TypeFloat32, 0xffffffff3f800000, 0x3f800000},
		{memory.TypeInt64, math.MaxUint64, math.MaxUint64},
	}
	for _, c := range cases {
		v, _ := NewNumericVector(1, c.typ, Layout{Sum: true})
		var s NumericSumCountState
		if !v.BorrowSumCountState(0, &s) {
			t.Fatal("bind")
		}
		s.Update(c.input, 1, 1)
		if s.SumBits() != c.want || !v.Result(0).SumValid {
			t.Fatalf("%s got %x want %x", c.typ, s.SumBits(), c.want)
		}
		s.Update(7, 1, 1)
		if s.SumBits() != 7 {
			t.Fatal("must replace, not add")
		}
	}
}

func TestNumericSumCountStateCountOnlyAllTypes(t *testing.T) {
	for typ := memory.TypeInvalid + 1; typ <= memory.TypeStruct; typ++ {
		for _, l := range []Layout{{CountRows: true}, {CountValues: true}, {CountRows: true, CountValues: true}} {
			v, err := NewNumericVector(1, typ, l)
			if err != nil {
				t.Fatal(err)
			}
			var s NumericSumCountState
			if !v.BorrowSumCountState(0, &s) {
				t.Fatalf("%s bind", typ)
			}
			s.Update(math.MaxUint64, 4, 2)
			r := v.Result(0)
			if s.SumBits() != 0 || r.SumValid || (l.CountRows && r.CountRows != 4) || (l.CountValues && r.CountValues != 2) {
				t.Fatalf("%s %+v", typ, r)
			}
		}
	}
}

func TestNumericSumCountStateRejectedAcquisition(t *testing.T) {
	good, _ := NewNumericVector(1, memory.TypeInt64, Layout{Sum: true})
	var s NumericSumCountState
	check := func(v *NumericVector, slot int) {
		t.Helper()
		good.BorrowSumCountState(0, &s)
		if v.BorrowSumCountState(slot, &s) || s != (NumericSumCountState{}) {
			t.Fatal("invalid acquisition did not clear")
		}
	}
	check(nil, 0)
	check(&NumericVector{}, 0)
	check(&good, -1)
	check(&good, 1)
	for _, l := range []Layout{{Min: true}, {Max: true}, {Avg: true}, {Sum: true, Min: true}, {Sum: true, Max: true}, {Sum: true, Avg: true}} {
		v, _ := NewNumericVector(1, memory.TypeInt64, l)
		check(&v, 0)
	}
	bad := good
	bad.typ = memory.TypeInvalid
	check(&bad, 0)
	bad = good
	bad.typ = memory.TypeString
	check(&bad, 0)
	bad = good
	bad.layout = Layout{}
	check(&bad, 0)
	bad = good
	bad.legacy.sums = nil
	check(&bad, 0)
	bad = good
	bad.legacy.sumValid = nil
	check(&bad, 0)
	native, _ := NewNumericVector(1, memory.TypeUint64, Layout{Sum: true, CountRows: true, CountValues: true})
	bad = native
	bad.sums = nil
	check(&bad, 0)
	bad = native
	bad.countRows = nil
	check(&bad, 0)
	bad = native
	bad.countValues = nil
	check(&bad, 0)
}

func TestNumericBundleSumCountStateGrowthAndClose(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	b, err := NewNumericBundle([]memory.DataType{memory.TypeInt64, memory.TypeString}, []Layout{{Sum: true, CountRows: true}, {CountValues: true}}, func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) })
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	var s NumericSumCountState
	if b.BorrowSumCountState(0, 0, &s) {
		t.Fatal("empty bundle")
	}
	if err = b.EnsureGroups(1); err != nil {
		t.Fatal(err)
	}
	if !b.BorrowSumCountState(0, 0, &s) {
		t.Fatal("bind")
	}
	s.Update(12, 4, 2)
	// Discard before growth and acquire the replacement backing afterward.
	s = NumericSumCountState{}
	if err = b.EnsureGroups(100); err != nil {
		t.Fatal(err)
	}
	if !b.BorrowSumCountState(0, 0, &s) || s.SumBits() != 12 {
		t.Fatal("rebind")
	}
	s.Update(15, 1, 1)
	if b.Result(0, 0).SumBits != 15 || b.Result(0, 0).CountRows != 5 {
		t.Fatal("growth state")
	}
	for _, bounds := range [][2]int{{-1, 0}, {b.Capacity(), 0}, {0, -1}, {0, 2}} {
		b.BorrowSumCountState(0, 0, &s)
		if b.BorrowSumCountState(bounds[0], bounds[1], &s) || s != (NumericSumCountState{}) {
			t.Fatal("bad bounds")
		}
	}
	if allocs := testing.AllocsPerRun(1000, func() { b.BorrowSumCountState(0, 0, &s); s.Update(s.SumBits()+1, 1, 1) }); allocs != 0 {
		t.Fatalf("allocations %v", allocs)
	}
	b.Close()
	if b.BorrowSumCountState(0, 0, &s) || s != (NumericSumCountState{}) {
		t.Fatal("closed")
	}
	var nilBundle *NumericBundle
	if nilBundle.BorrowSumCountState(0, 0, &s) {
		t.Fatal("nil")
	}
}

func TestNumericSumCountStateDoesNotAllocate(t *testing.T) {
	for typ := memory.TypeInvalid + 1; typ <= memory.TypeStruct; typ++ {
		layout := Layout{Sum: isAggregateNumeric(typ), CountRows: true, CountValues: true}
		v, err := NewNumericVector(1, typ, layout)
		if err != nil {
			t.Fatal(err)
		}
		var s NumericSumCountState
		if allocs := testing.AllocsPerRun(1000, func() {
			if !v.BorrowSumCountState(0, &s) {
				panic("bind")
			}
			s.Update(s.SumBits(), 1, 1)
			if v.BorrowSumCountState(-1, &s) {
				panic("invalid bind")
			}
		}); allocs != 0 {
			t.Fatalf("%s allocations %v", typ, allocs)
		}
	}
}
