package aggregate

import (
	"context"
	"gocolumn/memory"
	"math"
	"reflect"
	"testing"
)

func TestNumericBundleInitReviewReplacementAndValidation(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	types := []memory.DataType{memory.TypeInt64, memory.TypeFloat32, memory.TypeUint64}
	layout := Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true}
	b, err := NewNumericBundle(types, []Layout{layout, layout, layout}, func(n int64) (memory.Reservation, error) {
		return manager.Reserve(context.Background(), n)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.EnsureGroups(2); err != nil {
		t.Fatal(err)
	}
	for lane, typ := range types {
		b.AddBits(0, lane, 7, true)
		b.AddBits(1, lane, 11, true)
		state := NumericResult{Type: typ, Layout: layout, SumBits: 19, MinBits: 17, MaxBits: 23,
			SumValid: true, MinValid: true, MaxValid: true, CountRows: math.MaxUint64, CountValues: 9}
		if typ == memory.TypeFloat32 {
			state.SumBits, state.MinBits, state.MaxBits = 0x7fc00019, 0x7f800017, 0xff800023
		}
		untouched := b.Result(1, lane)
		charged := manager.Used()
		if err := b.InitResult(0, lane, state); err != nil {
			t.Fatal(err)
		}
		if got := b.Result(0, lane); !reflect.DeepEqual(got, state) {
			t.Fatalf("lane %d got %+v want %+v", lane, got, state)
		}
		if !reflect.DeepEqual(b.Result(1, lane), untouched) || manager.Used() != charged {
			t.Fatal("replacement touched another group or its budget")
		}
		if allocs := testing.AllocsPerRun(100, func() {
			if err := b.InitResult(0, lane, state); err != nil {
				panic(err)
			}
		}); allocs != 0 {
			t.Fatalf("replacement allocations: %v", allocs)
		}
		for _, bad := range []NumericResult{{Type: memory.TypeString, Layout: layout}, {Type: typ, Layout: Layout{Min: true}}} {
			if err := b.InitResult(0, lane, bad); err == nil {
				t.Fatal("accepted incompatible state")
			}
			if !reflect.DeepEqual(b.Result(0, lane), state) {
				t.Fatal("invalid state mutated destination")
			}
		}
		for _, slot := range [][2]int{{-1, lane}, {b.Capacity(), lane}, {0, -1}, {0, b.Columns()}} {
			if err := b.InitResult(slot[0], slot[1], state); err == nil {
				t.Fatalf("accepted invalid slot %v", slot)
			}
			if !reflect.DeepEqual(b.Result(0, lane), state) {
				t.Fatal("invalid slot mutated destination")
			}
		}
	}
	before := b.Result(0, 1)
	if err := b.EnsureGroups(b.Capacity() + 1); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b.Result(0, 1), before) {
		t.Fatal("growth changed initialized raw bits")
	}
	if err := b.InitResult(0, 1, NumericResult{Type: types[1], Layout: layout}); err != nil {
		t.Fatal(err)
	}
	if got := b.Result(0, 1); got.SumValid || got.MinValid || got.MaxValid || got.CountRows != 0 || got.CountValues != 0 {
		t.Fatalf("empty replacement did not clear: %+v", got)
	}
	b.Close()
	if err := b.InitResult(0, 0, before); err == nil {
		t.Fatal("closed bundle accepted replacement")
	}
	var nilBundle *NumericBundle
	if err := nilBundle.InitResult(0, 0, before); err == nil {
		t.Fatal("nil bundle accepted replacement")
	}
	if manager.Used() != 0 {
		t.Fatalf("retained %d bytes", manager.Used())
	}
}
