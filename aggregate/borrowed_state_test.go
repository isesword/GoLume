package aggregate

import (
	"math"
	"testing"
)

func TestBorrowSumCountStateCheckedLayout(t *testing.T) {
	accepted := NewInt64Vector(2, DefaultLayout())
	for mask := 0; mask < 64; mask++ {
		var view SumCountState
		if !accepted.BorrowSumCountState(&view) {
			t.Fatal("default layout rejected")
		}
		layout := Layout{Sum: mask&1 != 0, CountRows: mask&2 != 0, CountValues: mask&4 != 0, Min: mask&8 != 0, Max: mask&16 != 0, Avg: mask&32 != 0}
		vector := NewInt64Vector(2, layout)
		ok := vector.BorrowSumCountState(&view)
		if ok != (mask == 0 || mask == 3) {
			t.Fatalf("layout%d accepted=%v", mask, ok)
		}
		if !ok && (view.sums != nil || view.sumValid != nil || view.countRows != nil) {
			t.Fatalf("layout%d retains prior borrow", mask)
		}
	}
	var nilVector *Int64Vector
	var view SumCountState
	accepted.BorrowSumCountState(&view)
	if nilVector.BorrowSumCountState(&view) || view.sums != nil || view.sumValid != nil || view.countRows != nil {
		t.Fatal("nil vector must clear view")
	}
}

func TestBorrowSumCountStateIndependentTransitionsAndRebind(t *testing.T) {
	type row struct {
		value int64
		valid bool
	}
	sequences := [][]row{
		{{99, false}, {88, false}},
		{{99, false}, {3, true}, {0, false}, {-4, true}},
		{{math.MaxInt64, true}, {1, true}, {math.MinInt64, true}},
		{{math.MinInt64, true}, {-1, true}, {math.MaxInt64, true}},
	}
	vector := NewInt64Vector(3, DefaultLayout())
	live, ok := vector.SumCountUpdater()
	if !ok {
		t.Fatal("live updater rejected")
	}
	var borrowed SumCountState
	for generation := 0; generation < 4; generation++ {
		if !vector.BorrowSumCountState(&borrowed) {
			t.Fatal("borrow rejected")
		}
		for _, sequence := range sequences {
			for slot := 0; slot < 3; slot++ {
				var sum int64
				var valid bool
				var count uint64
				for index, r := range sequence {
					if index == 0 {
						vector.InitValue(slot, 77, true)
						vector.AddValue(slot, 31, true)
						borrowed.InitValue(slot, r.value, r.valid)
					} else {
						borrowed.AddValue(slot, r.value, r.valid)
					}
					count++
					if r.valid {
						sum += r.value
						valid = true
					}
					want := Int64Result{Sum: sum, SumValid: valid, CountRows: count}
					if got := vector.Result(slot); got != want {
						t.Fatalf("borrow generation%d slot%d row%d got%+v want%+v", generation, slot, index, got, want)
					}
				}
				// Compare every generic and live entry point with an independently
				// calculated expected result, not with another transition implementation.
				for _, kind := range []string{"generic", "live"} {
					sum, valid, count = 0, false, 0
					for index, r := range sequence {
						if kind == "generic" {
							if index == 0 {
								vector.InitValue(slot, r.value, r.valid)
							} else {
								vector.AddValue(slot, r.value, r.valid)
							}
						} else {
							if index == 0 {
								live.InitValue(slot, r.value, r.valid)
							} else {
								live.AddValue(slot, r.value, r.valid)
							}
						}
						count++
						if r.valid {
							sum += r.value
							valid = true
						}
						want := Int64Result{Sum: sum, SumValid: valid, CountRows: count}
						if got := vector.Result(slot); got != want {
							t.Fatalf("%s generation%d slot%d row%d got%+v want%+v", kind, generation, slot, index, got, want)
						}
					}
				}
			}
		}
		// This simulates rehash replacing backing arrays at the same vector.
		// Discard the borrowed view before replacement. The old live updater must
		// keep following vector replacement; the next loop explicitly reborrows.
		borrowed = SumCountState{}
		vector = NewInt64Vector(3, DefaultLayout())
		live.InitValue(0, 101, true)
		live.AddValue(0, -1, true)
		if got := vector.Result(0); got != (Int64Result{Sum: 100, SumValid: true, CountRows: 2}) {
			t.Fatalf("live updater lost replacement: %+v", got)
		}
	}
}

func TestBorrowSumCountStateDoesNotAllocate(t *testing.T) {
	vector := NewInt64Vector(1, DefaultLayout())
	if n := testing.AllocsPerRun(10, func() {
		var view SumCountState
		if !vector.BorrowSumCountState(&view) {
			panic("borrow failed")
		}
		view.InitValue(0, 3, true)
		view.AddValue(0, 4, true)
	}); n != 0 {
		t.Fatalf("borrow/update allocated %g times", n)
	}
}
