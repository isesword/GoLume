package aggregate

import (
	"math"
	"testing"
)

func TestSumCountInitUpdaterIndependentOracle(t *testing.T) {
	for layoutBits := 0; layoutBits < 64; layoutBits++ {
		layout := Layout{Sum: layoutBits&1 != 0, CountRows: layoutBits&2 != 0, CountValues: layoutBits&4 != 0, Min: layoutBits&8 != 0, Max: layoutBits&16 != 0, Avg: layoutBits&32 != 0}
		vector := NewInt64Vector(3, layout)
		updater, ok := vector.SumCountUpdater()
		if ok != (layoutBits == 0 || layoutBits == 3) {
			t.Fatalf("layout%d checked=%v", layoutBits, ok)
		}
		if !ok {
			continue
		}
		for repeat := 0; repeat < 3; repeat++ {
			for _, value := range []int64{0, math.MinInt64, math.MaxInt64, -1, 1} {
				for _, valid := range []bool{false, true} {
					for slot := 0; slot < 3; slot++ {
						// Pollute old storage before every initialization, including NULL.
						vector.InitValue(slot, 99, true)
						vector.AddValue(slot, 11, true)
						updater.InitValue(slot, value, valid)
						want := Int64Result{CountRows: 1}
						if valid {
							want.Sum, want.SumValid = value, true
						}
						if got := vector.Result(slot); got != want {
							t.Fatalf("layout%d slot%d value%d valid%v got=%+v want=%+v", layoutBits, slot, value, valid, got, want)
						}
						// The generic entry point calls the same private transition.
						vector.InitValue(slot, value, valid)
						if got := vector.Result(slot); got != want {
							t.Fatalf("generic got=%+v want=%+v", got, want)
						}
					}
				}
			}
			// A checked updater follows replacement arrays at the same vector.
			vector = NewInt64Vector(3, layout)
		}
	}
}
