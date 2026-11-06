package aggregate

import (
	"math"
	"testing"
)

func TestSumCountUpdaterLayoutAndScalarOracle(t *testing.T) {
	layouts := []Layout{{}, {Sum: true, CountRows: true}, {Sum: true}, {CountRows: true}, {CountValues: true}, {Min: true}, {Max: true}, {Avg: true}, {Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}}
	values := []int64{0, math.MaxInt64, 1, math.MinInt64, -1, 7, 0}
	for li, layout := range layouts {
		normalized := layout
		if normalized.IsZero() {
			normalized = DefaultLayout()
		}
		for pattern := 0; pattern < 3; pattern++ {
			vector := NewInt64Vector(1, layout)
			updater, ok := vector.SumCountUpdater()
			if ok != normalized.IsSumCountOnly() {
				t.Fatalf("layout=%+v selected=%v", layout, ok)
			}
			var sum, min, max int64
			var rows, count uint64
			for i, value := range values {
				valid := pattern == 0 || pattern == 1 && i%3 == 2
				if i == 0 {
					vector.InitValue(0, value, valid)
				} else if ok {
					updater.AddValue(0, value, valid)
				} else {
					vector.AddValue(0, value, valid)
				}
				rows++
				if valid {
					sum += value
					if count == 0 || value < min {
						min = value
					}
					if count == 0 || value > max {
						max = value
					}
					count++
				}
				var want Int64Result
				if normalized.Sum || normalized.Avg {
					want.Sum = sum
					want.SumValid = count > 0
				}
				if normalized.CountRows {
					want.CountRows = rows
				}
				if normalized.CountValues || normalized.Avg {
					want.CountValues = count
				}
				if normalized.Min {
					want.Min = min
					want.MinValid = count > 0
				}
				if normalized.Max {
					want.Max = max
					want.MaxValid = count > 0
				}
				if normalized.Avg && count > 0 {
					want.Avg = float64(sum) / float64(count)
					want.AvgValid = true
				}
				if got := vector.Result(0); got != want {
					t.Fatalf("layout%d pattern%d row%d got=%+v want=%+v", li, pattern, i, got, want)
				}
				if i == 2 {
					// Rehash replaces the vector's backing arrays, not its address. A view
					// selected before replacement must update the newly owned arrays.
					grown := NewInt64Vector(16, layout)
					vector.Copy(&grown, 0, 0)
					vector = grown
				}
			}
		}
	}
	var nilVector *Int64Vector
	if _, ok := nilVector.SumCountUpdater(); ok {
		t.Fatal("nil vector selected an updater")
	}
}

func TestSumCountUpdaterMatchesGeneralTransition(t *testing.T) {
	fast := NewInt64Vector(1, DefaultLayout())
	reference := NewInt64Vector(1, Layout{Sum: true, CountRows: true, CountValues: true})
	fast.InitValue(0, 0, false)
	reference.InitValue(0, 0, false)
	updater, ok := fast.SumCountUpdater()
	if !ok {
		t.Fatal("missing updater")
	}
	for i := 0; i < 1000; i++ {
		valid := i%7 != 0
		value := int64(i - 500)
		updater.AddValue(0, value, valid)
		reference.AddValue(0, value, valid)
		got, want := fast.Result(0), reference.Result(0)
		if got.Sum != want.Sum || got.SumValid != want.SumValid || got.CountRows != want.CountRows {
			t.Fatalf("different state at %d", i)
		}
	}
}
