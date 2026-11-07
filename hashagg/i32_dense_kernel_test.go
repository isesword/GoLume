package hashagg

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"gocolumn/memory"
)

func TestI32DenseAllLayoutsIndependentOracle(t *testing.T) {
	for mask := 0; mask < 64; mask++ {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			layout := i32Layout(mask)
			for _, n := range []int{0, 1, 63, 64, 65, 129, 257} {
				keys, vals := make([]int32, n), make([]int64, n)
				for i := range keys {
					keys[i] = int32(i%71 - 35)
					vals[i] = int64(i%17 - 8)
					switch i % 31 {
					case 0:
						keys[i], vals[i] = math.MinInt32, math.MinInt64
					case 1:
						keys[i], vals[i] = math.MaxInt32, math.MaxInt64
					}
				}
				ledger := &allocationLedger{}
				table, err := NewI32TableWithOptions(1, TableOptions{Aggregates: layout, Reserve: ledger.reserve})
				if err != nil {
					t.Fatal(err)
				}
				for generation := 0; generation < 3; generation++ {
					if generation == 2 {
						table.gen = math.MaxUint32
					}
					table.Reset()
					oracle := i32ScalarOracle{}
					for repeat := 0; repeat < 2; repeat++ {
						var want Summary
						for i, k := range keys {
							i32AddSummary(&want, oracle.add(k, true, vals[i], true))
						}
						got, err := table.AggregateValues(keys, vals)
						if err != nil || got != want {
							t.Fatalf("n%d generation%d repeat%d got%+v want%+v err%v", n, generation, repeat, got, want, err)
						}
						i32CheckOracle(t, table, oracle, layout)
					}
				}
				table.Close()
				ledger.checkReleased(t)
			}
		})
	}
}

func TestI32DenseGrowthFailureRetry(t *testing.T) {
	for _, load := range []float64{.01, .125, .5, .7, .9} {
		for fail := 3; fail <= 7; fail++ {
			t.Run(fmt.Sprintf("load%.3f/fail%d", load, fail), func(t *testing.T) {
				const n = 513
				keys, vals := make([]int32, n), make([]int64, n)
				for i := range keys {
					keys[i] = int32(i)
					vals[i] = int64(i - 19)
					if i%5 == 0 && i > 0 {
						keys[i] = keys[i-1]
					}
				}
				ledger := &allocationLedger{failAt: fail}
				table, err := NewI32TableWithOptions(1, TableOptions{MaxLoadFactor: load, Reserve: ledger.reserve})
				if err != nil {
					t.Fatal(err)
				}
				scalarLedger := &allocationLedger{failAt: fail}
				scalar, err := NewI32TableWithOptions(1, TableOptions{MaxLoadFactor: load, Reserve: scalarLedger.reserve})
				if err != nil {
					t.Fatal(err)
				}
				var want Summary
				oracle := i32ScalarOracle{}
				var scalarErr error
				for i, k := range keys {
					_, scalarErr = scalar.TryAdd(k, vals[i])
					if scalarErr != nil {
						break
					}
					i32AddSummary(&want, oracle.add(k, true, vals[i], true))
				}
				got, err := table.AggregateValues(keys, vals)
				if !errors.Is(err, memory.ErrBudgetExceeded) || !errors.Is(scalarErr, memory.ErrBudgetExceeded) || got != want {
					t.Fatalf("got%+v want%+v err%v scalarErr%v", got, want, err, scalarErr)
				}
				i32CheckOracle(t, table, oracle, table.opts.Aggregates)
				if table.Capacity() != scalar.Capacity() || !reflect.DeepEqual(table.keys, scalar.keys) || !reflect.DeepEqual(table.epoch, scalar.epoch) || ledger.used != scalarLedger.used || ledger.peak != scalarLedger.peak || ledger.attempts != scalarLedger.attempts {
					t.Fatal("partial layout or reservation mismatch")
				}
				for i := want.Rows; i < n; i++ {
					i32AddSummary(&want, oracle.add(keys[i], true, vals[i], true))
				}
				remaining, retryErr := table.AggregateValues(keys[got.Rows:], vals[got.Rows:])
				if retryErr != nil {
					t.Fatal(retryErr)
				}
				i32AddSummary(&got, remaining)
				if got != want {
					t.Fatalf("retry%+v want%+v", got, want)
				}
				i32CheckOracle(t, table, oracle, table.opts.Aggregates)
				table.Close()
				table.Close()
				scalar.Close()
				ledger.checkReleased(t)
				scalarLedger.checkReleased(t)
			})
		}
	}
}

func TestI32DenseCancellationBoundaries(t *testing.T) {
	for _, boundary := range []int{0, 1, 63, 64, 65, 127, 128} {
		t.Run(fmt.Sprint(boundary), func(t *testing.T) {
			calls := 0
			deny := true
			ledger := &allocationLedger{}
			reserve := func(n int64) (memory.Reservation, error) {
				calls++
				if deny && calls == 3 {
					return nil, context.Canceled
				}
				return ledger.reserve(n)
			}
			table, err := NewI32TableWithOptions(128, TableOptions{MaxLoadFactor: .5, Reserve: reserve})
			if err != nil {
				t.Fatal(err)
			}
			oracle := i32ScalarOracle{}
			for i := 0; i < 128-boundary; i++ {
				if _, err = table.TryAdd(int32(i), int64(i)); err != nil {
					t.Fatal(err)
				}
				oracle.add(int32(i), true, int64(i), true)
			}
			keys, vals := make([]int32, boundary+3), make([]int64, boundary+3)
			for i := range keys {
				keys[i] = int32(i + 10000)
				vals[i] = int64(i - 42)
			}
			var want Summary
			for i := 0; i < boundary; i++ {
				i32AddSummary(&want, oracle.add(keys[i], true, vals[i], true))
			}
			got, err := table.AggregateValues(keys, vals)
			if !errors.Is(err, context.Canceled) || got != want {
				t.Fatalf("boundary%d got%+v want%+v err%v", boundary, got, want, err)
			}
			i32CheckOracle(t, table, oracle, table.opts.Aggregates)
			deny = false
			part, err := table.AggregateValues(keys[boundary:], vals[boundary:])
			if err != nil {
				t.Fatal(err)
			}
			var remainder Summary
			for i := boundary; i < len(keys); i++ {
				i32AddSummary(&remainder, oracle.add(keys[i], true, vals[i], true))
			}
			if part != remainder {
				t.Fatalf("retry%+v want%+v", part, remainder)
			}
			i32CheckOracle(t, table, oracle, table.opts.Aggregates)
			table.Close()
			ledger.checkReleased(t)
		})
	}
}
