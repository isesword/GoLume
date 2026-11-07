package hashagg

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"gocolumn/aggregate"
	"gocolumn/memory"
)

type i32OracleKey struct {
	key   int32
	valid bool
}

type i32OracleState struct {
	sum, min, max int64
	rows, values  uint64
}

type i32ScalarOracle map[i32OracleKey]i32OracleState

// add deliberately uses neither hashagg nor aggregate transitions.
func (o i32ScalarOracle) add(key int32, keyValid bool, value int64, valueValid bool) Summary {
	if !keyValid {
		key = 0
	}
	id := i32OracleKey{key, keyValid}
	s, exists := o[id]
	out := Summary{Rows: 1}
	if !exists {
		out.Groups = 1
	}
	s.rows++
	if valueValid {
		if s.values == 0 || value < s.min {
			s.min = value
		}
		if s.values == 0 || value > s.max {
			s.max = value
		}
		s.sum += value
		s.values++
		out.Checksum = value
	}
	o[id] = s
	return out
}

func i32Layout(bits int) aggregate.Layout {
	return aggregate.Layout{Sum: bits&1 != 0, CountRows: bits&2 != 0, CountValues: bits&4 != 0, Min: bits&8 != 0, Max: bits&16 != 0, Avg: bits&32 != 0}
}

func (o i32ScalarOracle) results(layout aggregate.Layout) map[i32OracleKey]aggregate.Int64Result {
	if layout.IsZero() {
		layout = aggregate.DefaultLayout()
	}
	out := make(map[i32OracleKey]aggregate.Int64Result, len(o))
	for k, s := range o {
		var r aggregate.Int64Result
		if layout.Sum || layout.Avg {
			r.Sum, r.SumValid = s.sum, s.values != 0
		}
		if layout.CountRows {
			r.CountRows = s.rows
		}
		if layout.CountValues || layout.Avg {
			r.CountValues = s.values
		}
		if layout.Min {
			r.Min, r.MinValid = s.min, s.values != 0
		}
		if layout.Max {
			r.Max, r.MaxValid = s.max, s.values != 0
		}
		if layout.Avg && s.values != 0 {
			r.Avg, r.AvgValid = float64(s.sum)/float64(s.values), true
		}
		out[k] = r
	}
	return out
}

func i32Snapshot(tab *I32Table) map[i32OracleKey]aggregate.Int64Result {
	out := make(map[i32OracleKey]aggregate.Int64Result)
	_ = tab.Visit(func(k int32, valid bool, state aggregate.Int64Result) error {
		out[i32OracleKey{k, valid}] = state
		return nil
	})
	return out
}

func i32AddSummary(dst *Summary, src Summary) {
	dst.Rows += src.Rows
	dst.Groups += src.Groups
	dst.Checksum += src.Checksum
}

func i32CheckOracle(t *testing.T, tab *I32Table, oracle i32ScalarOracle, layout aggregate.Layout) {
	t.Helper()
	if got, want := i32Snapshot(tab), oracle.results(layout); !reflect.DeepEqual(got, want) || tab.Len() != len(want) {
		t.Fatalf("states differ: got=%v want=%v table size=%d", got, want, tab.Len())
	}
}

func i32BitmapMode(n, mode, period int) memory.Bitmap {
	if mode == 0 {
		return nil
	}
	bm := make(memory.Bitmap, memory.WordsFor(n))
	for i := 0; i < n; i++ {
		if mode == 3 || mode == 1 && i%period != 0 {
			memory.Set(bm, i)
		}
	}
	return bm
}

func TestI32BatchKernelAllLayoutsIndependentOracle(t *testing.T) {
	for layoutBits := 0; layoutBits < 64; layoutBits++ {
		for km := 0; km < 4; km++ {
			for vm := 0; vm < 4; vm++ {
				t.Run(fmt.Sprintf("layout%d/key%d/value%d", layoutBits, km, vm), func(t *testing.T) {
					layout := i32Layout(layoutBits)
					for _, n := range []int{0, 1, 63, 64, 65, 129, 257} {
						keys, vals := make([]int32, n), make([]int64, n)
						for i := range keys {
							keys[i], vals[i] = int32(i%97-48), int64(i%23-11)
							switch i % 29 {
							case 0:
								keys[i], vals[i] = math.MinInt32, math.MinInt64
							case 1:
								keys[i], vals[i] = math.MaxInt32, math.MaxInt64
							case 2:
								keys[i], vals[i] = 0, 1
							}
						}
						kv, vv := i32BitmapMode(n, km, 7), i32BitmapMode(n, vm, 5)
						m := &allocationLedger{}
						tab, err := NewI32TableWithOptions(1, TableOptions{Aggregates: layout, Reserve: m.reserve, MaxLoadFactor: .5})
						if err != nil {
							t.Fatal(err)
						}
						for epoch := 0; epoch < 3; epoch++ {
							// Include epoch rollover, where stale slots must be cleared.
							if epoch == 2 {
								tab.gen = math.MaxUint32
							}
							tab.Reset()
							oracle := i32ScalarOracle{}
							for selection := 0; selection < 3; selection++ {
								sel := make(memory.Bitmap, memory.WordsFor(n)+1)
								for i := range keys {
									if selection == 1 || selection == 2 && i%3 != 0 {
										memory.Set(sel, i)
									}
								}
								// Extra words and dirty high tail bits cannot read beyond keys/vals.
								if selection != 0 {
									for i := n; i < len(sel)*64; i++ {
										memory.Set(sel, i)
									}
								}
								for repeat := 0; repeat < 2; repeat++ {
									var want Summary
									for i, key := range keys {
										if memory.IsSet(sel, i) {
											i32AddSummary(&want, oracle.add(key, kv == nil || memory.IsSet(kv, i), vals[i], vv == nil || memory.IsSet(vv, i)))
										}
									}
									got, err := tab.AggregateSelectedNullableValues(keys, kv, vals, vv, sel)
									if err != nil || got != want {
										t.Fatalf("n%d epoch%d selection%d repeat%d got=%+v want=%+v err=%v", n, epoch, selection, repeat, got, want, err)
									}
									i32CheckOracle(t, tab, oracle, layout)
								}
							}
						}
						if m.used != tab.RetainedBytes() {
							t.Fatalf("charged=%d retained=%d", m.used, tab.RetainedBytes())
						}
						tab.Close()
						tab.Close()
						m.checkReleased(t)
						if tab.Len() != 0 || tab.Capacity() != 0 || tab.RetainedBytes() != 0 {
							t.Fatal("Close retained table state")
						}
					}
				})
			}
		}
	}
}

func TestI32BatchKernelGrowthFailureRetryMatchesScalar(t *testing.T) {
	for _, factor := range []float64{.01, .125, .5, .7, .9} {
		for _, failAt := range []int{3, 4, 5, 6} {
			for _, sparse := range []bool{false, true} {
				t.Run(fmt.Sprintf("factor%g/fail%d/sparse%v", factor, failAt, sparse), func(t *testing.T) {
					fastLedger, scalarLedger := &allocationLedger{failAt: failAt}, &allocationLedger{failAt: failAt}
					fast, err := NewI32TableWithOptions(1, TableOptions{MaxLoadFactor: factor, Reserve: fastLedger.reserve})
					if err != nil {
						t.Fatal(err)
					}
					scalar, err := NewI32TableWithOptions(1, TableOptions{MaxLoadFactor: factor, Reserve: scalarLedger.reserve})
					if err != nil {
						t.Fatal(err)
					}
					defer scalar.Close()
					defer fast.Close()
					// Each growth reserves precisely a doubled backing while the old
					// backing and NULL slot are still owned. No early growth is allowed.
					fastLedger.before = func(n int64) {
						slots := int64(cap(fast.keys)+cap(fast.epoch))*4 + physicalStateBytes(fast.state)
						actual := slots + physicalStateBytes(fast.nullState)
						if n != 2*slots || fastLedger.used != actual {
							t.Fatalf("reserve=%d slots=%d charged=%d actual=%d", n, slots, fastLedger.used, actual)
						}
						if fast.size+1 <= fast.maxEntries {
							t.Fatal("reserved before an insertion required growth")
						}
					}
					keys, vals := make([]int32, 513), make([]int64, 513)
					sel := make(memory.Bitmap, memory.WordsFor(len(keys)))
					for i := range keys {
						keys[i], vals[i] = int32(i/2-128), int64(i%31-15)
						if !sparse || i%3 != 0 {
							memory.Set(sel, i)
						}
					}
					var want Summary
					oracle := i32ScalarOracle{}
					for i, key := range keys {
						if !memory.IsSet(sel, i) {
							continue
						}
						_, err := scalar.TryAdd(key, vals[i])
						if err != nil {
							if !errors.Is(err, memory.ErrBudgetExceeded) {
								t.Fatal(err)
							}
							break
						}
						i32AddSummary(&want, oracle.add(key, true, vals[i], true))
					}
					got, err := fast.AggregateSelectedNullableValues(keys, nil, vals, nil, sel)
					if !errors.Is(err, memory.ErrBudgetExceeded) || got != want {
						t.Fatalf("partial=%+v want=%+v error=%v", got, want, err)
					}
					i32CheckOracle(t, fast, oracle, aggregate.DefaultLayout())
					if fast.Capacity() != scalar.Capacity() || !reflect.DeepEqual(fast.epoch, scalar.epoch) || !reflect.DeepEqual(fast.keys, scalar.keys) {
						t.Fatal("growth/slot placement diverged from scalar")
					}
					if fastLedger.used != scalarLedger.used || fastLedger.peak != scalarLedger.peak || fastLedger.attempts != scalarLedger.attempts {
						t.Fatal("reservation history diverged from scalar")
					}
					// Clear exactly the successful selected prefix, then retry. No
					// consumed row is duplicated or dropped at a bitmap boundary.
					remaining := append(memory.Bitmap(nil), sel...)
					left := got.Rows
					for i := range keys {
						if left > 0 && memory.IsSet(remaining, i) {
							remaining[i>>6] &^= uint64(1) << uint(i&63)
							left--
						}
					}
					fastLedger.failAt = 0
					want = Summary{}
					for i, key := range keys {
						if memory.IsSet(remaining, i) {
							i32AddSummary(&want, oracle.add(key, true, vals[i], true))
						}
					}
					got, err = fast.AggregateSelectedNullableValues(keys, nil, vals, nil, remaining)
					if err != nil || got != want {
						t.Fatalf("retry=%+v want=%+v err=%v", got, want, err)
					}
					i32CheckOracle(t, fast, oracle, aggregate.DefaultLayout())
					if fastLedger.used != fast.RetainedBytes() {
						t.Fatal("retained charge mismatch")
					}
					fast.Close()
					scalar.Close()
					fastLedger.checkReleased(t)
					scalarLedger.checkReleased(t)
				})
			}
		}
	}
}

func TestI32BatchKernelStopsOnlyAtActualGrowth(t *testing.T) {
	for _, boundary := range []int{0, 1, 63, 64, 65, 127, 128} {
		for _, sparse := range []bool{false, true} {
			t.Run(fmt.Sprintf("row%d/sparse%v", boundary, sparse), func(t *testing.T) {
				calls := 0
				ledger := &allocationLedger{}
				reserve := func(n int64) (memory.Reservation, error) {
					calls++
					if calls > 2 {
						return nil, context.Canceled
					}
					return ledger.reserve(n)
				}
				tab, err := NewI32TableWithOptions(1, TableOptions{MaxLoadFactor: .01, Reserve: reserve})
				if err != nil {
					t.Fatal(err)
				}
				defer tab.Close()
				_, err = tab.TryAdd(7, 0)
				if err != nil {
					t.Fatal(err)
				}
				keys, vals := make([]int32, 129), make([]int64, 129)
				sel := make(memory.Bitmap, memory.WordsFor(129)+1)
				prefix, selected := 0, 0
				for i := range keys {
					keys[i], vals[i] = 7, 1
					if !sparse || i%3 != 0 || i == boundary {
						memory.Set(sel, i)
						selected++
						if i < boundary {
							prefix++
						}
					}
				}
				for i := len(keys); i < len(sel)*64; i++ {
					memory.Set(sel, i)
				}
				keys[boundary] = 8
				got, err := tab.AggregateSelectedNullableValues(keys, nil, vals, nil, sel)
				want := Summary{Rows: prefix, Checksum: int64(prefix)}
				if !errors.Is(err, context.Canceled) || got != want || calls != 3 || tab.Len() != 1 {
					t.Fatalf("partial=%+v want=%+v err=%v calls=%d", got, want, err, calls)
				}
				wantState := aggregate.Int64Result{Sum: int64(prefix), SumValid: true, CountRows: uint64(prefix + 1)}
				if gotState := i32Snapshot(tab)[i32OracleKey{7, true}]; gotState != wantState {
					t.Fatalf("partial state=%+v want=%+v", gotState, wantState)
				}
				// An all-existing batch at full load does not request growth.
				keys[boundary] = 7
				got, err = tab.AggregateSelectedNullableValues(keys, nil, vals, nil, sel)
				if err != nil || got != (Summary{Rows: selected, Checksum: int64(selected)}) || calls != 3 {
					t.Fatalf("existing batch=%+v err=%v calls=%d", got, err, calls)
				}
				wantState.Sum += int64(selected)
				wantState.CountRows += uint64(selected)
				if gotState := i32Snapshot(tab)[i32OracleKey{7, true}]; gotState != wantState {
					t.Fatalf("full state=%+v want=%+v", gotState, wantState)
				}
				tab.Close()
				ledger.checkReleased(t)
			})
		}
	}
}
