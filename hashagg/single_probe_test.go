package hashagg

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"testing"
)

type collisionTable[K ~int32 | ~uint32 | ~int64] struct {
	reset, close func()
	add          func(K, bool, int64, bool) (bool, error)
	selected     func([]K, memory.Bitmap, []int64, memory.Bitmap, memory.Bitmap) (Summary, error)
	indexed      func([]K, memory.Bitmap, []int64, memory.Bitmap, []int) (Summary, error)
	capacity     func() int
	snapshot     func() map[string]aggregate.Int64Result
	len          func() int
}

func checkSingleProbeCollisions[K ~int32 | ~uint32 | ~int64](t *testing.T, makeTable func() collisionTable[K]) {
	// All keys collide in capacity16. The key that forces growth gains the high
	// bucket bit at capacity32, so reusing the old miss slot is observably wrong.
	keys := make([]K, 0, 9)
	candidate := uint64(0)
	for len(keys) < 8 {
		if Hash64(candidate)&15 == 0 {
			keys = append(keys, K(candidate))
		}
		candidate++
	}
	for Hash64(candidate)&31 != 16 {
		candidate++
	}
	keys = append(keys, K(candidate))
	type row struct {
		k          K
		keyValid   bool
		value      int64
		valueValid bool
	}
	for _, mode := range []string{"scalar", "selected", "indexed"} {
		t.Run(mode, func(t *testing.T) {
			table := makeTable()
			defer table.close()
			for epoch := 0; epoch < 2; epoch++ {
				if epoch > 0 {
					table.reset()
				}
				initialCapacity := table.capacity()
				expected := map[string]aggregate.Int64Result{}
				consume := func(rows []row) {
					var want Summary
					for _, r := range rows {
						key := r.k
						if !r.keyValid {
							key = 0
						}
						id := fmt.Sprint(key, "/", r.keyValid)
						state, exists := expected[id]
						if !exists {
							want.Groups++
						}
						state.CountRows++
						if r.valueValid {
							state.Sum += r.value
							state.SumValid = true
							want.Checksum += r.value
						}
						expected[id] = state
						want.Rows++
					}
					var got Summary
					var err error
					switch mode {
					case "scalar":
						for _, r := range rows {
							created, e := table.add(r.k, r.keyValid, r.value, r.valueValid)
							if e != nil {
								err = e
								break
							}
							if created {
								got.Groups++
							}
							if r.valueValid {
								got.Checksum += r.value
							}
							got.Rows++
						}
					default:
						ks := make([]K, len(rows))
						vs := make([]int64, len(rows))
						var kv, vv memory.Bitmap
						allKeys, allValues := true, true
						sel := make(memory.Bitmap, memory.WordsFor(len(rows)))
						indices := make([]int, len(rows))
						for i, r := range rows {
							ks[i] = r.k
							vs[i] = r.value
							allKeys = allKeys && r.keyValid
							allValues = allValues && r.valueValid
							memory.Set(sel, i)
							indices[i] = i
						}
						if !allKeys {
							kv = make(memory.Bitmap, len(sel))
							for i, r := range rows {
								if r.keyValid {
									memory.Set(kv, i)
								}
							}
						}
						if !allValues {
							vv = make(memory.Bitmap, len(sel))
							for i, r := range rows {
								if r.valueValid {
									memory.Set(vv, i)
								}
							}
						}
						if mode == "selected" {
							got, err = table.selected(ks, kv, vs, vv, sel)
						} else {
							got, err = table.indexed(ks, kv, vs, vv, indices)
						}
					}
					if err != nil || got != want {
						t.Fatalf("epoch%d got=%+v want=%+v err=%v", epoch, got, want, err)
					}
					states := table.snapshot()
					if len(states) != len(expected) || table.len() != len(expected) {
						t.Fatalf("duplicate/lost group: actual=%d unique=%d want=%d", table.len(), len(states), len(expected))
					}
					for key, want := range expected {
						if got := states[key]; got != want {
							t.Fatalf("key=%s got=%+v want=%+v", key, got, want)
						}
					}
				}
				first := make([]row, 8)
				for i := range first {
					first[i] = row{keys[i], true, int64(i + 1), true}
				}
				consume(first)
				consume([]row{{0, false, 99, false}})
				consume([]row{{keys[8], true, 33, false}, {keys[0], true, 4, true}, {keys[8], true, 7, true}, {0, false, 3, true}})
				if epoch == 0 && table.capacity() != initialCapacity*2 {
					t.Fatalf("growth capacity=%d initial=%d", table.capacity(), initialCapacity)
				}
				consume([]row{{keys[8], true, 2, true}, {keys[1], true, 11, false}})
			}
		})
	}
}

func TestSingleProbeCollisionGrowthAndReset(t *testing.T) {
	t.Run("i32", func(t *testing.T) {
		checkSingleProbeCollisions(t, func() collisionTable[int32] {
			tab := mustHashTable(NewI32TableWithOptions(1, TableOptions{MaxLoadFactor: .5}))
			return collisionTable[int32]{reset: tab.Reset, close: tab.Close, add: tab.TryAddNullableValue, selected: tab.AggregateSelectedNullableValues, indexed: tab.AggregateIndexedNullableValues, capacity: tab.Capacity, len: tab.Len, snapshot: func() map[string]aggregate.Int64Result {
				m := map[string]aggregate.Int64Result{}
				_ = tab.Visit(func(k int32, v bool, s aggregate.Int64Result) error { m[fmt.Sprint(k, "/", v)] = s; return nil })
				return m
			}}
		})
	})
	t.Run("u32", func(t *testing.T) {
		checkSingleProbeCollisions(t, func() collisionTable[uint32] {
			tab := mustHashTable(NewU32TableWithOptions(1, TableOptions{MaxLoadFactor: .5}))
			return collisionTable[uint32]{reset: tab.Reset, close: tab.Close, add: tab.TryAddNullableValue, selected: tab.AggregateSelectedNullableValues, indexed: tab.AggregateIndexedNullableValues, capacity: tab.Capacity, len: tab.Len, snapshot: func() map[string]aggregate.Int64Result {
				m := map[string]aggregate.Int64Result{}
				_ = tab.Visit(func(k uint32, v bool, s aggregate.Int64Result) error { m[fmt.Sprint(k, "/", v)] = s; return nil })
				return m
			}}
		})
	})
	t.Run("u64", func(t *testing.T) {
		checkSingleProbeCollisions(t, func() collisionTable[int64] {
			tab := mustHashTable(NewU64TableWithOptions(1, TableOptions{MaxLoadFactor: .5}))
			return collisionTable[int64]{reset: tab.Reset, close: tab.Close, add: func(k int64, kv bool, v int64, vv bool) (bool, error) {
				return tab.TryAddNullableValue(uint64(k), kv, v, vv)
			}, selected: tab.AggregateSelectedNullableValues, indexed: tab.AggregateIndexedNullableValues, capacity: tab.Capacity, len: tab.Len, snapshot: func() map[string]aggregate.Int64Result {
				m := map[string]aggregate.Int64Result{}
				_ = tab.Visit(func(k uint64, v bool, s aggregate.Int64Result) error { m[fmt.Sprint(k, "/", v)] = s; return nil })
				return m
			}}
		})
	})
}

func TestSingleProbeTinyLoadFactorReprobesAfterMultipleGrowths(t *testing.T) {
	// The configured load limit can stay one for several doublings. Insertion
	// must not assume that a single growth always creates logical capacity.
	for _, kind := range []string{"i32", "u32", "u64"} {
		t.Run(kind, func(t *testing.T) {
			fixture, err := makeOwnedFixture(kind, 1, TableOptions{MaxLoadFactor: .01})
			if err != nil {
				t.Fatal(err)
			}
			defer fixture.close()
			if err := fixture.add(0, true); err != nil {
				t.Fatal(err)
			}
			if err := fixture.add(7, true); err != nil {
				t.Fatal(err)
			}
			if fixture.capacity() < 200 || len(fixture.snapshot()) != 2 {
				t.Fatalf("capacity=%d groups=%d", fixture.capacity(), len(fixture.snapshot()))
			}
		})
	}
}
