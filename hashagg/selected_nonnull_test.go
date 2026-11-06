package hashagg

import (
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"reflect"
	"testing"
)

type selectedFixedTable[K, G any] interface {
	Reset()
	AggregateSelectedNullableValues([]K, memory.Bitmap, []int64, memory.Bitmap, memory.Bitmap) (Summary, error)
	AggregateIndexedNullableValues([]K, memory.Bitmap, []int64, memory.Bitmap, []int) (Summary, error)
	Materialize([]G) []G
	Capacity() int
}

// Compare the vector all-valid specialization with the nullable loop's exact
// equivalent input. This covers every physical aggregate layout, tail masks,
// repeated updates, reset epochs, collisions, and growth through the same API.
func checkSelectedFixed[K ~int32 | ~uint32 | ~int64, G any](t *testing.T, makeTable func(TableOptions) selectedFixedTable[K, G], add func(selectedFixedTable[K, G], K, bool, int64, bool) (bool, error)) {
	layouts := []aggregate.Layout{{}, {Sum: true}, {CountRows: true}, {CountValues: true}, {Min: true}, {Max: true}, {Avg: true}, {Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}}
	for li, layout := range layouts {
		for _, n := range []int{0, 1, 63, 64, 65, 129, 1025} {
			for _, density := range []int{0, 1, 3} {
				t.Run(fmt.Sprintf("layout%d/rows%d/select%d", li, n, density), func(t *testing.T) {
					keys := make([]K, n)
					values := make([]int64, n)
					valid := make(memory.Bitmap, memory.WordsFor(n))
					sel := make(memory.Bitmap, memory.WordsFor(n))
					for i := range keys {
						keys[i] = K(i%257 - 128)
						values[i] = int64(i%31 - 15)
						memory.Set(valid, i)
						if density > 0 && i%density == 0 {
							memory.Set(sel, i)
						}
					}
					// Garbage bits past the logical tail must not add rows or read values.
					if len(sel) > 0 && density > 0 && n%64 != 0 {
						sel[len(sel)-1] |= ^uint64(0) << uint(n%64)
					}
					opts := TableOptions{Aggregates: layout}
					fast, reference := makeTable(opts), makeTable(opts)
					for epoch := 0; epoch < 2; epoch++ {
						fast.Reset()
						reference.Reset()
						for repeat := 0; repeat < 2; repeat++ {
							got, err := fast.AggregateSelectedNullableValues(keys, nil, values, nil, sel)
							if err != nil {
								t.Fatal(err)
							}
							want, err := reference.AggregateSelectedNullableValues(keys, valid, values, valid, sel)
							if err != nil {
								t.Fatal(err)
							}
							if got != want || !reflect.DeepEqual(fast.Materialize(nil), reference.Materialize(nil)) {
								t.Fatalf("epoch=%d repeat=%d got=%+v want=%+v", epoch, repeat, got, want)
							}
						}
					}
				})
			}
		}
	}
	// Check the nullable fallback against scalar insertion/update semantics,
	// independently of either vector specialization.
	t.Run("mixed-null", func(t *testing.T) {
		for _, layout := range layouts {
			for mask := 0; mask < 4; mask++ {
				keys := make([]K, 1025)
				values := make([]int64, len(keys))
				selection := make(memory.Bitmap, memory.WordsFor(len(keys)))
				var kv, vv memory.Bitmap
				if mask&1 != 0 {
					kv = make(memory.Bitmap, len(selection))
				}
				if mask&2 != 0 {
					vv = make(memory.Bitmap, len(selection))
				}
				for i := range keys {
					keys[i], values[i] = K(i%257-128), int64(i%31-15)
					if i%3 != 0 {
						memory.Set(selection, i)
					}
					if kv != nil && i%7 != 0 {
						memory.Set(kv, i)
					}
					if vv != nil && i%5 != 0 {
						memory.Set(vv, i)
					}
				}
				fast, reference := makeTable(TableOptions{Aggregates: layout}), makeTable(TableOptions{Aggregates: layout})
				fast.Reset()
				reference.Reset()
				for repeat := 0; repeat < 2; repeat++ {
					got, err := fast.AggregateSelectedNullableValues(keys, kv, values, vv, selection)
					if err != nil {
						t.Fatal(err)
					}
					var want Summary
					for i, key := range keys {
						if !memory.IsSet(selection, i) {
							continue
						}
						keyValid, valueValid := kv == nil || memory.IsSet(kv, i), vv == nil || memory.IsSet(vv, i)
						created, err := add(reference, key, keyValid, values[i], valueValid)
						if err != nil {
							t.Fatal(err)
						}
						if created {
							want.Groups++
						}
						if valueValid {
							want.Checksum += values[i]
						}
						want.Rows++
					}
					if got != want || !reflect.DeepEqual(fast.Materialize(nil), reference.Materialize(nil)) {
						t.Fatalf("layout=%+v mask=%d repeat=%d got=%+v want=%+v", layout, mask, repeat, got, want)
					}
				}
			}
		}
	})

	t.Run("indexed", func(t *testing.T) {
		for _, layout := range layouts {
			for mask := 0; mask < 4; mask++ {
				keys := make([]K, 1025)
				values := make([]int64, len(keys))
				var kv, vv memory.Bitmap
				if mask&1 != 0 {
					kv = make(memory.Bitmap, memory.WordsFor(len(keys)))
				}
				if mask&2 != 0 {
					vv = make(memory.Bitmap, memory.WordsFor(len(keys)))
				}
				for i := range keys {
					keys[i], values[i] = K(i%257-128), int64(i%31-15)
					if kv != nil && i%7 != 0 {
						memory.Set(kv, i)
					}
					if vv != nil && i%5 != 0 {
						memory.Set(vv, i)
					}
				}
				var indices []int
				// Out-of-order and repeated indices test actual indexed execution,
				// not a sorted bitmap converted into an index vector.
				for i := len(keys) - 1; i >= 0; i-- {
					if i%3 != 0 {
						indices = append(indices, i)
					}
					if i%17 == 0 {
						indices = append(indices, i, i)
					}
				}
				for _, selection := range [][]int{nil, indices[:1], indices} {
					fast, reference := makeTable(TableOptions{Aggregates: layout}), makeTable(TableOptions{Aggregates: layout})
					fast.Reset()
					reference.Reset()
					for repeat := 0; repeat < 2; repeat++ {
						got, err := fast.AggregateIndexedNullableValues(keys, kv, values, vv, selection)
						if err != nil {
							t.Fatal(err)
						}
						var want Summary
						for _, i := range selection {
							keyValid, valueValid := kv == nil || memory.IsSet(kv, i), vv == nil || memory.IsSet(vv, i)
							created, err := add(reference, keys[i], keyValid, values[i], valueValid)
							if err != nil {
								t.Fatal(err)
							}
							if created {
								want.Groups++
							}
							if valueValid {
								want.Checksum += values[i]
							}
							want.Rows++
						}
						if got != want || !reflect.DeepEqual(fast.Materialize(nil), reference.Materialize(nil)) {
							t.Fatalf("layout=%+v mask=%d repeat=%d got=%+v want=%+v", layout, mask, repeat, got, want)
						}
					}
				}
			}
		}
	})
	t.Run("indexed-growth-denied", func(t *testing.T) {
		denied := errors.New("indexed growth denied")
		opts := TableOptions{MaxLoadFactor: .5, Reserve: denyFixedGrowth(denied)}
		fast, reference := makeTable(opts), makeTable(opts)
		fast.Reset()
		reference.Reset()
		keys := make([]K, 65)
		values := make([]int64, 65)
		indices := make([]int, 65)
		for i := range keys {
			keys[i] = K(i)
			values[i] = int64(i)
			indices[i] = i
		}
		got, err := fast.AggregateIndexedNullableValues(keys, nil, values, nil, indices)
		var want Summary
		for _, i := range indices {
			created, refErr := add(reference, keys[i], true, values[i], true)
			if errors.Is(refErr, denied) {
				break
			}
			if refErr != nil {
				t.Fatal(refErr)
			}
			if created {
				want.Groups++
			}
			want.Checksum += values[i]
			want.Rows++
		}
		if !errors.Is(err, denied) || got != want || !reflect.DeepEqual(fast.Materialize(nil), reference.Materialize(nil)) {
			t.Fatalf("got=%+v err=%v want=%+v", got, err, want)
		}
	})

	// Failed growth must preserve the partial summary and state, and propagate
	// the budget error instead of inserting an unreserved group.
	t.Run("growth-denied", func(t *testing.T) {
		denied := errors.New("growth denied")
		opts := TableOptions{MaxLoadFactor: .5, Reserve: denyFixedGrowth(denied)}
		fast, reference := makeTable(opts), makeTable(opts)
		fast.Reset()
		reference.Reset()
		keys := make([]K, 65)
		values := make([]int64, 65)
		valid := memory.Bitmap{^uint64(0), 1}
		for i := range keys {
			keys[i] = K(i)
			values[i] = int64(i)
		}
		got, err := fast.AggregateSelectedNullableValues(keys, nil, values, nil, valid)
		want, refErr := reference.AggregateSelectedNullableValues(keys, valid, values, valid, valid)
		if !errors.Is(err, denied) || !errors.Is(refErr, denied) || got != want || !reflect.DeepEqual(fast.Materialize(nil), reference.Materialize(nil)) {
			t.Fatalf("got=%+v err=%v want=%+v err=%v", got, err, want, refErr)
		}
	})
}

func TestSelectedNonNullFixedTables(t *testing.T) {
	t.Run("i32", func(t *testing.T) {
		checkSelectedFixed(t, func(o TableOptions) selectedFixedTable[int32, GroupI32] {
			return mustHashTable(NewI32TableWithOptions(1, o))
		}, func(tab selectedFixedTable[int32, GroupI32], key int32, kv bool, value int64, vv bool) (bool, error) {
			return tab.(*I32Table).TryAddNullableValue(key, kv, value, vv)
		})
	})
	t.Run("u32", func(t *testing.T) {
		checkSelectedFixed(t, func(o TableOptions) selectedFixedTable[uint32, GroupU32] {
			return mustHashTable(NewU32TableWithOptions(1, o))
		}, func(tab selectedFixedTable[uint32, GroupU32], key uint32, kv bool, value int64, vv bool) (bool, error) {
			return tab.(*U32Table).TryAddNullableValue(key, kv, value, vv)
		})
	})
	t.Run("u64", func(t *testing.T) {
		checkSelectedFixed(t, func(o TableOptions) selectedFixedTable[int64, GroupU64] {
			return mustHashTable(NewU64TableWithOptions(1, o))
		}, func(tab selectedFixedTable[int64, GroupU64], key int64, kv bool, value int64, vv bool) (bool, error) {
			return tab.(*U64Table).TryAddNullableValue(uint64(key), kv, value, vv)
		})
	})
}
