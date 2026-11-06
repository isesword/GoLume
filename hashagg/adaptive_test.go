package hashagg

import (
	"context"
	"errors"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"testing"
)

func TestI32TableAdaptiveGrowth(t *testing.T) {
	var reserved int64
	tab := mustHashTable(NewI32TableWithOptions(8, TableOptions{Reserve: func(n int64) (memory.Reservation, error) {
		reserved += n
		return memory.NewBudgetManager(0).Reserve(context.Background(), n)
	}}))
	initial := tab.Capacity()
	tab.Reset()
	for i := 0; i < 10_000; i++ {
		created, err := tab.TryAdd(int32(i), int64(i))
		if err != nil {
			t.Fatal(err)
		}
		if !created {
			t.Fatalf("key %d was not a new group", i)
		}
	}
	if tab.Len() != 10_000 {
		t.Fatalf("groups=%d want=10000", tab.Len())
	}
	if tab.Capacity() <= initial {
		t.Fatalf("capacity did not grow: initial=%d current=%d", initial, tab.Capacity())
	}
	if reserved <= 0 {
		t.Fatal("growth did not request retained-memory reservation")
	}

	// Updates after rehash must preserve the existing aggregate state.
	for i := 0; i < 10_000; i++ {
		created, err := tab.TryAdd(int32(i), 1)
		if err != nil || created {
			t.Fatalf("update key=%d created=%v err=%v", i, created, err)
		}
	}
	groups := tab.Materialize(nil)
	if len(groups) != 10_000 {
		t.Fatalf("materialized=%d want=10000", len(groups))
	}
}

func TestI32TableGrowthBudgetFailure(t *testing.T) {
	denied := errors.New("denied")
	tab := mustHashTable(NewI32TableWithOptions(8, TableOptions{MaxLoadFactor: 0.5, Reserve: denyFixedGrowth(denied)}))
	tab.Reset()
	for i := 0; ; i++ {
		_, err := tab.TryAdd(int32(i), 1)
		if err != nil {
			if !errors.Is(err, denied) {
				t.Fatalf("err=%v want denied", err)
			}
			break
		}
		if i > 10_000 {
			t.Fatal("table never requested growth")
		}
	}
}

func TestNullableSingleKeyKeepsNullSeparateFromZero(t *testing.T) {
	tab := NewI32Table(2)
	tab.Reset()
	rows := []struct {
		key   int32
		valid bool
		value int64
	}{{0, true, 10}, {0, false, 20}, {0, false, 30}, {0, true, 40}}
	for _, r := range rows {
		if _, err := tab.TryAddNullable(r.key, r.valid, r.value); err != nil {
			t.Fatal(err)
		}
	}
	if tab.Len() != 2 {
		t.Fatalf("groups=%d want=2", tab.Len())
	}
	var validZero, nullGroup bool
	for _, g := range tab.Materialize(nil) {
		switch {
		case g.Valid && g.Key == 0:
			validZero = g.Sum == 50 && g.Count == 2
		case !g.Valid:
			nullGroup = g.Sum == 50 && g.Count == 2
		}
	}
	if !validZero || !nullGroup {
		t.Fatalf("validZero=%v nullGroup=%v", validZero, nullGroup)
	}
}

func TestAggregateSelectedNullable(t *testing.T) {
	tab := NewU32Table(2)
	tab.Reset()
	keys := []uint32{0, 0, 1, 1}
	vals := []int64{10, 20, 30, 40}
	valid := make(memory.Bitmap, memory.WordsFor(len(keys)))
	memory.Set(valid, 0)
	memory.Set(valid, 2)
	memory.Set(valid, 3)
	sel := make(memory.Bitmap, memory.WordsFor(len(keys)))
	for i := range keys {
		memory.Set(sel, i)
	}
	s, err := tab.AggregateSelectedNullable(keys, valid, vals, sel)
	if err != nil {
		t.Fatal(err)
	}
	if s.Groups != 3 || s.Rows != 4 || s.Checksum != 100 {
		t.Fatalf("summary=%+v", s)
	}
}

func TestPairTableNullableValidityMask(t *testing.T) {
	tab := NewPairTable(2)
	tab.Reset()
	cases := []struct {
		k    Key128
		mask uint8
	}{{Key128{A: 99, B: 1}, 2}, {Key128{A: 0, B: 1}, 3}, {Key128{A: 77, B: 2}, 2}, {Key128{A: 66, B: 1}, 2}}
	for _, c := range cases {
		if _, err := tab.TryAddNullable(c.k, c.mask, 1); err != nil {
			t.Fatal(err)
		}
	}
	// (NULL,1) rows must merge regardless of the garbage payload in A, while
	// (0,1) and (NULL,2) remain distinct groups.
	if tab.Len() != 3 {
		t.Fatalf("groups=%d want=3", tab.Len())
	}
	for _, g := range tab.Materialize(nil) {
		if g.ValidMask == 2 && g.Key.B == 1 && g.Count != 2 {
			t.Fatalf("(NULL,1) count=%d want=2", g.Count)
		}
	}
}

func TestI32GrowthPreservesAdvancedAggregateState(t *testing.T) {
	tab := mustHashTable(NewI32TableWithOptions(1, TableOptions{Aggregates: aggregate.Layout{
		Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true,
	}}))
	tab.Reset()
	for i := 0; i < 4096; i++ {
		key := int32(i % 257)
		valid := i%5 != 0
		if _, err := tab.TryAddNullableValue(key, true, int64(i), valid); err != nil {
			t.Fatal(err)
		}
	}
	for _, g := range tab.Materialize(nil) {
		if g.State.CountRows == 0 {
			t.Fatalf("empty aggregate state after grow for key=%d", g.Key)
		}
		if g.State.CountValues > g.State.CountRows {
			t.Fatalf("invalid counts after grow for key=%d: %+v", g.Key, g.State)
		}
		if g.State.CountValues == 0 {
			if g.State.SumValid || g.State.MinValid || g.State.MaxValid || g.State.AvgValid {
				t.Fatalf("NULL-only state became valid after grow: %+v", g.State)
			}
		} else if !g.State.SumValid || !g.State.MinValid || !g.State.MaxValid || !g.State.AvgValid {
			t.Fatalf("valid state lost during grow: %+v", g.State)
		}
	}
}
