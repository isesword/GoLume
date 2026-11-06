package hashagg

import (
	"gocolumn/aggregate"
	"testing"
)

func TestFreshTablesAreReadyWithoutReset(t *testing.T) {
	t.Run("i32", func(t *testing.T) {
		tab := NewI32Table(1)
		defer tab.Close()
		seen := 0
		if err := tab.Visit(func(int32, bool, aggregate.Int64Result) error { seen++; return nil }); err != nil {
			t.Fatal(err)
		}
		if seen != 0 || tab.Len() != 0 || len(tab.Materialize(nil)) != 0 {
			t.Fatal("fresh table is not empty")
		}
		for _, row := range []struct {
			k     int32
			valid bool
		}{{0, true}, {7, true}, {0, false}} {
			created, err := tab.TryAddNullableValue(row.k, row.valid, 9, true)
			if err != nil || !created {
				t.Fatalf("row=%+v created=%v err=%v", row, created, err)
			}
		}
		if tab.Len() != 3 {
			t.Fatalf("groups=%d", tab.Len())
		}
		for _, group := range tab.Materialize(nil) {
			if group.Sum != 9 || group.Count != 1 {
				t.Fatalf("group=%+v", group)
			}
		}
		tab.Reset()
		if len(tab.Materialize(nil)) != 0 {
			t.Fatal("Reset did not clear")
		}
	})
	t.Run("u32", func(t *testing.T) {
		tab := NewU32Table(1)
		defer tab.Close()
		seen := 0
		if err := tab.Visit(func(uint32, bool, aggregate.Int64Result) error { seen++; return nil }); err != nil {
			t.Fatal(err)
		}
		if seen != 0 || tab.Len() != 0 || len(tab.Materialize(nil)) != 0 {
			t.Fatal("fresh table is not empty")
		}
		for _, row := range []struct {
			k     uint32
			valid bool
		}{{0, true}, {7, true}, {0, false}} {
			created, err := tab.TryAddNullableValue(row.k, row.valid, 9, true)
			if err != nil || !created {
				t.Fatalf("row=%+v created=%v err=%v", row, created, err)
			}
		}
		if tab.Len() != 3 {
			t.Fatalf("groups=%d", tab.Len())
		}
		for _, group := range tab.Materialize(nil) {
			if group.Sum != 9 || group.Count != 1 {
				t.Fatalf("group=%+v", group)
			}
		}
		tab.Reset()
		if len(tab.Materialize(nil)) != 0 {
			t.Fatal("Reset did not clear")
		}
	})
	t.Run("u64", func(t *testing.T) {
		tab := NewU64Table(1)
		defer tab.Close()
		seen := 0
		if err := tab.Visit(func(uint64, bool, aggregate.Int64Result) error { seen++; return nil }); err != nil {
			t.Fatal(err)
		}
		if seen != 0 || tab.Len() != 0 || len(tab.Materialize(nil)) != 0 {
			t.Fatal("fresh table is not empty")
		}
		for _, row := range []struct {
			k     uint64
			valid bool
		}{{0, true}, {7, true}, {0, false}} {
			created, err := tab.TryAddNullableValue(row.k, row.valid, 9, true)
			if err != nil || !created {
				t.Fatalf("row=%+v created=%v err=%v", row, created, err)
			}
		}
		if tab.Len() != 3 {
			t.Fatalf("groups=%d", tab.Len())
		}
		for _, group := range tab.Materialize(nil) {
			if group.Sum != 9 || group.Count != 1 {
				t.Fatalf("group=%+v", group)
			}
		}
		tab.Reset()
		if len(tab.Materialize(nil)) != 0 {
			t.Fatal("Reset did not clear")
		}
	})
	t.Run("pair", func(t *testing.T) {
		tab := NewPairTable(1)
		defer tab.Close()
		seen := 0
		if err := tab.Visit(func(Key128, uint8, aggregate.Int64Result) error { seen++; return nil }); err != nil {
			t.Fatal(err)
		}
		if seen != 0 || tab.Len() != 0 || len(tab.Materialize(nil)) != 0 {
			t.Fatal("fresh table is not empty")
		}
		for _, row := range []struct {
			k    Key128
			mask uint8
		}{{Key128{}, 3}, {Key128{7, 8}, 3}, {Key128{}, 0}, {Key128{0, 8}, 2}} {
			created, err := tab.TryAddNullableValue(row.k, row.mask, 9, true)
			if err != nil || !created {
				t.Fatalf("row=%+v created=%v err=%v", row, created, err)
			}
		}
		if tab.Len() != 4 {
			t.Fatalf("groups=%d", tab.Len())
		}
		for _, group := range tab.Materialize(nil) {
			if group.Sum != 9 || group.Count != 1 {
				t.Fatalf("group=%+v", group)
			}
		}
		tab.Reset()
		if len(tab.Materialize(nil)) != 0 {
			t.Fatal("Reset did not clear")
		}
	})
	t.Run("string", func(t *testing.T) {
		tab, err := NewStringTable(1, 16, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tab.Close()
		seen := 0
		if err := tab.Visit(func([]byte, bool, aggregate.Int64Result) error { seen++; return nil }); err != nil {
			t.Fatal(err)
		}
		if seen != 0 || tab.Len() != 0 || len(tab.Materialize(nil)) != 0 {
			t.Fatal("fresh table is not empty")
		}
		for _, row := range []struct {
			k     string
			valid bool
		}{{"", true}, {"seven", true}, {"", false}} {
			created, err := tab.AddNullableValue([]byte(row.k), row.valid, 9, true)
			if err != nil || !created {
				t.Fatalf("row=%+v created=%v err=%v", row, created, err)
			}
		}
		if tab.Len() != 3 {
			t.Fatalf("groups=%d", tab.Len())
		}
		for _, group := range tab.Materialize(nil) {
			if group.Sum != 9 || group.Count != 1 {
				t.Fatalf("group=%+v", group)
			}
		}
		tab.Reset()
		if len(tab.Materialize(nil)) != 0 {
			t.Fatal("Reset did not clear")
		}
	})
}

func checkFreshSelectedTables[K ~int32 | ~uint32 | ~int64, G any](t *testing.T, makeTable func() selectedFixedTable[K, G]) {
	t.Helper()
	for _, indexed := range []bool{false, true} {
		table := makeTable()
		defer table.(interface{ Close() }).Close()
		keys := []K{0, 7, 0}
		values := []int64{1, 2, 3}
		var got Summary
		var err error
		if indexed {
			got, err = table.AggregateIndexedNullableValues(keys, []uint64{3}, values, nil, []int{2, 0, 1, 2})
		} else {
			got, err = table.AggregateSelectedNullableValues(keys, []uint64{3}, values, nil, []uint64{7})
		}
		want := Summary{Groups: 3, Rows: 3, Checksum: 6}
		if indexed {
			want.Rows = 4
			want.Checksum = 9
		}
		if err != nil || got != want || len(table.Materialize(nil)) != 3 {
			t.Fatalf("indexed=%v got=%+v want=%+v err=%v", indexed, got, want, err)
		}
	}
}

func TestFreshFixedTablesVectorUpdatesWithoutReset(t *testing.T) {
	t.Run("i32", func(t *testing.T) {
		checkFreshSelectedTables(t, func() selectedFixedTable[int32, GroupI32] { return NewI32Table(1) })
	})
	t.Run("u32", func(t *testing.T) {
		checkFreshSelectedTables(t, func() selectedFixedTable[uint32, GroupU32] { return NewU32Table(1) })
	})
	t.Run("u64", func(t *testing.T) {
		checkFreshSelectedTables(t, func() selectedFixedTable[int64, GroupU64] { return NewU64Table(1) })
	})
}
