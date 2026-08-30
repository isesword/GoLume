package hashagg

import (
	"gocolumn/aggregate"
	"testing"
)

func mergeState() aggregate.Int64Result {
	return aggregate.Int64Result{Sum: 7, SumValid: true, CountRows: 3, CountValues: 2, Min: 2, MinValid: true, Max: 5, MaxValid: true}
}

func assertMergedState(t *testing.T, got aggregate.Int64Result) {
	t.Helper()
	if got.Sum != 14 || !got.SumValid || got.CountRows != 6 || got.CountValues != 4 || got.Min != 2 || !got.MinValid || got.Max != 5 || !got.MaxValid {
		t.Fatalf("merged state=%+v", got)
	}
}

func TestSpecializedTablesMergeNullablePartialStates(t *testing.T) {
	layout := aggregate.Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true}
	state := mergeState()
	t.Run("i32", func(t *testing.T) {
		table := NewI32TableWithOptions(1, TableOptions{Aggregates: layout})
		table.Reset()
		_, _ = table.TryMergeNullable(4, true, state)
		_, _ = table.TryMergeNullable(4, true, state)
		if table.RetainedBytes() <= 0 {
			t.Fatal("retained bytes not reported")
		}
		if err := table.Visit(func(key int32, valid bool, got aggregate.Int64Result) error {
			if key != 4 || !valid {
				t.Fatalf("key=%d valid=%v", key, valid)
			}
			assertMergedState(t, got)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("u64-null", func(t *testing.T) {
		table := NewU64TableWithOptions(1, TableOptions{Aggregates: layout})
		table.Reset()
		_, _ = table.TryMergeNullable(0, false, state)
		_, _ = table.TryMergeNullable(0, false, state)
		if err := table.Visit(func(_ uint64, valid bool, got aggregate.Int64Result) error {
			if valid {
				t.Fatal("expected NULL key")
			}
			assertMergedState(t, got)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("u32", func(t *testing.T) {
		table := NewU32TableWithOptions(1, TableOptions{Aggregates: layout})
		table.Reset()
		_, _ = table.TryMergeNullable(4, true, state)
		_, _ = table.TryMergeNullable(4, true, state)
		if err := table.Visit(func(_ uint32, _ bool, got aggregate.Int64Result) error { assertMergedState(t, got); return nil }); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("pair", func(t *testing.T) {
		table := NewPairTableWithOptions(1, TableOptions{Aggregates: layout})
		table.Reset()
		key := Key128{A: 1, B: 2}
		_, _ = table.TryMergeNullable(key, 2, state)
		_, _ = table.TryMergeNullable(key, 2, state)
		if err := table.Visit(func(gotKey Key128, mask uint8, got aggregate.Int64Result) error {
			if gotKey != (Key128{B: 2}) || mask != 2 {
				t.Fatalf("key=%+v mask=%d", gotKey, mask)
			}
			assertMergedState(t, got)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("string", func(t *testing.T) {
		table := NewStringTableWithOptions(1, 16, TableOptions{Aggregates: layout})
		table.Reset()
		_, _ = table.TryMergeNullable([]byte("中"), true, state)
		_, _ = table.TryMergeNullable([]byte("中"), true, state)
		if err := table.Visit(func(key []byte, valid bool, got aggregate.Int64Result) error {
			if string(key) != "中" || !valid {
				t.Fatalf("key=%q valid=%v", key, valid)
			}
			assertMergedState(t, got)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}
