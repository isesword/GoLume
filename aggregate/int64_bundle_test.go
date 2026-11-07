package aggregate

import (
	"context"
	"errors"
	"gocolumn/memory"
	"math"
	"testing"
)

func TestInt64BundleStableIDsAndMerge(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	layouts := []Layout{{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}, {Min: true, Max: true, CountValues: true}}
	reserve := func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }
	b, err := NewInt64Bundle(layouts, reserve)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for group := 0; group < 257; group++ {
		if err := b.EnsureGroups(group + 1); err != nil {
			t.Fatal(err)
		}
		b.AddValue(group, 0, int64(group), true)
		b.AddValue(group, 0, 999, false)
		b.AddValue(group, 1, -int64(group), true)
	}
	for group := 0; group < 257; group++ {
		a, c := b.Result(group, 0), b.Result(group, 1)
		if a.Sum != int64(group) || a.CountRows != 2 || a.CountValues != 1 || !a.AvgValid || a.Avg != float64(group) || c.Min != -int64(group) || c.CountValues != 1 {
			t.Fatalf("group %d: %+v %+v", group, a, c)
		}
	}
	b.Merge(0, 0, b.Result(4, 0))
	if got := b.Result(0, 0); got.Sum != 4 || got.CountRows != 4 || got.CountValues != 2 || got.Avg != 2 {
		t.Fatalf("merge %+v", got)
	}
	b.ClearGroup(0)
	if got := b.Result(0, 0); got.SumValid || got.CountRows != 0 || got.AvgValid {
		t.Fatalf("clear %+v", got)
	}
	if manager.Used() != b.RetainedBytes() {
		t.Fatalf("charge=%d retained=%d", manager.Used(), b.RetainedBytes())
	}
	b.Close()
	b.Close()
	if manager.Used() != 0 {
		t.Fatalf("leaked %d", manager.Used())
	}
	if err := b.EnsureGroups(1); err == nil {
		t.Fatal("closed bundle accepted growth")
	}
}

func TestInt64BundleGrowthFailureIsTransactional(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	fail := false
	sentinel := errors.New("injected")
	b, err := NewInt64Bundle([]Layout{{Sum: true}}, func(n int64) (memory.Reservation, error) {
		if fail {
			return nil, sentinel
		}
		return manager.Reserve(context.Background(), n)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.EnsureGroups(1); err != nil {
		t.Fatal(err)
	}
	b.AddValue(0, 0, 42, true)
	used, capacity := manager.Used(), b.Capacity()
	fail = true
	if err := b.EnsureGroups(capacity + 1); !errors.Is(err, sentinel) {
		t.Fatalf("growth %v", err)
	}
	if manager.Used() != used || b.Capacity() != capacity || b.Result(0, 0).Sum != 42 {
		t.Fatal("failed growth mutated bundle")
	}
	if _, err := Int64BundleBytes(math.MaxInt, []Layout{{Sum: true, CountRows: true}}); err == nil {
		t.Fatal("overflow accepted")
	}
}
