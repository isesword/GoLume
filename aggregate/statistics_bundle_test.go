package aggregate

import (
	"context"
	"errors"
	"gocolumn/memory"
	"math"
	"testing"
)

func TestStatisticsBundleBudgetGrowthMerge(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	types := []memory.DataType{memory.TypeInt64, memory.TypeBool}
	layouts := []StatisticsLayout{{Moments: true, First: true, Last: true}, {First: true, Last: true}}
	fail := false
	sentinel := errors.New("injected reserve failure")
	b, err := NewStatisticsBundle(types, layouts, func(n int64) (memory.Reservation, error) {
		if fail {
			return nil, sentinel
		}
		return manager.Reserve(context.Background(), n)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	types[0] = memory.TypeString
	layouts[0] = StatisticsLayout{}
	for g := 0; g < 129; g++ {
		if err = b.EnsureGroups(g + 1); err != nil {
			t.Fatal(err)
		}
		if err = b.AddBits(g, 0, math.MaxInt64-2, true, SourceOrdinal{uint64(g), 0}); err != nil {
			t.Fatal(err)
		}
		if err = b.AddBits(g, 0, math.MaxInt64, true, SourceOrdinal{uint64(g), 1}); err != nil {
			t.Fatal(err)
		}
		if err = b.AddBits(g, 1, 1, true, SourceOrdinal{uint64(g), 0}); err != nil {
			t.Fatal(err)
		}
		if err = b.AddBits(g, 1, 0, false, SourceOrdinal{uint64(g), 1}); err != nil {
			t.Fatal(err)
		}
	}
	for g := 0; g < 129; g++ {
		r := b.Result(g, 0)
		s, e := RestoreMoments(r.Moments)
		if e != nil {
			t.Fatal(e)
		}
		if v, ok := s.Variance(1); !ok || v != 2 {
			t.Fatalf("group%d variance%g/%v", g, v, ok)
		}
		r = b.Result(g, 1)
		if !r.Endpoints.FirstValid || r.Endpoints.LastValid {
			t.Fatal("endpoint null changed")
		}
	}
	before, used, capacity := b.Result(0, 0), manager.Used(), b.Capacity()
	fail = true
	if err = b.EnsureGroups(capacity + 1); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if b.Result(0, 0) != before || b.Capacity() != capacity || manager.Used() != used {
		t.Fatal("failed growth changed state")
	}
	fail = false
	bad := b.Result(1, 0)
	bad.Endpoints.FirstOrdinal = before.Endpoints.FirstOrdinal
	bad.Endpoints.LastOrdinal = before.Endpoints.LastOrdinal
	bad.Endpoints.FirstBits = 1
	if err = b.Merge(0, 0, bad); err == nil || b.Result(0, 0) != before {
		t.Fatal("failed combined merge changed moments")
	}
	if err = b.AddBits(0, 0, 7, true, before.Endpoints.FirstOrdinal); err == nil || b.Result(0, 0) != before {
		t.Fatal("failed endpoint update changed moments")
	}
	if err = b.Merge(0, 0, b.Result(1, 0)); err != nil {
		t.Fatal(err)
	}
	if b.Result(0, 0).Moments.Count != 4 {
		t.Fatal("merge lost count")
	}
	if err = b.ClearGroup(0); err != nil {
		t.Fatal(err)
	}
	if b.Result(0, 0).Moments.Count != 0 || b.Result(0, 0).Endpoints.Seen {
		t.Fatal("clear retained state")
	}
	if manager.Used() != b.RetainedBytes() {
		t.Fatal("reservation mismatch")
	}
	b.Close()
	b.Close()
	if manager.Used() != 0 {
		t.Fatal("leaked reservations")
	}
	if err = b.EnsureGroups(1); err == nil {
		t.Fatal("closed growth accepted")
	}
}

func TestStatisticsBundleInvalidDescriptorsAndOverflow(t *testing.T) {
	for _, tc := range []struct {
		types   []memory.DataType
		layouts []StatisticsLayout
	}{
		{nil, nil}, {[]memory.DataType{memory.TypeInt64}, nil}, {[]memory.DataType{memory.TypeInt64}, []StatisticsLayout{{}}}, {[]memory.DataType{memory.TypeString}, []StatisticsLayout{{Moments: true}}}, {[]memory.DataType{memory.TypeInvalid}, []StatisticsLayout{{First: true}}},
	} {
		if _, err := StatisticsBundleBytes(8, tc.types, tc.layouts); err == nil {
			t.Fatal("accepted invalid descriptor")
		}
	}
	if _, err := StatisticsBundleBytes(math.MaxInt, []memory.DataType{memory.TypeFloat64}, []StatisticsLayout{{Moments: true, Last: true}}); err == nil {
		t.Fatal("accepted overflow")
	}
	if _, err := NewStatisticsBundle([]memory.DataType{memory.TypeInt64}, []StatisticsLayout{{Moments: true}}, nil); err == nil {
		t.Fatal("accepted missing budget")
	}
}
