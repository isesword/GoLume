package aggregate

import (
	"context"
	"errors"
	"gocolumn/memory"
	"math"
	"reflect"
	"testing"
	"unsafe"
)

func numericBundleDescriptors() ([]memory.DataType, []Layout) {
	return []memory.DataType{memory.TypeInt64, memory.TypeFloat32, memory.TypeUint64, memory.TypeString}, []Layout{numericAll, numericAll, numericAll, {CountRows: true, CountValues: true}}
}

func TestNumericBundleMixedGrowthPreservesStableIDs(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 24)
	types, layouts := numericBundleDescriptors()
	peak := int64(0)
	b, err := NewNumericBundle(types, layouts, func(bytes int64) (memory.Reservation, error) {
		lease, err := manager.Reserve(context.Background(), bytes)
		peak = max(peak, manager.Used())
		return lease, err
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	legacy := make([]Int64Vector, 257)
	for group := 0; group < len(legacy); group++ {
		if err := b.EnsureGroups(group + 1); err != nil {
			t.Fatal(err)
		}
		legacy[group] = NewInt64Vector(1, numericAll)
		for _, value := range []int64{math.MaxInt64, int64(group)} {
			b.AddBits(group, 0, uint64(value), true)
			legacy[group].AddValue(0, value, true)
		}
		b.AddBits(group, 0, 123, false)
		legacy[group].AddValue(0, 123, false)
		b.AddBits(group, 1, uint64(math.Float32bits(float32(group)+0.25)), true)
		b.AddBits(group, 1, uint64(math.Float32bits(17)), false)
		b.AddBits(group, 2, uint64(1)<<63+uint64(group), true)
		b.AddBits(group, 3, math.MaxUint64, true)
		b.AddBits(group, 3, 0, false)
	}
	for group := range legacy {
		r := b.Result(group, 0)
		old := legacy[group].Result(0)
		avg, valid := r.Value(KindAvg)
		if r.SumBits != uint64(old.Sum) || r.CountRows != old.CountRows || r.CountValues != old.CountValues || valid != old.AvgValid || math.Float64frombits(avg) != old.Avg {
			t.Fatalf("legacy group %d got %+v old %+v", group, r, old)
		}
		f := b.Result(group, 1)
		if math.Float32frombits(uint32(f.SumBits)) != float32(group)+0.25 || f.CountRows != 2 || f.CountValues != 1 {
			t.Fatalf("float group %d %+v", group, f)
		}
		u := b.Result(group, 2)
		if u.SumBits != uint64(1)<<63+uint64(group) || u.MinBits != u.SumBits || u.MaxBits != u.SumBits {
			t.Fatalf("uint group %d %+v", group, u)
		}
		c := b.Result(group, 3)
		if c.CountRows != 2 || c.CountValues != 1 {
			t.Fatalf("counts group %d %+v", group, c)
		}
	}
	if manager.Used() != b.RetainedBytes() {
		t.Fatalf("charge %d retained %d", manager.Used(), b.RetainedBytes())
	}
	if peak <= manager.Used() {
		t.Fatalf("no charged replacement overlap: peak=%d current=%d", peak, manager.Used())
	}
	if b.Columns() != 4 || b.Capacity() < len(legacy) {
		t.Fatal("bundle shape incorrect")
	}
	b.Close()
	b.Close()
	if manager.Used() != 0 || b.RetainedBytes() != 0 || b.Columns() != 0 || b.Capacity() != 0 {
		t.Fatalf("closed state retained %d", manager.Used())
	}
}

func TestNumericBundleMergeClearAndOwnedDescriptors(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	types, layouts := numericBundleDescriptors()
	b, err := NewNumericBundle(types, layouts, func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) })
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if cap(b.types) != len(types) || cap(b.layouts) != len(layouts) {
		t.Fatal("descriptors have spare capacity")
	}
	types[0] = memory.TypeString
	layouts[0] = Layout{CountRows: true}
	if err := b.EnsureGroups(2); err != nil {
		t.Fatal(err)
	}
	b.AddBits(0, 0, math.MaxInt64, true)
	b.AddBits(1, 0, 1, true)
	if err := b.Merge(0, 0, b.Result(1, 0)); err != nil {
		t.Fatal(err)
	}
	got := b.Result(0, 0)
	if got.Type != memory.TypeInt64 || got.Layout != numericAll || got.SumBits != uint64(1)<<63 || got.CountValues != 2 {
		t.Fatalf("owned int64 descriptor/state %+v", got)
	}
	b.AddBits(0, 1, uint64(math.Float32bits(3)), true)
	b.AddBits(1, 1, uint64(math.Float32bits(9)), true)
	b.AddBits(1, 1, uint64(math.Float32bits(12)), true)
	if err := b.Merge(0, 1, b.Result(1, 1)); err != nil {
		t.Fatal(err)
	}
	avg, valid := b.Result(0, 1).Value(KindAvg)
	if !valid || math.Float32frombits(uint32(avg)) != 8 {
		t.Fatal("mean averaged means instead of raw totals")
	}
	before := b.Result(0, 0)
	if err := b.Merge(0, 0, b.Result(0, 1)); err == nil {
		t.Fatal("type mismatch accepted")
	}
	if !reflect.DeepEqual(before, b.Result(0, 0)) {
		t.Fatal("bad merge modified state")
	}
	bad := before
	bad.Layout.Min = false
	if err := b.Merge(0, 0, bad); err == nil {
		t.Fatal("layout mismatch accepted")
	}
	if err := b.ClearGroup(0); err != nil {
		t.Fatal(err)
	}
	for lane := 0; lane < b.Columns(); lane++ {
		r := b.Result(0, lane)
		if r.CountRows != 0 || r.CountValues != 0 || r.SumValid || r.MinValid || r.MaxValid {
			t.Fatalf("cleared lane %d %+v", lane, r)
		}
	}
	if err := b.ClearGroup(-1); err == nil {
		t.Fatal("negative group accepted")
	}
	if err := b.ClearGroup(b.Capacity()); err == nil {
		t.Fatal("unallocated group accepted")
	}
	if b.Result(1, 0).SumBits != 1 {
		t.Fatal("clear modified another group")
	}
}

func TestNumericBundleGrowthFailureAndMetadataAccounting(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	types, layouts := numericBundleDescriptors()
	fail := false
	sentinel := errors.New("injected growth failure")
	b, err := NewNumericBundle(types, layouts, func(n int64) (memory.Reservation, error) {
		if fail {
			return nil, sentinel
		}
		return manager.Reserve(context.Background(), n)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	meta := int64(len(types)) * (int64(unsafe.Sizeof(memory.TypeInvalid)) + int64(unsafe.Sizeof(Layout{})))
	if b.RetainedBytes() != meta || manager.Used() != meta {
		t.Fatalf("descriptor charge=%d want=%d", b.RetainedBytes(), meta)
	}
	if err := b.EnsureGroups(1); err != nil {
		t.Fatal(err)
	}
	b.AddBits(0, 2, math.MaxUint64, true)
	used, capacity, before := manager.Used(), b.Capacity(), b.Result(0, 2)
	backing, err := NumericBundleBytes(capacity, types, layouts)
	if err != nil {
		t.Fatal(err)
	}
	if used != meta+backing {
		t.Fatalf("backing charge=%d meta=%d arrays=%d", used, meta, backing)
	}
	fail = true
	if err := b.EnsureGroups(capacity + 1); !errors.Is(err, sentinel) {
		t.Fatalf("failure %v", err)
	}
	if manager.Used() != used || b.Capacity() != capacity || b.Result(0, 2) != before {
		t.Fatal("failed replacement mutated state or accounting")
	}
	if err := b.EnsureGroups(1); err != nil {
		t.Fatal("existing groups should not reserve")
	}
	if err := b.EnsureGroups(-1); err == nil {
		t.Fatal("negative groups accepted")
	}
	b.Close()
	if manager.Used() != 0 {
		t.Fatalf("failed replacement leaked %d", manager.Used())
	}
	if err := b.EnsureGroups(0); err == nil {
		t.Fatal("closed growth accepted")
	}
	if err := b.ClearGroup(0); err == nil {
		t.Fatal("closed clear accepted")
	}
}

func TestNumericBundleInvalidDescriptorsAndReservations(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	reserve := func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }
	for _, tc := range []struct {
		types   []memory.DataType
		layouts []Layout
	}{{nil, nil}, {[]memory.DataType{memory.TypeInt64}, nil}, {[]memory.DataType{memory.TypeInt64}, []Layout{{}}}, {[]memory.DataType{memory.TypeString}, []Layout{{Sum: true}}}, {[]memory.DataType{memory.TypeInvalid}, []Layout{{CountRows: true}}}} {
		if _, err := NewNumericBundle(tc.types, tc.layouts, reserve); err == nil {
			t.Fatalf("descriptors accepted %+v", tc)
		}
		if manager.Used() != 0 {
			t.Fatal("invalid descriptors allocated")
		}
	}
	types, layouts := numericBundleDescriptors()
	if _, err := NewNumericBundle(types, layouts, nil); err == nil {
		t.Fatal("nil callback accepted")
	}
	if _, err := NumericBundleBytes(-1, types, layouts); err == nil {
		t.Fatal("negative capacity accepted")
	}
	if _, err := NumericBundleBytes(math.MaxInt, types, layouts); err == nil {
		t.Fatal("overflow accepted")
	}
	for _, mode := range []string{"nil", "wrong-size", "lease-with-error"} {
		_, err := NewNumericBundle(types, layouts, func(n int64) (memory.Reservation, error) {
			if mode == "nil" {
				return nil, nil
			}
			if mode == "wrong-size" {
				return manager.Reserve(context.Background(), n+1)
			}
			lease, err := manager.Reserve(context.Background(), n)
			if err != nil {
				return nil, err
			}
			return lease, errors.New("injected after reservation")
		})
		if err == nil || manager.Used() != 0 {
			t.Fatalf("mode %s err=%v leaked=%d", mode, err, manager.Used())
		}
	}
}

func TestNumericBundleUpdatesAllocateNothing(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	types, layouts := numericBundleDescriptors()
	b, err := NewNumericBundle(types, layouts, func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) })
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.EnsureGroups(2); err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(100, func() {
		for lane := 0; lane < 4; lane++ {
			b.AddBits(0, lane, 1, true)
			_ = b.Merge(1, lane, b.Result(0, lane))
		}
		_ = b.ClearGroup(0)
	}); got != 0 {
		t.Fatalf("hot-path allocations %g", got)
	}
}
