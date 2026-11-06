package hashagg

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"reflect"
	"runtime"
	"testing"
)

// allocationLedger observes independently measured backing capacities. It does
// not use any table Estimate/RetainedBytes formula to define expected charges.
type allocationLedger struct {
	limit, used, peak int64
	attempts, failAt  int
	leases            []*allocationLease
	before            func(int64)
}
type allocationLease struct {
	owner    *allocationLedger
	bytes    int64
	releases int
}

func (l *allocationLease) Bytes() int64 { return l.bytes }
func (l *allocationLease) Release() {
	l.releases++
	if l.releases == 1 {
		l.owner.used -= l.bytes
	}
}
func (m *allocationLedger) reserve(n int64) (memory.Reservation, error) {
	m.attempts++
	if m.before != nil {
		m.before(n)
	}
	if m.attempts == m.failAt || m.limit > 0 && n > m.limit-m.used {
		return nil, memory.ErrBudgetExceeded
	}
	lease := &allocationLease{owner: m, bytes: n}
	m.leases = append(m.leases, lease)
	m.used += n
	if m.used > m.peak {
		m.peak = m.used
	}
	return lease, nil
}
func (m *allocationLedger) checkReleased(t *testing.T) {
	t.Helper()
	if m.used != 0 {
		t.Fatalf("remaining charge %d", m.used)
	}
	for i, l := range m.leases {
		if l.releases != 1 {
			t.Fatalf("lease %d release calls=%d", i, l.releases)
		}
	}
}

func physicalStateBytes(state aggregate.Int64Vector) int64 {
	v := reflect.ValueOf(state)
	var n int64
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if f.Kind() == reflect.Slice {
			n += int64(f.Cap()) * int64(f.Type().Elem().Size())
		}
	}
	return n
}

type ownedTableFixture struct {
	close    func()
	reset    func()
	add      func(int, bool) error
	capacity func() int
	slots    func() int64
	actual   func() int64
	reported func() int64
	snapshot func() map[string]aggregate.Int64Result
}

func makeOwnedFixture(kind string, hint int, opts TableOptions) (*ownedTableFixture, error) {
	f := &ownedTableFixture{}
	switch kind {
	case "i32":
		tab, err := NewI32TableWithOptions(hint, opts)
		if err != nil {
			return nil, err
		}
		f.close, f.reset, f.capacity, f.reported = tab.Close, tab.Reset, tab.Capacity, tab.RetainedBytes
		f.slots = func() int64 { return int64(cap(tab.keys))*4 + int64(cap(tab.epoch))*4 + physicalStateBytes(tab.state) }
		f.actual = func() int64 { return f.slots() + physicalStateBytes(tab.nullState) }
		f.add = func(i int, v bool) error {
			_, err := tab.TryAddNullableValue(int32(i), i >= 0, int64(i+100), v)
			return err
		}
		f.snapshot = func() map[string]aggregate.Int64Result {
			m := map[string]aggregate.Int64Result{}
			_ = tab.Visit(func(k int32, v bool, s aggregate.Int64Result) error { m[fmt.Sprint(k, "/", v)] = s; return nil })
			return m
		}
	case "u32":
		tab, err := NewU32TableWithOptions(hint, opts)
		if err != nil {
			return nil, err
		}
		f.close, f.reset, f.capacity, f.reported = tab.Close, tab.Reset, tab.Capacity, tab.RetainedBytes
		f.slots = func() int64 { return int64(cap(tab.keys))*4 + int64(cap(tab.epoch))*4 + physicalStateBytes(tab.state) }
		f.actual = func() int64 { return f.slots() + physicalStateBytes(tab.nullState) }
		f.add = func(i int, v bool) error {
			_, err := tab.TryAddNullableValue(uint32(i), i >= 0, int64(i+100), v)
			return err
		}
		f.snapshot = func() map[string]aggregate.Int64Result {
			m := map[string]aggregate.Int64Result{}
			_ = tab.Visit(func(k uint32, v bool, s aggregate.Int64Result) error { m[fmt.Sprint(k, "/", v)] = s; return nil })
			return m
		}
	case "u64":
		tab, err := NewU64TableWithOptions(hint, opts)
		if err != nil {
			return nil, err
		}
		f.close, f.reset, f.capacity, f.reported = tab.Close, tab.Reset, tab.Capacity, tab.RetainedBytes
		f.slots = func() int64 { return int64(cap(tab.keys))*8 + int64(cap(tab.epoch))*4 + physicalStateBytes(tab.state) }
		f.actual = func() int64 { return f.slots() + physicalStateBytes(tab.nullState) }
		f.add = func(i int, v bool) error {
			_, err := tab.TryAddNullableValue(uint64(i), i >= 0, int64(i+100), v)
			return err
		}
		f.snapshot = func() map[string]aggregate.Int64Result {
			m := map[string]aggregate.Int64Result{}
			_ = tab.Visit(func(k uint64, v bool, s aggregate.Int64Result) error { m[fmt.Sprint(k, "/", v)] = s; return nil })
			return m
		}
	case "pair":
		tab, err := NewPairTableWithOptions(hint, opts)
		if err != nil {
			return nil, err
		}
		f.close, f.reset, f.capacity, f.reported = tab.Close, tab.Reset, tab.Capacity, tab.RetainedBytes
		f.slots = func() int64 {
			return int64(cap(tab.a)+cap(tab.b))*8 + int64(cap(tab.validMask)) + int64(cap(tab.epoch))*4 + physicalStateBytes(tab.state)
		}
		f.actual = f.slots
		f.add = func(i int, v bool) error {
			mask := uint8(3)
			if i < 0 {
				mask = 0
			}
			_, err := tab.TryAddNullableValue(Key128{A: uint64(i), B: uint64(i + 1)}, mask, int64(i+100), v)
			return err
		}
		f.snapshot = func() map[string]aggregate.Int64Result {
			m := map[string]aggregate.Int64Result{}
			_ = tab.Visit(func(k Key128, v uint8, s aggregate.Int64Result) error { m[fmt.Sprint(k, "/", v)] = s; return nil })
			return m
		}
	case "string":
		tab, err := NewStringTableWithOptions(hint, 1<<16, opts)
		if err != nil {
			return nil, err
		}
		f.close, f.reset, f.capacity, f.reported = tab.Close, tab.Reset, tab.Capacity, tab.RetainedBytes
		f.slots = func() int64 {
			return int64(cap(tab.off)+cap(tab.lens)+cap(tab.epoch))*4 + physicalStateBytes(tab.state)
		}
		f.actual = func() int64 { return f.slots() + int64(cap(tab.arena)) + physicalStateBytes(tab.nullState) }
		// Prebuild keys so the rejected insertion measures only table allocations.
		keys := make([][]byte, 16384)
		for i := range keys {
			keys[i] = []byte(fmt.Sprint(i))
		}
		f.add = func(i int, v bool) error {
			var key []byte
			if i >= 0 {
				key = keys[i]
			}
			_, err := tab.AddNullableValue(key, i >= 0, int64(i+100), v)
			return err
		}
		f.snapshot = func() map[string]aggregate.Int64Result {
			m := map[string]aggregate.Int64Result{}
			_ = tab.Visit(func(k []byte, v bool, s aggregate.Int64Result) error {
				m[fmt.Sprint(string(k), "/", v)] = s
				return nil
			})
			return m
		}
	}
	f.reset()
	return f, nil
}

func TestHashTableReplacementOwnsPeakAndReleasesOld(t *testing.T) {
	layout := aggregate.Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}
	for _, kind := range []string{"i32", "u32", "u64", "pair", "string"} {
		t.Run(kind, func(t *testing.T) {
			m := &allocationLedger{}
			f, err := makeOwnedFixture(kind, 1024, TableOptions{Reserve: m.reserve, Aggregates: layout, MaxLoadFactor: .5})
			if err != nil {
				t.Fatal(err)
			}
			defer f.close()
			initialCap, initialBytes, oldSlots := f.capacity(), f.actual(), f.slots()
			if m.used != initialBytes || f.reported() != initialBytes {
				t.Fatalf("initial actual=%d charged=%d reported=%d", initialBytes, m.used, f.reported())
			}
			// Final replacement alone fits; old+new overlap does not.
			newSlots := oldSlots * 2
			m.limit = initialBytes + newSlots - 1
			if err := f.add(-1, false); err != nil {
				t.Fatal(err)
			}
			next := 0
			for ; next < initialCap; next++ {
				if err := f.add(next, next%5 != 0); err != nil {
					if !errors.Is(err, memory.ErrBudgetExceeded) {
						t.Fatal(err)
					}
					break
				}
			}
			if next == initialCap {
				t.Fatal("growth never rejected")
			}
			before := f.snapshot()
			usedBefore := m.used
			capBefore := f.capacity()
			var start, end runtime.MemStats
			runtime.ReadMemStats(&start)
			err = f.add(next, next%5 != 0)
			runtime.ReadMemStats(&end)
			if !errors.Is(err, memory.ErrBudgetExceeded) || f.capacity() != capBefore || m.used != usedBefore || f.actual() != initialBytes || !reflect.DeepEqual(before, f.snapshot()) {
				t.Fatal("rejected overlap mutated state/capacity/charge")
			}
			if allocated := end.TotalAlloc - start.TotalAlloc; allocated >= uint64(newSlots/2) {
				t.Fatalf("denied reserve allocated %d bytes, replacement=%d", allocated, newSlots)
			}
			// A failed reservation is retryable and does not leave a poisoned owner.
			m.limit = 0
			m.before = func(request int64) {
				if request != newSlots || m.used != initialBytes || f.actual() != initialBytes {
					t.Fatalf("preallocation request=%d used=%d actual=%d", request, m.used, f.actual())
				}
			}
			if err = f.add(next, next%5 != 0); err != nil {
				t.Fatal(err)
			}
			m.before = nil
			if f.capacity() != initialCap*2 || m.peak != initialBytes+newSlots || m.used != f.actual() || f.reported() != f.actual() {
				t.Fatalf("replacement cap=%d peak=%d wantpeak=%d used=%d actual=%d", f.capacity(), m.peak, initialBytes+newSlots, m.used, f.actual())
			}
			after := f.snapshot()
			for key, state := range before {
				if after[key] != state {
					t.Fatalf("state changed across rehash for %s", key)
				}
			}
			// Repeated replacements charge only currently retained buffers, not history.
			for i := next + 1; i < initialCap*2; i++ {
				if err := f.add(i, i%5 != 0); err != nil {
					t.Fatal(err)
				}
				if m.used != f.actual() {
					t.Fatalf("historical charge retained: used=%d actual=%d", m.used, f.actual())
				}
			}
			f.close()
			f.close()
			if f.actual() != 0 || f.reported() != 0 {
				t.Fatal("Close retained backing arrays")
			}
			m.checkReleased(t)
		})
	}
}

func TestHashTableConstructorRollback(t *testing.T) {
	for _, kind := range []string{"i32", "u32", "u64", "pair", "string"} {
		components := 2
		if kind == "pair" {
			components = 1
		}
		if kind == "string" {
			components = 3
		}
		for failAt := 1; failAt <= components; failAt++ {
			t.Run(fmt.Sprintf("%s/component%d", kind, failAt), func(t *testing.T) {
				m := &allocationLedger{failAt: failAt}
				table, err := makeOwnedFixture(kind, 1024, TableOptions{Reserve: m.reserve})
				if table != nil || !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatalf("table=%v err=%v", table, err)
				}
				m.checkReleased(t)
			})
		}
	}
}

func TestStringArenaReplacementOwnsPeak(t *testing.T) {
	m := &allocationLedger{}
	table, err := NewStringTableWithOptions(128, 64, TableOptions{Reserve: m.reserve})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	table.Reset()
	actual := func() int64 {
		return int64(cap(table.off)+cap(table.lens)+cap(table.epoch))*4 + physicalStateBytes(table.state) + physicalStateBytes(table.nullState) + int64(cap(table.arena))
	}
	if _, err = table.Add([]byte("first"), 5); err != nil {
		t.Fatal(err)
	}
	oldArena := cap(table.arena)
	initial := actual()
	key := make([]byte, 1024)
	for i := range key {
		key[i] = 'x'
	}
	newArena := int64(2048)
	m.limit = initial + newArena - 1
	before := table.Materialize(nil)
	var start, end runtime.MemStats
	runtime.ReadMemStats(&start)
	_, err = table.Add(key, 11)
	runtime.ReadMemStats(&end)
	if !errors.Is(err, memory.ErrBudgetExceeded) || table.ArenaCap() != oldArena || m.used != initial || !reflect.DeepEqual(before, table.Materialize(nil)) {
		t.Fatal("denied arena growth changed state")
	}
	if end.TotalAlloc-start.TotalAlloc >= uint64(newArena) {
		t.Fatalf("denied arena allocated %d", end.TotalAlloc-start.TotalAlloc)
	}
	m.limit = 0
	if _, err = table.Add(key, 11); err != nil {
		t.Fatal(err)
	}
	if m.peak != initial+newArena || m.used != actual() || table.RetainedBytes() != actual() {
		t.Fatalf("arena peak=%d want=%d used=%d actual=%d", m.peak, initial+newArena, m.used, actual())
	}
	table.Close()
	table.Close()
	m.checkReleased(t)
}

func TestHashTableReservationContract(t *testing.T) {
	t.Run("nil-token", func(t *testing.T) {
		table, err := NewI32TableWithOptions(1, TableOptions{Reserve: func(int64) (memory.Reservation, error) { return nil, nil }})
		if err == nil || table != nil {
			t.Fatal("accepted missing token")
		}
	})
	t.Run("wrong-size", func(t *testing.T) {
		m := memory.NewBudgetManager(0)
		table, err := NewI32TableWithOptions(1, TableOptions{Reserve: func(n int64) (memory.Reservation, error) { return m.Reserve(context.Background(), n-1) }})
		if err == nil || table != nil || m.Used() != 0 {
			t.Fatal("accepted/leaked wrong token")
		}
	})
}

func TestHashTableLeaseBytesForEveryAggregateLayout(t *testing.T) {
	layouts := []aggregate.Layout{{}, {Sum: true}, {CountRows: true}, {CountValues: true}, {Min: true}, {Max: true}, {Avg: true}, {Sum: true, CountRows: true, CountValues: true, Min: true, Max: true}}
	for _, kind := range []string{"i32", "u32", "u64", "pair", "string"} {
		for i, layout := range layouts {
			t.Run(fmt.Sprintf("%s/layout%d", kind, i), func(t *testing.T) {
				m := &allocationLedger{}
				f, err := makeOwnedFixture(kind, 1, TableOptions{Reserve: m.reserve, Aggregates: layout})
				if err != nil {
					t.Fatal(err)
				}
				for key := -1; key < 100; key++ {
					if err := f.add(key, key%5 != 0); err != nil {
						t.Fatal(err)
					}
					if m.used != f.actual() || f.reported() != f.actual() {
						t.Fatalf("key=%d used=%d reported=%d actual=%d", key, m.used, f.reported(), f.actual())
					}
				}
				f.reset()
				if m.used != f.actual() {
					t.Fatal("Reset lost reservation")
				}
				f.close()
				m.checkReleased(t)
			})
		}
	}
}

func TestHashTableConstructorPanicRollsBack(t *testing.T) {
	m := &allocationLedger{}
	calls := 0
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected reserve panic")
			}
		}()
		_, _ = NewStringTableWithOptions(1, 16, TableOptions{Reserve: func(n int64) (memory.Reservation, error) {
			calls++
			if calls == 2 {
				panic("injected reservation panic")
			}
			return m.reserve(n)
		}})
	}()
	m.checkReleased(t)
}

func TestHashTableImpossibleCapacityRejectedBeforeReserve(t *testing.T) {
	for _, kind := range []string{"i32", "u32", "u64", "pair", "string"} {
		t.Run(kind, func(t *testing.T) {
			m := &allocationLedger{}
			f, err := makeOwnedFixture(kind, int(^uint(0)>>1), TableOptions{Reserve: m.reserve})
			if err == nil || f != nil || m.attempts != 0 {
				t.Fatalf("table=%v err=%v reserve attempts=%d", f, err, m.attempts)
			}
		})
	}
}

func TestStringArenaOffsetRangeBeforeReservation(t *testing.T) {
	maximum := uint64(^uint32(0))
	if maximum+1 > uint64(int(^uint(0)>>1)) {
		t.Skip("int cannot exceed uint32 offsets")
	}
	m := &allocationLedger{}
	table, err := NewStringTableWithOptions(1, int(maximum+1), TableOptions{Reserve: m.reserve})
	if err == nil || table != nil || m.attempts != 0 {
		t.Fatal("oversized hint reached allocation")
	}
	table, err = NewStringTableWithOptions(1, 16, TableOptions{Reserve: m.reserve})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	table.Reset()
	attempts := m.attempts
	if err = table.ensureArena(int(maximum + 1)); err == nil || m.attempts != attempts {
		t.Fatal("unrepresentable length reached reserve")
	}
	// Rounding a representable maximum length to a 2^32 capacity is valid on
	// 64-bit systems, but reservation rejection must precede that allocation.
	initial := m.used
	m.limit = initial + int64(maximum)
	if err = table.ensureArena(int(maximum)); !errors.Is(err, memory.ErrBudgetExceeded) || m.attempts != attempts+1 || m.used != initial {
		t.Fatalf("rounded capacity err=%v attempts=%d used=%d", err, m.attempts, m.used)
	}
	if _, err = table.Add([]byte("first"), 1); err != nil {
		t.Fatal(err)
	}
	attempts = m.attempts
	if err = table.ensureArena(int(maximum)); err == nil || m.attempts != attempts {
		t.Fatal("existing length plus key exceeded offsets")
	}
	table.Close()
	m.checkReleased(t)
}
