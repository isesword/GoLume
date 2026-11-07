package hashagg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"math/rand"
	"testing"
)

func TestGroupIndexStableIDsAndReservations(t *testing.T) {
	manager := memory.NewBudgetManager(0)
	var peak int64
	index, err := NewGroupIndex(1, func(n int64) (memory.Reservation, error) {
		lease, err := manager.Reserve(context.Background(), n)
		peak = max(peak, manager.Used())
		return lease, err
	})
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("group-%04d-%s", i, bytes.Repeat([]byte{'x'}, i%79)))
		id, inserted, err := index.LookupOrInsert(keys[i])
		if err != nil || !inserted || id != i {
			t.Fatalf("insert %d: id=%d inserted=%v err=%v", i, id, inserted, err)
		}
		if manager.Used() != index.RetainedBytes() {
			t.Fatalf("insert %d: used=%d retained=%d", i, manager.Used(), index.RetainedBytes())
		}
	}
	for i := len(keys) - 1; i >= 0; i-- {
		id, inserted, err := index.LookupOrInsert(keys[i])
		if err != nil || inserted || id != i || !bytes.Equal(index.Key(i), keys[i]) {
			t.Fatalf("lookup %d: id=%d inserted=%v err=%v key=%q", i, id, inserted, err, index.Key(i))
		}
	}
	if index.Len() != len(keys) || peak <= manager.Used() {
		t.Fatalf("len=%d peak=%d retained=%d", index.Len(), peak, manager.Used())
	}
	index.Close()
	index.Close()
	if index.Len() != 0 || index.RetainedBytes() != 0 || manager.Used() != 0 || index.Key(0) != nil {
		t.Fatalf("closed index retains state: len=%d retained=%d used=%d", index.Len(), index.RetainedBytes(), manager.Used())
	}
	if _, _, err := index.LookupOrInsert([]byte("closed")); err == nil {
		t.Fatal("closed insertion accepted")
	}
}

func TestGroupIndexCollisionsAndCopiedKeys(t *testing.T) {
	index, err := NewGroupIndex(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	keys := make([][]byte, 0, 10)
	mask := uint64(len(index.slots) - 1)
	// Force a linear-probe cluster to wrap around the last physical slot.
	for i := 0; len(keys) < cap(keys); i++ {
		key := []byte(fmt.Sprintf("collision-%d", i))
		if HashBytes(key)&mask == mask {
			keys = append(keys, key)
		}
	}
	for i, key := range keys {
		input := bytes.Clone(key)
		id, inserted, err := index.LookupOrInsert(input)
		if err != nil || !inserted || id != i {
			t.Fatalf("insert collision %d: %d %v %v", i, id, inserted, err)
		}
		clear(input)
	}
	for i, key := range keys {
		id, inserted, err := index.LookupOrInsert(key)
		if err != nil || inserted || id != i || !bytes.Equal(index.Key(i), key) {
			t.Fatalf("lookup collision %d: %d %v %v", i, id, inserted, err)
		}
		if cap(index.Key(i)) != len(key) {
			t.Fatal("borrowed key exposes spare arena capacity")
		}
	}
}

func TestGroupIndexEmptyKeysAndNoLookupAllocations(t *testing.T) {
	index, err := NewGroupIndex(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	id, inserted, err := index.LookupOrInsert(nil)
	if err != nil || !inserted || id != 0 {
		t.Fatalf("empty insert: %d %v %v", id, inserted, err)
	}
	id, inserted, err = index.LookupOrInsert([]byte{})
	if err != nil || inserted || id != 0 || index.Len() != 1 {
		t.Fatalf("empty lookup: %d %v %v", id, inserted, err)
	}
	id, inserted, err = index.LookupOrInsert([]byte{0})
	if err != nil || !inserted || id != 1 {
		t.Fatalf("NULL marker insert: %d %v %v", id, inserted, err)
	}
	if index.Key(-1) != nil || index.Key(index.Len()) != nil {
		t.Fatal("invalid ID returned a key")
	}
	key := []byte{0}
	if n := testing.AllocsPerRun(100, func() { _, _, _ = index.LookupOrInsert(key) }); n != 0 {
		t.Fatalf("existing-key lookup allocated %v objects", n)
	}
}

func TestGroupIndexGrowthDenialPreservesIDs(t *testing.T) {
	for _, denial := range []string{"slots", "arena-after-slots"} {
		t.Run(denial, func(t *testing.T) {
			manager := memory.NewBudgetManager(0)
			denied := errors.New("injected reserve denial")
			failBytes := int64(-1)
			index, err := NewGroupIndex(1, func(n int64) (memory.Reservation, error) {
				if n == failBytes {
					return nil, denied
				}
				return manager.Reserve(context.Background(), n)
			})
			if err != nil {
				t.Fatal(err)
			}
			defer index.Close()
			count := index.maxEntries
			for i := 0; i < count; i++ {
				if id, inserted, err := index.LookupOrInsert([]byte{byte(i)}); err != nil || !inserted || id != i {
					t.Fatalf("initial insert %d: %d %v %v", i, id, inserted, err)
				}
			}
			key := bytes.Repeat([]byte{255}, 80)
			if denial == "slots" {
				failBytes = int64(len(index.slots)*2) * groupIndexSlotBytes
			} else {
				failBytes = 128
			}
			if _, inserted, err := index.LookupOrInsert(key); !errors.Is(err, denied) || inserted {
				t.Fatalf("denied insertion: %v %v", inserted, err)
			}
			if index.Len() != count || manager.Used() != index.RetainedBytes() {
				t.Fatalf("failed insert changed count/accounting: %d %d %d", index.Len(), manager.Used(), index.RetainedBytes())
			}
			for i := 0; i < count; i++ {
				if id, inserted, err := index.LookupOrInsert([]byte{byte(i)}); err != nil || inserted || id != i {
					t.Fatalf("preserved lookup %d: %d %v %v", i, id, inserted, err)
				}
			}
			failBytes = -1
			if id, inserted, err := index.LookupOrInsert(key); err != nil || !inserted || id != count {
				t.Fatalf("retry insertion: %d %v %v", id, inserted, err)
			}
			index.Close()
			if manager.Used() != 0 {
				t.Fatalf("retained %d bytes", manager.Used())
			}
		})
	}
}

func TestGroupIndexArenaGrowthWithBorrowedInput(t *testing.T) {
	index, err := NewGroupIndex(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	original := bytes.Repeat([]byte{'z'}, 60)
	if _, _, err = index.LookupOrInsert(original); err != nil {
		t.Fatal(err)
	}
	// A novel substring of an existing borrowed key forces arena replacement.
	key := index.Key(0)[:40]
	id, inserted, err := index.LookupOrInsert(key)
	if err != nil || !inserted || id != 1 || !bytes.Equal(index.Key(1), original[:40]) {
		t.Fatalf("borrowed insertion: %d %v %v", id, inserted, err)
	}
	if !bytes.Equal(index.Key(0), original) {
		t.Fatal("original key changed")
	}
}

func TestGroupIndexRejectsInvalidReservationsAndCapacity(t *testing.T) {
	for _, mode := range []string{"nil", "wrong-size", "lease-with-error"} {
		t.Run(mode, func(t *testing.T) {
			manager := memory.NewBudgetManager(0)
			index, err := NewGroupIndex(1, func(n int64) (memory.Reservation, error) {
				if mode == "nil" {
					return nil, nil
				}
				if mode == "wrong-size" {
					return manager.Reserve(context.Background(), n+1)
				}
				lease, e := manager.Reserve(context.Background(), n)
				if e != nil {
					return nil, e
				}
				return lease, errors.New("denied with lease")
			})
			if err == nil || index != nil || manager.Used() != 0 {
				t.Fatalf("invalid reserve: index=%v err=%v used=%d", index, err, manager.Used())
			}
		})
	}
	called := false
	if index, err := NewGroupIndex(int(^uint(0)>>1), func(n int64) (memory.Reservation, error) { called = true; return nil, nil }); err == nil || index != nil || called {
		t.Fatalf("capacity overflow index=%v err=%v called=%v", index, err, called)
	}
	var index *GroupIndex
	index.Close()
	if index.Len() != 0 || index.RetainedBytes() != 0 || index.Key(0) != nil {
		t.Fatal("nil inspection failed")
	}
	if _, _, err := index.LookupOrInsert(nil); err == nil {
		t.Fatal("nil insertion accepted")
	}
}

func TestGroupIndexRandomizedReference(t *testing.T) {
	index, err := NewGroupIndex(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	rng := rand.New(rand.NewSource(411))
	want := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("%d/%d", rng.Intn(51), rng.Intn(17)))
		old, exists := want[string(key)]
		id, inserted, err := index.LookupOrInsert(key)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			if inserted || id != old {
				t.Fatalf("lookup %q = %d,%v want %d", key, id, inserted, old)
			}
		} else {
			if !inserted || id != len(want) {
				t.Fatalf("new ID %d want %d", id, len(want))
			}
			want[string(key)] = id
		}
	}
	if index.Len() != len(want) {
		t.Fatalf("count=%d want=%d", index.Len(), len(want))
	}
}

func TestGroupIndexLookupIsReadOnly(t *testing.T) {
	calls := 0
	manager := memory.NewBudgetManager(0)
	index, err := NewGroupIndex(1, func(n int64) (memory.Reservation, error) { calls++; return manager.Reserve(context.Background(), n) })
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	key := []byte("known")
	if _, _, err := index.LookupOrInsert(key); err != nil {
		t.Fatal(err)
	}
	count, retained, initialCalls := index.Len(), index.RetainedBytes(), calls
	if id, ok := index.Lookup(key); !ok || id != 0 {
		t.Fatalf("known lookup: %d %v", id, ok)
	}
	if _, ok := index.Lookup([]byte("absent")); ok {
		t.Fatal("absent key found")
	}
	if index.Len() != count || index.RetainedBytes() != retained || calls != initialCalls {
		t.Fatal("Lookup mutated or reserved")
	}
	if n := testing.AllocsPerRun(100, func() { _, _ = index.Lookup(key) }); n != 0 {
		t.Fatalf("Lookup allocated %v objects", n)
	}
	index.Close()
	if _, ok := index.Lookup(key); ok {
		t.Fatal("closed lookup found key")
	}
	var missing *GroupIndex
	if _, ok := missing.Lookup(nil); ok {
		t.Fatal("nil lookup found key")
	}
}
