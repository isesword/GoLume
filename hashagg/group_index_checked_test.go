package hashagg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"testing"
)

func TestN18GroupIndexProbeCancellation(t *testing.T) {
	x, e := NewGroupIndex(256, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer x.Close()
	keys := [][]byte{}
	mask := uint64(len(x.slots) - 1)
	for i := 0; len(keys) < 131; i++ {
		k := []byte(fmt.Sprint("collision-", i))
		if HashBytes(k)&mask == mask {
			keys = append(keys, k)
		}
	}
	for i := 0; i < 130; i++ {
		if _, _, e = x.LookupOrInsertWithCheck(keys[i], func() error { return nil }); e != nil {
			t.Fatal(e)
		}
	}
	calls := 0
	_, _, e = x.LookupWithCheck(keys[130], func() error {
		calls++
		if calls == 3 {
			return context.Canceled
		}
		return nil
	})
	if !errors.Is(e, context.Canceled) || calls != 3 {
		t.Fatal(e, calls)
	}
	for i, k := range keys[:130] {
		id, ok, e := x.LookupWithCheck(k, nil)
		if e != nil || !ok || id != i {
			t.Fatal(i, id, ok, e)
		}
	}
}
func TestN18GroupIndexGrowAndCopyCancellation(t *testing.T) {
	fixture := func() (*GroupIndex, *memory.BudgetManager) {
		m := memory.NewBudgetManager(0)
		x, e := NewGroupIndex(1, func(n int64) (memory.Reservation, error) { return m.Reserve(context.Background(), n) })
		if e != nil {
			t.Fatal(e)
		}
		n := x.maxEntries
		for i := 0; i < n; i++ {
			if _, _, e = x.LookupOrInsert([]byte(fmt.Sprint(i))); e != nil {
				t.Fatal(e)
			}
		}
		return x, m
	}
	key := bytes.Repeat([]byte{'x'}, 12000)
	x, m := fixture()
	calls := 0
	_, _, e := x.LookupOrInsertWithCheck(key, func() error { calls++; return nil })
	if e != nil {
		t.Fatal(e)
	}
	x.Close()
	if m.Used() != 0 {
		t.Fatal(m.Used())
	}
	for fail := 1; fail <= calls; fail++ {
		x, m := fixture()
		old := x.Len()
		count := 0
		_, _, e = x.LookupOrInsertWithCheck(key, func() error {
			count++
			if count == fail {
				return context.Canceled
			}
			return nil
		})
		if !errors.Is(e, context.Canceled) || x.Len() != old {
			t.Fatal(fail, e, x.Len(), old)
		}
		for i := 0; i < old; i++ {
			id, ok := x.Lookup([]byte(fmt.Sprint(i)))
			if !ok || id != i {
				t.Fatal("corrupted old key", fail, i)
			}
		}
		if m.Used() != x.RetainedBytes() {
			t.Fatal("leak", fail, m.Used(), x.RetainedBytes())
		}
		x.Close()
		if m.Used() != 0 {
			t.Fatal("close leak")
		}
	}
	t.Log("checked insertion cancel boundaries", calls)
}

func TestN18CheckedExistingLookupNoAlloc(t *testing.T) {
	x, _ := NewGroupIndex(1, nil)
	defer x.Close()
	key := []byte("123456789")
	x.LookupOrInsert(key)
	check := func() error { return nil }
	if a := testing.AllocsPerRun(1000, func() {
		_, _, e := x.LookupOrInsertWithCheck(key, check)
		if e != nil {
			panic(e)
		}
	}); a != 0 {
		t.Fatal(a)
	}
}
