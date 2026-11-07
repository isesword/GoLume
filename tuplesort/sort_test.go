package tuplesort

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestDistinct(t *testing.T) {
	for _, block := range []int{64, 65536} {
		t.Run(fmt.Sprint(block), func(t *testing.T) {
			ctx := context.Background()
			mgr := memory.NewBudgetManager(2 << 20)
			store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			s, e := New(Config{Manager: mgr, Spill: store, BlockBytes: block})
			if e != nil {
				t.Fatal(e)
			}
			for i := 0; i < 1000; i++ {
				key := []byte(fmt.Sprintf("k%02d", i%31))
				if e = s.Add(ctx, key); e != nil {
					t.Fatal(e)
				}
				key[0] = 'x'
			}
			for i := 0; i < 100; i++ {
				if e = s.Add(ctx, nil); e != nil {
					t.Fatal(e)
				}
			}
			if e = s.Finish(ctx); e != nil {
				t.Fatal(e)
			}
			var got []string
			for {
				key, ok, e := s.Next(ctx)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				got = append(got, string(key))
			}
			want := []string{""}
			for i := 0; i < 31; i++ {
				want = append(want, fmt.Sprintf("k%02d", i))
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v", got)
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			if mgr.Used() != 0 {
				t.Fatalf("leak %d", mgr.Used())
			}
		})
	}
}
func TestKeepDuplicates(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewBudgetManager(1 << 20)
	store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	s, _ := New(Config{Manager: mgr, Spill: store, BlockBytes: 32, KeepDuplicates: true})
	defer s.Close()
	for i := 0; i < 100; i++ {
		if e = s.Add(ctx, []byte("x")); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Finish(ctx); e != nil {
		t.Fatal(e)
	}
	n := 0
	for {
		_, ok, e := s.Next(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		n++
	}
	if n != 100 {
		t.Fatal(n)
	}
}
func TestFailuresCleanup(t *testing.T) {
	for _, kind := range []string{"memory", "quota", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			limit := int64(1 << 20)
			quota := int64(1 << 20)
			if kind == "memory" {
				limit = 4200
			}
			if kind == "quota" {
				quota = 1
			}
			mgr := memory.NewBudgetManager(limit)
			store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: quota})
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			s, _ := New(Config{Manager: mgr, Spill: store, BlockBytes: 32})
			var got error
			if kind == "large" {
				got = s.Add(ctx, make([]byte, 25))
			} else {
				for i := 0; i < 50; i++ {
					got = s.Add(ctx, []byte(fmt.Sprint(i)))
					if got != nil {
						break
					}
				}
				if kind == "cancel" {
					c, cancel := context.WithCancel(ctx)
					cancel()
					got = s.Finish(c)
				}
			}
			if got == nil {
				t.Fatal("expected failure")
			}
			if kind == "memory" && !errors.Is(got, memory.ErrBudgetExceeded) {
				t.Fatal(got)
			}
			if mgr.Used() != 0 {
				t.Fatalf("leak %d", mgr.Used())
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestEmptyAndNoSpill(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewBudgetManager(1 << 20)
	s, _ := New(Config{Manager: mgr})
	if e := s.Finish(ctx); e != nil {
		t.Fatal(e)
	}
	if _, ok, e := s.Next(ctx); e != nil || ok {
		t.Fatal(ok, e)
	}
	s.Close()
	s, _ = New(Config{Manager: mgr, BlockBytes: 32})
	if e := s.Add(ctx, []byte{0, 255}); e != nil {
		t.Fatal(e)
	}
	if e := s.Finish(ctx); e != nil {
		t.Fatal(e)
	}
	k, ok, e := s.Next(ctx)
	if e != nil || !ok || !reflect.DeepEqual(k, []byte{0, 255}) {
		t.Fatal(k, ok, e)
	}
	s.Close()
	if mgr.Used() != 0 {
		t.Fatal(mgr.Used())
	}
}

type corruptStore struct {
	memory.SpillManager
	mutate func([]byte)
}

func (c corruptStore) Restore(ctx context.Context, h memory.SpillHandle, f [32]byte, m memory.Manager) (*memory.RestoredSpill, error) {
	r, e := c.SpillManager.Restore(ctx, h, f, m)
	if e == nil {
		c.mutate(r.Object.Payload)
	}
	return r, e
}
func TestCorruptionCleanup(t *testing.T) {
	for _, mode := range []string{"version", "count", "length", "trailing"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			mgr := memory.NewBudgetManager(1 << 20)
			store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			wrapped := corruptStore{SpillManager: store, mutate: func(p []byte) {
				switch mode {
				case "version":
					p[0] = 2
				case "count":
					for i := 8; i < 16; i++ {
						p[i] = 255
					}
				case "length":
					for i := 16; i < 24; i++ {
						p[i] = 255
					}
				case "trailing":
					for i := 8; i < 16; i++ {
						p[i] = 0
					}
					p[8] = 1
				}
			}}
			s, _ := New(Config{Manager: mgr, Spill: wrapped, BlockBytes: 32})
			var got error
			for i := 0; i < 100; i++ {
				got = s.Add(ctx, []byte(fmt.Sprint(i)))
				if got != nil {
					break
				}
			}
			if got == nil {
				got = s.Finish(ctx)
			}
			if !errors.Is(got, memory.ErrSpillCorrupt) {
				t.Fatalf("got %v", got)
			}
			if mgr.Used() != 0 {
				t.Fatalf("memory leak %d", mgr.Used())
			}
		})
	}
}

func TestResidentGrowthAndOversizedSpill(t *testing.T) {
	for _, spill := range []bool{false, true} {
		t.Run(fmt.Sprint(spill), func(t *testing.T) {
			ctx := context.Background()
			mgr := memory.NewBudgetManager(4 << 20)
			var store memory.SpillManager
			if spill {
				var e error
				store, e = memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20})
				if e != nil {
					t.Fatal(e)
				}
				defer store.Close()
			}
			s, _ := New(Config{Manager: mgr, Spill: store, BlockBytes: 32})
			huge := string(make([]byte, 10000))
			want := []string{huge, "a", "b", "c"}
			for i := 0; i < 50; i++ {
				for _, key := range []string{"c", huge, "b", "a"} {
					if e := s.Add(ctx, []byte(key)); e != nil {
						t.Fatal(e)
					}
				}
			}
			if e := s.Finish(ctx); e != nil {
				t.Fatal(e)
			}
			var got []string
			for {
				k, ok, e := s.Next(ctx)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				got = append(got, string(k))
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("wrong values, count %d", len(got))
			}
			if mgr.Used() != 0 {
				t.Fatalf("retained at EOF: %d", mgr.Used())
			}
			if _, ok, e := s.Next(ctx); e != nil || ok {
				t.Fatal(ok, e)
			}
		})
	}
}
func TestResidentGrowthBudgetAdmission(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewBudgetManager(5000)
	s, _ := New(Config{Manager: mgr, BlockBytes: 32})
	var e error
	for i := 0; i < 1000; i++ {
		e = s.Add(ctx, []byte(fmt.Sprint(i)))
		if e != nil {
			break
		}
	}
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal(e)
	}
	if mgr.Used() != 0 {
		t.Fatal(mgr.Used())
	}
}
func TestOversizedSingletonBudgetCleanup(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewBudgetManager(5000)
	store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	s, _ := New(Config{Manager: mgr, Spill: store, BlockBytes: 32})
	e = s.Add(ctx, make([]byte, 10000))
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal(e)
	}
	if mgr.Used() != 0 {
		t.Fatal(mgr.Used())
	}
}

func TestHighCardinalityWithin128KiB(t *testing.T) {
	for _, groups := range []int{1, 37} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			ctx := context.Background()
			mgr := memory.NewBudgetManager(128 << 10)
			store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			s, _ := New(Config{Manager: mgr, Spill: store, BlockBytes: 1024})
			defer s.Close()
			for repeat := 0; repeat < 3; repeat++ {
				for i := 999; i >= 0; i-- {
					k := []byte(fmt.Sprintf("g%03d/v%05d", i%groups, i))
					if e = s.Add(ctx, k); e != nil {
						t.Fatal(e)
					}
				}
			}
			if e = s.Finish(ctx); e != nil {
				t.Fatal(e)
			}
			count := 0
			prev := ""
			for {
				k, ok, e := s.Next(ctx)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				if string(k) <= prev {
					t.Fatal("not strictly ordered")
				}
				prev = string(k)
				count++
			}
			if count != 1000 {
				t.Fatal(count)
			}
			stats := s.Stats()
			if stats.SpillRuns < 2 || stats.SpillBytes <= 0 || stats.MergeLevels < 2 {
				t.Fatal(stats)
			}
			if mgr.Used() != 0 {
				t.Fatal(mgr.Used())
			}
		})
	}
}
func TestSegmentMetadataAdmission(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewBudgetManager(128 << 10)
	store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 32 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	s, _ := New(Config{Manager: mgr, Spill: store, BlockBytes: 64})
	for i := 0; i < 100000; i++ {
		e = s.Add(ctx, []byte(fmt.Sprintf("v%09d", i)))
		if e != nil {
			break
		}
	}
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal(e)
	}
	if s.Stats().MergeLevels < 2 {
		t.Fatal(s.Stats())
	}
	if mgr.Used() != 0 {
		t.Fatal(mgr.Used())
	}
}

func (c corruptStore) SpillWithMetadata(ctx context.Context, o memory.SpillObject, m memory.Manager) (memory.SpillHandle, error) {
	return c.SpillManager.(memory.MetadataSpillManager).SpillWithMetadata(ctx, o, m)
}

type legacyOnlyStore struct{ memory.SpillManager }

func TestUnknownStoreFailsBounded(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewBudgetManager(1 << 20)
	store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	s, _ := New(Config{Manager: mgr, Spill: legacyOnlyStore{store}, BlockBytes: 32})
	for i := 0; i < 100; i++ {
		e = s.Add(ctx, []byte{byte(i)})
		if e != nil {
			break
		}
	}
	if !errors.Is(e, memory.ErrSpillUnsupported) {
		t.Fatal(e)
	}
	if store.Used() != 0 || mgr.Used() != 0 {
		t.Fatalf("failed capability check retained resources")
	}
}
