package engine

import (
	"context"
	"errors"
	"gocolumn/memory"
	"os"
	"path/filepath"
	"testing"
)

type n30ReviewManager struct {
	memory.Manager
	calls int
}

func (m *n30ReviewManager) Reserve(c context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	return m.Manager.Reserve(c, n)
}
func TestN30ReviewPreflight(t *testing.T) {
	ctx := context.Background()
	meta, records := reviewPartial(t)
	mgr := memory.NewBudgetManager(1 << 20)
	store := reviewStore(t)
	f, e := spillDecimalPartialRun(ctx, store, meta, records, mgr)
	if e != nil {
		t.Fatal(e)
	}
	defer f.close(ctx)
	alias := *f
	mismatch := *f
	mismatch.metadata.Scale = 1
	other := reviewStore(t)
	foreign, e := spillDecimalPartialRun(ctx, other, meta, records, mgr)
	if e != nil {
		t.Fatal(e)
	}
	defer foreign.close(ctx)
	for _, tc := range []struct {
		name string
		in   []*decimalPartialFile
	}{{"metadata", []*decimalPartialFile{&mismatch}}, {"sameowner", []*decimalPartialFile{f, f}}, {"samehandle", []*decimalPartialFile{f, &alias}}, {"foreignstore", []*decimalPartialFile{foreign}}} {
		t.Run(tc.name, func(t *testing.T) {
			cm := &n30ReviewManager{Manager: mgr}
			before := mgr.Used()
			o, e := mergeDecimalPartialRuns(ctx, store, meta, tc.in, cm)
			if o != nil {
				_ = o.close(ctx)
			}
			if e == nil || cm.calls != 0 || mgr.Used() != before {
				t.Fatalf("preflight e=%v calls=%d", e, cm.calls)
			}
		})
	}
}
func TestN30ReviewCorruptFile(t *testing.T) {
	ctx := context.Background()
	meta, r := reviewPartial(t)
	mgr := memory.NewBudgetManager(1 << 20)
	store := reviewStore(t)
	f, e := spillDecimalPartialRun(ctx, store, meta, r, mgr)
	if e != nil {
		t.Fatal(e)
	}
	defer f.close(ctx)
	entries, e := os.ReadDir(store.Directory())
	if e != nil || len(entries) != 1 {
		t.Fatal(e)
	}
	path := filepath.Join(store.Directory(), entries[0].Name())
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	original := append([]byte(nil), b...)
	b[len(b)-1] ^= 1
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	before := mgr.Used()
	disk := store.Used()
	o, e := mergeDecimalPartialRuns(ctx, store, meta, []*decimalPartialFile{f}, mgr)
	if o != nil {
		_ = o.close(ctx)
	}
	if !errors.Is(e, memory.ErrSpillCorrupt) || o != nil || mgr.Used() != before || store.Used() != disk || f.handle == nil {
		t.Fatalf("corrupt e=%v owner=%v", e, o)
	}
	if e = os.WriteFile(path, original, 0600); e != nil {
		t.Fatal(e)
	}
	decoded, e := restoreDecimalPartialRun(ctx, f, mgr)
	if e != nil {
		t.Fatal(e)
	}
	decoded.close()
}

type n30CommittedCancel struct {
	context.Context
	dir          string
	initial      map[string]bool
	observed     bool
	block        bool
	path, backup string
	fault        error
}

func (c *n30CommittedCancel) Err() error {
	if c.observed {
		return context.Canceled
	}
	entries, e := os.ReadDir(c.dir)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		if c.initial[entry.Name()] || filepath.Ext(entry.Name()) != ".gls" {
			continue
		}
		c.observed = true
		c.path = filepath.Join(c.dir, entry.Name())
		if c.block {
			if e = os.Rename(c.path, c.backup); e == nil {
				e = os.Mkdir(c.path, 0700)
			}
			if e == nil {
				e = os.WriteFile(filepath.Join(c.path, "block"), []byte("x"), 0600)
			}
			c.fault = e
		}
		return context.Canceled
	}
	return nil
}
func TestN30ReviewPostOutputCancellation(t *testing.T) {
	for _, block := range []bool{false, true} {
		name := "cleanup-success"
		if block {
			name = "cleanup-retry"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			meta, r := reviewPartial(t)
			mgr := memory.NewBudgetManager(1 << 20)
			store := reviewStore(t)
			f, e := spillDecimalPartialRun(ctx, store, meta, r, mgr)
			if e != nil {
				t.Fatal(e)
			}
			defer f.close(ctx)
			entries, _ := os.ReadDir(store.Directory())
			initial := map[string]bool{}
			for _, entry := range entries {
				initial[entry.Name()] = true
			}
			c := &n30CommittedCancel{Context: ctx, dir: store.Directory(), initial: initial, block: block, backup: filepath.Join(t.TempDir(), "output-backup")}
			before := mgr.Used()
			disk := store.Used()
			out, e := mergeDecimalPartialRuns(c, store, meta, []*decimalPartialFile{f}, mgr)
			if !c.observed || c.fault != nil || !errors.Is(e, context.Canceled) {
				t.Fatalf("cancel observed=%v fault=%v err=%v", c.observed, c.fault, e)
			}
			if block {
				if out == nil || out.lease == nil || out.pageCount != 1 || out.pages[0] == nil || out.pages[0].handle == nil || mgr.Used() <= before || store.Used() <= disk {
					t.Fatal("failed cleanup lost retry owner")
				}
				if e = out.close(ctx); e == nil {
					t.Fatal("blocked unlink unexpectedly succeeded")
				}
				if e = os.RemoveAll(c.path); e != nil {
					t.Fatal(e)
				}
				if e = os.Rename(c.backup, c.path); e != nil {
					t.Fatal(e)
				}
				if e = out.close(ctx); e != nil {
					t.Fatal(e)
				}
				if e = out.close(ctx); e != nil {
					t.Fatal(e)
				}
			} else if out != nil {
				t.Fatal("clean cancel returned owner")
			}
			if mgr.Used() != before || store.Used() != disk {
				t.Fatalf("cleanup leak mem %d/%d disk %d/%d", mgr.Used(), before, store.Used(), disk)
			}
			decoded, e := restoreDecimalPartialRun(ctx, f, mgr)
			if e != nil {
				t.Fatal("borrowed input lost", e)
			}
			decoded.close()
		})
	}
}
func TestN30ReviewOutputOwnership(t *testing.T) {
	ctx := context.Background()
	meta, r := reviewPartial(t)
	mgr := memory.NewBudgetManager(1 << 20)
	store := reviewStore(t)
	f, e := spillDecimalPartialRun(ctx, store, meta, r, mgr)
	if e != nil {
		t.Fatal(e)
	}
	out, e := mergeDecimalPartialRuns(ctx, store, meta, []*decimalPartialFile{f}, mgr)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.close(ctx); e != nil {
		t.Fatal(e)
	}
	decoded, e := restoreDecimalPartialRun(ctx, out.pages[0], mgr)
	if e != nil {
		t.Fatal(e)
	}
	if e = out.close(ctx); e != nil {
		t.Fatal(e)
	}
	if e = out.close(ctx); e != nil {
		t.Fatal(e)
	}
	if e = store.Close(); e != nil {
		t.Fatal(e)
	}
	if len(decoded.records) != 1 || decoded.records[0] != r[0] {
		t.Fatal("output aliases input/file storage")
	}
	decoded.close()
	if mgr.Used() != 0 {
		t.Fatal("retained memory", mgr.Used())
	}
}
