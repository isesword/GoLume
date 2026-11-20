package engine

import (
	"context"
	"errors"
	"gocolumn/memory"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func n31ReviewInputs(t *testing.T, store *memory.QuerySpillStore, mgr memory.Manager, n int) (decimalPartialMetadata, []decimalPartialLogicalRun) {
	t.Helper()
	m, r := reviewPartial(t)
	in := make([]decimalPartialLogicalRun, n)
	for i := range in {
		r[0].Key = int64(i)
		f, e := spillDecimalPartialRun(context.Background(), store, m, r, mgr)
		if e != nil {
			t.Fatal(e)
		}
		in[i].pages = []*decimalPartialFile{f}
		t.Cleanup(func() { _ = f.close(context.Background()) })
	}
	return m, in
}
func TestN31ReviewCrossPageOrder(t *testing.T) {
	for _, key := range []int64{7, 6} {
		t.Run(string(rune('0'+key)), func(t *testing.T) {
			ctx := context.Background()
			store := reviewStore(t)
			mgr := memory.NewBudgetManager(4 << 20)
			m, r := reviewPartial(t)
			r[0].Key = 7
			a, e := spillDecimalPartialRun(ctx, store, m, r, mgr)
			if e != nil {
				t.Fatal(e)
			}
			defer a.close(ctx)
			r[0].Key = key
			b, e := spillDecimalPartialRun(ctx, store, m, r, mgr)
			if e != nil {
				t.Fatal(e)
			}
			defer b.close(ctx)
			before := mgr.Used()
			disk := store.Used()
			out, e := mergeDecimalPartialRunsMultiPass(ctx, store, m, []decimalPartialLogicalRun{{pages: []*decimalPartialFile{a, b}}}, mgr)
			if out != nil {
				_ = out.close(ctx)
			}
			if !errors.Is(e, memory.ErrSpillCorrupt) || out != nil || mgr.Used() != before || store.Used() != disk {
				t.Fatalf("crosspage e=%v memory=%d/%d", e, mgr.Used(), before)
			}
		})
	}
}
func TestN31ReviewLateCorruption(t *testing.T) {
	ctx := context.Background()
	store := reviewStore(t)
	mgr := memory.NewBudgetManager(4 << 20)
	m, r := reviewPartial(t)
	records := make([]decimalPartialRecord, 256)
	for i := range records {
		records[i] = r[0]
		records[i].Key = int64(i)
	}
	a, e := spillDecimalPartialRun(ctx, store, m, records, mgr)
	if e != nil {
		t.Fatal(e)
	}
	defer a.close(ctx)
	prior, _ := os.ReadDir(store.Directory())
	known := map[string]bool{}
	for _, x := range prior {
		known[x.Name()] = true
	}
	r[0].Key = 300
	b, e := spillDecimalPartialRun(ctx, store, m, r, mgr)
	if e != nil {
		t.Fatal(e)
	}
	defer b.close(ctx)
	entries, _ := os.ReadDir(store.Directory())
	var path string
	for _, x := range entries {
		if !known[x.Name()] {
			path = filepath.Join(store.Directory(), x.Name())
		}
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	data[len(data)-1] ^= 1
	if e = os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	before := mgr.Used()
	disk := store.Used()
	watch := &n31ObserveOutput{Context: ctx, dir: store.Directory()}
	out, e := mergeDecimalPartialRunsMultiPass(watch, store, m, []decimalPartialLogicalRun{{pages: []*decimalPartialFile{a, b}}}, mgr)
	if !watch.observed {
		t.Fatal("late corruption did not follow committed output")
	}
	if out != nil {
		_ = out.close(ctx)
	}
	if !errors.Is(e, memory.ErrSpillCorrupt) || out != nil || mgr.Used() != before || store.Used() != disk {
		t.Fatalf("late corrupt e=%v owner=%v", e, out)
	}
	entries, _ = os.ReadDir(store.Directory())
	if len(entries) != 2 {
		t.Fatalf("generated files leaked: %d", len(entries))
	}
	// Restore the caller-owned corrupt input bytes after the failure check.
	data[len(data)-1] ^= 1
	if e = os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
}

type n31OldGenerationFault struct {
	context.Context
	dir, backup, path string
	initial           map[string]bool
	observed          bool
	fault             error
}

func (c *n31OldGenerationFault) Err() error {
	if c.observed {
		return nil
	}
	entries, e := os.ReadDir(c.dir)
	if e != nil {
		return e
	}
	var generated []string
	for _, x := range entries {
		if !c.initial[x.Name()] && filepath.Ext(x.Name()) == ".gls" {
			generated = append(generated, x.Name())
		}
	}
	if len(generated) < 3 {
		return nil
	}
	sort.Strings(generated)
	c.observed = true
	c.path = filepath.Join(c.dir, generated[0])
	e = os.Rename(c.path, c.backup)
	if e == nil {
		e = os.Mkdir(c.path, 0700)
	}
	if e == nil {
		e = os.WriteFile(filepath.Join(c.path, "block"), []byte("x"), 0600)
	}
	c.fault = e
	return nil
}
func TestN31ReviewOldGenerationDeleteRetry(t *testing.T) {
	ctx := context.Background()
	store := reviewStore(t)
	mgr := memory.NewBudgetManager(4 << 20)
	m, in := n31ReviewInputs(t, store, mgr, 9)
	entries, _ := os.ReadDir(store.Directory())
	initial := map[string]bool{}
	for _, x := range entries {
		initial[x.Name()] = true
	}
	c := &n31OldGenerationFault{Context: ctx, dir: store.Directory(), backup: filepath.Join(t.TempDir(), "saved"), initial: initial}
	before := mgr.Used()
	disk := store.Used()
	out, e := mergeDecimalPartialRunsMultiPass(c, store, m, in, mgr)
	if !c.observed || c.fault != nil || e == nil || out == nil || out.lease == nil {
		t.Fatalf("generation failure observed=%v fault=%v e=%v owner=%v", c.observed, c.fault, e, out)
	}
	retained := 0
	for _, f := range out.cleanup {
		if f != nil && f.handle != nil {
			retained++
		}
	}
	if retained != 1 {
		t.Fatalf("retained owners=%d", retained)
	}
	if e = out.close(ctx); e == nil {
		t.Fatal("blocked delete succeeded")
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
	if mgr.Used() != before || store.Used() != disk {
		t.Fatalf("leak %d/%d disk%d/%d", mgr.Used(), before, store.Used(), disk)
	}
	for _, run := range in {
		decoded, e := restoreDecimalPartialRun(ctx, run.pages[0], mgr)
		if e != nil {
			t.Fatal(e)
		}
		decoded.close()
	}
}
func TestN31ReviewPostOutputCancel(t *testing.T) {
	ctx := context.Background()
	store := reviewStore(t)
	mgr := memory.NewBudgetManager(4 << 20)
	m, in := n31ReviewInputs(t, store, mgr, 9)
	entries, _ := os.ReadDir(store.Directory())
	initial := map[string]bool{}
	for _, x := range entries {
		initial[x.Name()] = true
	}
	c := &n30CommittedCancel{Context: ctx, dir: store.Directory(), initial: initial}
	before := mgr.Used()
	disk := store.Used()
	out, e := mergeDecimalPartialRunsMultiPass(c, store, m, in, mgr)
	if out != nil {
		_ = out.close(ctx)
	}
	if !c.observed || !errors.Is(e, context.Canceled) || out != nil || mgr.Used() != before || store.Used() != disk {
		t.Fatalf("cancel observed=%v e=%v", c.observed, e)
	}
}

type n31ObserveOutput struct {
	context.Context
	dir      string
	observed bool
}

func (c *n31ObserveOutput) Err() error {
	entries, e := os.ReadDir(c.dir)
	if e != nil {
		return e
	}
	committed := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".gls" {
			committed++
		}
	}
	if committed > 2 {
		c.observed = true
	}
	return nil
}
