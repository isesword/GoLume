package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
)

// This probe observes real committed store bytes and explicit Reserve boundaries.
// It does not claim to inject arbitrary Go allocations or filesystem syscalls.
type n31BudgetProbe struct {
	memory.Manager
	calls, failAt, cancelAt int
	cancel                  context.CancelFunc
	observer                *n31BudgetContext
	peakMemory              int64
}

func (m *n31BudgetProbe) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	m.observer.observe()
	if m.calls == m.failAt {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peakMemory {
		m.peakMemory = m.Used()
	}
	if e == nil && m.calls == m.cancelAt {
		m.cancel()
	}
	return r, e
}

type n31BudgetContext struct {
	context.Context
	t                                  *testing.T
	store                              *memory.QuerySpillStore
	dir                                string
	baseline, peakDisk, peakPhysical   int64
	cancel                             context.CancelFunc
	cancelAfterCommit, committedCancel bool
}

func (c *n31BudgetContext) observe() {
	if c == nil || c.store == nil {
		return
	}
	used := c.store.Used()
	if used > c.peakDisk {
		c.peakDisk = used
	}
	if c.peakPhysical < c.peakDisk {
		disk, _ := n30BudgetDisk(c.t, c.dir)
		if disk > c.peakPhysical {
			c.peakPhysical = disk
		}
	}
	if c.cancelAfterCommit && !c.committedCancel && used > c.baseline && c.peakPhysical > c.baseline {
		c.committedCancel = true
		c.cancel()
	}
}
func (c *n31BudgetContext) Err() error { c.observe(); return c.Context.Err() }
func n31BudgetInputs(t *testing.T, store *memory.QuerySpillStore, m memory.Manager) ([]decimalPartialLogicalRun, []*decimalPartialFile) {
	t.Helper()
	runs := make([]decimalPartialLogicalRun, 9)
	var files []*decimalPartialFile
	for run := range runs {
		records := make([]decimalPartialRecord, 64)
		for i := range records {
			key := int64(i*9 + run)
			r := decimalPartialRecord{Key: key, KeyValid: true, CountRows: 2, CountValid: 1}
			if e := r.Sum.Add(decimal128.FromI64(key + 1)); e != nil {
				t.Fatal(e)
			}
			records[i] = r
		}
		f, e := spillDecimalPartialRun(context.Background(), store, decimalPartialMetadata{Precision: 9, Scale: 2}, records, m)
		if e != nil {
			t.Fatal(e)
		}
		files = append(files, f)
		runs[run] = decimalPartialLogicalRun{pages: []*decimalPartialFile{f}}
	}
	return runs, files
}
func n31BudgetInputsIntact(t *testing.T, files []*decimalPartialFile, m memory.Manager) {
	t.Helper()
	for run, f := range files {
		d, e := restoreDecimalPartialRun(context.Background(), f, m)
		if e != nil {
			t.Fatal("borrowed input disappeared", e)
		}
		if len(d.records) != 64 {
			t.Fatal("input length changed")
		}
		for i, r := range d.records {
			key := int64(i*9 + run)
			v, valid, e := r.Sum.Finalize()
			if e != nil || !valid || r.Key != key || !r.KeyValid || v != decimal128.FromI64(key+1) || r.CountRows != 2 || r.CountValid != 1 {
				t.Fatal("borrowed input mutated", run, i)
			}
		}
		d.close()
	}
}

type n31BudgetResult struct {
	calls                        int
	baseline, peak, physicalPeak int64
}

func n31BudgetRun(t *testing.T, quota int64, failAt, cancelAt int, cancelCommitted, quotaFailure bool) n31BudgetResult {
	t.Helper()
	dir := t.TempDir()
	store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: dir, MaxBytes: quota})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer := &n31BudgetContext{Context: ctx, t: t, store: store, dir: dir, cancel: cancel}
	m := &n31BudgetProbe{Manager: memory.NewBudgetManager(4 << 20), observer: observer, cancel: cancel}
	runs, files := n31BudgetInputs(t, store, m)
	defer func() {
		for _, f := range files {
			f.close(context.Background())
		}
	}()
	baseline, baseMemory := store.Used(), m.Used()
	disk, n := n30BudgetDisk(t, dir)
	if disk != baseline || n != 9 {
		t.Fatal("actual input files baseline", disk, baseline, n)
	}
	m.calls = 0
	m.failAt = failAt
	m.cancelAt = cancelAt
	m.peakMemory = baseMemory
	observer.baseline = baseline
	observer.peakDisk = baseline
	observer.peakPhysical = baseline
	observer.cancelAfterCommit = cancelCommitted
	out, e := mergeDecimalPartialRunsMultiPass(observer, store, decimalPartialMetadata{Precision: 9, Scale: 2}, runs, m)
	observer.observe()
	result := n31BudgetResult{m.calls, baseline, observer.peakDisk, observer.peakPhysical}
	if failAt > 0 && !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatalf("observed reserve %d: %v", failAt, e)
	}
	if (cancelAt > 0 || cancelCommitted) && !errors.Is(e, context.Canceled) {
		t.Fatalf("observed cancel %d/committed%v: %v", cancelAt, cancelCommitted, e)
	}
	if cancelCommitted && !observer.committedCancel {
		t.Fatal("did not cancel after an actual intermediate file commit")
	}
	if quotaFailure && !errors.Is(e, memory.ErrSpillQuotaExceeded) {
		t.Fatal("exact peak minus one must fail disk quota", e)
	}
	if failAt == 0 && cancelAt == 0 && !cancelCommitted && !quotaFailure && e != nil {
		t.Fatal(e)
	}
	m.failAt = 0
	m.cancelAt = 0
	observer.cancelAfterCommit = false
	if e == nil {
		if out == nil || out.pageCount != 3 || out.passes < 2 {
			t.Fatal("not an actual two-pass cross-page merge")
		}
		if observer.peakDisk != observer.peakPhysical {
			t.Fatal("peak must be backed by measured physical file bytes", observer.peakDisk, observer.peakPhysical)
		}
		if observer.peakDisk <= store.Used() {
			t.Fatal("intermediate generations did not create measured disk peak")
		}
		disk, n = n30BudgetDisk(t, dir)
		if disk != store.Used() || n != 12 {
			t.Fatal("intermediate files leaked after successful merge", disk, store.Used(), n)
		}
		at := int64(0)
		for page := 0; page < out.pageCount; page++ {
			d, err := restoreDecimalPartialRun(context.Background(), out.pages[page], m)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range d.records {
				v, valid, err := r.Sum.Finalize()
				if err != nil || !valid || r.Key != at || v != decimal128.FromI64(at+1) || r.CountRows != 2 || r.CountValid != 1 {
					t.Fatal("output exact key/value lifecycle", at)
				}
				at++
			}
			d.close()
		}
		if at != 576 {
			t.Fatal("output cardinality", at)
		}
		t.Logf("actual inputs=%d peakUsed=%d peakPhysical=%d finalUsed=%d reserveCalls=%d memoryPeak=%d passes=%d", baseline, observer.peakDisk, observer.peakPhysical, store.Used(), result.calls, m.peakMemory, out.passes)
	}
	if out != nil {
		if err := out.close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := out.close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if store.Used() != baseline || m.Used() != baseMemory {
		t.Fatal("intermediate metadata/output leak after cleanup", store.Used(), baseline, m.Used(), baseMemory)
	}
	n31BudgetInputsIntact(t, files, m)
	for _, f := range files {
		if err := f.close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	disk, n = n30BudgetDisk(t, dir)
	if disk != 0 || n != 0 || store.Used() != 0 || m.Used() != 0 {
		t.Fatal("final ownership cleanup", disk, n, store.Used(), m.Used())
	}
	return result
}
func TestN31MultipassBudgetActualDiskQuota(t *testing.T) {
	measured := n31BudgetRun(t, 8<<20, 0, 0, false, false)
	t.Run("exact-observed-peak", func(t *testing.T) {
		again := n31BudgetRun(t, measured.peak, 0, 0, false, false)
		if again.peak != measured.peak || again.baseline != measured.baseline {
			t.Fatal("nonrepeatable physical disk measurement")
		}
	})
	t.Run("observed-peak-minus-one", func(t *testing.T) { n31BudgetRun(t, measured.peak-1, 0, 0, false, true) })
}
func TestN31MultipassBudgetObservedReserveFailures(t *testing.T) {
	measured := n31BudgetRun(t, 8<<20, 0, 0, false, false)
	if measured.calls == 0 {
		t.Fatal("no reserve calls observed")
	}
	for at := 1; at <= measured.calls; at++ {
		t.Run(fmt.Sprintf("reserve-%03d", at), func(t *testing.T) { n31BudgetRun(t, 8<<20, at, 0, false, false) })
	}
}
func TestN31MultipassBudgetObservedCancellation(t *testing.T) {
	measured := n31BudgetRun(t, 8<<20, 0, 0, false, false)
	for at := 1; at <= measured.calls; at++ {
		t.Run(fmt.Sprintf("cancel-%03d", at), func(t *testing.T) { n31BudgetRun(t, 8<<20, 0, at, false, false) })
	}
	t.Run("after-intermediate-file-commit", func(t *testing.T) { n31BudgetRun(t, 8<<20, 0, 0, true, false) })
}

func TestN31MultipassBudgetFiniteAdmission(t *testing.T) {
	for _, which := range []string{"nil-context", "nil-store", "zero-quota-store", "nil-manager", "unlimited-manager", "one-byte-manager"} {
		t.Run(which, func(t *testing.T) {
			dir := t.TempDir()
			store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: dir, MaxBytes: 1 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			var manager memory.Manager = memory.NewBudgetManager(1 << 20)
			ctx := context.Background()
			selectedStore := store
			switch which {
			case "nil-context":
				ctx = nil
			case "nil-store":
				selectedStore = nil
			case "zero-quota-store":
				selectedStore = &memory.QuerySpillStore{}
			case "nil-manager":
				manager = nil
			case "unlimited-manager":
				manager = memory.NewBudgetManager(0)
			case "one-byte-manager":
				manager = memory.NewBudgetManager(1)
			}
			out, e := mergeDecimalPartialRunsMultiPass(ctx, selectedStore, decimalPartialMetadata{Precision: 9, Scale: 2}, nil, manager)
			if out != nil {
				if ce := out.close(context.Background()); ce != nil {
					t.Fatal(ce)
				}
			}
			if e == nil {
				t.Fatal("invalid finite resource admission accepted")
			}
			if which == "one-byte-manager" && !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatal("wrong finite budget failure", e)
			}
			if manager != nil && manager.Used() != 0 {
				t.Fatal("admission leaked memory")
			}
			if store.Used() != 0 {
				t.Fatal("admission leaked disk")
			}
			if e := store.Close(); e != nil {
				t.Fatal(e)
			}
			disk, files := n30BudgetDisk(t, dir)
			if disk != 0 || files != 0 {
				t.Fatal("admission retained files")
			}
		})
	}
}
