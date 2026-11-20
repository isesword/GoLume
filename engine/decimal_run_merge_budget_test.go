package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
)

// n30BudgetProbe injects only observed public Manager.Reserve boundaries.
// It makes no claim to instrument arbitrary Go/runtime allocation failures.
type n30BudgetProbe struct {
	memory.Manager
	calls, failAt, cancelAt int
	cancel                  context.CancelFunc
	store                   *memory.QuerySpillStore
	peakDisk, peakMemory    int64
}

func (m *n30BudgetProbe) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.store != nil && m.store.Used() > m.peakDisk {
		m.peakDisk = m.store.Used()
	}
	if m.failAt == m.calls {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peakMemory {
		m.peakMemory = m.Used()
	}
	if e == nil && m.cancelAt == m.calls {
		m.cancel()
	}
	return r, e
}
func n30BudgetDisk(t *testing.T, dir string) (int64, int) {
	t.Helper()
	var bytes int64
	files := 0
	e := filepath.WalkDir(dir, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			info, e := d.Info()
			if e != nil {
				return e
			}
			bytes += info.Size()
			files++
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	return bytes, files
}
func n30BudgetInputs(t *testing.T, store *memory.QuerySpillStore, m memory.Manager) []*decimalPartialFile {
	t.Helper()
	var inputs []*decimalPartialFile
	for run := 0; run < 2; run++ {
		records := make([]decimalPartialRecord, 256)
		for i := range records {
			key := int64(i*2 + run)
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
		inputs = append(inputs, f)
	}
	return inputs
}
func n30BudgetCheckInputs(t *testing.T, inputs []*decimalPartialFile, m memory.Manager) {
	t.Helper()
	for run, f := range inputs {
		d, e := restoreDecimalPartialRun(context.Background(), f, m)
		if e != nil {
			t.Fatal("input ownership lost", e)
		}
		if len(d.records) != 256 {
			t.Fatal("input mutated")
		}
		for i, r := range d.records {
			key := int64(i*2 + run)
			v, valid, e := r.Sum.Finalize()
			if e != nil || !valid || r.Key != key || !r.KeyValid || v != decimal128.FromI64(key+1) || r.CountValid != 1 || r.CountRows != 2 {
				t.Fatal("borrowed input content changed", run, i)
			}
		}
		d.close()
	}
}

type n30BudgetObservation struct {
	calls                              int
	baselineDisk, peakDisk, peakMemory int64
}

func n30BudgetRun(t *testing.T, quota int64, failAt, cancelAt int, quotaFailure bool) n30BudgetObservation {
	t.Helper()
	dir := t.TempDir()
	store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: dir, MaxBytes: quota})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	m := &n30BudgetProbe{Manager: memory.NewBudgetManager(2 << 20), store: store}
	inputs := n30BudgetInputs(t, store, m)
	defer func() {
		for _, f := range inputs {
			f.close(context.Background())
		}
	}()
	baselineDisk, baselineMemory := store.Used(), m.Used()
	disk, files := n30BudgetDisk(t, dir)
	if disk != baselineDisk || files != 2 {
		t.Fatal("input baseline must be real file bytes", disk, baselineDisk, files)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.calls = 0
	m.failAt = failAt
	m.cancelAt = cancelAt
	m.cancel = cancel
	m.peakDisk = baselineDisk
	m.peakMemory = baselineMemory
	out, e := mergeDecimalPartialRuns(ctx, store, decimalPartialMetadata{Precision: 9, Scale: 2}, inputs, m)
	calls := m.calls
	if store.Used() > m.peakDisk {
		m.peakDisk = store.Used()
	}
	obs := n30BudgetObservation{calls, baselineDisk, m.peakDisk, m.peakMemory}
	if failAt > 0 && !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatalf("observed reserve %d not surfaced: %v", failAt, e)
	}
	if cancelAt > 0 && !errors.Is(e, context.Canceled) {
		t.Fatalf("observed cancel %d not surfaced: %v", cancelAt, e)
	}
	if quotaFailure && !errors.Is(e, memory.ErrSpillQuotaExceeded) {
		t.Fatalf("peak-minus-one must report precise disk quota error: %v", e)
	}
	if failAt == 0 && cancelAt == 0 && !quotaFailure && e != nil {
		t.Fatal(e)
	}
	if e == nil {
		disk, files = n30BudgetDisk(t, dir)
		if out == nil || files != 4 || disk != store.Used() || disk <= baselineDisk {
			t.Fatal("missing committed output files", disk, files, store.Used())
		}
		t.Logf("measured inputs=%d peak=%d files=%d reserveCalls=%d memoryPeak=%d", baselineDisk, store.Used(), files, calls, m.peakMemory)
	}
	// Disable fault hooks before ownership verification and cleanup.
	m.failAt = 0
	m.cancelAt = 0
	if out != nil {
		if ce := out.close(context.Background()); ce != nil {
			t.Fatal("output cleanup", ce)
		}
		if ce := out.close(context.Background()); ce != nil {
			t.Fatal("repeat output close", ce)
		}
	}
	if store.Used() != baselineDisk || m.Used() != baselineMemory {
		t.Fatal("merge retained resource beyond input baseline", store.Used(), baselineDisk, m.Used(), baselineMemory)
	}
	n30BudgetCheckInputs(t, inputs, m)
	for _, f := range inputs {
		if e := f.close(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	if e := store.Close(); e != nil {
		t.Fatal(e)
	}
	disk, files = n30BudgetDisk(t, dir)
	if m.Used() != 0 || store.Used() != 0 || disk != 0 || files != 0 {
		t.Fatal("final ownership leak", m.Used(), store.Used(), disk, files)
	}
	return obs
}
func TestN30DecimalMergeBudgetMeasuredQuota(t *testing.T) {
	measured := n30BudgetRun(t, 8<<20, 0, 0, false)
	t.Run("exact-observed-peak", func(t *testing.T) {
		again := n30BudgetRun(t, measured.peakDisk, 0, 0, false)
		if again.peakDisk != measured.peakDisk || again.baselineDisk != measured.baselineDisk {
			t.Fatal("non-repeatable physical byte measurement", measured, again)
		}
	})
	t.Run("observed-peak-minus-one", func(t *testing.T) { n30BudgetRun(t, measured.peakDisk-1, 0, 0, true) })
}
func TestN30DecimalMergeBudgetObservedReserveFailures(t *testing.T) {
	measured := n30BudgetRun(t, 8<<20, 0, 0, false)
	if measured.calls == 0 {
		t.Fatal("no reservations observed")
	}
	for at := 1; at <= measured.calls; at++ {
		t.Run(fmt.Sprintf("reserve-%02d", at), func(t *testing.T) { n30BudgetRun(t, 8<<20, at, 0, false) })
	}
}
func TestN30DecimalMergeBudgetObservedCancellation(t *testing.T) {
	measured := n30BudgetRun(t, 8<<20, 0, 0, false)
	for at := 1; at <= measured.calls; at++ {
		t.Run(fmt.Sprintf("cancel-after-reserve-%02d", at), func(t *testing.T) { n30BudgetRun(t, 8<<20, 0, at, false) })
	}
}
