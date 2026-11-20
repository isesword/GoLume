package engine

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/memory"
)

func reviewPartial(t *testing.T) (decimalPartialMetadata, []decimalPartialRecord) {
	t.Helper()
	var s aggregate.DecimalSumState
	if err := s.Add(decimal128.FromI64(7)); err != nil {
		t.Fatal(err)
	}
	return decimalPartialMetadata{Precision: 3, Scale: 0}, []decimalPartialRecord{{Key: 3, KeyValid: true, Sum: s, CountValid: 1, CountRows: 2}}
}
func reviewStore(t *testing.T) *memory.QuerySpillStore {
	t.Helper()
	s, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := s.Close(); e != nil {
			t.Error(e)
		}
	})
	return s
}
func TestN29DecimalPartialReviewFinite(t *testing.T) {
	ctx := context.Background()
	m, r := reviewPartial(t)
	finite := memory.NewBudgetManager(1 << 20)
	unlimited := memory.NewBudgetManager(0)
	enc, e := encodeDecimalPartialRun(ctx, m, r, finite)
	if e != nil {
		t.Fatal(e)
	}
	defer enc.close()
	t.Run("encode", func(t *testing.T) {
		o, e := encodeDecimalPartialRun(ctx, m, r, unlimited)
		if o != nil {
			o.close()
		}
		if e == nil {
			t.Fatal("unlimited encode accepted")
		}
	})
	t.Run("decode", func(t *testing.T) {
		o, e := decodeDecimalPartialRun(ctx, m, enc.payload, unlimited)
		if o != nil {
			o.close()
		}
		if e == nil {
			t.Fatal("unlimited decode accepted")
		}
	})
	store := reviewStore(t)
	t.Run("spill", func(t *testing.T) {
		o, e := spillDecimalPartialRun(ctx, store, m, r, unlimited)
		if o != nil {
			_ = o.close(ctx)
		}
		if e == nil {
			t.Fatal("unlimited spill accepted")
		}
	})
	f, e := spillDecimalPartialRun(ctx, store, m, r, finite)
	if e != nil {
		t.Fatal(e)
	}
	defer f.close(ctx)
	t.Run("restore", func(t *testing.T) {
		o, e := restoreDecimalPartialRun(ctx, f, unlimited)
		if o != nil {
			o.close()
		}
		if e == nil {
			t.Fatal("unlimited restore accepted")
		}
	})
}
func TestN29DecimalPartialReviewDeleteRetry(t *testing.T) {
	ctx := context.Background()
	m, r := reviewPartial(t)
	mgr := memory.NewBudgetManager(1 << 20)
	store := reviewStore(t)
	f, e := spillDecimalPartialRun(ctx, store, m, r, mgr)
	if e != nil {
		t.Fatal(e)
	}
	entries, e := os.ReadDir(store.Directory())
	if e != nil || len(entries) != 1 {
		t.Fatalf("files %v %v", entries, e)
	}
	path := filepath.Join(store.Directory(), entries[0].Name())
	backup := filepath.Join(t.TempDir(), "saved")
	if e = os.Rename(path, backup); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(path, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(path, "block"), []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	before := mgr.Used()
	disk := store.Used()
	handle := f.handle
	if e = f.close(ctx); e == nil {
		t.Fatal("nonempty-dir deletion unexpectedly succeeded")
	}
	if f.handle != handle || f.lease == nil || mgr.Used() != before || store.Used() != disk {
		t.Fatal("failed Delete dropped ownership")
	}
	if e = os.RemoveAll(path); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(backup, path); e != nil {
		t.Fatal(e)
	}
	if e = f.close(nil); e == nil {
		t.Fatal("nil close context accepted for live handle")
	}
	if e = f.close(ctx); e != nil {
		t.Fatal(e)
	}
	if e = f.close(ctx); e != nil {
		t.Fatal(e)
	}
	if mgr.Used() != 0 || store.Used() != 0 {
		t.Fatalf("leak %d %d", mgr.Used(), store.Used())
	}
}

type reviewCommittedContext struct {
	context.Context
	dir      string
	observed bool
}

func (c *reviewCommittedContext) Err() error {
	entries, _ := os.ReadDir(c.dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".gls") {
			c.observed = true
			return context.Canceled
		}
	}
	if c.observed {
		return context.Canceled
	}
	return nil
}
func TestN29DecimalPartialReviewPostCommitCancel(t *testing.T) {
	m, r := reviewPartial(t)
	mgr := memory.NewBudgetManager(1 << 20)
	store := reviewStore(t)
	ctx := &reviewCommittedContext{Context: context.Background(), dir: store.Directory()}
	f, e := spillDecimalPartialRun(ctx, store, m, r, mgr)
	if f != nil {
		defer f.close(context.Background())
	}
	if !ctx.observed || !errors.Is(e, context.Canceled) || f != nil {
		t.Fatalf("postcommit cancellation observed=%v owner=%v err=%v", ctx.observed, f, e)
	}
	entries, _ := os.ReadDir(store.Directory())
	if len(entries) != 0 || mgr.Used() != 0 || store.Used() != 0 {
		t.Fatalf("cleanup files=%d mem=%d disk=%d", len(entries), mgr.Used(), store.Used())
	}
}

type reviewCancelManager struct {
	memory.Manager
	cancel context.CancelFunc
	calls  int
}

func (m *reviewCancelManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	r, e := m.Manager.Reserve(ctx, n)
	m.calls++
	if e == nil && m.calls == 2 {
		m.cancel()
	}
	return r, e
}
func TestN29DecimalPartialReviewRestoreCancelAndOwnership(t *testing.T) {
	ctx := context.Background()
	m, r := reviewPartial(t)
	mgr := memory.NewBudgetManager(1 << 20)
	store := reviewStore(t)
	f, e := spillDecimalPartialRun(ctx, store, m, r, mgr)
	if e != nil {
		t.Fatal(e)
	}
	defer f.close(ctx)
	before := mgr.Used()
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	cm := &reviewCancelManager{Manager: mgr, cancel: cancel}
	out, e := restoreDecimalPartialRun(canceled, f, cm)
	if out != nil {
		out.close()
	}
	if !errors.Is(e, context.Canceled) || cm.calls != 2 || mgr.Used() != before || f.handle == nil {
		t.Fatalf("restore cancel calls=%d err=%v mem=%d/%d", cm.calls, e, mgr.Used(), before)
	}
	out, e = restoreDecimalPartialRun(ctx, f, mgr)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.close(ctx); e != nil {
		t.Fatal(e)
	}
	if e = store.Close(); e != nil {
		t.Fatal(e)
	}
	v, valid, e := out.records[0].Sum.Finalize()
	if e != nil || !valid || v != decimal128.FromI64(7) {
		t.Fatalf("owned value %v %v %v", v, valid, e)
	}
	out.close()
	out.close()
	if mgr.Used() != 0 {
		t.Fatalf("leak %d", mgr.Used())
	}
}

func TestN29DecimalPartialReviewMaxRows(t *testing.T) {
	ctx := context.Background()
	m, r := reviewPartial(t)
	r[0].CountRows = math.MaxUint64
	mgr := memory.NewBudgetManager(1 << 20)
	enc, e := encodeDecimalPartialRun(ctx, m, r, mgr)
	if e != nil {
		t.Fatal(e)
	}
	dec, e := decodeDecimalPartialRun(ctx, m, enc.payload, mgr)
	if e != nil {
		t.Fatal(e)
	}
	enc.close()
	if dec.records[0].CountRows != math.MaxUint64 || dec.records[0].CountValid != 1 {
		t.Fatal("row count truncated")
	}
	before := dec.records[0]
	zero := decimalPartialRecord{Key: 3, KeyValid: true}
	if e = mergeDecimalPartialRecord(ctx, &dec.records[0], m, m, zero); e != nil || dec.records[0] != before {
		t.Fatalf("max rows no-op merge %v", e)
	}
	dec.close()
	if mgr.Used() != 0 {
		t.Fatal("owner leak")
	}
}

func TestN29DecimalPartialReviewNullMerge(t *testing.T) {
	ctx := context.Background()
	m, r := reviewPartial(t)
	a, b := r[0], r[0]
	a.KeyValid = false
	b.KeyValid = false
	a.Key = 999
	b.Key = -456
	if e := mergeDecimalPartialRecord(ctx, &a, m, m, b); e != nil {
		t.Fatal(e)
	}
	v, ok, e := a.Sum.Finalize()
	if e != nil || !ok || v != decimal128.FromI64(14) || a.Key != 0 || a.CountValid != 2 || a.CountRows != 4 {
		t.Fatalf("NULL merge %+v %v", a, e)
	}
	before := a
	b.KeyValid = true
	b.Key = 0
	if e := mergeDecimalPartialRecord(ctx, &a, m, m, b); e == nil || a != before {
		t.Fatal("NULL/key0 mismatch mutated state")
	}
	allNull := decimalPartialRecord{KeyValid: false, CountRows: math.MaxUint64}
	zero := decimalPartialRecord{}
	if e := mergeDecimalPartialRecord(ctx, &allNull, m, m, zero); e != nil {
		t.Fatal(e)
	}
	if _, ok, e := allNull.Sum.Finalize(); ok || e != nil {
		t.Fatal("allNULL sum must remain NULL")
	}
	before = allNull
	zero.CountRows = 1
	if e := mergeDecimalPartialRecord(ctx, &allNull, m, m, zero); e == nil || allNull != before {
		t.Fatal("MaxUint64+1 did not reject atomically")
	}
}
