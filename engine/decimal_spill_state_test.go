package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/memory"
)

func n29PartialRecord(t *testing.T, key int64, valid bool, values ...memory.Decimal128) decimalPartialRecord {
	t.Helper()
	r := decimalPartialRecord{Key: key, KeyValid: valid}
	for _, v := range values {
		if e := r.Sum.Add(v); e != nil {
			t.Fatal(e)
		}
	}
	r.CountValid = r.Sum.Count()
	r.CountRows = r.CountValid + 1
	return r
}
func TestN29DecimalPartialRoundTripAndMerge(t *testing.T) {
	ctx := context.Background()
	m := decimalPartialMetadata{38, 9}
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	a := n29PartialRecord(t, 12, true, max, max, max)
	b := n29PartialRecord(t, 12, true, max.Negate(), max.Negate(), max.Negate())
	if _, _, e := a.Sum.Finalize(); !errors.Is(e, aggregate.ErrDecimalSumOverflow) {
		t.Fatal("not a wide partial", e)
	}
	manager := memory.NewBudgetManager(1 << 20)
	encoded, e := encodeDecimalPartialRun(ctx, m, []decimalPartialRecord{a, b, n29PartialRecord(t, 999, false)}, manager)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := decodeDecimalPartialRun(ctx, m, encoded.payload, manager)
	if e != nil {
		t.Fatal(e)
	}
	if decoded.records[2].Key != 0 || decoded.records[2].KeyValid {
		t.Fatal("NULL not canonical")
	}
	for i := range encoded.payload {
		encoded.payload[i] = 255
	}
	encoded.close()
	if e := mergeDecimalPartialRecord(ctx, &decoded.records[0], m, m, decoded.records[1]); e != nil {
		t.Fatal(e)
	}
	value, valid, e := decoded.records[0].Sum.Finalize()
	if e != nil || !valid || value != decimal128.FromI64(0) || decoded.records[0].CountRows != 8 {
		t.Fatal(value, valid, e)
	}
	decoded.close()
	decoded.close()
	if manager.Used() != 0 {
		t.Fatal("leak", manager.Used())
	}
}
func TestN29DecimalPartialCorruption(t *testing.T) {
	m := decimalPartialMetadata{3, 1}
	ctx := context.Background()
	manager := memory.NewBudgetManager(1 << 20)
	r := n29PartialRecord(t, 0, false, decimal128.FromI64(7))
	encoded, e := encodeDecimalPartialRun(ctx, m, []decimalPartialRecord{r}, manager)
	if e != nil {
		t.Fatal(e)
	}
	defer encoded.close()
	original := append([]byte(nil), encoded.payload...)
	cases := map[string]func([]byte) []byte{
		"magic":             func(b []byte) []byte { b[0] ^= 1; return b },
		"version":           func(b []byte) []byte { b[8] = 2; return b },
		"flags":             func(b []byte) []byte { b[10] = 1; return b },
		"kind":              func(b []byte) []byte { b[12] = 2; return b },
		"precision":         func(b []byte) []byte { b[16] = 4; return b },
		"scale":             func(b []byte) []byte { b[20] = 2; return b },
		"count":             func(b []byte) []byte { binary.LittleEndian.PutUint64(b[24:32], math.MaxUint64); return b },
		"trailing":          func(b []byte) []byte { return append(b, 0) },
		"short":             func(b []byte) []byte { return b[:len(b)-1] },
		"null-key":          func(b []byte) []byte { b[32] = 1; return b },
		"validity":          func(b []byte) []byte { b[40] = 2; return b },
		"reserved":          func(b []byte) []byte { b[41] = 1; return b },
		"validcount":        func(b []byte) []byte { b[88] = 0; return b },
		"rowcount":          func(b []byte) []byte { binary.LittleEndian.PutUint64(b[96:104], 0); return b },
		"coefficient-bound": func(b []byte) []byte { binary.LittleEndian.PutUint64(b[48:56], 1000); return b },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			before := manager.Used()
			out, e := decodeDecimalPartialRun(ctx, m, mutate(append([]byte(nil), original...)), manager)
			if out != nil {
				out.close()
				t.Fatal("partial decoder output")
			}
			if !errors.Is(e, memory.ErrSpillCorrupt) {
				t.Fatal("not corruption", e)
			}
			if manager.Used() != before {
				t.Fatal("failure leak")
			}
		})
	}
}
func TestN29DecimalPartialMergeAtomicFailures(t *testing.T) {
	ctx := context.Background()
	m := decimalPartialMetadata{3, 1}
	base := n29PartialRecord(t, 1, true, decimal128.FromI64(3))
	for _, mode := range []string{"key", "metadata", "rows", "count", "precision", "cancel"} {
		dst, src := base, base
		sm := m
		runCtx := ctx
		switch mode {
		case "key":
			src.Key = 2
		case "metadata":
			sm.Scale = 2
		case "rows":
			dst.CountRows = math.MaxUint64
		case "count":
			src.CountValid++
		case "precision":
			src = n29PartialRecord(t, 1, true, decimal128.FromI64(1000))
		case "cancel":
			c, cancel := context.WithCancel(ctx)
			cancel()
			runCtx = c
		}
		before := dst
		if e := mergeDecimalPartialRecord(runCtx, &dst, m, sm, src); e == nil {
			t.Fatal("invalid merge", mode)
		}
		if !reflect.DeepEqual(dst, before) {
			t.Fatal("merge mutated on error", mode)
		}
	}
}

type n29PartialManager struct {
	*memory.BudgetManager
	calls, fail int
}

func (m *n29PartialManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	return m.BudgetManager.Reserve(ctx, n)
}
func TestN29DecimalPartialFiniteStoreBudgetFailures(t *testing.T) {
	ctx := context.Background()
	meta := decimalPartialMetadata{3, 1}
	records := []decimalPartialRecord{n29PartialRecord(t, 3, true, decimal128.FromI64(9))}
	run := func(t *testing.T, fail int) int {
		dir := t.TempDir()
		store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: dir, MaxBytes: 1 << 20})
		if e != nil {
			t.Fatal(e)
		}
		manager := &n29PartialManager{BudgetManager: memory.NewBudgetManager(1 << 20), fail: fail}
		file, e := spillDecimalPartialRun(ctx, store, meta, records, manager)
		if e == nil {
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) == 0 {
				t.Fatal("no actual file", err)
			}
			restored, err := restoreDecimalPartialRun(ctx, file, manager)
			e = err
			if restored != nil {
				if !reflect.DeepEqual(restored.records, records) {
					t.Fatal("typed roundtrip")
				}
				restored.close()
			}
		}
		if fail > 0 && !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatalf("failure %d not surfaced: %v", fail, e)
		}
		if file != nil {
			if err := file.close(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if e := store.Close(); e != nil {
			t.Fatal(e)
		}
		if manager.Used() != 0 || store.Used() != 0 {
			t.Fatal("retained budgets", manager.Used(), store.Used())
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			t.Fatal("retained files", len(entries), err)
		}
		return manager.calls
	}
	calls := run(t, 0)
	for fail := 1; fail <= calls; fail++ {
		t.Run(string(rune('A'+fail)), func(t *testing.T) { run(t, fail) })
	}
}
