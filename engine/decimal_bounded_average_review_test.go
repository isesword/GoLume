package engine

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

func n33ReviewAverageOptions(o DecimalBoundedGroupedOptions, p int32) DecimalBoundedGroupedOptions {
	o.Mode = DecimalBoundedAverageCounts
	o.AveragePrecision = p
	o.AverageScale = 0
	o.AverageRounding = aggregate.DecimalAverageHalfEven
	return o
}
func TestN33ReviewAverageOnlyWideAndCopiedOptions(t *testing.T) {
	ctx := context.Background()
	opts, mgr, store := n32ReviewOptions(t, 38)
	opts = n33ReviewAverageOptions(opts, 38)
	coefficient, _ := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	v := decimal128.FromBigInt(coefficient)
	row := n32ReviewRow(1, 0)
	row.Value = v
	in := &n32ReviewInput{precision: 38, rows: []DecimalBoundedInputRow{row, row}}
	result, e := ExecuteDecimalBoundedGrouped(ctx, in, opts)
	if e != nil {
		t.Fatal("AVG-only falsely narrowed wide sum", e)
	}
	defer result.Close(ctx)
	opts.Mode = DecimalBoundedSumCounts
	opts.AveragePrecision = 1
	opts.AverageRounding = aggregate.DecimalAverageTowardZero
	b, ok, e := result.Next(ctx)
	if e != nil || !ok || b.Rows != 1 {
		t.Fatalf("output %v %v %v", b, ok, e)
	}
	if b.Mode != DecimalBoundedAverageCounts || b.AveragePrecision != 38 || b.AverageScale != 0 || !b.Records[0].AverageValid || b.Records[0].Average != v || b.Records[0].SumValid || b.Records[0].Sum != (decimal128.Num{}) || b.Records[0].CountValid != 2 {
		t.Fatalf("AVG-only output mismatch %+v", b.Records[0])
	}
	if in.closes != 1 {
		t.Fatal("input not closed")
	}
	saved := b.Records[0]
	if e = result.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if *b != (DecimalBoundedOutputBatch{}) {
		t.Fatal("Close kept borrowed AVG batch")
	}
	if saved.Average != v {
		t.Fatal("copied value changed")
	}
	if mgr.Used() != 0 || store.Used() != 0 {
		t.Fatal("owner leak")
	}
}
func TestN33ReviewLateAverageOverflow(t *testing.T) {
	ctx := context.Background()
	opts, mgr, store := n32ReviewOptions(t, 3)
	opts = n33ReviewAverageOptions(opts, 1)
	in := &n32ReviewInput{precision: 3}
	for i := 0; i < 256; i++ {
		in.rows = append(in.rows, n32ReviewRow(int64(i), 1))
	}
	in.rows = append(in.rows, n32ReviewRow(300, 10))
	result, e := ExecuteDecimalBoundedGrouped(ctx, in, opts)
	if result != nil {
		_ = result.Close(ctx)
	}
	if !errors.Is(e, aggregate.ErrDecimalAverageOverflow) || result != nil || in.closes != 1 {
		t.Fatalf("late AVG overflow publication result=%v err=%v close=%d", result, e, in.closes)
	}
	if mgr.Used() != 0 || store.Used() != 0 {
		t.Fatal("late overflow leak")
	}
}
func TestN33ReviewAverageReleaseCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts, mgr, store := n32ReviewOptions(t, 3)
	opts = n33ReviewAverageOptions(opts, 3)
	hooked := &n32ReviewReleaseCancelManager{Manager: mgr, cancel: cancel}
	opts.Manager = hooked
	in := &n32ReviewInput{precision: 3, rows: []DecimalBoundedInputRow{n32ReviewRow(1, 7)}}
	result, e := ExecuteDecimalBoundedGrouped(ctx, in, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer result.Close(context.Background())
	hooked.active = true
	b, ok, e := result.Next(ctx)
	if hooked.releases < 2 || ctx.Err() == nil {
		t.Fatal("cancel hook did not fire")
	}
	if !errors.Is(e, context.Canceled) || ok || b != nil {
		t.Fatalf("AVG published after release cancellation: b=%v ok=%v err=%v", b != nil, ok, e)
	}
	if mgr.Used() != 0 || store.Used() != 0 {
		t.Fatal("cancel leak")
	}
}
func TestN33ReviewAverageInputClose(t *testing.T) {
	for _, cancelOnClose := range []bool{false, true} {
		name := "error"
		if cancelOnClose {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts, mgr, store := n32ReviewOptions(t, 3)
			opts = n33ReviewAverageOptions(opts, 3)
			sentinel := errors.New("AVG input Close error")
			in := &n32ReviewInput{precision: 3, rows: []DecimalBoundedInputRow{n32ReviewRow(1, 7)}, closeErr: sentinel}
			want := error(sentinel)
			if cancelOnClose {
				in.closeErr = nil
				in.cancel = cancel
				want = context.Canceled
			}
			result, e := ExecuteDecimalBoundedGrouped(ctx, in, opts)
			if result != nil {
				_ = result.Close(context.Background())
			}
			if !errors.Is(e, want) || result != nil || in.closes != 1 {
				t.Fatalf("close error lost %v result=%v calls=%d", e, result, in.closes)
			}
			if mgr.Used() != 0 || store.Used() != 0 {
				t.Fatal("close leak")
			}
		})
	}
}
func TestN33ReviewAverageCleanupRetry(t *testing.T) {
	ctx := context.Background()
	opts, mgr, store := n32ReviewOptions(t, 3)
	opts = n33ReviewAverageOptions(opts, 3)
	in := &n32ReviewInput{precision: 3, rows: []DecimalBoundedInputRow{n32ReviewRow(1, 7)}}
	result, e := ExecuteDecimalBoundedGrouped(ctx, in, opts)
	if e != nil {
		t.Fatal(e)
	}
	b, ok, e := result.Next(ctx)
	if e != nil || !ok {
		t.Fatal(e)
	}
	entries, e := os.ReadDir(store.Directory())
	if e != nil || len(entries) != 1 {
		t.Fatalf("files=%d err=%v", len(entries), e)
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
	if e = result.Close(ctx); e == nil {
		t.Fatal("blocked unlink succeeded")
	}
	if *b != (DecimalBoundedOutputBatch{}) {
		t.Fatal("failed Close left borrowed batch")
	}
	if mgr.Used() == 0 || store.Used() == 0 {
		t.Fatal("failed deletion lost charge")
	}
	if e = os.RemoveAll(path); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(backup, path); e != nil {
		t.Fatal(e)
	}
	if e = result.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if e = result.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if mgr.Used() != 0 || store.Used() != 0 {
		t.Fatal("retry leak")
	}
}
