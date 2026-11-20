package engine

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

type n32ReviewInput struct {
	rows                  []DecimalBoundedInputRow
	precision             int32
	offset, calls, closes int
	previous              *DecimalBoundedInputBatch
	closeErr, nextErr     error
	cancel                context.CancelFunc
}

func (s *n32ReviewInput) Next(ctx context.Context, b *DecimalBoundedInputBatch) (bool, error) {
	s.calls++
	if s.previous != nil {
		for i := range s.previous.Records {
			s.previous.Records[i].Value = decimal128.FromI64(-99)
		}
	}
	if s.nextErr != nil && s.offset > 0 {
		return false, s.nextErr
	}
	if s.offset == len(s.rows) {
		return false, nil
	}
	*b = DecimalBoundedInputBatch{Rows: 1, Precision: s.precision, Scale: 0}
	b.Records[0] = s.rows[s.offset]
	s.offset++
	s.previous = b
	return true, nil
}
func (s *n32ReviewInput) Close() error {
	s.closes++
	if s.previous != nil {
		for i := range s.previous.Records {
			s.previous.Records[i].Value = decimal128.FromI64(-88)
		}
	}
	if s.cancel != nil {
		s.cancel()
	}
	return s.closeErr
}
func n32ReviewOptions(t *testing.T, p int32) (DecimalBoundedGroupedOptions, *memory.BudgetManager, *memory.QuerySpillStore) {
	t.Helper()
	mgr := memory.NewBudgetManager(8 << 20)
	store := reviewStore(t)
	return DecimalBoundedGroupedOptions{Manager: mgr, Store: store, Precision: p, Scale: 0, TableCapacity: 256}, mgr, store
}
func n32ReviewRow(k, v int64) DecimalBoundedInputRow {
	return DecimalBoundedInputRow{Key: k, KeyValid: true, Value: decimal128.FromI64(v), ValueValid: true}
}
func TestN32ReviewInputErrorsAndCancel(t *testing.T) {
	sentinel := errors.New("review input failure")
	for _, name := range []string{"next-error", "close-error", "close-cancel", "invalid-options"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts, mgr, store := n32ReviewOptions(t, 3)
			in := &n32ReviewInput{precision: 3, rows: []DecimalBoundedInputRow{n32ReviewRow(1, 7)}}
			want := sentinel
			switch name {
			case "next-error":
				in.nextErr = sentinel
			case "close-error":
				in.closeErr = sentinel
			case "close-cancel":
				in.cancel = cancel
				want = context.Canceled
			case "invalid-options":
				opts.TableCapacity = 257
				want = nil
			}
			result, e := ExecuteDecimalBoundedGrouped(ctx, in, opts)
			if result != nil {
				_ = result.Close(context.Background())
			}
			if e == nil || (want != nil && !errors.Is(e, want)) || result != nil || in.closes != 1 {
				t.Fatalf("result=%v err=%v closes=%d", result, e, in.closes)
			}
			if name == "invalid-options" && in.calls != 0 {
				t.Fatal("invalid options read input")
			}
			if mgr.Used() != 0 || store.Used() != 0 {
				t.Fatalf("leak %d %d", mgr.Used(), store.Used())
			}
		})
	}
}
func TestN32ReviewBorrowedAndInputPoison(t *testing.T) {
	ctx := context.Background()
	opts, mgr, store := n32ReviewOptions(t, 3)
	opts.TableCapacity = 1
	in := &n32ReviewInput{precision: 3, rows: []DecimalBoundedInputRow{n32ReviewRow(1, 7), n32ReviewRow(2, 9), n32ReviewRow(1, 5)}}
	result, e := ExecuteDecimalBoundedGrouped(ctx, in, opts)
	if e != nil {
		t.Fatal(e)
	}
	if in.closes != 1 {
		t.Fatal("input not closed before publication")
	}
	b, ok, e := result.Next(ctx)
	if e != nil || !ok || b.Rows != 2 {
		t.Fatalf("output %v %v %v", b, ok, e)
	}
	if b.Records[0].Sum != decimal128.FromI64(12) || b.Records[1].Sum != decimal128.FromI64(9) {
		t.Fatal("input alias poisoning affected sums")
	}
	saved := b.Records[0]
	_, ok, e = result.Next(ctx)
	if e != nil || ok {
		t.Fatalf("EOF %v %v", ok, e)
	}
	if *b != (DecimalBoundedOutputBatch{}) {
		t.Fatal("EOF kept borrowed batch live")
	}
	if e = result.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if e = result.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if saved.Sum != decimal128.FromI64(12) {
		t.Fatal("copied scalar changed")
	}
	if mgr.Used() != 0 || store.Used() != 0 {
		t.Fatalf("leak %d %d", mgr.Used(), store.Used())
	}
}
func TestN32ReviewLatePrecisionPublication(t *testing.T) {
	ctx := context.Background()
	opts, mgr, store := n32ReviewOptions(t, 38)
	in := &n32ReviewInput{precision: 38}
	for i := 0; i < 256; i++ {
		in.rows = append(in.rows, n32ReviewRow(int64(i), 1))
	}
	coefficient, _ := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	v := decimal128.FromBigInt(coefficient)
	for i := 0; i < 2; i++ {
		row := n32ReviewRow(300, 0)
		row.Value = v
		in.rows = append(in.rows, row)
	}
	result, e := ExecuteDecimalBoundedGrouped(ctx, in, opts)
	if result != nil {
		_ = result.Close(ctx)
	}
	if !errors.Is(e, aggregate.ErrDecimalSumOverflow) || result != nil || in.closes != 1 {
		t.Fatalf("late overflow published result=%v err=%v close=%d", result, e, in.closes)
	}
	if mgr.Used() != 0 || store.Used() != 0 {
		t.Fatalf("overflow leak %d %d", mgr.Used(), store.Used())
	}
}
func TestN32ReviewStoreCloseAndRetry(t *testing.T) {
	for _, mode := range []string{"external-store-close", "unlink-retry"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			opts, mgr, store := n32ReviewOptions(t, 3)
			in := &n32ReviewInput{precision: 3, rows: []DecimalBoundedInputRow{n32ReviewRow(1, 7)}}
			result, e := ExecuteDecimalBoundedGrouped(ctx, in, opts)
			if e != nil {
				t.Fatal(e)
			}
			if mode == "external-store-close" {
				if e = store.Close(); e != nil {
					t.Fatal(e)
				}
				b, ok, e := result.Next(ctx)
				if e == nil || ok || b != nil {
					t.Fatalf("closed store published b=%v ok=%v err=%v", b, ok, e)
				}
			} else {
				entries, e := os.ReadDir(store.Directory())
				if e != nil || len(entries) == 0 {
					t.Fatalf("no output files %v", e)
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
					t.Fatal("blocked unlink accepted")
				}
				if mgr.Used() == 0 || store.Used() == 0 {
					t.Fatal("failed delete dropped accounting")
				}
				if e = os.RemoveAll(path); e != nil {
					t.Fatal(e)
				}
				if e = os.Rename(backup, path); e != nil {
					t.Fatal(e)
				}
			}
			if e = result.Close(ctx); e != nil {
				t.Fatal(e)
			}
			if e = result.Close(ctx); e != nil {
				t.Fatal(e)
			}
			if mgr.Used() != 0 || store.Used() != 0 {
				t.Fatalf("owner leak %d %d", mgr.Used(), store.Used())
			}
		})
	}
}

type n32ReviewReleaseCancelManager struct {
	memory.Manager
	active   bool
	releases int
	cancel   context.CancelFunc
}
type n32ReviewReleaseLease struct {
	memory.Reservation
	owner *n32ReviewReleaseCancelManager
	done  bool
}

func (m *n32ReviewReleaseCancelManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	r, e := m.Manager.Reserve(ctx, n)
	if e != nil {
		return r, e
	}
	return &n32ReviewReleaseLease{Reservation: r, owner: m}, nil
}
func (r *n32ReviewReleaseLease) Release() {
	if r.done {
		return
	}
	r.done = true
	r.Reservation.Release()
	if r.owner.active {
		r.owner.releases++
		if r.owner.releases == 2 {
			r.owner.cancel()
		}
	}
}
func TestN32ReviewFinalReleaseCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts, mgr, store := n32ReviewOptions(t, 3)
	hooked := &n32ReviewReleaseCancelManager{Manager: mgr, cancel: cancel}
	opts.Manager = hooked
	input := &n32ReviewInput{precision: 3, rows: []DecimalBoundedInputRow{n32ReviewRow(1, 7)}}
	result, e := ExecuteDecimalBoundedGrouped(ctx, input, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer result.Close(context.Background())
	hooked.active = true
	batch, ok, e := result.Next(ctx)
	if hooked.releases < 2 || ctx.Err() == nil {
		t.Fatal("did not cancel at decoded owner release")
	}
	if !errors.Is(e, context.Canceled) || ok || batch != nil {
		t.Fatalf("success published after final release cancellation: batch=%v ok=%v err=%v", batch != nil, ok, e)
	}
	if mgr.Used() != 0 || store.Used() != 0 {
		t.Fatalf("canceled output leak %d %d", mgr.Used(), store.Used())
	}
}
