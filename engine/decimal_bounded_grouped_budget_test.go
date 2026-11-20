package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
	"strings"
	"testing"
)

// Observations concern explicit reservations, not the Go heap or RSS. Physical
// bytes come from stat of actual files, never a codec-size estimate.
type n32BudgetObserver struct {
	context.Context
	t                      *testing.T
	store                  *memory.QuerySpillStore
	dir                    string
	peakDisk, peakPhysical int64
	peakFiles              int
	cancel                 context.CancelFunc
	cancelFiles            int
	cancelled              bool
}

func (o *n32BudgetObserver) observe() {
	if o == nil || o.store == nil {
		return
	}
	used := o.store.Used()
	if used > o.peakDisk {
		o.peakDisk = used
	}
	if o.peakPhysical < o.peakDisk || o.cancelFiles > 0 {
		physical, files := n30BudgetDisk(o.t, o.dir)
		if physical > o.peakPhysical {
			o.peakPhysical = physical
		}
		if files > o.peakFiles {
			o.peakFiles = files
		}
		if o.cancelFiles > 0 && files >= o.cancelFiles && !o.cancelled {
			o.cancelled = true
			o.cancel()
		}
	}
}
func (o *n32BudgetObserver) Err() error { o.observe(); return o.Context.Err() }

type n32BudgetProbe struct {
	memory.Manager
	observer                *n32BudgetObserver
	calls, failAt, cancelAt int
	peakMemory              int64
	firstReservations       [2]int64
}

func (m *n32BudgetProbe) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls <= 2 {
		m.firstReservations[m.calls-1] = n
	}
	m.observer.observe()
	if m.calls == m.failAt {
		return nil, memory.ErrBudgetExceeded
	}
	r, err := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peakMemory {
		m.peakMemory = m.Used()
	}
	if err == nil && m.calls == m.cancelAt {
		m.observer.cancel()
	}
	return r, err
}

// Fixture memory is caller-owned and excluded from the executor's memory claim.
// It computes rows on demand and retains no input batch or output pointer.
type n32BudgetInput struct {
	total, batchRows, sent, closed int
	sameKey                        bool
}

func (s *n32BudgetInput) Next(ctx context.Context, b *DecimalBoundedInputBatch) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.sent >= s.total {
		return false, nil
	}
	n := s.batchRows
	if n > s.total-s.sent {
		n = s.total - s.sent
	}
	b.Rows = n
	b.Precision = 9
	b.Scale = 2
	for i := 0; i < n; i++ {
		k := s.sent + i
		if s.sameKey {
			k = 0
		}
		b.Records[i] = DecimalBoundedInputRow{Key: int64(k), KeyValid: k != 0, Value: decimal128.FromI64(int64(k + 1)), ValueValid: k%7 != 0}
	}
	s.sent += n
	return true, nil
}
func (s *n32BudgetInput) Close() error { s.closed++; return nil }

type n32BudgetMeasurements struct {
	calls                  int
	memory, disk, physical int64
	files                  int
	stats                  DecimalBoundedGroupedStats
}

func n32BudgetRun(t *testing.T, memLimit, diskLimit int64, failAt, cancelAt, cancelFiles int, want error, reused ...*memory.QuerySpillStore) n32BudgetMeasurements {
	t.Helper()
	var dir string
	var store *memory.QuerySpillStore
	var err error
	if len(reused) > 0 {
		store = reused[0]
		dir = store.Directory()
	} else {
		dir = t.TempDir()
		store, err = memory.NewQuerySpillStore(memory.SpillOptions{Directory: dir, MaxBytes: diskLimit})
		if err != nil {
			t.Fatal(err)
		}
	}
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	obs := &n32BudgetObserver{Context: base, t: t, store: store, dir: dir, cancel: cancel, cancelFiles: cancelFiles}
	m := &n32BudgetProbe{Manager: memory.NewBudgetManager(memLimit), observer: obs, failAt: failAt, cancelAt: cancelAt}
	input := &n32BudgetInput{total: 160, batchRows: 37}
	out, err := ExecuteDecimalBoundedGrouped(obs, input, DecimalBoundedGroupedOptions{Manager: m, Store: store, Precision: 9, Scale: 2, TableCapacity: 8})
	var stats DecimalBoundedGroupedStats
	seen := make(map[int64]bool)
	if out != nil {
		stats = out.Stats
	}
	if err == nil {
		for {
			var b *DecimalBoundedOutputBatch
			var ok bool
			b, ok, err = out.Next(obs)
			if err != nil || !ok {
				break
			}
			if b == nil || b.Precision != 38 || b.Scale != 2 || b.Rows < 1 || b.Rows > 256 {
				t.Fatal("output schema/shape")
			}
			for _, r := range b.Records[:b.Rows] {
				k := r.Key
				if !r.KeyValid {
					k = 0
				}
				if k < 0 || k >= 160 || seen[k] {
					t.Fatalf("unexpected/duplicate key %d", k)
				}
				seen[k] = true
				valid := k%7 != 0
				var count uint64
				if valid {
					count = 1
				}
				if r.KeyValid != (k != 0) || r.CountRows != 1 || r.CountValid != count || r.SumValid != valid || (valid && r.Sum != decimal128.FromI64(k+1)) {
					t.Fatalf("row oracle key=%d row=%+v", k, r)
				}
			}
		}
	}
	obs.observe()
	if want == nil {
		t.Logf("actual first reservations: fixed result/input scratch=%d table=%d; whole execution peak=%d", m.firstReservations[0], m.firstReservations[1], m.peakMemory)
	}
	got := n32BudgetMeasurements{m.calls, m.peakMemory, obs.peakDisk, obs.peakPhysical, obs.peakFiles, stats}
	if want != nil {
		if !errors.Is(err, want) {
			t.Fatalf("want %v got %v", want, err)
		}
	} else {
		if err != nil {
			t.Fatal(err)
		}
		if len(seen) != 160 || stats.TableCapacity != 8 || stats.InputRows != 160 || stats.InputBatches != 5 || stats.Flushes != 20 || stats.MergePasses != 2 || stats.Groups != 160 {
			t.Fatalf("predictable flush/result got seen=%d stats=%+v", len(seen), stats)
		}
		if got.disk <= 0 || got.disk != got.physical || got.files <= 20 {
			t.Fatalf("no actual multipass disk peak: %+v", got)
		}
	}
	if input.closed != 1 {
		t.Fatalf("input Close=%d", input.closed)
	}
	if cancelFiles > 0 && !obs.cancelled {
		t.Fatal("requested committed-file cancellation did not occur")
	}
	m.failAt = 0
	m.cancelAt = 0
	obs.cancelFiles = 0
	if out != nil {
		if e := out.Close(context.Background()); e != nil {
			t.Fatal(e)
		}
		if e := out.Close(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	if m.Used() != 0 || store.Used() != 0 {
		t.Fatalf("owner leak memory=%d disk=%d", m.Used(), store.Used())
	}
	if bytes, files := n30BudgetDisk(t, dir); bytes != 0 || files != 0 {
		t.Fatalf("physical leak bytes=%d files=%d", bytes, files)
	}
	if len(reused) == 0 {
		if e := store.Close(); e != nil {
			t.Fatal(e)
		}
	}
	return got
}
func TestN32BoundedBudgetMeasuredQuota(t *testing.T) {
	got := n32BudgetRun(t, 4<<20, 8<<20, 0, 0, 0, nil)
	t.Logf("actual peak memory=%d disk=%d physical=%d files=%d reserveCalls=%d stats=%+v; input fixture excluded; fixed table capacity=8", got.memory, got.disk, got.physical, got.files, got.calls, got.stats)
	t.Run("disk_exact", func(t *testing.T) { n32BudgetRun(t, 4<<20, got.disk, 0, 0, 0, nil) })
	t.Run("disk_minus_one", func(t *testing.T) { n32BudgetRun(t, 4<<20, got.disk-1, 0, 0, 0, memory.ErrSpillQuotaExceeded) })
	// Store metadata includes its real path length. Reuse one empty store so
	// exact and minus-one replay identical paths as well as identical input.
	stable, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer stable.Close()
	measured := n32BudgetRun(t, 4<<20, 8<<20, 0, 0, 0, nil, stable)
	t.Run("memory_exact", func(t *testing.T) { n32BudgetRun(t, measured.memory, 8<<20, 0, 0, 0, nil, stable) })
	t.Run("memory_minus_one", func(t *testing.T) {
		n32BudgetRun(t, measured.memory-1, 8<<20, 0, 0, 0, memory.ErrBudgetExceeded, stable)
	})
}
func TestN32BoundedBudgetReserveFailures(t *testing.T) {
	got := n32BudgetRun(t, 4<<20, 8<<20, 0, 0, 0, nil)
	for at := 1; at <= got.calls; at++ {
		t.Run(fmt.Sprintf("reserve_%03d", at), func(t *testing.T) { n32BudgetRun(t, 4<<20, 8<<20, at, 0, 0, memory.ErrBudgetExceeded) })
	}
}
func TestN32BoundedBudgetCancellation(t *testing.T) {
	got := n32BudgetRun(t, 4<<20, 8<<20, 0, 0, 0, nil)
	for at := 1; at <= got.calls; at++ {
		t.Run(fmt.Sprintf("reserve_%03d", at), func(t *testing.T) { n32BudgetRun(t, 4<<20, 8<<20, 0, at, 0, context.Canceled) })
	}
	for _, files := range []int{1, 21} {
		t.Run(fmt.Sprintf("committed_files_%d", files), func(t *testing.T) { n32BudgetRun(t, 4<<20, 8<<20, 0, 0, files, context.Canceled) })
	}
}
func TestN32BoundedBudgetAdmissionAndHardLimits(t *testing.T) {
	for _, name := range []string{"nil_context", "nil_input", "nil_manager", "unlimited_manager", "nil_store", "unlimited_store", "capacity_negative", "capacity_257", "precision_zero", "scale_exceeds_precision", "run_limit", "row_limit", "batch_limit", "auto_finite"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: dir, MaxBytes: 8 << 20})
			if e != nil {
				t.Fatal(e)
			}
			m := memory.NewBudgetManager(4 << 20)
			opts := DecimalBoundedGroupedOptions{Manager: m, Store: store, Precision: 9, Scale: 2, TableCapacity: 8}
			input := &n32BudgetInput{total: 1, batchRows: 1}
			var src DecimalBoundedInput = input
			ctx := context.Background()
			var msg string
			switch name {
			case "nil_context":
				ctx = nil
			case "nil_input":
				src = nil
			case "nil_manager":
				opts.Manager = nil
			case "unlimited_manager":
				opts.Manager = memory.NewBudgetManager(0)
			case "nil_store":
				opts.Store = nil
			case "unlimited_store":
				opts.Store = &memory.QuerySpillStore{}
			case "capacity_negative":
				opts.TableCapacity = -1
			case "capacity_257":
				opts.TableCapacity = 257
			case "precision_zero":
				opts.Precision = 0
			case "scale_exceeds_precision":
				opts.Scale = 10
			case "run_limit":
				input.total = 65
				opts.TableCapacity = 1
				msg = "run"
			case "row_limit":
				input.total = 65537
				input.batchRows = 256
				input.sameKey = true
				msg = "row"
			case "batch_limit":
				input.total = 4097
				input.sameKey = true
				msg = "batch"
			case "auto_finite":
				opts.TableCapacity = 0
			}
			out, err := ExecuteDecimalBoundedGrouped(ctx, src, opts)
			if name == "auto_finite" {
				if err != nil || out == nil || out.Stats.TableCapacity < 1 || out.Stats.TableCapacity > 256 {
					t.Fatalf("auto finite: out=%v err=%v", out, err)
				}
			} else if err == nil {
				t.Fatal("admission/limit unexpectedly accepted")
			} else if msg != "" && !strings.Contains(strings.ToLower(err.Error()), msg) {
				t.Fatalf("wrong limit error: %v", err)
			}
			if out != nil {
				if e := out.Close(context.Background()); e != nil {
					t.Fatal(e)
				}
			}
			if src != nil && input.closed != 1 {
				t.Fatalf("input close=%d", input.closed)
			}
			if m.Used() != 0 || store.Used() != 0 {
				t.Fatalf("admission leaked %d/%d", m.Used(), store.Used())
			}
			if opts.Manager != nil && opts.Manager.Used() != 0 {
				t.Fatal("alternate manager leak")
			}
			if bytes, files := n30BudgetDisk(t, dir); bytes != 0 || files != 0 {
				t.Fatal("admission file leak")
			}
			if e := store.Close(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
