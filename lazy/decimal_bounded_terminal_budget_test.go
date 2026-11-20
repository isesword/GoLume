package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/engine"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/memory"
)

// The on-disk fixture is written independently of the terminal. Caller-owned
// writer allocations are not included in the query-memory/RSS claim.
func n34BudgetParquet(t *testing.T) string {
	t.Helper()
	f, e := memory.Decimal128Field("value", true, 9, 2)
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Rows = 512
	k := &memory.Int64Vector{Values: make([]int64, 512)}
	v := &memory.Decimal128Vector{Precision: 9, Scale: 2, Values: make([]memory.Decimal128, 512), Validity: make(memory.Bitmap, memory.WordsFor(512))}
	for i := 0; i < 512; i++ {
		k.Values[i] = int64(i % 64)
		v.Values[i] = decimal128.FromI64(int64(i%64 + 1))
		if i%64%7 != 0 {
			memory.Set(v.Validity, i)
		}
	}
	b.Int64s["key"] = k
	b.Decimals["value"] = v
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, f)
	r, e := memory.ArrowRecord(am.DefaultAllocator, schema, b)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Release()
	path := filepath.Join(t.TempDir(), "input.parquet")
	fp, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w, e := pqarrow.NewFileWriter(r.Schema(), fp, pq.NewWriterProperties(pq.WithDictionaryDefault(false)), pqarrow.DefaultWriterProps())
	if e != nil {
		fp.Close()
		t.Fatal(e)
	}
	if e = w.Write(r); e != nil {
		w.Close()
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	return path
}
func n34BudgetDisk(t *testing.T, dir string) (int64, int) {
	t.Helper()
	var bytes int64
	var files int
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

// Err observes the actual shared QueryBudget and filesystem, without replacing
// its concrete manager or inserting a separate reservation authority.
type n34BudgetContext struct {
	mu                      sync.Mutex
	stopped                 bool
	maxSource, coupledOther int64
	checks, cancelCheck     int
	context.Context
	t                                    *testing.T
	q                                    *memory.QueryBudget
	store                                *memory.QuerySpillStore
	peakDisk, peakPhysical, peakObserved int64
	peakFiles                            int
	cancel                               context.CancelFunc
	cancelFiles                          int
	cancelled                            bool
}

func (c *n34BudgetContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return c.Context.Err()
	}
	c.checks++
	if c.cancelCheck > 0 && c.checks == c.cancelCheck {
		c.cancelled = true
		c.cancel()
	}
	if c.q != nil {
		snap := c.q.Snapshot()
		scan := snap.Operators["SourceScan"]
		if scan > c.maxSource {
			c.maxSource = scan
		}
		if scan >= 512*24 && snap.UsedBytes-scan > c.coupledOther {
			c.coupledOther = snap.UsedBytes - scan
		}
	}
	if c.q != nil && c.q.Used() > c.peakObserved {
		c.peakObserved = c.q.Used()
	}
	if c.store != nil {
		used := c.store.Used()
		if used > c.peakDisk {
			c.peakDisk = used
		}
		if c.peakPhysical < c.peakDisk || c.cancelFiles > 0 {
			bytes, files := n34BudgetDisk(c.t, c.store.Directory())
			if bytes > c.peakPhysical {
				c.peakPhysical = bytes
			}
			if files > c.peakFiles {
				c.peakFiles = files
			}
			if c.cancelFiles > 0 && files >= c.cancelFiles && !c.cancelled {
				c.cancelled = true
				c.cancel()
			}
		}
	}
	return c.Context.Err()
}

func n34BudgetQuery(t *testing.T, limit int64) *memory.QueryBudget {
	t.Helper()
	q, e := memory.NewEngineBudget(limit).BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
	if e != nil {
		t.Fatal(e)
	}
	return q
}
func n34BudgetSession() *Session {
	o := engine.DefaultOptions()
	o.Workers = 1
	o.Pipeline.Workers = 1
	o.PreparedCacheBytes = 0
	o.MemoryLimitBytes = 16 << 20
	return NewSession(o)
}
func n34BudgetOptions(q *memory.QueryBudget, store *memory.QuerySpillStore) DecimalBoundedTerminalOptions {
	return DecimalBoundedTerminalOptions{Budget: q, Store: store, TableCapacity: 16, Mode: engine.DecimalBoundedAverageCounts, AveragePrecision: 12, AverageScale: 3, AverageRounding: aggregate.DecimalAverageTowardZero}
}

type n34BudgetMeasurement struct {
	memory, disk, physical, source, other int64
	files, checks                         int
}

func n34BudgetRun(t *testing.T, path string, limit, quota int64, cancelCheck, cancelFiles int, want error, reused ...*memory.QuerySpillStore) n34BudgetMeasurement {
	t.Helper()
	s := n34BudgetSession()
	defer s.Close()
	f, e := s.ScanParquet(path)
	if e != nil {
		t.Fatal(e)
	}
	q := n34BudgetQuery(t, limit)
	defer q.Close()
	var store *memory.QuerySpillStore
	if len(reused) > 0 {
		store = reused[0]
	} else {
		store, e = memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: quota})
		if e != nil {
			t.Fatal(e)
		}
		defer store.Close()
	}
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &n34BudgetContext{Context: base, t: t, q: q, store: store, cancel: cancel, cancelFiles: cancelFiles, cancelCheck: cancelCheck}
	out, e := f.CollectDecimalBounded(c, "key", "value", n34BudgetOptions(q, store))
	seen := map[int64]bool{}
	if e == nil {
		if q.Snapshot().Closed {
			t.Fatal("upstream EOF closed caller query")
		}
		lease, err := q.Reserve(context.Background(), 1)
		if err != nil {
			t.Fatal("query unusable after input EOF", err)
		}
		lease.Release()
		if out.Stats.InputRows != 512 || out.Stats.Flushes != 32 || out.Stats.MergePasses != 2 || out.Stats.Groups != 64 {
			t.Fatalf("not actual multipass: %+v", out.Stats)
		}
		for {
			var b *engine.DecimalBoundedOutputBatch
			var ok bool
			b, ok, e = out.Next(c)
			if e != nil || !ok {
				break
			}
			if b.AveragePrecision != 12 || b.AverageScale != 3 {
				t.Fatal("metadata")
			}
			for _, r := range b.Records[:b.Rows] {
				k := r.Key
				valid := k%7 != 0
				var count uint64
				if valid {
					count = 8
				}
				if k < 0 || k >= 64 || seen[k] || !r.KeyValid || r.CountRows != 8 || r.CountValid != count || r.AverageValid != valid || r.SumValid || (valid && r.Average != decimal128.FromI64((k+1)*10)) {
					t.Fatalf("oracle row %+v", r)
				}
				seen[k] = true
			}
		}
	}
	c.Err()
	c.mu.Lock()
	got := n34BudgetMeasurement{q.Snapshot().PeakBytes, c.peakDisk, c.peakPhysical, c.maxSource, c.coupledOther, c.peakFiles, c.checks}
	c.stopped = true
	c.mu.Unlock()
	if want == nil {
		if e != nil {
			t.Fatal(e)
		}
		if len(seen) != 64 || got.disk == 0 || got.disk != got.physical {
			t.Fatalf("missing real outputs/files: seen=%d measured=%+v", len(seen), got)
		}
		if got.source < 512*24 || got.other < 33000 {
			t.Fatalf("full upstream and engine not jointly observed: %+v", got)
		}
	} else if !errors.Is(e, want) {
		t.Fatalf("want %v got %v", want, e)
	}
	if cancelCheck > 0 || cancelFiles > 0 {
		if !c.cancelled {
			t.Fatal("requested cancellation not hit")
		}
	}
	c.cancelFiles = 0
	c.cancelCheck = 0
	if out != nil {
		if e := out.Close(context.Background()); e != nil {
			t.Fatal(e)
		}
		if e := out.Close(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	if q.Snapshot().Closed {
		t.Fatal("terminal closed caller budget")
	}
	if q.Used() != 0 || store.Used() != 0 {
		t.Fatalf("leak query=%d store=%d", q.Used(), store.Used())
	}
	if bytes, files := n34BudgetDisk(t, store.Directory()); bytes != 0 || files != 0 {
		t.Fatal("physical leak")
	}
	r, e := q.Reserve(context.Background(), 1)
	if e != nil {
		t.Fatal("caller budget cannot outlive terminal", e)
	}
	r.Release()
	return got
}
func TestN34TerminalBudgetMeasuredQuota(t *testing.T) {
	path := n34BudgetParquet(t)
	got := n34BudgetRun(t, path, 16<<20, 8<<20, 0, 0, nil)
	t.Logf("actual shared query peak=%d SourceScanMax=%d concurrent non-source=%d disk=%d stat=%d files=%d contextChecks=%d", got.memory, got.source, got.other, got.disk, got.physical, got.files, got.checks)
	t.Run("disk_exact", func(t *testing.T) { n34BudgetRun(t, path, 16<<20, got.disk, 0, 0, nil) })
	t.Run("disk_minus_one", func(t *testing.T) { n34BudgetRun(t, path, 16<<20, got.disk-1, 0, 0, memory.ErrSpillQuotaExceeded) })
	stable, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer stable.Close()
	same := n34BudgetRun(t, path, 16<<20, 8<<20, 0, 0, nil, stable)
	t.Run("memory_exact", func(t *testing.T) { n34BudgetRun(t, path, same.memory, 8<<20, 0, 0, nil, stable) })
	t.Run("memory_minus_one", func(t *testing.T) {
		n34BudgetRun(t, path, same.memory-1, 8<<20, 0, 0, memory.ErrBudgetExceeded, stable)
	})
}
func TestN34TerminalBudgetWholeUpstreamBatch(t *testing.T) {
	path := n34BudgetParquet(t)
	s := n34BudgetSession()
	defer s.Close()
	f, e := s.ScanParquet(path)
	if e != nil {
		t.Fatal(e)
	}
	q := n34BudgetQuery(t, 16<<20)
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	rd, e := f.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 1 << 20, MaxBatchBytes: 1 << 20, MaxInFlightBytes: 8 << 20})
	if e != nil {
		t.Fatal(e)
	}
	if !rd.Next(ctx) {
		t.Fatal(rd.Err())
	}
	if rd.Batch().Len() != 512 {
		t.Fatalf("fixture must expose real >256 batch, got %d", rd.Batch().Len())
	}
	if q.Used() < 512*24 {
		t.Fatalf("full batch not charged %d", q.Used())
	}
	t.Logf("independent live full 512-row batch query bytes=%d snapshot=%+v", q.Used(), q.Snapshot())
	if e := rd.Close(); e != nil {
		t.Fatal(e)
	}
	if q.Used() != 0 {
		t.Fatal("reader leak")
	}
	// Finite limits that could fit only a 256-row typed adapter must not admit
	// this real decoder/whole-batch path by silently excluding upstream memory.
	for _, limit := range []int64{1, 4096, 16384, 32768} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) { n34BudgetRun(t, path, limit, 8<<20, 0, 0, memory.ErrBudgetExceeded) })
	}
}
func TestN34TerminalBudgetCancellation(t *testing.T) {
	path := n34BudgetParquet(t)
	got := n34BudgetRun(t, path, 16<<20, 8<<20, 0, 0, nil)
	// Concrete QueryBudget is deliberately not replaced with a fault wrapper.
	// Sample repeatable context boundaries across the complete execution instead.
	for _, at := range []int{1, 2, 3, got.checks / 4, got.checks / 2, got.checks * 3 / 4} {
		t.Run(fmt.Sprintf("context_%d", at), func(t *testing.T) { n34BudgetRun(t, path, 16<<20, 8<<20, at, 0, context.Canceled) })
	}
	for _, files := range []int{1, 33} {
		t.Run(fmt.Sprintf("files_%d", files), func(t *testing.T) { n34BudgetRun(t, path, 16<<20, 8<<20, 0, files, context.Canceled) })
	}
}
func TestN34TerminalBudgetAdmission(t *testing.T) {
	path := n34BudgetParquet(t)
	for _, name := range []string{"nil_budget", "unlimited_budget", "closed_budget", "different_context_budget", "nil_store", "unlimited_store"} {
		t.Run(name, func(t *testing.T) {
			s := n34BudgetSession()
			defer s.Close()
			f, e := s.ScanParquet(path)
			if e != nil {
				t.Fatal(e)
			}
			q := n34BudgetQuery(t, 16<<20)
			defer q.Close()
			store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			opts := n34BudgetOptions(q, store)
			ctx := context.Background()
			switch name {
			case "nil_budget":
				opts.Budget = nil
			case "unlimited_budget":
				opts.Budget = n34BudgetQuery(t, 0)
				defer opts.Budget.Close()
			case "closed_budget":
				q.Close()
			case "different_context_budget":
				other := n34BudgetQuery(t, 16<<20)
				defer other.Close()
				ctx = memory.WithQueryBudget(ctx, other)
			case "nil_store":
				opts.Store = nil
			case "unlimited_store":
				opts.Store = &memory.QuerySpillStore{}
			}
			out, e := f.CollectDecimalBounded(ctx, "key", "value", opts)
			if e == nil {
				if out != nil {
					out.Close(context.Background())
				}
				t.Fatal("invalid budget path admitted")
			}
			if out != nil {
				if e := out.Close(context.Background()); e != nil {
					t.Fatal(e)
				}
			}
			if q.Used() != 0 || store.Used() != 0 {
				t.Fatal("admission leak")
			}
		})
	}
}
