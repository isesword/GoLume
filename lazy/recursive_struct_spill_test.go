package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"io/fs"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func recursiveCNoFiles(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			t.Errorf("spill file retained: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func recursiveCCheck(t *testing.T, df DataFrame, depth, repeats int) {
	t.Helper()
	for _, name := range []string{"id", "root"} {
		c, err := df.Column(name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.ToAnySlice()
		if err != nil {
			t.Fatal(err)
		}
		want := make([]any, 0, 65*repeats)
		for row := 64; row >= 0; row-- {
			for i := 0; i < repeats; i++ {
				if name == "id" {
					want = append(want, int64(row))
				} else {
					want = append(want, recursiveBRaw(depth, 1, row))
				}
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("sorted recursive column %s mismatch\ngot%v\nwant%v", name, got, want)
		}
	}
}

func TestRecursiveStructCSpill(t *testing.T) {
	for _, depth := range []int{2, 8} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("d%d/m%d", depth, mode), func(t *testing.T) {
				schema, b := recursiveBFixture(t, depth, 65)
				src, err := inmemory.New(schema, b, b, b, b)
				if err != nil {
					t.Fatal(err)
				}
				opts := engine.DefaultOptions()
				opts.PreparedCacheBytes = 0
				opts.KernelMode = mode
				opts.ExecutionPolicy = engine.ExecutionBounded
				opts.MemoryLimitBytes = 2 << 20
				dir := t.TempDir()
				opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 32 << 20}
				s := NewSession(opts)
				defer s.Close()
				q := s.FromSource(src).Sort(Desc("id"))
				budget, err := s.Engine().BeginQueryMemory()
				if err != nil {
					t.Fatal(err)
				}
				observer := &recursiveCRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
				df, err := q.Collect(observer)
				if err != nil {
					budget.Close()
					t.Fatal(err)
				}
				snap := budget.Snapshot()
				if snap.SpilledBytes <= 0 {
					df.Close()
					budget.Close()
					t.Fatal("bounded configuration is not proof: no actual spill")
				}
				t.Logf("actual recursive payload spill=%d peak=%d", snap.SpilledBytes, snap.PeakBytes)
				if observer.maxRuns < 2 || observer.diskBytes == 0 {
					t.Fatalf("no observed multiple live disk runs: %d/%d", observer.maxRuns, observer.diskBytes)
				}
				t.Logf("observed actual disk runs=%d bytes=%d before merge completed", observer.maxRuns, observer.diskBytes)
				recursiveCCheck(t, df, depth, 4)
				budget.Close()
				recursiveCNoFiles(t, dir)
				// Query reader has already closed; closing the session cannot invalidate owned output.
				s.Close()
				recursiveCCheck(t, df, depth, 4)
				df.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("success owner leak")
				}
				recursiveCNoFiles(t, dir)
				tinyOpts := opts
				tinyOpts.MemoryLimitBytes = 1
				tiny := NewSession(tinyOpts)
				denied, e := tiny.FromSource(src).Sort(Desc("id")).Collect(context.Background())
				if e == nil {
					denied.Close()
				}
				tiny.Close()
				if !errors.Is(e, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
					t.Fatalf("low budget %v used%d", e, tiny.Engine().Memory().Used())
				}
				recursiveCNoFiles(t, dir)
				canceled := NewSession(opts)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				out, e := canceled.FromSource(src).Sort(Desc("id")).Collect(ctx)
				if e == nil {
					out.Close()
				}
				canceled.Close()
				if !errors.Is(e, context.Canceled) || canceled.Engine().Memory().Used() != 0 {
					t.Fatalf("cancel %v used%d", e, canceled.Engine().Memory().Used())
				}
				recursiveCNoFiles(t, dir)
				// Cancel a query that has actually spilled after its first merged output batch.
				active := NewSession(opts)
				ab, ae := active.Engine().BeginQueryMemory()
				if ae != nil {
					t.Fatal(ae)
				}
				ac, stop := context.WithCancel(memory.WithQueryBudget(context.Background(), ab))
				reader, ae := active.FromSource(src).Sort(Desc("id")).CollectBatches(ac, BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 1 << 20})
				if ae != nil {
					stop()
					ab.Close()
					active.Close()
					t.Fatal(ae)
				}
				if !reader.Next(ac) {
					t.Fatal("first spilling batch", reader.Err())
				}
				if ab.Snapshot().SpilledBytes == 0 {
					t.Fatal("active cancellation did not reach spill")
				}

				stop()
				if reader.Next(ac) {
					t.Fatal("canceled reader produced batch")
				}
				if !errors.Is(reader.Err(), context.Canceled) {
					t.Fatal("active cancel", reader.Err())
				}
				reader.Close()
				ab.Close()
				active.Close()
				if active.Engine().Memory().Used() != 0 {
					t.Fatal("active cancellation leak")
				}
				recursiveCNoFiles(t, dir)
				// Key and expression-selection scope are not expanded by payload spill.
				boundary := NewSession(opts)
				defer boundary.Close()
				bad, e := boundary.FromSource(src).Sort(Asc("root")).Collect(context.Background())
				if e == nil {
					bad.Close()
					t.Fatal("recursive sort key accepted")
				}
				p, e := expr.Compile(schema, recursiveBLeaf(depth, "v"))
				if e != nil {
					t.Fatal(e)
				}
				if p.SupportsSelection() {
					t.Fatal("recursive expression selected fast path enabled")
				}
			})
		}
	}
}

// Observe transient runs during synchronous context checkpoints: by the first
// output batch the sorter may already have merged and deleted every run.
type recursiveCRunContext struct {
	mu sync.Mutex
	context.Context
	dir       string
	maxRuns   int
	diskBytes int64
}

func (c *recursiveCRunContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.maxRuns < 2 {
		n := 0
		var size int64
		_ = filepath.WalkDir(c.dir, func(_ string, d fs.DirEntry, e error) error {
			if e == nil && !d.IsDir() {
				if info, e := d.Info(); e == nil && info.Size() > 0 {
					n++
					size += info.Size()
				}
			}
			return nil
		})
		if n > c.maxRuns {
			c.maxRuns = n
			c.diskBytes = size
		}
	}
	return c.Context.Err()
}
