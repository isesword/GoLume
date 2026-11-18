package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

// Separate C acceptance: no List/Struct key or selected-expression expansion.
func listStructCCheck(t *testing.T, df DataFrame, ancestors, repeats int) {
	t.Helper()
	for _, name := range []string{"left", "right", "outer", "id", "index", "len", "code"} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, 0, 65*repeats)
		for row := 64; row >= 0; row-- {
			m := listStructBExpected(row, ancestors)
			for j := 0; j < repeats; j++ {
				want = append(want, m[name])
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("C sorted full payload %s got%v want%v", name, got, want)
		}
	}
}
func listStructCQuery(t *testing.T, s *Session, ancestors int) LazyFrame {
	t.Helper()
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	rec := listStructBArrow(t, alloc, ancestors, 65)
	// Four separately retained immutable records; ties carry identical payload, so
	// no undocumented stable-order guarantee is required by the oracle.
	records := []arrow.RecordBatch{rec, rec, rec, rec}
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeStructList: true}, records...)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	return q.WithColumns(ListLength(Col("left")).Alias("len"), StructGet(ListGet(Col("left"), Col("index")), "code.中").Alias("code")).Sort(Desc("id"))
}
func TestListStructCActualSpill(t *testing.T) {
	for _, ancestors := range []int{1, 7} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("a%d/m%d", ancestors, mode), func(t *testing.T) {
				opts := engine.DefaultOptions()
				opts.PreparedCacheBytes = 0
				opts.KernelMode = mode
				opts.ExecutionPolicy = engine.ExecutionBounded
				opts.MemoryLimitBytes = 4 << 20
				dir := t.TempDir()
				opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 32 << 20}
				s := NewSession(opts)
				defer s.Close()
				q := listStructCQuery(t, s, ancestors)
				budget, e := s.Engine().BeginQueryMemory()
				if e != nil {
					t.Fatal(e)
				}
				observer := &recursiveCRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
				df, e := q.Collect(observer)
				if e != nil {
					budget.Close()
					t.Fatal(e)
				}
				snap := budget.Snapshot()
				if snap.SpilledBytes <= 0 || observer.maxRuns < 2 || observer.diskBytes <= 0 {
					df.Close()
					budget.Close()
					t.Fatalf("actual disk proof absent: spill%d runs%d bytes%d", snap.SpilledBytes, observer.maxRuns, observer.diskBytes)
				}
				t.Logf("ListStruct actual spill=%d peak=%d observed nonempty runs=%d bytes=%d", snap.SpilledBytes, snap.PeakBytes, observer.maxRuns, observer.diskBytes)
				listStructCCheck(t, df, ancestors, 4)
				budget.Close()
				recursiveCNoFiles(t, dir)
				s.Close()
				listStructCCheck(t, df, ancestors, 4)
				df.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("owned output leak")
				}
				tinyOpts := opts
				tinyOpts.MemoryLimitBytes = 1
				tiny := NewSession(tinyOpts)
				tq := listStructCQuery(t, tiny, ancestors)
				td, te := tq.Collect(context.Background())
				if te == nil {
					td.Close()
				}
				tiny.Close()
				if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
					t.Fatal("low budget", te)
				}
				recursiveCNoFiles(t, dir)
				active := NewSession(opts)
				aq := listStructCQuery(t, active, ancestors)
				ab, e := active.Engine().BeginQueryMemory()
				if e != nil {
					t.Fatal(e)
				}
				ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), ab))
				reader, e := aq.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 2 << 20})
				if e != nil {
					cancel()
					ab.Close()
					active.Close()
					t.Fatal(e)
				}
				if !reader.Next(ctx) {
					t.Fatal("first output", reader.Err())
				}
				if ab.Snapshot().SpilledBytes <= 0 {
					t.Fatal("cancel test did not actually spill")
				}
				cancel()
				if reader.Next(ctx) || !errors.Is(reader.Err(), context.Canceled) {
					t.Fatal("post-spill output cancel", reader.Err())
				}
				reader.Close()
				ab.Close()
				active.Close()
				if active.Engine().Memory().Used() != 0 {
					t.Fatal("cancel leak")
				}
				recursiveCNoFiles(t, dir)
				// Pre-cancel remains distinct from cancellation after a restored output.
				pre := NewSession(opts)
				pq := listStructCQuery(t, pre, ancestors)
				pc, stop := context.WithCancel(context.Background())
				stop()
				pd, pe := pq.Collect(pc)
				if pe == nil {
					pd.Close()
				}
				pre.Close()
				if !errors.Is(pe, context.Canceled) || pre.Engine().Memory().Used() != 0 {
					t.Fatal("pre-cancel", pe)
				}
				recursiveCNoFiles(t, dir)
				// Key and expression selection capabilities are not authorized by payload sort.
				keySession := NewSession(opts)
				rec := listStructBArrow(t, am.DefaultAllocator, ancestors, 2)
				kq, e := keySession.ScanArrowWithOptions(ArrowScanOptions{NativeStructList: true}, rec)
				rec.Release()
				if e != nil {
					t.Fatal(e)
				}
				for _, key := range []string{"left", "outer"} {
					kd, ke := kq.Sort(Asc(key)).Collect(context.Background())
					if ke == nil {
						kd.Close()
						t.Fatal("nested key accepted", key)
					}
				}
				sc, e := kq.Schema()
				if e != nil {
					t.Fatal(e)
				}
				program, e := expr.Compile(sc, StructGet(ListGet(Col("left"), expr.I64(0)), "code.中"))
				if e != nil {
					t.Fatal(e)
				}
				if program.SupportsSelection() {
					t.Fatal("selected unexpectedly expanded")
				}
				keySession.Close()
			})
		}
	}
}
