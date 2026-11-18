package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/memory"
	"reflect"
	"runtime"
	"testing"
)

func nestedListCRaw(row, depth int) map[string]any {
	m := nestedListBExpected(row, depth)
	m["oldints"] = []any{int64(row), nil, int64(row + 1)}
	if row%5 == 0 {
		m["oldints"] = []any{}
	}
	if row%7 == 0 {
		m["oldints"] = nil
	}
	m["records"] = listStructBList(row, 1)
	return m
}
func nestedListCArrow(t *testing.T, alloc am.Allocator, depth int) arrow.RecordBatch {
	base := nestedListBArrow(t, alloc, depth, 0)
	fields := append([]arrow.Field(nil), base.Schema().Fields()...)
	base.Release()
	fields = append(fields, arrow.Field{Name: "oldints", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: true}), Nullable: true}, arrow.Field{Name: "records", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.StructOf(listStructBFields()...), Nullable: true}), Nullable: true})
	rb := array.NewRecordBuilder(alloc, arrow.NewSchema(fields, nil))
	for row := 0; row < 65; row++ {
		m := nestedListCRaw(row, depth)
		for i, f := range fields {
			listStructBAppend(rb.Field(i), m[f.Name])
		}
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	return rec
}

func nestedListCQuery(t *testing.T, s *Session, depth int) LazyFrame {
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	rec := nestedListCArrow(t, alloc, depth)
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, rec, rec, rec, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	return q.Sort(Desc("id"))
}
func nestedListCCheck(t *testing.T, df DataFrame, depth int) {
	t.Helper()
	for _, name := range []string{"ints", "texts", "outer", "id", "index", "oldints", "records"} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, 0, 260)
		for row := 64; row >= 0; row-- {
			for repeat := 0; repeat < 4; repeat++ {
				want = append(want, nestedListCRaw(row, depth)[name])
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("C full nested sorted oracle %s got%v want%v", name, got, want)
		}
	}
	sc := df.Schema()
	if sc.Fields[0].Children[0].Type != memory.TypeList || sc.Fields[0].Children[0].Children[0].Type != memory.TypeInt64 || sc.Fields[1].Children[0].Children[0].Type != memory.TypeString {
		t.Fatal("C metadata", sc)
	}
}
func TestNestedListCActualSpill(t *testing.T) {
	for _, depth := range []int{1, 8} {
		t.Run(fmt.Sprintf("outer%d", depth), func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.PreparedCacheBytes = 0
			opts.ExecutionPolicy = engine.ExecutionBounded
			opts.MemoryLimitBytes = 8 << 20
			dir := t.TempDir()
			opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
			s := NewSession(opts)
			defer s.Close()
			q := nestedListCQuery(t, s, depth)
			budget, e := s.Engine().BeginQueryMemory()
			if e != nil {
				t.Fatal(e)
			}
			defer budget.Close()
			observer := &recursiveCRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
			done := make(chan struct{})
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				for {
					select {
					case <-done:
						return
					default:
						observer.Err()
						runtime.Gosched()
					}
				}
			}()
			df, e := q.Collect(observer)
			close(done)
			<-stopped
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			if budget.Snapshot().SpilledBytes <= 0 || observer.maxRuns < 2 || observer.diskBytes <= 0 {
				t.Fatal("actual multi-run proof absent", budget.Snapshot(), observer.maxRuns, observer.diskBytes)
			}
			t.Logf("actual nested List spill=%d nonempty runs=%d observed bytes=%d", budget.Snapshot().SpilledBytes, observer.maxRuns, observer.diskBytes)
			nestedListCCheck(t, df, depth)
			budget.Close()
			recursiveCNoFiles(t, dir)
			s.Close()
			nestedListCCheck(t, df, depth)
			df.Close()
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("C owned leak")
			}
			tinyOpts := opts
			tinyOpts.MemoryLimitBytes = 1
			tiny := NewSession(tinyOpts)
			tq := nestedListCQuery(t, tiny, depth)
			td, te := tq.Collect(context.Background())
			if te == nil {
				td.Close()
			}
			tiny.Close()
			if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
				t.Fatal("C low budget", te)
			}
			recursiveCNoFiles(t, dir)
			active := NewSession(opts)
			defer active.Close()
			aq := nestedListCQuery(t, active, depth)
			ab, e := active.Engine().BeginQueryMemory()
			if e != nil {
				t.Fatal(e)
			}
			defer ab.Close()
			ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), ab))
			defer cancel()
			reader, e := aq.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 2 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer reader.Close()
			if !reader.Next(ctx) {
				reader.Close()
				t.Fatal("first restored output", reader.Err())
			}
			if ab.Snapshot().SpilledBytes <= 0 {
				t.Fatal("cancel case not spilled")
			}
			cancel()
			if reader.Next(ctx) || !errors.Is(reader.Err(), context.Canceled) {
				t.Fatal("post-spill output cancel", reader.Err())
			}
			reader.Close()
			ab.Close()
			active.Close()
			if active.Engine().Memory().Used() != 0 {
				t.Fatal("cancel owner leak")
			}
			recursiveCNoFiles(t, dir)
			pre := NewSession(opts)
			pq := nestedListCQuery(t, pre, depth)
			pc, stop := context.WithCancel(context.Background())
			stop()
			pd, pe := pq.Collect(pc)
			if pe == nil {
				pd.Close()
			}
			pre.Close()
			if !errors.Is(pe, context.Canceled) || pre.Engine().Memory().Used() != 0 {
				t.Fatal("pre cancel", pe)
			}
			recursiveCNoFiles(t, dir)
		})
	}
}
