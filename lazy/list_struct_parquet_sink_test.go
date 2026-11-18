package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func listStructSinkArrowValue(a arrow.Array, row int) any {
	if a.IsNull(row) {
		return nil
	}
	switch v := a.(type) {
	case *array.List:
		lo, hi := v.ValueOffsets(row)
		out := make([]any, int(hi-lo))
		for i := range out {
			out[i] = listStructSinkArrowValue(v.ListValues(), int(lo)+i)
		}
		return out
	case *array.Struct:
		m := map[string]any{}
		for i, f := range v.DataType().(*arrow.StructType).Fields() {
			m[f.Name] = listStructSinkArrowValue(v.Field(i), row)
		}
		return m
	case *array.Int8:
		return v.Value(row)
	case *array.Int16:
		return v.Value(row)
	case *array.Int32:
		return v.Value(row)
	case *array.Int64:
		return v.Value(row)
	case *array.Uint8:
		return v.Value(row)
	case *array.Uint16:
		return v.Value(row)
	case *array.Uint32:
		return v.Value(row)
	case *array.Uint64:
		return v.Value(row)
	case *array.Float32:
		return v.Value(row)
	case *array.Float64:
		return v.Value(row)
	case *array.Boolean:
		return v.Value(row)
	case *array.Date32:
		return int32(v.Value(row))
	case *array.Timestamp:
		return int64(v.Value(row))
	case *array.String:
		return v.Value(row)
	default:
		panic(fmt.Sprintf("unsupported independent oracle %T", a))
	}
}
func listStructSinkRead(t *testing.T, paths []string, depth int, wantRows []int) {
	t.Helper()
	got := map[string][]any{}
	for _, path := range paths {
		rd, e := file.OpenParquetFile(path, false)
		if e != nil {
			t.Fatal(e)
		}
		ar, e := pqarrow.NewFileReader(rd, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
		if e != nil {
			t.Fatal(e)
		}
		tab, e := ar.ReadTable(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		for i, f := range tab.Schema().Fields() {
			for _, chunk := range tab.Column(i).Data().Chunks() {
				for row := 0; row < chunk.Len(); row++ {
					got[f.Name] = append(got[f.Name], listStructSinkArrowValue(chunk, row))
				}
			}
		}
		tab.Release()
		rd.Close()
	}
	for name, values := range got {
		want := make([]any, 0, len(wantRows))
		for _, row := range wantRows {
			want = append(want, listStructDRaw(row, depth)[name])
		}
		if !reflect.DeepEqual(values, want) {
			t.Fatalf("independent parquet %s got%v want%v", name, values, want)
		}
	}
	if len(wantRows) > 0 && len(got) == 0 {
		t.Fatal("empty oracle")
	}
}
func TestListStructParquetSinkPublic(t *testing.T) {
	for _, depth := range []int{1, 7} {
		t.Run(fmt.Sprintf("depth%d", depth), func(t *testing.T) {
			path := listStructDFixture(t, depth, depth == 7)
			opts := engine.DefaultOptions()
			opts.PreparedCacheBytes = 0
			s := NewSession(opts)
			defer s.Close()
			q, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			q = q.WithColumns(ListLength(Col("left")).Alias("len"), StructGet(ListGet(Col("left"), Col("index")), "code.中").Alias("code"), Named(listStructBOuterExpr(depth), "events")).Filter(expr.GT(Col("id"), expr.I64(31)))
			rows := make([]int, 33)
			for i := range rows {
				rows[i] = 32 + i
			}
			for _, shard := range []bool{false, true} {
				t.Run(fmt.Sprintf("shard%v", shard), func(t *testing.T) {
					out := filepath.Join(t.TempDir(), "result")
					o := ParquetSinkOptions{RowGroupRows: 5}
					if shard {
						o.MaxRowsPerFile = 7
						o.TargetBytesPerFile = 1 << 20
					}
					result, e := q.SinkParquet(context.Background(), out, o)
					if e != nil {
						t.Fatal(e)
					}
					if result.Rows != 33 || result.Bytes <= 0 || len(result.Files) == 0 {
						t.Fatal(result)
					}
					if shard && len(result.Files) != 5 {
						t.Fatal("shards", result)
					}
					listStructSinkRead(t, result.Files, depth, rows)
					total := int64(0)
					for _, p := range result.Files {
						rq, e := s.ScanParquetWithOptions(p, ParquetScanOptions{NativeNested: true})
						if e != nil {
							t.Fatal(e)
						}
						df, e := rq.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						listStructDMetadata(t, df.Schema(), depth)
						c, _ := df.Column("id")
						ids, _ := c.ToAnySlice()
						if shard && len(ids) > 7 {
							t.Fatal("hard shard rows")
						}
						for j, id := range ids {
							if id != int64(32+int(total)+j) {
								t.Fatal("shard order", id)
							}
						}
						total += int64(len(ids))
						df.Close()
					}
					if total != 33 {
						t.Fatal(total)
					}
				})
			}
			empty := q.Filter(expr.GT(Col("id"), expr.I64(100)))
			ep := filepath.Join(t.TempDir(), "empty.parquet")
			res, e := empty.SinkParquet(context.Background(), ep, ParquetSinkOptions{})
			if e != nil || res.Rows != 0 || len(res.Files) != 1 {
				t.Fatal("empty", res, e)
			}
			listStructSinkRead(t, res.Files, depth, nil)
			parent := t.TempDir()
			existing := filepath.Join(parent, "existing.parquet")
			sentinel := []byte("keep old target")
			os.WriteFile(existing, sentinel, 0600)
			if _, e = q.SinkParquet(context.Background(), existing, ParquetSinkOptions{}); !errors.Is(e, ErrSinkTargetExists) {
				t.Fatal("existing", e)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, e = q.SinkParquet(ctx, existing, ParquetSinkOptions{SinkOptions: SinkOptions{Overwrite: true}}); !errors.Is(e, context.Canceled) {
				t.Fatal("pre cancel", e)
			}
			got, _ := os.ReadFile(existing)
			if !reflect.DeepEqual(got, sentinel) {
				t.Fatal("target changed on precommit failure")
			}
			entries, _ := os.ReadDir(parent)
			if len(entries) != 1 {
				t.Fatal("staging leak", entries)
			}
			if _, e = q.SinkParquet(context.Background(), existing, ParquetSinkOptions{SinkOptions: SinkOptions{Overwrite: true}}); e != nil {
				t.Fatal(e)
			}
			listStructSinkRead(t, []string{existing}, depth, rows)
			tinyOpts := opts
			tinyOpts.MemoryLimitBytes = 1
			tiny := NewSession(tinyOpts)
			tq, e := tiny.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			target := filepath.Join(t.TempDir(), "budget.parquet")
			_, e = tq.SinkParquet(context.Background(), target, ParquetSinkOptions{})
			tiny.Close()
			if !errors.Is(e, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
				t.Fatal("budget", e)
			}
			if _, e = os.Stat(target); !os.IsNotExist(e) {
				t.Fatal("budget target", e)
			}
			s.Close()
			listStructSinkRead(t, []string{existing}, depth, rows)
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("sink leak")
			}
		})
	}
}

// Deterministic seam tests validate sink transaction behavior; these fake encoders
// do not claim to inject a precise failure inside the third-party Parquet codec.
type listStructSinkFailAllocator struct {
	am.Allocator
	active bool
	calls  int
	failed bool
}

func (a *listStructSinkFailAllocator) Allocate(n int) []byte {
	if a.active {
		a.calls++
		if a.calls == 3 {
			a.failed = true
			return a.Allocator.Allocate(1 << 30)
		}
	}
	return a.Allocator.Allocate(n)
}
func (a *listStructSinkFailAllocator) Reallocate(n int, b []byte) []byte {
	if a.active {
		a.calls++
		if a.calls == 3 {
			a.failed = true
			return a.Allocator.Allocate(1 << 30)
		}
	}
	return a.Allocator.Reallocate(n, b)
}

type listStructSinkRealFail struct {
	recordSinkWriter
	allocator *listStructSinkFailAllocator
}

func (w *listStructSinkRealFail) Write(r arrow.RecordBatch) error {
	w.allocator.active = true
	defer func() { w.allocator.active = false }()
	return w.recordSinkWriter.Write(r)
}

type listStructSinkCloseCancel struct {
	recordSinkWriter
	cancel context.CancelFunc
}

func (w *listStructSinkCloseCancel) Close() error {
	e := w.recordSinkWriter.Close()
	w.cancel()
	return e
}

type listStructSinkFaultWriter struct {
	alloc   am.Allocator
	point   string
	failure error
	cancel  context.CancelFunc
}

func (w *listStructSinkFaultWriter) Write(arrow.RecordBatch) error {
	if w.point == "write" {
		return w.failure
	}
	if w.point == "allocation" {
		w.alloc.Allocate(1 << 30)
	}
	if w.point == "cancel" {
		w.cancel()
		w.alloc.Allocate(16)
	}
	return nil
}
func (w *listStructSinkFaultWriter) Close() error {
	if w.point == "close-cancel" {
		w.cancel()
		return nil
	}
	if w.point == "close-swallow" {
		return listStructSinkSwallow(func() { w.alloc.Allocate(1 << 30) })
	}
	if w.point == "close" {
		return w.failure
	}
	return nil
}
func TestListStructParquetSinkFaults(t *testing.T) {
	for _, point := range []string{"factory", "factory-swallow", "write", "close", "close-swallow", "close-cancel", "close-cancel-new", "allocation", "real-allocation", "cancel"} {
		t.Run(point, func(t *testing.T) {
			path := listStructDFixture(t, 1, false)
			s := NewSession(engine.DefaultOptions())
			defer s.Close()
			q, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			budget, e := memory.NewEngineBudget(8 << 20).BeginQuery(memory.BudgetOptions{})
			if e != nil {
				t.Fatal(e)
			}
			defer budget.Close()
			ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), budget))
			defer cancel()
			lease, e := budget.Operator("caller", memory.MemoryRetainedState).Reserve(ctx, 8)
			if e != nil {
				t.Fatal(e)
			}
			defer lease.Release()
			failure := errors.New("injected sink error")
			var realAllocator *listStructSinkFailAllocator
			factory := func(out io.Writer, sc *arrow.Schema, a am.Allocator) (recordSinkWriter, error) {
				if point == "factory-swallow" {
					return nil, listStructSinkSwallow(func() { a.Allocate(1 << 30) })
				}
				if point == "factory" {
					return nil, failure
				}
				if point == "real-allocation" {
					realAllocator = &listStructSinkFailAllocator{Allocator: a}
					w, e := pqarrow.NewFileWriter(sc, out, pq.NewWriterProperties(pq.WithAllocator(realAllocator)), pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(realAllocator)))
					if e != nil {
						return nil, e
					}
					return &listStructSinkRealFail{w, realAllocator}, nil
				}
				if point == "close-cancel" || point == "close-cancel-new" {
					w, e := pqarrow.NewFileWriter(sc, out, pq.NewWriterProperties(pq.WithAllocator(a)), pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(a)))
					if e != nil {
						return nil, e
					}
					return &listStructSinkCloseCancel{w, cancel}, nil
				}
				return &listStructSinkFaultWriter{a, point, failure, cancel}, nil
			}
			parent := t.TempDir()
			out := filepath.Join(parent, "old")
			sentinel := []byte("old bytes")
			if point != "close-cancel-new" {
				os.WriteFile(out, sentinel, 0600)
			}
			_, e = q.sinkFiles(ctx, out, ".test", SinkOptions{Overwrite: true}, factory, "TestSinkFailure")
			if realAllocator != nil {
				if !realAllocator.failed || realAllocator.calls < 3 {
					t.Fatal("real writer allocation point not reached", realAllocator.calls)
				}
				t.Logf("real pqarrow Write allocator callback rejected on call 3, calls=%d", realAllocator.calls)
			}
			want := failure
			if point == "allocation" || point == "real-allocation" || point == "factory-swallow" || point == "close-swallow" {
				want = memory.ErrBudgetExceeded
			}
			if point == "cancel" || point == "close-cancel" || point == "close-cancel-new" {
				want = context.Canceled
			}
			if !errors.Is(e, want) {
				t.Errorf("fault identity %v want %v", e, want)
			}
			got, readErr := os.ReadFile(out)
			if point == "close-cancel-new" {
				if !os.IsNotExist(readErr) {
					t.Fatal("new target published", readErr)
				}
				sentinel = nil
			}
			if !reflect.DeepEqual(got, sentinel) {
				t.Fatal("old target changed")
			}
			entries, _ := os.ReadDir(parent)
			wantEntries := 1
			if point == "close-cancel-new" {
				wantEntries = 0
			}
			if len(entries) != wantEntries {
				t.Fatal("stage leak", entries)
			}
			if budget.Snapshot().Closed || budget.Used() != 8 {
				t.Fatal("caller budget", budget.Snapshot())
			}
		})
	}
}

func TestListStructParquetSinkActualSpill(t *testing.T) {
	path := listStructDFixture(t, 7, true)
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 4 << 20
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 32 << 20}
	s := NewSession(opts)
	defer s.Close()
	q, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	observer := &recursiveCRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
	out := filepath.Join(t.TempDir(), "sorted.parquet")
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
	result, e := q.Sort(Desc("id")).SinkParquet(observer, out, ParquetSinkOptions{RowGroupRows: 17, SinkOptions: SinkOptions{Batch: BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 2 << 20}}})
	close(done)
	<-stopped
	if e != nil {
		t.Fatal(e)
	}
	if budget.Snapshot().SpilledBytes <= 0 || observer.maxRuns < 2 || observer.diskBytes <= 0 {
		t.Fatal("actual spill absent", budget.Snapshot(), observer.maxRuns)
	}
	t.Logf("actual sort→sink spilled=%d runs=%d disk=%d", budget.Snapshot().SpilledBytes, observer.maxRuns, observer.diskBytes)
	budget.Close()
	recursiveCNoFiles(t, dir)
	s.Close()
	rows := make([]int, 65)
	for i := range rows {
		rows[i] = 64 - i
	}
	listStructSinkRead(t, result.Files, 7, rows)
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("spill sink leak")
	}
}

func TestListStructParquetSinkByteShards(t *testing.T) {
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "items", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.StructOf(listStructBFields()...), Nullable: true}), Nullable: true}}
	as := arrow.NewSchema(fields, nil)
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	rb := array.NewRecordBuilder(alloc, as)
	want := make([]any, 5)
	for row := 0; row < 5; row++ {
		item := listStructBElement(row, 1, 0)
		n := 2048
		if row == 0 {
			n = 8192
		}
		item["label"] = strings.Repeat("x", n)
		want[row] = []any{item}
		listStructBAppend(rb.Field(0), int64(row))
		listStructBAppend(rb.Field(1), want[row])
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeStructList: true}, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	result, e := q.SinkParquet(context.Background(), filepath.Join(t.TempDir(), "byte-shards"), ParquetSinkOptions{SinkOptions: SinkOptions{TargetBytesPerFile: 1024}})
	if e != nil {
		t.Fatal(e)
	}
	if result.Rows != 5 || len(result.Files) != 5 {
		t.Fatal("byte-only shards/oversized rows", result)
	}
	var got []any
	var ids []any
	for _, path := range result.Files {
		rd, e := file.OpenParquetFile(path, false)
		if e != nil {
			t.Fatal(e)
		}
		ar, e := pqarrow.NewFileReader(rd, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
		if e != nil {
			t.Fatal(e)
		}
		tab, e := ar.ReadTable(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if tab.NumRows() != 1 {
			t.Fatal("oversized single-row progress", tab.NumRows())
		}
		for _, c := range tab.Column(1).Data().Chunks() {
			for i := 0; i < c.Len(); i++ {
				got = append(got, listStructSinkArrowValue(c, i))
			}
		}
		for _, c := range tab.Column(0).Data().Chunks() {
			for i := 0; i < c.Len(); i++ {
				ids = append(ids, listStructSinkArrowValue(c, i))
			}
		}
		tab.Release()
		rd.Close()
	}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(ids, []any{int64(0), int64(1), int64(2), int64(3), int64(4)}) {
		t.Fatal("byte shard independent oracle", ids)
	}
	// No compressed file-size upper bound: the soft target admits a single large row.
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("byte shard leak")
	}
}

func listStructSinkSwallow(fn func()) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("encoder converted allocation panic")
		}
	}()
	fn()
	return nil
}
func TestListStructParquetSinkStickyContracts(t *testing.T) {
	budget, e := memory.NewEngineBudget(4096).BeginQuery(memory.BudgetOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	a := newSinkBudgetAllocator(context.Background(), budget.Operator("test", memory.MemoryWorkspace))
	defer a.Close()
	ordinary := errors.New("ordinary codec error")
	if got := callBudgetedNativeSink(a, func() error { return ordinary }); got != ordinary {
		t.Fatal("ordinary identity", got)
	}
	func() {
		defer func() {
			if recover() != "unknown panic" {
				t.Fatal("unknown panic changed")
			}
		}()
		callBudgetedNativeSink(a, func() error { panic("unknown panic") })
	}()
	first := errors.New("first failure")
	a.recordFailure(first)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				a.recordFailure(context.Canceled)
				_ = a.failure()
			}
		}()
	}
	wg.Wait()
	called := false
	got := callBudgetedNativeSink(a, func() error { called = true; return ordinary })
	if !called || !errors.Is(got, first) || !errors.Is(got, ordinary) || a.failure() != first {
		t.Fatal("sticky first/cleanup", got)
	}
	if budget.Used() != 0 {
		t.Fatal("sticky lease leak")
	}
}

type listStructSinkCloseCounter struct{ closed *int }

func (w *listStructSinkCloseCounter) Write(arrow.RecordBatch) error { return nil }
func (w *listStructSinkCloseCounter) Close() error                  { *w.closed++; return nil }
func TestListStructParquetSinkFactoryTerminalWriter(t *testing.T) {
	path := listStructDFixture(t, 1, false)
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 8 << 20
	s := NewSession(opts)
	defer s.Close()
	q, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	closed := 0
	factory := func(_ io.Writer, _ *arrow.Schema, a am.Allocator) (recordSinkWriter, error) {
		_ = listStructSinkSwallow(func() { a.Allocate(1 << 30) })
		return &listStructSinkCloseCounter{&closed}, nil
	}
	parent := t.TempDir()
	target := filepath.Join(parent, "new")
	_, e = q.sinkFiles(context.Background(), target, ".test", SinkOptions{}, factory, "FactoryTerminal")
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Error("sticky factory error", e)
	}
	if closed != 1 {
		t.Error("returned terminal writer Close count", closed)
	}
	entries, _ := os.ReadDir(parent)
	if len(entries) != 0 {
		t.Error("staging leak", entries)
	}
	if s.Engine().Memory().Used() != 0 {
		t.Error("factory leak")
	}
}

func TestListStructParquetSinkTypedNilFactory(t *testing.T) {
	for _, kind := range []string{"typednil-error", "nil-nil", "typednil-nil"} {
		t.Run(kind, func(t *testing.T) {
			path := listStructDFixture(t, 1, false)
			s := NewSession(engine.DefaultOptions())
			defer s.Close()
			q, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			failure := errors.New("typed nil factory failure")
			factory := func(_ io.Writer, _ *arrow.Schema, _ am.Allocator) (recordSinkWriter, error) {
				if kind == "nil-nil" {
					return nil, nil
				}
				var w *pqarrow.FileWriter
				if kind == "typednil-nil" {
					return w, nil
				}
				return w, failure
			}
			parent := t.TempDir()
			target := filepath.Join(parent, "typednil")
			_, e = q.sinkFiles(context.Background(), target, ".parquet", SinkOptions{}, factory, "TypedNilFactory")
			if e == nil || (kind == "typednil-error" && !errors.Is(e, failure)) {
				t.Fatal("nil factory error identity", e)
			}
			entries, _ := os.ReadDir(parent)
			if len(entries) != 0 {
				t.Fatal("typednil stage leak", entries)
			}
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("typednil owner leak")
			}
		})
	}
}
