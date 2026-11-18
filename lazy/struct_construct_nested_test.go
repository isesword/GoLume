package lazy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func structNestedQuery(t *testing.T, s *Session, depth, repeats int) LazyFrame {
	t.Helper()
	rec := nestedListCArrow(t, am.DefaultAllocator, depth)
	records := make([]arrow.RecordBatch, repeats)
	for i := range records {
		records[i] = rec
	}
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, records...)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	// Repeated getter references exercise CSE without sharing mutable output owners.
	box := StructFrom(Col("ints").Alias("ints"), Col("texts").Alias("texts"), Col("outer").Alias("outer"), Col("records").Alias("records"), Col("oldints").Alias("oldints"), Col("id").Alias("id"))
	q = q.SelectExprs(Col("id").Alias("key"), box.Alias("box"))
	rebuilt := StructFrom(StructGet(Col("box"), "ints").Alias("ints"), StructGet(Col("box"), "texts").Alias("texts"), StructGet(Col("box"), "outer").Alias("outer"), StructGet(Col("box"), "records").Alias("records"), StructGet(Col("box"), "oldints").Alias("oldints"), StructGet(Col("box"), "id").Alias("id"))
	return q.SelectExprs(Col("key").Alias("key"), rebuilt.Alias("made"), StructGet(Col("box"), "ints").Alias("again"))
}
func structNestedCheck(t *testing.T, df DataFrame, depth int, rows []int) {
	t.Helper()
	c, e := df.Column("made")
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	want := make([]any, len(rows))
	for i, r := range rows {
		m := nestedListCRaw(r, depth)
		w := map[string]any{}
		for _, k := range []string{"ints", "texts", "outer", "records", "oldints", "id"} {
			w[k] = m[k]
		}
		want[i] = w
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("nested constructor got=%#v want=%#v", got, want)
	}
	if c.NullCount() != 0 {
		t.Fatal("constructor parent became NULL")
	}
	c, e = df.Column("again")
	if e != nil {
		t.Fatal(e)
	}
	got, e = c.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	for i, r := range rows {
		want[i] = nestedListBRaw(r, false)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("shared getter alias changed")
	}
}

// Independent IPC value reader preserves old temporal storage values.
func structNestedArrowValue(a arrow.Array, row int) any {
	if a.IsNull(row) {
		return nil
	}
	switch v := a.(type) {
	case *array.Struct:
		out := map[string]any{}
		for i, f := range v.DataType().(*arrow.StructType).Fields() {
			out[f.Name] = structNestedArrowValue(v.Field(i), row)
		}
		return out
	case *array.List:
		lo, hi := v.ValueOffsets(row)
		out := make([]any, int(hi-lo))
		for i := range out {
			out[i] = structNestedArrowValue(v.ListValues(), int(lo)+i)
		}
		return out
	case *array.Date64:
		return int64(v.Value(row))
	case *array.Time32:
		return int32(v.Value(row))
	case *array.Time64:
		return int64(v.Value(row))
	case *array.Duration:
		return int64(v.Value(row))
	default:
		return listStructSinkArrowValue(a, row)
	}
}
func structNestedIPC(t *testing.T, s *Session, q LazyFrame, path string) DataFrame {
	t.Helper()
	if _, e := q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	rd, e := ipc.NewFileReader(f)
	if e != nil {
		t.Fatal(e)
	}
	defer rd.Close()
	var records []arrow.RecordBatch
	for i := 0; i < rd.NumRecords(); i++ {
		r, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		records = append(records, r)
	}
	aq, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, records...)
	for _, r := range records {
		r.Release()
	}
	if e != nil {
		t.Fatal(e)
	}
	df, e := aq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	return df
}
func TestStructConstructNestedTransport(t *testing.T) {
	for _, depth := range []int{1, 7} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			s := NewSession(engine.DefaultOptions())
			defer s.Close()
			q := structNestedQuery(t, s, depth, 1)
			rows := make([]int, 65)
			for i := range rows {
				rows[i] = i
			}
			df, e := q.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			structNestedCheck(t, df, depth, rows)
			ad := structNestedIPC(t, s, q, filepath.Join(t.TempDir(), "made.arrow"))
			defer ad.Close()
			structNestedCheck(t, ad, depth, rows)
			path := filepath.Join(t.TempDir(), "made.parquet")
			if _, e = q.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 7}); e != nil {
				t.Fatal(e)
			}
			pq, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			pd, e := pq.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer pd.Close()
			structNestedCheck(t, pd, depth, rows)
			filtered := q.Filter(expr.GT(StructGet(Col("made"), "id"), expr.I64(31)))
			fd, e := filtered.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			structNestedCheck(t, fd, depth, rows[32:])
			fd.Close()
			// Guard a scalar consumer of a repeated container getter, without enabling selected-container execution.
			guarded := q.SelectExprs(If(expr.GT(Col("key"), expr.I64(31)), ListLength(StructGet(Col("made"), "ints")), expr.I64(-1)).Alias("guarded"))
			gd, e := guarded.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			gc, e := gd.Column("guarded")
			if e != nil {
				t.Fatal(e)
			}
			gv, e := gc.ToAnySlice()
			if e != nil {
				t.Fatal(e)
			}
			for r, v := range gv {
				var w any = int64(-1)
				if r > 31 {
					a := nestedListBRaw(r, false)
					w = nil
					if a != nil {
						w = int64(len(a.([]any)))
					}
				}
				if !reflect.DeepEqual(v, w) {
					t.Fatalf("guard row %d: %v != %v", r, v, w)
				}
			}
			gd.Close()
			s.Close()
			structNestedCheck(t, df, depth, rows)
			structNestedCheck(t, ad, depth, rows)
			structNestedCheck(t, pd, depth, rows)
			df.Close()
			ad.Close()
			pd.Close()
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("owned transport leak")
			}
		})
	}
}
func TestStructConstructNestedDepthAndFailures(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := structNestedQuery(t, s, 8, 1) // new enclosing parent would be Struct depth nine
	if _, e := q.Schema(); e == nil {
		t.Fatal("depth-nine wrapper accepted")
	}
	valid := structNestedQuery(t, s, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	df, e := valid.Collect(ctx)
	if e == nil {
		df.Close()
	}
	if !errors.Is(e, context.Canceled) {
		t.Fatal("cancel identity", e)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1
	tiny := NewSession(opts)
	defer tiny.Close()
	bad := structNestedQuery(t, tiny, 1, 1)
	df, e = bad.Collect(context.Background())
	if e == nil {
		df.Close()
	}
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("budget identity", e)
	}
	tiny.Close()
	if tiny.Engine().Memory().Used() != 0 {
		t.Fatal("failed query retained memory")
	}
	// N9 also admits scalar-key TopK over these existing container payloads.
	df, e = valid.Sort(Asc("key")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	rows := make([]int, 65)
	for i := range rows {
		rows[i] = i
	}
	structNestedCheck(t, df, 1, rows)
	df.Close()
	df, e = valid.TopK(3, Asc("key")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	structNestedCheck(t, df, 1, rows[:3])
	df.Close()
}
func TestStructConstructNestedLegacyTwenty(t *testing.T) {
	for i, tc := range structGetCases() {
		t.Run(fmt.Sprintf("%d_%s", i, tc.typ), func(t *testing.T) {
			schema, b := structConstructInput(tc, 17)
			f, e := memory.NestedListField("arrays", true, memory.ListInt64Field("item", true, true))
			if e != nil {
				t.Fatal(e)
			}
			schema, e = memory.NewSchemaWithNestedListsChecked(append(schema.Fields, f)...)
			if e != nil {
				t.Fatal(e)
			}
			b.Arrays["arrays"] = nestedListBNativeNode(17, false)
			src, e := inmemory.New(schema, b)
			if e != nil {
				t.Fatal(e)
			}
			s := NewSession(engine.DefaultOptions())
			defer s.Close()
			q := s.FromSource(src).SelectExprs(StructFrom(Col("a.b:中").Alias("old"), Col("arrays").Alias("arrays")).Alias("made"))
			df, e := q.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			c, e := df.Column("made")
			if e != nil {
				t.Fatal(e)
			}
			got, e := c.ToAnySlice()
			if e != nil {
				t.Fatal(e)
			}
			for row, v := range got {
				want := map[string]any{"old": structConstructValue(tc, row), "arrays": nestedListBRaw(row, false)}
				if !reflect.DeepEqual(v, want) {
					t.Fatalf("old20 row%d: %#v != %#v", row, v, want)
				}
			}
			sc := df.Schema().Fields[0]
			if sc.Children[0].Type != tc.typ {
				t.Fatal("old logical type lost", sc)
			}
			// IPC supports old temporal fields that native Parquet intentionally does not.
			path := filepath.Join(t.TempDir(), "old20.arrow")
			if _, e = q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
				t.Fatal(e)
			}
			file, e := os.Open(path)
			if e != nil {
				t.Fatal(e)
			}
			rd, e := ipc.NewFileReader(file)
			if e != nil {
				file.Close()
				t.Fatal(e)
			}
			if rd.NumRecords() == 0 {
				t.Fatal("missing IPC data")
			}
			r, e := rd.RecordBatchAt(0)
			if e != nil {
				t.Fatal(e)
			}
			if r.NumRows() != 17 {
				t.Fatal("IPC row count")
			}
			for row := 0; row < 17; row++ {
				want := map[string]any{"old": structConstructValue(tc, row), "arrays": nestedListBRaw(row, false)}
				if v := structNestedArrowValue(r.Column(0), row); !reflect.DeepEqual(v, want) {
					t.Fatalf("IPC old20 row%d: %#v != %#v", row, v, want)
				}
			}
			// Arrow logical type must be the exact schema export type, including time units.
			expected, e := memory.ArrowSchema(df.Schema())
			if e != nil {
				t.Fatal(e)
			}
			if !r.Schema().Equal(expected) {
				t.Fatal("IPC logical metadata changed", r.Schema(), expected)
			}
			r.Release()
			rd.Close()
			file.Close()
			s.Close()
			again, e := c.ToAnySlice()
			if e != nil || !reflect.DeepEqual(again, got) {
				t.Fatal("old20 ownership", e)
			}
			df.Close()
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("old20 leak")
			}
		})
	}
}
func TestStructConstructNestedActualSpill(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 8 << 20
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	q := structNestedQuery(t, s, 1, 4).Sort(Desc("key"))
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	observer := &recursiveCRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
	done, stopped := make(chan struct{}), make(chan struct{})
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
		t.Fatal("actual multi-run proof missing", budget.Snapshot(), observer.maxRuns, observer.diskBytes)
	}
	rows := make([]int, 0, 260)
	for r := 64; r >= 0; r-- {
		for j := 0; j < 4; j++ {
			rows = append(rows, r)
		}
	}
	structNestedCheck(t, df, 1, rows)
	budget.Close()
	recursiveCNoFiles(t, dir)
	s.Close()
	structNestedCheck(t, df, 1, rows)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("spill owner leak")
	}
}
