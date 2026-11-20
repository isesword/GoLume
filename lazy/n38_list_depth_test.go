package lazy

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/memory"
)

func n38TripleType(leaf arrow.DataType, required bool) arrow.DataType {
	for i := 0; i < 3; i++ {
		leaf = arrow.ListOfField(arrow.Field{Name: "item", Type: leaf, Nullable: !required})
	}
	return leaf
}
func n38Append(b array.Builder, v any, depth int) {
	if depth == 0 {
		if v == nil {
			b.AppendNull()
			return
		}
		switch x := b.(type) {
		case *array.Int64Builder:
			x.Append(v.(int64))
		case *array.StringBuilder:
			x.Append(v.(string))
		default:
			panic("fixture leaf")
		}
		return
	}
	x := b.(*array.ListBuilder)
	if v == nil {
		x.Append(false)
		n38HiddenChild(x.ValueBuilder(), depth-1)
		return
	}
	x.Append(true)
	for _, child := range v.([]any) {
		n38Append(x.ValueBuilder(), child, depth-1)
	}
}

// Hidden physical children under NULL ancestors must never surface as rows.
func n38HiddenChild(b array.Builder, depth int) {
	if depth > 0 {
		x := b.(*array.ListBuilder)
		x.Append(true)
		n38HiddenChild(x.ValueBuilder(), depth-1)
		return
	}
	switch x := b.(type) {
	case *array.Int64Builder:
		x.Append(123456789)
	case *array.StringBuilder:
		x.Append("hidden-NULL-descendant")
	}
}
func n38Read(a arrow.Array, i int) any {
	if a.IsNull(i) {
		return nil
	}
	switch x := a.(type) {
	case *array.List:
		lo, hi := x.ValueOffsets(i)
		v := make([]any, 0, hi-lo)
		for j := lo; j < hi; j++ {
			v = append(v, n38Read(x.ListValues(), int(j)))
		}
		return v
	case *array.Struct:
		out := map[string]any{}
		dt := x.DataType().(*arrow.StructType)
		for j, f := range dt.Fields() {
			out[f.Name] = n38Read(x.Field(j), i)
		}
		return out
	case *array.Int64:
		return x.Value(i)
	case *array.String:
		return x.Value(i)
	case *array.LargeString:
		return x.Value(i)
	default:
		panic(fmt.Sprintf("unexpected array %T", a))
	}
}
func n38Fixture(t *testing.T, required bool) (string, [][]any, *arrow.Schema) {
	t.Helper()
	big := strings.Repeat("深🙂", 8192)
	ints := []any{nil, []any{}, []any{nil, []any{}}, []any{[]any{nil, []any{}}}, []any{[]any{[]any{int64(-9), nil, int64(42)}, []any{int64(0)}}}, []any{[]any{[]any{int64(99)}}}}
	strs := []any{nil, []any{}, []any{nil, []any{}}, []any{[]any{nil, []any{}}}, []any{[]any{[]any{"a", nil, big}, []any{""}}}, []any{[]any{[]any{"tail"}}}}
	if required {
		ints = []any{[]any{[]any{[]any{int64(-1), int64(2)}}}, []any{}, []any{[]any{}}, []any{[]any{[]any{}}}}
		strs = []any{[]any{[]any{[]any{big, ""}}}, []any{}, []any{[]any{}}, []any{[]any{[]any{}}}}
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "ints", Type: n38TripleType(arrow.PrimitiveTypes.Int64, required), Nullable: !required}, {Name: "strs", Type: n38TripleType(arrow.BinaryTypes.String, required), Nullable: !required}}, nil)
	b := array.NewRecordBuilder(am.DefaultAllocator, schema)
	defer b.Release()
	want := make([][]any, len(ints))
	for i := range ints {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		n38Append(b.Field(1), ints[i], 3)
		n38Append(b.Field(2), strs[i], 3)
		want[i] = []any{int64(i), ints[i], strs[i]}
	}
	r := b.NewRecordBatch()
	defer r.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{r})
	defer tbl.Release()
	path := filepath.Join(t.TempDir(), "input.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if e = pqarrow.WriteTable(tbl, f, 2, pq.NewWriterProperties(pq.WithDictionaryDefault(false)), pqarrow.DefaultWriterProps()); e != nil {
		t.Fatalf("fixture encoding barrier:%v", e)
	}
	return path, want, schema
}
func n38CheckRecord(t *testing.T, r arrow.RecordBatch, want [][]any, at *int, expected *arrow.Schema) {
	t.Helper()
	for i := 0; i < int(r.NumRows()); i++ {
		if *at >= len(want) {
			t.Fatal("extra row")
		}
		for j, name := range []string{"k", "ints", "strs"} {
			ids := r.Schema().FieldIndices(name)
			if len(ids) != 1 {
				t.Fatal("missing field", name)
			}
			field := r.Schema().Field(ids[0])
			exp := expected.Field(expected.FieldIndices(name)[0])
			if field.Nullable != exp.Nullable {
				t.Fatalf("%s nullable metadata changed", name)
			}
			n38CheckType(t, field.Type, exp.Type)
			got := n38Read(r.Column(ids[0]), i)
			if !reflect.DeepEqual(got, want[*at][j]) {
				t.Fatalf("row%d field%s structure/value mismatch", *at, name)
			}
		}
		*at++
	}
}
func TestN38ListDepthPublicRoundTrip(t *testing.T) {
	for _, required := range []bool{false, true} {
		for _, batch := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("required%v_batch%d", required, batch), func(t *testing.T) {
				in, want, schema := n38Fixture(t, required)
				n38RoundTrip(t, in, want, batch, schema)
			})
		}
	}
}
func n38RoundTrip(t *testing.T, in string, want [][]any, batchRows int, expected *arrow.Schema) {
	o := engine.DefaultOptions()
	o.Workers = 1
	o.Pipeline.Workers = 1
	o.Pipeline.MinMorselRows = 1
	o.Pipeline.MaxMorselRows = batchRows
	o.MemoryLimitBytes = 32 << 20
	o.ExecutionPolicy = engine.ExecutionMemoryOnly
	o.PreparedCacheBytes = 0
	s := NewSession(o)
	defer s.Close()
	q, e := s.ScanParquetWithOptions(in, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatalf("FIRST BARRIER NativeNested scan schema: %v", e)
	}
	q = q.Select("strs", "k", "ints")
	scanCtx, scanDone := n38OwnerTrace(t, s, "scan")
	defer scanDone()
	rd, e := q.CollectBatches(scanCtx, BatchOptions{})
	if e != nil {
		t.Fatalf("FIRST BARRIER native batch open:%v", e)
	}
	defer rd.Close()
	at := 0
	var old BatchView
	for rd.Next(scanCtx) {
		if old.Valid() {
			t.Fatal("borrowed view survived Next")
		}
		v := rd.Batch()
		r, e := memory.ArrowRecord(am.DefaultAllocator, v.Schema(), v.state.batches[0])
		if e != nil {
			t.Fatalf("native Arrow export barrier:%v", e)
		}
		n38CheckRecord(t, r, want, &at, expected)
		r.Release()
		old = v
	}
	e = rd.Err()
	rd.Close()
	scanDone()
	if e != nil {
		t.Fatalf("native scan decode barrier:%v", e)
	}
	if at != len(want) || old.Valid() {
		t.Fatal("batch rows/lifetime")
	}
	for _, kind := range []string{"ipc", "parquet"} {
		t.Run(kind, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "output."+kind)
			at := 0
			sinkCtx, sinkDone := n38OwnerTrace(t, s, kind)
			defer sinkDone()
			if kind == "ipc" {
				if _, e := q.SinkIPC(sinkCtx, out, IPCSinkOptions{}); e != nil {
					t.Fatalf("IPC sink:%v", e)
				}
				f, e := os.Open(out)
				if e != nil {
					t.Fatal(e)
				}
				defer f.Close()
				r, e := ipc.NewFileReader(f)
				if e != nil {
					t.Fatal(e)
				}
				defer r.Close()
				for index := 0; index < r.NumRecords(); index++ {
					record, e := r.RecordBatchAt(index)
					if e != nil {
						t.Fatal(e)
					}
					n38CheckRecord(t, record, want, &at, expected)
					record.Release()
				}
			} else {
				if _, e := q.SinkParquet(sinkCtx, out, ParquetSinkOptions{RowGroupRows: 2}); e != nil {
					t.Fatalf("Parquet sink:%v", e)
				}
				f, e := file.OpenParquetFile(out, false)
				if e != nil {
					t.Fatal(e)
				}
				defer f.Close()
				ar, e := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
				if e != nil {
					t.Fatal(e)
				}
				tbl, e := ar.ReadTable(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer tbl.Release()
				r := array.NewTableReader(tbl, 2)
				defer r.Release()
				for r.Next() {
					n38CheckRecord(t, r.RecordBatch(), want, &at, expected)
				}
				if r.Err() != nil {
					t.Fatal(r.Err())
				}
			}
			sinkDone()
			if at != len(want) {
				t.Fatalf("readback rows%d want%d", at, len(want))
			}
			if s.Engine().Memory().Used() != 0 {
				t.Fatalf("retained bytes%d", s.Engine().Memory().Used())
			}
		})
	}
}

// Struct is outside the list chain: this preserves the already admitted parent
// shape, and does not introduce List<Struct> or alternating recursive profiles.
func TestN38ListDepthPublicParentStructAndDepth2Sibling(t *testing.T) {
	dt := arrow.StructOf(arrow.Field{Name: "cube", Type: n38TripleType(arrow.PrimitiveTypes.Int64, false), Nullable: true}, arrow.Field{Name: "sibling", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	depth2 := arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.BinaryTypes.String, Nullable: true}), Nullable: true})
	schema := arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "ints", Type: dt, Nullable: true}, {Name: "strs", Type: depth2, Nullable: true}}, nil)
	b := array.NewRecordBuilder(am.DefaultAllocator, schema)
	defer b.Release()
	st := b.Field(1).(*array.StructBuilder)
	cubes := []any{nil, []any{[]any{[]any{int64(-5), nil, int64(9)}}}, []any{}, []any{nil, []any{[]any{}}}}
	strings2 := []any{[]any{[]any{"a", nil, ""}}, nil, []any{}, []any{nil, []any{"last"}}}
	want := make([][]any, 4)
	for i := 0; i < 4; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		st.Append(i != 0)
		if i != 0 {
			n38Append(st.FieldBuilder(0), cubes[i], 3)
			st.FieldBuilder(1).(*array.Int64Builder).Append(int64(i * 10))
		}
		n38Append(b.Field(2), strings2[i], 2)
		var parent any
		if i != 0 {
			parent = map[string]any{"cube": cubes[i], "sibling": int64(i * 10)}
		}
		want[i] = []any{int64(i), parent, strings2[i]}
	}
	r := b.NewRecordBatch()
	defer r.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{r})
	defer tbl.Release()
	path := filepath.Join(t.TempDir(), "struct.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if e = pqarrow.WriteTable(tbl, f, 2, pq.NewWriterProperties(pq.WithDictionaryDefault(false)), pqarrow.DefaultWriterProps()); e != nil {
		t.Fatalf("parent fixture encode:%v", e)
	}
	for _, batch := range []int{1, 3} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) { n38RoundTrip(t, path, want, batch, schema) })
	}
}

// Observes query-accounted source and native encoders at context-check instants.
// The external Arrow oracle allocations are not claimed as query-budget charges.
type n38TraceContext struct {
	context.Context
	q                  *memory.QueryBudget
	mu                 sync.Mutex
	scan, ipc, parquet memory.BudgetSnapshot
}

func (c *n38TraceContext) Err() error {
	err := c.Context.Err()
	snap := c.q.Snapshot()
	c.mu.Lock()
	defer c.mu.Unlock()
	if snap.Operators["SourceScan"] > 0 && snap.UsedBytes > c.scan.UsedBytes {
		c.scan = snap
	}
	if snap.Operators["IPCSinkEncode"] > 0 && snap.UsedBytes > c.ipc.UsedBytes {
		c.ipc = snap
	}
	if snap.Operators["ParquetSinkEncode"] > 0 && snap.UsedBytes > c.parquet.UsedBytes {
		c.parquet = snap
	}
	return err
}
func n38OwnerTrace(t *testing.T, s *Session, stage string) (context.Context, func()) {
	t.Helper()
	q, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	observer := &n38TraceContext{Context: context.Background(), q: q}
	var once sync.Once
	done := func() {
		once.Do(func() {
			end := q.Snapshot()
			observer.mu.Lock()
			t.Logf("OWNER stage=%s finalUsed=%d finalClosed=%v peak=%d scan=%+v ipc=%+v parquet=%+v", stage, end.UsedBytes, end.Closed, end.PeakBytes, observer.scan, observer.ipc, observer.parquet)
			observer.mu.Unlock()
			if end.Closed {
				t.Error("borrowed budget unexpectedly closed by reader/sink")
			}
			if e := q.Close(); e != nil {
				t.Error(e)
			}
		})
	}
	return memory.WithQueryBudget(observer, q), done
}

func n38CheckType(t *testing.T, got, want arrow.DataType) {
	t.Helper()
	if got.ID() != want.ID() {
		t.Fatalf("type %v want %v", got, want)
	}
	switch w := want.(type) {
	case *arrow.ListType:
		g := got.(*arrow.ListType)
		if g.ElemField().Nullable != w.ElemField().Nullable {
			t.Fatal("list item nullability changed")
		}
		n38CheckType(t, g.Elem(), w.Elem())
	case *arrow.StructType:
		g := got.(*arrow.StructType)
		if g.NumFields() != w.NumFields() {
			t.Fatal("struct field count changed")
		}
		for i, f := range w.Fields() {
			a := g.Field(i)
			if a.Name != f.Name || a.Nullable != f.Nullable {
				t.Fatal("struct field metadata changed")
			}
			n38CheckType(t, a.Type, f.Type)
		}
	}
}
