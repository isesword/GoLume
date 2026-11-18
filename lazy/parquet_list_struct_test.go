package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func listStructDRaw(row, depth int) map[string]any {
	m := listStructBExpected(row, depth)
	var ints any
	if row%7 != 0 {
		a := make([]any, row%4)
		for i := range a {
			if i != 1 {
				a[i] = int64(row*10 + i)
			}
		}
		ints = a
	}
	m["ints"] = ints
	m["text.中"] = []string{"", "中🙂", "a\x00b"}[row%3]
	if row%9 == 0 {
		m["text.中"] = nil
	}
	return m
}
func listStructDFixture(t *testing.T, depth int, dict bool) string {
	t.Helper()
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	base := listStructBArrow(t, alloc, depth, 0)
	fields := append([]arrow.Field(nil), base.Schema().Fields()...)
	base.Release()
	fields = append(fields, arrow.Field{Name: "ints", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: true}), Nullable: true}, arrow.Field{Name: "text.中", Type: arrow.BinaryTypes.String, Nullable: true})
	as := arrow.NewSchema(fields, nil)
	rb := array.NewRecordBuilder(alloc, as)
	for row := 0; row < 65; row++ {
		m := listStructDRaw(row, depth)
		for i, f := range fields {
			listStructBAppend(rb.Field(i), m[f.Name])
		}
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	defer rec.Release()
	tab := array.NewTableFromRecords(as, []arrow.RecordBatch{rec})
	defer tab.Release()
	path := filepath.Join(t.TempDir(), "list-struct.parquet")
	out, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	version := pq.DataPageV1
	if dict {
		version = pq.DataPageV2
	}
	e = pqarrow.WriteTable(tab, out, 23, pq.NewWriterProperties(pq.WithDataPageVersion(version), pq.WithDictionaryDefault(dict), pq.WithDataPageSize(64), pq.WithBatchSize(16)), pqarrow.DefaultWriterProps())
	out.Close()
	if e != nil {
		t.Fatal(e)
	}
	in, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer in.Close()
	rd, e := file.NewParquetReader(in)
	if e != nil {
		t.Fatal(e)
	}
	defer rd.Close()
	if rd.NumRowGroups() != 3 {
		t.Fatal("row groups", rd.NumRowGroups())
	}
	dictCount, dataCount := 0, 0
	for leaf := 0; leaf < 14; leaf++ {
		pr, e := rd.RowGroup(0).GetColumnPageReader(leaf)
		if e != nil {
			t.Fatal(e)
		}
		for pr.Next() {
			p := pr.Page()
			switch p.Type() {
			case file.PageTypeDictionaryPage:
				dictCount++
			case file.PageTypeDataPage, file.PageTypeDataPageV2:
				dataCount++
				want := file.PageTypeDataPage
				if dict {
					want = file.PageTypeDataPageV2
				}
				if p.Type() != want {
					t.Fatal("page version", p.Type(), want)
				}
			}
		}
		e = pr.Err()
		pr.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
	if dataCount < 14 || (dict && dictCount == 0) || (!dict && dictCount != 0) {
		t.Fatal("actual pages", dataCount, dictCount)
	}
	t.Logf("actual groups=3 first grouped-list data pages=%d dictionary pages=%d", dataCount, dictCount)
	return path
}
func listStructDCheck(t *testing.T, df DataFrame, depth int, names []string, descending, filtered bool) {
	t.Helper()
	for _, name := range names {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, 0, 65)
		for i := 0; i < 65; i++ {
			row := i
			if descending {
				row = 64 - i
			}
			if filtered && row <= 31 {
				continue
			}
			want = append(want, listStructDRaw(row, depth)[name])
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("D oracle %s got%v want%v", name, got, want)
		}
	}
}
func listStructDMetadata(t *testing.T, sc memory.Schema, depth int) {
	t.Helper()
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeDate32, memory.TypeTimestamp, memory.TypeString}
	for _, root := range []int{0, 1} {
		f := sc.Fields[root]
		if f.Type != memory.TypeList || !f.Nullable || len(f.Children) != 1 || f.Children[0].Type != memory.TypeStruct || !f.Children[0].Nullable || len(f.Children[0].Children) != 14 {
			t.Fatal("list metadata", f)
		}
		for i, c := range f.Children[0].Children {
			if c.Type != types[i] || c.Name != listStructBFields()[i].Name || c.Nullable != listStructBFields()[i].Nullable {
				t.Fatal("typed child metadata", i, c)
			}
		}
	}
	f := sc.Fields[2]
	for level := 1; level <= depth; level++ {
		if f.Type != memory.TypeStruct || f.Nullable != (level%2 == 1) {
			t.Fatal("outer metadata", level, f)
		}
		if level < depth {
			if f.Children[0].Name != "node.中" {
				t.Fatal("literal path")
			}
			f = f.Children[0]
		} else if f.Children[0].Type != memory.TypeList || f.Children[0].Children[0].Type != memory.TypeStruct {
			t.Fatal("outer list metadata", f)
		}
	}
}
func TestParquetListStructD(t *testing.T) {
	for _, depth := range []int{1, 7} {
		t.Run(fmt.Sprintf("ancestors%d", depth), func(t *testing.T) {
			path := listStructDFixture(t, depth, depth == 7)
			opts := engine.DefaultOptions()
			opts.PreparedCacheBytes = 0
			s := NewSession(opts)
			defer s.Close()
			q, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			sc, e := q.Schema()
			if e != nil {
				t.Fatal(e)
			}
			listStructDMetadata(t, sc, depth)
			left := sc.Fields[0]
			if left.Type != memory.TypeList || !left.Nullable || len(left.Children) != 1 || left.Children[0].Type != memory.TypeStruct || !left.Children[0].Nullable || len(left.Children[0].Children) != 14 {
				t.Fatal("group metadata", left)
			}
			for i, f := range left.Children[0].Children {
				if f.Name != listStructBFields()[i].Name || f.Nullable != listStructBFields()[i].Nullable {
					t.Fatal("item metadata", i, f)
				}
			}
			derived := q.WithColumns(ListLength(Col("left")).Alias("len"), StructGet(ListGet(Col("left"), Col("index")), "code.中").Alias("code"), Named(listStructBOuterExpr(depth), "events"))
			names := []string{"left", "right", "outer", "id", "index", "ints", "text.中", "len", "code", "events"}
			df, e := derived.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			listStructDCheck(t, df, depth, names, false, false)
			pd, e := derived.Select("right", "outer", "id", "text.中", "events").Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			listStructDCheck(t, pd, depth, []string{"right", "outer", "id", "text.中", "events"}, false, false)
			pd.Close()
			fd, e := derived.Filter(expr.GT(Col("id"), expr.I64(31))).Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			listStructDCheck(t, fd, depth, names, false, true)
			fd.Close()
			ip := filepath.Join(t.TempDir(), "owned.arrow")
			if _, e = derived.SinkIPC(context.Background(), ip, IPCSinkOptions{}); e != nil {
				t.Fatal(e)
			}
			in, e := os.Open(ip)
			if e != nil {
				t.Fatal(e)
			}
			rd, e := ipc.NewFileReader(in)
			if e != nil {
				t.Fatal(e)
			}
			var records []arrow.RecordBatch
			for i := 0; i < rd.NumRecords(); i++ {
				r, e := rd.RecordBatchAt(i)
				if e != nil {
					t.Fatal(e)
				}
				records = append(records, r)
			}
			aq, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeStructList: true}, records...)
			for _, r := range records {
				r.Release()
			}
			rd.Close()
			in.Close()
			if e != nil {
				t.Fatal(e)
			}
			ad, e := aq.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer ad.Close()
			listStructDCheck(t, ad, depth, names, false, false)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			cd, ce := derived.Collect(ctx)
			if ce == nil {
				cd.Close()
			}
			if !errors.Is(ce, context.Canceled) {
				t.Fatal("cancel", ce)
			}
			tinyOpts := opts
			tinyOpts.MemoryLimitBytes = 1
			tiny := NewSession(tinyOpts)
			tq, te := tiny.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if te != nil {
				t.Fatal(te)
			}
			td, te := tq.Collect(context.Background())
			if te == nil {
				td.Close()
			}
			tiny.Close()
			if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
				t.Fatal("budget", te)
			}
			reader, e := derived.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 2 << 20})
			if e != nil {
				t.Fatal(e)
			}
			if !reader.Next(context.Background()) {
				t.Fatal(reader.Err())
			}
			reader.Close()
			sinkPath := filepath.Join(t.TempDir(), "supported.parquet")
			if _, e = q.SinkParquet(context.Background(), sinkPath, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
				t.Fatal(e)
			}
			rq, e := s.ScanParquetWithOptions(sinkPath, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			roundDF, e := rq.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			listStructDCheck(t, roundDF, depth, []string{"left", "right", "outer", "id", "index", "ints", "text.中"}, false, false)
			roundDF.Close()
			s.Close()
			listStructDMetadata(t, df.Schema(), depth)
			listStructDMetadata(t, ad.Schema(), depth)
			listStructDCheck(t, df, depth, names, false, false)
			listStructDCheck(t, ad, depth, names, false, false)
			df.Close()
			ad.Close()
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("owned leak")
			}
			if depth == 7 {
				listStructDSpill(t, path, depth)
			}
		})
	}
}
func listStructDSpill(t *testing.T, path string, depth int) {
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
	df, e := q.Sort(Desc("id")).Collect(observer)
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	if budget.Snapshot().SpilledBytes <= 0 || observer.maxRuns < 2 || observer.diskBytes <= 0 {
		t.Fatalf("spill proof bytes%d runs%d disk%d", budget.Snapshot().SpilledBytes, observer.maxRuns, observer.diskBytes)
	}
	t.Logf("Parquet ListStruct actual spill=%d runs=%d disk=%d", budget.Snapshot().SpilledBytes, observer.maxRuns, observer.diskBytes)
	names := []string{"left", "right", "outer", "id", "index", "ints", "text.中"}
	listStructDCheck(t, df, depth, names, true, false)
	budget.Close()
	recursiveCNoFiles(t, dir)
	s.Close()
	listStructDCheck(t, df, depth, names, true, false)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("spill owner leak")
	}
}
