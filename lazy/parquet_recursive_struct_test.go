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
	"gocolumn/kernel"
	"gocolumn/memory"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func recursiveDRaw(depth, level, row int) any {
	if level%2 == 1 && row%(level+3) == 0 {
		return nil
	}
	if level < depth {
		return map[string]any{"node.中": recursiveDRaw(depth, level+1, row)}
	}
	m := map[string]any{"v": int64(1000 + row), "s": nil, "ints": nil, "words": nil}
	if row%5 == 0 {
		return m
	}
	words := []string{"", "中🙂", "a\x00b"}
	m["s"] = words[row%3]
	n := row % 4
	if row == 21 {
		n = 129
	}
	ints, ss := make([]any, n), make([]any, n)
	for j := 0; j < n; j++ {
		if j%3 != 1 {
			ints[j] = int64((row + j) % 3)
			ss[j] = words[(row+j)%3]
		}
	}
	m["ints"] = ints
	m["words"] = ss
	return m
}
func recursiveDAppend(b array.Builder, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch a := b.(type) {
	case *array.StructBuilder:
		a.Append(true)
		m := v.(map[string]any)
		for i, f := range a.Type().(*arrow.StructType).Fields() {
			recursiveDAppend(a.FieldBuilder(i), m[f.Name])
		}
	case *array.Int64Builder:
		a.Append(v.(int64))
	case *array.StringBuilder:
		a.Append(v.(string))
	case *array.ListBuilder:
		a.Append(true)
		for _, e := range v.([]any) {
			recursiveDAppend(a.ValueBuilder(), e)
		}
	default:
		panic(fmt.Sprintf("fixture builder %T", b))
	}
}
func recursiveDFixture(t *testing.T, depth int, writer string) string {
	t.Helper()
	schema, _ := recursiveBFixture(t, depth, 0)
	as, e := memory.ArrowSchema(schema)
	if e != nil {
		t.Fatal(e)
	}
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	rb := array.NewRecordBuilder(alloc, as)
	for row := 0; row < 65; row++ {
		recursiveDAppend(rb.Field(0), recursiveDRaw(depth, 1, row))
		recursiveDAppend(rb.Field(1), int64(row))
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	defer rec.Release()
	tab := array.NewTableFromRecords(as, []arrow.RecordBatch{rec})
	defer tab.Release()
	path := filepath.Join(t.TempDir(), "recursive.parquet")
	out, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	version := pq.DataPageV1
	if writer == "v2dict" {
		version = pq.DataPageV2
	}
	e = pqarrow.WriteTable(tab, out, 23, pq.NewWriterProperties(pq.WithDataPageVersion(version), pq.WithDictionaryDefault(writer == "v2dict"), pq.WithDataPageSize(64), pq.WithBatchSize(64)), pqarrow.DefaultWriterProps())
	out.Close()
	if e != nil {
		t.Fatal(e)
	}
	in, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	rd, e := file.NewParquetReader(in)
	if e != nil {
		t.Fatal(e)
	}
	defer rd.Close()
	defer in.Close()
	if rd.NumRowGroups() != 3 {
		t.Fatal("row groups", rd.NumRowGroups())
	}
	dictionaries := 0
	for _, leaf := range []int{1, 2, 3} {
		pr, e := rd.RowGroup(0).GetColumnPageReader(leaf)
		if e != nil {
			t.Fatal(e)
		}
		pages := 0
		dict := false
		for pr.Next() {
			p := pr.Page()
			switch p.Type() {
			case file.PageTypeDictionaryPage:
				dict = true
			case file.PageTypeDataPage, file.PageTypeDataPageV2:
				pages++
				want := file.PageTypeDataPage
				if writer == "v2dict" {
					want = file.PageTypeDataPageV2
				}
				if p.Type() != want {
					t.Fatal("actual page version", p.Type(), want)
				}
			}
		}
		e = pr.Err()
		pr.Close()
		if e != nil {
			t.Fatal(e)
		}
		if pages == 0 {
			t.Fatal("no data pages")
		}
		if dict {
			dictionaries++
		}
		if writer == "v1plain" && dict {
			t.Fatal("plain unexpectedly dictionary")
		}
		t.Logf("d%d %s leaf%d actual pages=%d dict=%v", depth, writer, leaf, pages, dict)
	}
	if writer == "v2dict" && dictionaries == 0 {
		t.Fatal("no actual dictionary page")
	}
	return path
}
func recursiveDCheck(t *testing.T, df DataFrame, depth int, descending bool) {
	t.Helper()
	for _, name := range []string{"id", "root"} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, 65)
		for i := range want {
			r := i
			if descending {
				r = 64 - i
			}
			if name == "id" {
				want[i] = int64(r)
			} else {
				want[i] = recursiveDRaw(depth, 1, r)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("recursive Parquet oracle %s got%v want%v", name, got, want)
		}
	}
}
func TestParquetRecursiveStructD(t *testing.T) {
	for _, depth := range []int{2, 8} {
		for _, writer := range []string{"v1plain", "v2dict"} {
			t.Run(fmt.Sprintf("d%d/%s", depth, writer), func(t *testing.T) {
				path := recursiveDFixture(t, depth, writer)
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
					t.Run(fmt.Sprintf("m%d", mode), func(t *testing.T) {
						opts := engine.DefaultOptions()
						opts.KernelMode = mode
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
						f := sc.Fields[0]
						for level := 1; level <= depth; level++ {
							if f.Type != memory.TypeStruct || f.Nullable != (level%2 == 1) {
								t.Fatal("ancestor metadata", level, f)
							}
							if level < depth {
								f = f.Children[0]
							}
						}
						if len(f.Children) != 4 || f.Children[0].Nullable || !f.Children[1].Nullable {
							t.Fatal("leaf metadata")
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
							t.Fatal("native reader budget", te)
						}
						cancelCtx, cancel := context.WithCancel(context.Background())
						cancel()
						cd, ce := q.Collect(cancelCtx)
						if ce == nil {
							cd.Close()
						}
						if !errors.Is(ce, context.Canceled) {
							t.Fatal("native reader cancel", ce)
						}
						df, e := q.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						recursiveDCheck(t, df, depth, false)
						df.Close()
						projected := q.SelectExprs(Named(recursiveBLeaf(depth, "v"), "v"), expr.ListLength(recursiveBLeaf(depth, "ints")).Alias("n"), expr.ListGet(recursiveBLeaf(depth, "words"), expr.I64(0)).Alias("head"))
						pd, e := projected.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						for _, name := range []string{"v", "n", "head"} {
							c, e := pd.Column(name)
							if e != nil {
								t.Fatal(e)
							}
							got, e := c.ToAnySlice()
							if e != nil {
								t.Fatal(e)
							}
							want := make([]any, 65)
							for row := range want {
								node := recursiveDRaw(depth, 1, row)
								for l := 1; l < depth && node != nil; l++ {
									node = node.(map[string]any)["node.中"]
								}
								if node == nil {
									continue
								}
								m := node.(map[string]any)
								switch name {
								case "v":
									want[row] = m["v"]
								case "n":
									if m["ints"] != nil {
										want[row] = int64(len(m["ints"].([]any)))
									}
								case "head":
									if m["words"] != nil && len(m["words"].([]any)) > 0 {
										want[row] = m["words"].([]any)[0]
									}
								}
							}
							if !reflect.DeepEqual(got, want) {
								t.Fatal("getter oracle", name, got, want)
							}
						}
						pd.Close()
						rewritten := filepath.Join(t.TempDir(), "rewrite.parquet")
						if _, e = q.SinkParquet(context.Background(), rewritten, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
							t.Fatal(e)
						}
						rq, e := s.ScanParquetWithOptions(rewritten, ParquetScanOptions{NativeNested: true})
						if e != nil {
							t.Fatal(e)
						}
						df, e = rq.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						recursiveDCheck(t, df, depth, false)
						df.Close()
						ip := filepath.Join(t.TempDir(), "tree.arrow")
						if _, e = q.SinkIPC(context.Background(), ip, IPCSinkOptions{}); e != nil {
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
						iq, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeRecursiveStruct: true}, records...)
						for _, r := range records {
							r.Release()
						}
						rd.Close()
						in.Close()
						if e != nil {
							t.Fatal(e)
						}
						df, e = iq.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						s.Close()
						recursiveDCheck(t, df, depth, false)
						df.Close()
						if s.Engine().Memory().Used() != 0 {
							t.Fatal("owned leak")
						}
						if depth == 8 && writer == "v2dict" {
							so := opts
							so.ExecutionPolicy = engine.ExecutionBounded
							so.MemoryLimitBytes = 2 << 20
							dir := t.TempDir()
							so.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 32 << 20}
							ss := NewSession(so)
							sq, e := ss.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
							if e != nil {
								t.Fatal(e)
							}
							budget, e := ss.Engine().BeginQueryMemory()
							if e != nil {
								t.Fatal(e)
							}
							ctx := &recursiveCRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
							out, e := sq.Sort(Desc("id")).Collect(ctx)
							if e != nil {
								t.Fatal(e)
							}
							if budget.Snapshot().SpilledBytes == 0 || ctx.maxRuns < 2 {
								t.Fatal("Parquet-to-spill actual evidence missing")
							}
							t.Logf("Parquet-to-spill bytes=%d runs=%d", budget.Snapshot().SpilledBytes, ctx.maxRuns)
							budget.Close()
							ss.Close()
							recursiveDCheck(t, out, depth, true)
							out.Close()
							recursiveCNoFiles(t, dir)
							if ss.Engine().Memory().Used() != 0 {
								t.Fatal("spill owner leak")
							}
						}
						for _, old := range []ParquetScanOptions{{}} {
							o := NewSession(opts)
							_, e := o.ScanParquetWithOptions(path, old)
							o.Close()
							if e == nil {
								t.Fatal("old flag widened", old)
							}
						}
					})
				}
			})
		}
	}
}

func TestParquetRecursiveStructDDepth9Rejected(t *testing.T) {
	var dt arrow.DataType = arrow.StructOf(arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Int64})
	var raw any = map[string]any{"v": int64(1)}
	for level := 1; level < 9; level++ {
		dt = arrow.StructOf(arrow.Field{Name: "node.中", Type: dt})
		raw = map[string]any{"node.中": raw}
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "root", Type: dt}}, nil)
	rb := array.NewRecordBuilder(am.DefaultAllocator, schema)
	recursiveDAppend(rb.Field(0), raw)
	rec := rb.NewRecordBatch()
	rb.Release()
	defer rec.Release()
	tab := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer tab.Release()
	path := filepath.Join(t.TempDir(), "depth9.parquet")
	out, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	e = pqarrow.WriteTable(tab, out, 1, pq.NewWriterProperties(), pqarrow.DefaultWriterProps())
	out.Close()
	if e != nil {
		t.Fatal(e)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	if _, e = s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true}); e == nil {
		t.Fatal("depth9 native Parquet accepted")
	}
}
