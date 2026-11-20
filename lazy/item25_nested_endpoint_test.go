package lazy

import (
	"context"
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
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func item25Get(v any) any {
	if v == nil {
		return nil
	}
	a := v.([]any)
	if len(a) == 0 {
		return nil
	}
	return a[0]
}
func item25Root(i int, text bool) any {
	var v any = int64(i*10 + 7)
	if text {
		v = fmt.Sprintf("leaf-%02d-界", i)
	}
	switch i % 6 {
	case 0:
		return nil
	case 1:
		return []any{}
	case 2:
		return []any{nil}
	case 3:
		return []any{[]any{}}
	case 4:
		return []any{[]any{[]any{nil, v}}}
	default:
		return []any{[]any{[]any{v}}}
	}
}
func item25Fixture(t *testing.T, text bool) string {
	t.Helper()
	leaf := arrow.DataType(arrow.PrimitiveTypes.Int64)
	if text {
		leaf = arrow.BinaryTypes.String
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "root", Type: n38TripleType(leaf, false), Nullable: true}}, nil)
	rb := array.NewRecordBuilder(am.DefaultAllocator, schema)
	defer rb.Release()
	for i := 0; i < 48; i++ {
		rb.Field(0).(*array.Int64Builder).Append(int64((47 - i) % 4))
		rb.Field(1).(*array.Int64Builder).Append(int64(i))
		n38Append(rb.Field(2), item25Root(i, text), 3)
	}
	r := rb.NewRecordBatch()
	defer r.Release()
	tab := array.NewTableFromRecords(schema, []arrow.RecordBatch{r})
	defer tab.Release()
	p := filepath.Join(t.TempDir(), "input.parquet")
	f, e := os.Create(p)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if e = pqarrow.WriteTable(tab, f, 2, pq.NewWriterProperties(pq.WithDictionaryDefault(false)), pqarrow.DefaultWriterProps()); e != nil {
		t.Fatal(e)
	}
	return p
}
func TestItem25NestedNavigationExternalEndpoints(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("string%v", text), func(t *testing.T) {
			path := item25Fixture(t, text)
			o := engine.DefaultOptions()
			o.Workers = 1
			o.Pipeline.Workers = 1
			o.Pipeline.MinMorselRows = 1
			o.Pipeline.MaxMorselRows = 1
			o.ExecutionPolicy = engine.ExecutionBounded
			o.MemoryLimitBytes = 32 << 20
			o.PreparedCacheBytes = 0
			spill := t.TempDir()
			o.Spill = engine.SpillOptions{Directory: spill, MaxBytes: 64 << 20}
			s := NewSession(o)
			defer s.Close()
			base, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			// An expression-owned List2 lives beside the original native List3 through
			// external sorting; downstream navigation acts on the restored List2 owner.
			picked := ListGet(Col("root"), expr.I64(0))
			q := base.SelectExprs(Col("k").Alias("k"), Col("id").Alias("id"), Col("root").Alias("root"), picked.Alias("picked")).Sort(Asc("k"))
			q = q.SelectExprs(Col("id").Alias("id"), Col("root").Alias("root"), Col("picked").Alias("picked"), ListGet(ListGet(Col("picked"), expr.I64(0)), expr.I64(0)).Alias("leaf"))
			report, e := q.Analyze(context.Background())
			if e != nil {
				t.Fatalf("cross-feature analyze: %v\n%s", e, report.String())
			}
			actual := false
			for _, op := range report.ExecuteOperators {
				if op.Attributes["strategy"] == "external-stable" {
					runs, _ := strconv.Atoi(op.Attributes["spill_runs"])
					bytes, _ := strconv.ParseInt(op.Attributes["spill_bytes"], 10, 64)
					if runs < 2 || bytes <= 0 {
						t.Fatal("not real multirun", op.Attributes)
					}
					t.Logf("actual external attributes=%v", op.Attributes)
					actual = true
				}
			}
			if !actual {
				t.Fatal("no executed external diagnostics", report.String())
			}
			// Fixed ordinal oracle: k=(47-id)%4, ascending stable ties => id mod4=3,2,1,0.
			ids := make([]int, 0, 48)
			for _, rem := range []int{3, 2, 1, 0} {
				for i := rem; i < 48; i += 4 {
					ids = append(ids, i)
				}
			}
			for _, kind := range []string{"ipc", "parquet"} {
				t.Run(kind, func(t *testing.T) {
					p := filepath.Join(t.TempDir(), "out."+kind)
					at := 0
					check := func(r arrow.RecordBatch) {
						if len(r.Schema().Fields()) != 4 {
							t.Fatal(r.Schema())
						}
						if !r.Schema().Field(1).Nullable || !r.Schema().Field(2).Nullable || !r.Schema().Field(3).Nullable {
							t.Fatal("nullability", r.Schema())
						}
						leaf := arrow.DataType(arrow.PrimitiveTypes.Int64)
						if text {
							leaf = arrow.BinaryTypes.String
						}
						n38CheckType(t, r.Schema().Field(1).Type, n38TripleType(leaf, false))
						n38CheckType(t, r.Schema().Field(2).Type, arrow.ListOf(arrow.ListOf(leaf)))
						n38CheckType(t, r.Schema().Field(3).Type, leaf)
						for row := 0; row < int(r.NumRows()); row++ {
							if at >= len(ids) {
								t.Fatal("extra row")
							}
							id := ids[at]
							root := item25Root(id, text)
							pick := item25Get(root)
							want := []any{int64(id), root, pick, item25Get(item25Get(pick))}
							for col, w := range want {
								if got := n38Read(r.Column(col), row); !reflect.DeepEqual(got, w) {
									t.Fatalf("ordinal%d col%d got%#v want%#v", at, col, got, w)
								}
							}
							at++
						}
					}
					if kind == "ipc" {
						if _, e = q.SinkIPC(context.Background(), p, IPCSinkOptions{}); e != nil {
							t.Fatal(e)
						}
						f, e := os.Open(p)
						if e != nil {
							t.Fatal(e)
						}
						defer f.Close()
						r, e := ipc.NewFileReader(f)
						if e != nil {
							t.Fatal(e)
						}
						defer r.Close()
						for i := 0; i < r.NumRecords(); i++ {
							b, e := r.RecordBatchAt(i)
							if e != nil {
								t.Fatal(e)
							}
							check(b)
							b.Release()
						}
					} else {
						if _, e = q.SinkParquet(context.Background(), p, ParquetSinkOptions{RowGroupRows: 2}); e != nil {
							t.Fatal(e)
						}
						f, e := file.OpenParquetFile(p, false)
						if e != nil {
							t.Fatal(e)
						}
						defer f.Close()
						r, e := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
						if e != nil {
							t.Fatal(e)
						}
						tab, e := r.ReadTable(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						defer tab.Release()
						rr := array.NewTableReader(tab, 2)
						defer rr.Release()
						for rr.Next() {
							check(rr.RecordBatch())
						}
						if rr.Err() != nil {
							t.Fatal(rr.Err())
						}
					}
					if at != 48 || s.Engine().Memory().Used() != 0 {
						t.Fatal("rows/leak", at, s.Engine().Memory().Used())
					}
					if e := filepath.WalkDir(spill, func(path string, entry os.DirEntry, err error) error {
						if err != nil {
							return err
						}
						if !entry.IsDir() {
							return fmt.Errorf("spill artifact remained: %s", path)
						}
						return nil
					}); e != nil {
						t.Fatal(e)
					}

				})
			}
			t.Log("fixture rows48 rowgroup2 batch1 workers1 memory32MiB disk64MiB; Int64/String fixed ordinal oracle; actual multi-run + two independent readbacks")
		})
	}
}
