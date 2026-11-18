package lazy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

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
	"gocolumn/source/inmemory"
)

// Input generation is shared; expectations below use literal values and row rules,
// not another query or the sink's readback as an oracle.
func fixedParquetFixture(t *testing.T, tc structGetCase, writer string) string {
	t.Helper()
	schema, b, _, _ := structGetFixture(tc, 130)
	schema = memory.NewSchema(append(schema.Fields, memory.Field{Name: "id", Type: memory.TypeInt64})...)
	ids := make([]int64, 130)
	for i := range ids {
		ids[i] = int64(i)
	}
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	path := filepath.Join(t.TempDir(), "native.parquet")
	if writer == "sink" {
		src, e := inmemory.New(schema, b)
		if e != nil {
			t.Fatal(e)
		}
		s := NewSession(engine.DefaultOptions())
		defer s.Close()
		if _, e = s.FromSource(src).SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 31}); e != nil {
			t.Fatal(e)
		}
	} else {
		alloc := am.NewCheckedAllocator(am.DefaultAllocator)
		defer alloc.AssertSize(t, 0)
		// Independent Arrow builders: do not reuse the memory.ArrowRecord payload converter.
		as, e := memory.ArrowSchema(schema)
		if e != nil {
			t.Fatal(e)
		}
		rb := array.NewRecordBuilder(alloc, as)
		root := rb.Field(0).(*array.StructBuilder)
		for row := 0; row < 130; row++ {
			root.Append(row%7 != 0)
			if row%7 != 0 {
				child := root.FieldBuilder(0)
				if row%5 == 0 {
					child.AppendNull()
				} else {
					switch tc.typ {
					case memory.TypeDate32:
						child.(*array.Date32Builder).Append(arrow.Date32(tc.value.(int32)))
					case memory.TypeTimestamp:
						child.(*array.TimestampBuilder).Append(arrow.Timestamp(tc.value.(int64)))
					case memory.TypeBool:
						child.(*array.BooleanBuilder).Append(true)
					default:
						if e := child.AppendValueFromString(fmt.Sprint(tc.value)); e != nil {
							t.Fatal(e)
						}
					}
				}
				root.FieldBuilder(1).(*array.Int64Builder).Append(int64(row + 10))
			}
			rb.Field(1).(*array.Int64Builder).Append(int64(row))
		}
		rec := rb.NewRecordBatch()
		rb.Release()
		tab := array.NewTableFromRecords(rec.Schema(), []arrow.RecordBatch{rec})
		rec.Release()
		defer tab.Release()
		out, e := os.Create(path)
		if e != nil {
			t.Fatal(e)
		}
		e = pqarrow.WriteTable(tab, out, 31, pq.NewWriterProperties(), pqarrow.DefaultWriterProps())
		out.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	r, e := file.NewParquetReader(f)
	if e != nil {
		f.Close()
		t.Fatal(e)
	}
	if r.NumRowGroups() != 5 {
		t.Fatal("fixture must exercise five row groups", r.NumRowGroups())
	}
	r.Close()
	f.Close()
	return path
}
func fixedParquetWant(tc structGetCase, row int) any {
	if row%7 == 0 || row%5 == 0 {
		return nil
	}
	if tc.typ == memory.TypeBool {
		return true
	}
	return tc.value
}
func fixedParquetCheck(t *testing.T, df DataFrame, tc structGetCase, filtered bool) {
	t.Helper()
	c, e := df.Column("v")
	if e != nil {
		t.Fatal(e)
	}
	if c.Type() != tc.typ {
		t.Fatal("logical type", c.Type(), tc.typ)
	}
	values, e := c.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	var want []any
	for r := 0; r < 130; r++ {
		if filtered && (r%7 == 0 || r+10 <= 60) {
			continue
		}
		want = append(want, fixedParquetWant(tc, r))
	}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("got %#v want %#v", values, want)
	}
}
func TestParquetFixedStructEntrypoints(t *testing.T) {
	for _, tc := range structGetCases() {
		switch tc.typ {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeDate32, memory.TypeTimestamp:
		default:
			continue
		}
		if tc.typ == memory.TypeTimestamp {
			tc.value = int64(1710054000000000000)
		}
		for _, writer := range []string{"sink", "arrow"} {
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				t.Run(fmt.Sprintf("%s/%s/%d", tc.typ, writer, mode), func(t *testing.T) {
					path := fixedParquetFixture(t, tc, writer)
					opts := engine.DefaultOptions()
					opts.KernelMode = mode
					opts.PreparedCacheBytes = 0
					s := NewSession(opts)
					defer func() {
						s.Close()
						if s.Engine().Memory().Used() != 0 {
							t.Error("reservation leak")
						}
					}()
					// A nullable parent + nullable child remains rejected by the legacy flat mode.
					if _, e := s.ScanParquet(path); e == nil {
						t.Fatal("default unexpectedly enabled nested read")
					}
					scan, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
					if e != nil {
						t.Fatal(e)
					}
					schema, e := scan.Schema()
					if e != nil {
						t.Fatal(e)
					}
					root := schema.Fields[0]
					if root.Type != memory.TypeStruct || !root.Nullable || len(root.Children) != 2 || root.Children[0].Type != tc.typ || !root.Children[0].Nullable || root.Children[1].Nullable {
						t.Fatal("schema", root)
					}
					q := scan.SelectExprs(StructGet(Col("root"), "a.b:中").Alias("v"))
					// Scalar-only projection must skip the Struct without losing the row domain.
					scalar, e := scan.SelectExprs(Col("id").Alias("id")).Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					ic, _ := scalar.Column("id")
					iv, e := ic.ToAnySlice()
					if e != nil {
						t.Fatal(e)
					}
					if len(iv) != 130 {
						t.Fatal("scalar projection length", len(iv))
					}
					for row, v := range iv {
						if v != int64(row) {
							t.Fatal("scalar projection", row, v)
						}
					}
					scalar.Close()
					// Repeated evaluation must start at the first row group, not reuse an exhausted cursor.
					for pass := 0; pass < 2; pass++ {
						df, e := q.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						fixedParquetCheck(t, df, tc, false)
						df.Close()
					}
					whole, e := scan.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					col, e := whole.Column("root")
					if e != nil {
						t.Fatal(e)
					}
					vv, e := col.ToAnySlice()
					if e != nil {
						t.Fatal(e)
					}
					if len(vv) != 130 {
						t.Fatal(len(vv))
					}
					for row, v := range vv {
						if row%7 == 0 {
							if v != nil {
								t.Fatal("parent NULL", row, v)
							}
							continue
						}
						m := v.(map[string]any)
						if m["a.b:中"] != fixedParquetWant(tc, row) || m["sibling"] != int64(row+10) {
							t.Fatal("independent parent/child NULL", row, m)
						}
					}
					whole.Close()
					filtered := scan.Filter(expr.GT(StructGet(Col("root"), "sibling"), expr.I64(60))).SelectExprs(StructGet(Col("root"), "a.b:中").Alias("v"))
					df, e := filtered.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					fixedParquetCheck(t, df, tc, true)
					df.Close()
					rebuilt := scan.SelectExprs(StructFrom(StructGet(Col("root"), "a.b:中").Alias("v"), StructGet(Col("root"), "sibling").Alias("n")).Alias("rebuilt"))
					ip := filepath.Join(t.TempDir(), "rebuilt.arrow")
					if _, e := rebuilt.SinkIPC(context.Background(), ip, IPCSinkOptions{}); e != nil {
						t.Fatal(e)
					}
					f, e := os.Open(ip)
					if e != nil {
						t.Fatal(e)
					}
					rd, e := ipc.NewFileReader(f)
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
					re, e := s.ScanArrow(records...)
					for _, r := range records {
						r.Release()
					}
					rd.Close()
					f.Close()
					if e != nil {
						t.Fatal(e)
					}
					df, e = re.SelectExprs(StructGet(Col("rebuilt"), "v").Alias("v")).Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					fixedParquetCheck(t, df, tc, false)
					df.Close()
					if tc.typ == memory.TypeTimestamp {
						z := zoneLazyHandle(t, "new_york")
						df, e := q.SelectExprs(DateInZone(Col("v"), z).Alias("day")).Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						dc, _ := df.Column("day")
						days, e := dc.ToAnySlice()
						if e != nil {
							t.Fatal(e)
						}
						if dc.Type() != memory.TypeDate32 {
							t.Fatal(dc.Type())
						}
						for row, d := range days {
							var want any
							if row%7 != 0 && row%5 != 0 {
								want = int32(19792)
							}
							if d != want {
								t.Fatal("NY local date", row, d, want)
							}
						}
						df.Close()
					}
					stream, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
					if e != nil {
						t.Fatal(e)
					}
					if !stream.Next(context.Background()) {
						t.Fatal(stream.Err())
					}
					stream.Close()
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					df, e = q.Collect(ctx)
					if e == nil {
						df.Close()
					}
					if !errors.Is(e, context.Canceled) {
						t.Fatal("cancel", e)
					}
					tinyOpts := opts
					tinyOpts.MemoryLimitBytes = 1
					tiny := NewSession(tinyOpts)
					tq, e := tiny.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
					if e != nil {
						t.Fatal(e)
					}
					df, e = tq.Collect(context.Background())
					if e == nil {
						df.Close()
					}
					if !errors.Is(e, memory.ErrBudgetExceeded) {
						tiny.Close()
						t.Fatal("budget", e)
					}
					tiny.Close()
					if tiny.Engine().Memory().Used() != 0 {
						t.Fatal("budget cleanup")
					}
					df, e = q.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					// Collected output must not borrow decoder pages or a live session.
					s.Close()
					fixedParquetCheck(t, df, tc, false)
					df.Close()
				})
			}
		}
	}
}
