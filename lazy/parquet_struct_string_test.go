package lazy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

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
)

func parquetStringLiteral(row int) string {
	return []string{"", "中🙂", "AbC", "éclair", "重复", "a\x00b", strings.Repeat("z", 4096)}[row%7]
}
func parquetStringWant(row int) any {
	if row%11 == 0 || row%5 == 0 {
		return nil
	}
	return parquetStringLiteral(row)
}
func parquetStringFixture(t *testing.T, writer string) string {
	t.Helper()
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "root", Nullable: true, Type: arrow.StructOf(arrow.Field{Name: "text.中", Type: arrow.BinaryTypes.String, Nullable: true}, arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64}, arrow.Field{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true})},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "top", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	rb := array.NewRecordBuilder(alloc, schema)
	root := rb.Field(0).(*array.StructBuilder)
	for row := 0; row < 257; row++ {
		root.Append(row%11 != 0)
		if row%11 != 0 {
			if row%5 == 0 {
				root.FieldBuilder(0).AppendNull()
			} else {
				root.FieldBuilder(0).(*array.StringBuilder).Append(parquetStringLiteral(row))
			}
			root.FieldBuilder(1).(*array.Int64Builder).Append(int64(row))
			if row%13 == 0 {
				root.FieldBuilder(2).AppendNull()
			} else {
				root.FieldBuilder(2).(*array.TimestampBuilder).Append(arrow.Timestamp(1710054000000000000))
			}
		}
		rb.Field(1).(*array.Int64Builder).Append(int64(row))
		if row%17 == 0 {
			rb.Field(2).AppendNull()
		} else {
			rb.Field(2).(*array.StringBuilder).Append(parquetStringLiteral(row))
		}
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	defer rec.Release()
	path := filepath.Join(t.TempDir(), "string.parquet")
	if writer == "sink" {
		s := NewSession(engine.DefaultOptions())
		scan, e := s.ScanArrow(rec)
		if e != nil {
			t.Fatal(e)
		}
		_, e = scan.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 31})
		s.Close()
		if e != nil {
			t.Fatal(e)
		}
	} else {
		tab := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
		defer tab.Release()
		out, e := os.Create(path)
		if e != nil {
			t.Fatal(e)
		}
		e = pqarrow.WriteTable(tab, out, 31, pq.NewWriterProperties(pq.WithDictionaryDefault(writer == "dictionary")), pqarrow.DefaultWriterProps())
		out.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	rd, e := file.NewParquetReader(f)
	if e != nil {
		t.Fatal(e)
	}
	if rd.NumRowGroups() != 9 {
		t.Fatal("row groups", rd.NumRowGroups())
	}
	dictGroups := 0
	for group := 0; group < rd.NumRowGroups(); group++ {
		cc, e := rd.MetaData().RowGroup(group).ColumnChunk(0)
		if e != nil {
			t.Fatal(e)
		}
		if cc.HasDictionaryPage() {
			dictGroups++
		}
		if writer == "plain" && cc.HasDictionaryPage() {
			t.Fatal("plain fixture has dictionary")
		}
	}
	if writer == "dictionary" && dictGroups == 0 {
		t.Fatal("dictionary fixture has no dictionary group")
	}
	t.Logf("writer=%s dictionaryGroups=%d/9", writer, dictGroups)
	rd.Close()
	f.Close()
	return path
}
func parquetStringCheck(t *testing.T, df DataFrame, filtered bool) {
	t.Helper()
	names := []string{"id", "v", "chars", "bytes", "day", "top"}
	cols := map[string][]any{}
	for _, name := range names {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		v, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		cols[name] = v
		if name == "day" && c.Type() != memory.TypeDate32 {
			t.Fatal("date metadata")
		}
		if (name == "v" || name == "top") && c.Type() != memory.TypeString {
			t.Fatal("string metadata")
		}
	}
	at := 0
	for row := 0; row < 257; row++ {
		v := parquetStringWant(row)
		if filtered && (row <= 60 || v == nil || !strings.Contains(parquetStringLiteral(row), "中")) {
			continue
		}
		var chars, bytes, day, top any
		if v != nil {
			chars = int64(utf8.RuneCountInString(v.(string)))
			bytes = int64(len(v.(string)))
		}
		if row%11 != 0 && row%13 != 0 {
			day = int32(19792)
		}
		if row%17 != 0 {
			top = parquetStringLiteral(row)
		}
		wants := []any{int64(row), v, chars, bytes, day, top}
		if at >= len(cols["id"]) {
			t.Fatal("missing row", row)
		}
		for i, n := range names {
			if cols[n][at] != wants[i] {
				t.Fatal(n, row, cols[n][at], wants[i])
			}
		}
		at++
	}
	if at != len(cols["id"]) {
		t.Fatal("extra rows", len(cols["id"]), at)
	}
}
func TestParquetStructStringComposition(t *testing.T) {
	for _, writer := range []string{"sink", "plain", "dictionary"} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("%s/%d", writer, mode), func(t *testing.T) {
				path := parquetStringFixture(t, writer)
				opts := engine.DefaultOptions()
				opts.PreparedCacheBytes = 0
				opts.KernelMode = mode
				s := NewSession(opts)
				defer func() {
					s.Close()
					if s.Engine().Memory().Used() != 0 {
						t.Error("reservation leak")
					}
				}()
				if _, e := s.ScanParquet(path); e == nil {
					t.Fatal("default nested broadened")
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
				if root.Type != memory.TypeStruct || !root.Nullable || len(root.Children) != 3 || root.Children[0].Type != memory.TypeString || !root.Children[0].Nullable || root.Children[1].Nullable {
					t.Fatal(root)
				}
				topOnly, e := scan.SelectExprs(Col("top").Alias("top")).Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				topCol, _ := topOnly.Column("top")
				topValues, e := topCol.ToAnySlice()
				if e != nil {
					t.Fatal(e)
				}
				if len(topValues) != 257 {
					t.Fatal("top projection rows")
				}
				for row, v := range topValues {
					var want any
					if row%17 != 0 {
						want = parquetStringLiteral(row)
					}
					if v != want {
						t.Fatal("top-only projection", row, v, want)
					}
				}
				topOnly.Close()
				value := StructGet(Col("root"), "text.中")
				day := DateInZone(StructGet(Col("root"), "ts"), zoneLazyHandle(t, "new_york"))
				query := func(q LazyFrame) LazyFrame {
					return q.SelectExprs(Col("id").Alias("id"), value.Alias("v"), expr.Length(value).Alias("chars"), expr.ByteLength(value).Alias("bytes"), day.Alias("day"), Col("top").Alias("top"))
				}
				q := query(scan)
				for pass := 0; pass < 2; pass++ {
					df, e := q.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					parquetStringCheck(t, df, false)
					df.Close()
				}
				filtered := query(scan.Filter(expr.And(expr.GT(StructGet(Col("root"), "n"), expr.I64(60)), expr.Contains(value, expr.Str("中")))))
				df, e := filtered.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				parquetStringCheck(t, df, true)
				df.Close()
				whole, e := scan.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				c, _ := whole.Column("root")
				vv, e := c.ToAnySlice()
				if e != nil {
					t.Fatal(e)
				}
				if len(vv) != 257 {
					t.Fatal(len(vv))
				}
				for row, v := range vv {
					if row%11 == 0 {
						if v != nil {
							t.Fatal("parent NULL", row)
						}
					} else {
						m := v.(map[string]any)
						if m["text.中"] != parquetStringWant(row) || m["n"] != int64(row) {
							t.Fatal("child NULL/empty", row, m)
						}
					}
				}
				whole.Close()
				// Materialize an always-valid Struct from the nullable extracted string and local day.
				rebuilt := q.SelectExprs(Col("id").Alias("id"), Col("chars").Alias("chars"), Col("bytes").Alias("bytes"), Col("top").Alias("top"), StructFrom(Col("v").Alias("v"), Col("day").Alias("day")).Alias("made"))
				ip := filepath.Join(t.TempDir(), "string.arrow")
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
				var rr []arrow.RecordBatch
				for i := 0; i < rd.NumRecords(); i++ {
					r, e := rd.RecordBatchAt(i)
					if e != nil {
						t.Fatal(e)
					}
					rr = append(rr, r)
				}
				back, e := s.ScanArrow(rr...)
				for _, r := range rr {
					r.Release()
				}
				rd.Close()
				f.Close()
				if e != nil {
					t.Fatal(e)
				}
				df, e = back.SelectExprs(Col("id").Alias("id"), StructGet(Col("made"), "v").Alias("v"), Col("chars").Alias("chars"), Col("bytes").Alias("bytes"), StructGet(Col("made"), "day").Alias("day"), Col("top").Alias("top")).Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				parquetStringCheck(t, df, false)
				df.Close()
				stream, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 32768})
				if e != nil {
					t.Fatal(e)
				}
				row, batches := 0, 0
				for stream.Next(context.Background()) {
					batch := stream.Batch()
					c, e := batch.Column("v")
					if e != nil {
						t.Fatal(e)
					}
					v, e := c.ToAnySlice()
					if e != nil {
						t.Fatal(e)
					}
					for _, got := range v {
						if got != parquetStringWant(row) {
							t.Fatal("batch boundary", row, got)
						}
						row++
					}
					batches++
				}
				e = stream.Err()
				stream.Close()
				if e != nil {
					t.Fatal(e)
				}
				t.Logf("stream rows=%d batches=%d", row, batches)
				if row != 257 || batches <= 9 {
					t.Fatal("multi-batch evidence", row, batches)
				}
				// Five projected leaves require a metadata allowance larger than 8 KiB.
				denied, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
				if e == nil {
					if denied.Next(context.Background()) {
						denied.Close()
						t.Fatal("undersized hard cap accepted")
					}
					e = denied.Err()
					denied.Close()
				}
				if !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal("hard-cap rejection", e)
				}
				early, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 32768})
				if e != nil {
					t.Fatal(e)
				}
				if !early.Next(context.Background()) {
					t.Fatal(early.Err())
				}
				early.Close()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				df, e = q.Collect(ctx)
				if e == nil {
					df.Close()
				}
				if !errors.Is(e, context.Canceled) {
					t.Fatal(e)
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
					t.Fatal(e)
				}
				tiny.Close()
				if tiny.Engine().Memory().Used() != 0 {
					t.Fatal("denial leak")
				}
				df, e = q.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				s.Close()
				parquetStringCheck(t, df, false)
				df.Close()
			})
		}
	}
}
