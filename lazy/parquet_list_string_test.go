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
	"strings"
	"testing"
	"unicode/utf8"
)

var nativeStringListWords = []string{"", "中🙂", "AbC", "a\x00b", "éclair", strings.Repeat("z", 128)}

func nativeStringListWant(row int, l, e bool) any {
	if l && row%11 == 0 {
		return nil
	}
	n := row % 5
	if row%7 == 0 {
		n = 0
	}
	if row == 30 {
		n = 4096
	}
	if row == 62 {
		n = 2048
	}
	out := make([]any, n)
	for i := range out {
		if e && i%5 == 2 {
			continue
		}
		out[i] = nativeStringListWords[(row+i)%len(nativeStringListWords)]
		if row == 17 && i == 0 {
			out[i] = strings.Repeat("界", 2000)
		}
	}
	return out
}
func nativeStringListScalars(row int, l, e bool) (any, any, any, any, any) {
	items := nativeStringListWant(row, l, e)
	var length, head, third, chars, day any
	if items != nil {
		v := items.([]any)
		length = int64(len(v))
		if len(v) > 0 {
			head = v[0]
		}
		if len(v) > 2 {
			third = v[2]
		}
		if head != nil {
			chars = int64(utf8.RuneCountInString(head.(string)))
		}
	}
	if row%13 != 0 {
		day = int32(19792)
	}
	return length, head, third, chars, day
}
func nativeStringListFixture(t *testing.T, writer string, l, e bool) string {
	t.Helper()
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "items.中", Nullable: l, Type: arrow.ListOfField(arrow.Field{Name: "element", Type: arrow.BinaryTypes.String, Nullable: e})}, {Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "ts", Nullable: true, Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}}}, nil)
	rb := array.NewRecordBuilder(alloc, schema)
	lb := rb.Field(0).(*array.ListBuilder)
	vb := lb.ValueBuilder().(*array.StringBuilder)
	for row := 0; row < 130; row++ {
		want := nativeStringListWant(row, l, e)
		lb.Append(want != nil)
		if want != nil {
			for _, v := range want.([]any) {
				if v == nil {
					vb.AppendNull()
				} else {
					vb.Append(v.(string))
				}
			}
		}
		rb.Field(1).(*array.Int64Builder).Append(int64(row))
		if row%13 == 0 {
			rb.Field(2).AppendNull()
		} else {
			rb.Field(2).(*array.TimestampBuilder).Append(1710054000000000000)
		}
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	defer rec.Release()
	path := filepath.Join(t.TempDir(), "list-string.parquet")
	if writer == "sink" {
		s := NewSession(engine.DefaultOptions())
		scan, err := s.ScanArrow(rec)
		if err != nil {
			t.Fatal(err)
		}
		_, err = scan.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 31})
		s.Close()
		if err != nil {
			t.Fatal(err)
		}
	} else {
		version := pq.DataPageV1
		if strings.HasPrefix(writer, "v2") {
			version = pq.DataPageV2
		}
		dict := strings.HasSuffix(writer, "dict")
		tab := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
		defer tab.Release()
		out, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		err = pqarrow.WriteTable(tab, out, 31, pq.NewWriterProperties(pq.WithDataPageVersion(version), pq.WithDictionaryDefault(dict), pq.WithDataPageSize(512), pq.WithBatchSize(256)), pqarrow.DefaultWriterProps())
		out.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	in, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rd, err := file.NewParquetReader(in)
	if err != nil {
		t.Fatal(err)
	}
	if rd.NumRowGroups() != 5 {
		t.Fatal("rowgroups")
	}
	pr, err := rd.RowGroup(0).GetColumnPageReader(0)
	if err != nil {
		t.Fatal(err)
	}
	pages, maxLevels, dict := 0, int32(0), false
	for pr.Next() {
		p := pr.Page()
		switch p.Type() {
		case file.PageTypeDictionaryPage:
			dict = true
		case file.PageTypeDataPage, file.PageTypeDataPageV2:
			pages++
			if p.NumValues() > maxLevels {
				maxLevels = p.NumValues()
			}
			if writer != "sink" {
				want := file.PageTypeDataPage
				if strings.HasPrefix(writer, "v2") {
					want = file.PageTypeDataPageV2
				}
				if p.Type() != want {
					t.Fatal("actual page version", p.Type(), want)
				}
			}
		}
	}
	err = pr.Err()
	pr.Close()
	if err != nil {
		t.Fatal(err)
	}
	if writer != "sink" {
		// Arrow V2 keeps the long row whole; only V1 fixtures prove a row spanning pages.
		if pages < 2 || (strings.HasPrefix(writer, "v1") && maxLevels >= 4096) {
			t.Fatal("long-row page evidence", pages, maxLevels)
		}
		if dict != strings.HasSuffix(writer, "dict") {
			t.Fatal("actual dictionary", writer, dict)
		}
	}
	t.Logf("writer=%s pages=%d maxLevels=%d dictionary=%v", writer, pages, maxLevels, dict)
	rd.Close()
	in.Close()
	return path
}
func nativeStringListCheck(t *testing.T, df DataFrame, l, e, filtered bool) {
	t.Helper()
	names := []string{"id", "items", "length", "head", "third", "chars", "day", "made"}
	cols := map[string][]any{}
	for _, n := range names {
		c, err := df.Column(n)
		if err != nil {
			t.Fatal(err)
		}
		v, err := c.ToAnySlice()
		if err != nil {
			t.Fatal(err)
		}
		cols[n] = v
		if n == "items" && c.Type() != memory.TypeList {
			t.Fatal("list type")
		}
		if n == "day" && c.Type() != memory.TypeDate32 {
			t.Fatal("date type")
		}
	}
	at := 0
	for row := 0; row < 130; row++ {
		items := nativeStringListWant(row, l, e)
		length, head, third, chars, day := nativeStringListScalars(row, l, e)
		if filtered && (row <= 20 || head == nil || !strings.Contains(head.(string), "中")) {
			continue
		}
		want := []any{int64(row), items, length, head, third, chars, day, map[string]any{"items": items, "date": day}}
		if at >= len(cols["id"]) {
			t.Fatal("missing", row)
		}
		for i, n := range names {
			if !reflect.DeepEqual(cols[n][at], want[i]) {
				t.Fatalf("%s row=%d mismatch", n, row)
			}
		}
		at++
	}
	if at != len(cols["id"]) {
		t.Fatal("extra")
	}
}
func nativeStringListGroup(t *testing.T, df DataFrame, l, e bool) {
	t.Helper()
	cols := map[string][]any{}
	for _, n := range []string{"head", "count", "sum"} {
		c, err := df.Column(n)
		if err != nil {
			t.Fatal(err)
		}
		v, err := c.ToAnySlice()
		if err != nil {
			t.Fatal(err)
		}
		cols[n] = v
	}
	want := map[any][2]int64{}
	for row := 0; row < 130; row++ {
		length, head, _, _, _ := nativeStringListScalars(row, l, e)
		w := want[head]
		w[0]++
		if length != nil {
			w[1] += length.(int64)
		}
		want[head] = w
	}
	if len(cols["head"]) != len(want) {
		t.Fatal("groups")
	}
	seen := map[any]bool{}
	for i, k := range cols["head"] {
		w, ok := want[k]
		if !ok || seen[k] || cols["count"][i] != uint64(w[0]) || cols["sum"][i] != w[1] {
			t.Fatal("string group oracle", k)
		}
		seen[k] = true
	}
}
func TestParquetNativeStringListComposition(t *testing.T) {
	for bits := 0; bits < 4; bits++ {
		l, e := bits&1 != 0, bits&2 != 0
		for _, writer := range []string{"sink", "v1plain", "v1dict", "v2plain", "v2dict"} {
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				t.Run(fmt.Sprintf("LE%d/%s/%d", bits, writer, mode), func(t *testing.T) {
					path := nativeStringListFixture(t, writer, l, e)
					opts := engine.DefaultOptions()
					opts.KernelMode = mode
					opts.PreparedCacheBytes = 0
					s := NewSession(opts)
					defer func() {
						s.Close()
						if s.Engine().Memory().Used() != 0 {
							t.Error("leak")
						}
					}()
					for _, o := range []ParquetScanOptions{{}} {
						if _, err := s.ScanParquetWithOptions(path, o); err == nil {
							t.Fatal("scope/conflict", o)
						}
					}
					scan, err := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
					if err != nil {
						t.Fatal(err)
					}
					schema, err := scan.Schema()
					if err != nil {
						t.Fatal(err)
					}
					f := schema.Fields[0]
					if f.Type != memory.TypeList || f.Nullable != l || len(f.Children) != 1 || f.Children[0].Type != memory.TypeString || f.Children[0].Nullable != e {
						t.Fatal("L/E schema", f)
					}
					items := Col("items.中")
					head := ListGet(items, expr.I64(0))
					day := DateInZone(Col("ts"), zoneLazyHandle(t, "new_york"))
					project := func(q LazyFrame) LazyFrame {
						return q.SelectExprs(Col("id").Alias("id"), items.Alias("items"), ListLength(items).Alias("length"), head.Alias("head"), ListGet(items, expr.I64(2)).Alias("third"), expr.Length(head).Alias("chars"), day.Alias("day"), StructFrom(items.Alias("items"), day.Alias("date")).Alias("made"))
					}
					q := project(scan)
					for pass := 0; pass < 2; pass++ {
						df, err := q.Collect(context.Background())
						if err != nil {
							t.Fatal(err)
						}
						nativeStringListCheck(t, df, l, e, false)
						df.Close()
					}
					filtered := project(scan.Filter(expr.And(expr.GT(Col("id"), expr.I64(20)), expr.Contains(head, expr.Str("中")))))
					df, err := filtered.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					nativeStringListCheck(t, df, l, e, true)
					df.Close()
					group := q.SelectExprs(Col("head").Alias("head"), Col("length").Alias("length")).GroupBy("head").Agg(Count("count"), Sum("length", "sum"))
					df, err = group.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					nativeStringListGroup(t, df, l, e)
					df.Close()
					rewritten := filepath.Join(t.TempDir(), "rewrite.parquet")
					if _, err := scan.SinkParquet(context.Background(), rewritten, ParquetSinkOptions{RowGroupRows: 17}); err != nil {
						t.Fatal(err)
					}
					re, err := s.ScanParquetWithOptions(rewritten, ParquetScanOptions{NativeNested: true})
					if err != nil {
						t.Fatal(err)
					}
					rs, err := re.Schema()
					if err != nil {
						t.Fatal(err)
					}
					rf := rs.Fields[0]
					if rf.Type != memory.TypeList || rf.Nullable != l || len(rf.Children) != 1 || rf.Children[0].Type != memory.TypeString || rf.Children[0].Nullable != e {
						t.Fatal("rewritten metadata")
					}
					df, err = project(re).Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					nativeStringListCheck(t, df, l, e, false)
					df.Close()
					// The unified bounded forest also accepts a Struct with List<String>.
					if bits == 3 && writer == "v1plain" && mode == kernel.ModeScalar {
						nested := filepath.Join(t.TempDir(), "nested.parquet")
						if _, err := q.SelectExprs(Col("made").Alias("made")).SinkParquet(context.Background(), nested, ParquetSinkOptions{}); err != nil {
							t.Fatal(err)
						}
						if _, err := s.ScanParquetWithOptions(nested, ParquetScanOptions{NativeNested: true}); err != nil {
							t.Fatal("bounded Struct List<String> rejected", err)
						}
					}
					ip := filepath.Join(t.TempDir(), "list-string.arrow")
					if _, err := q.SinkIPC(context.Background(), ip, IPCSinkOptions{}); err != nil {
						t.Fatal(err)
					}
					in, err := os.Open(ip)
					if err != nil {
						t.Fatal(err)
					}
					rd, err := ipc.NewFileReader(in)
					if err != nil {
						t.Fatal(err)
					}
					var rr []arrow.RecordBatch
					for i := 0; i < rd.NumRecords(); i++ {
						r, err := rd.RecordBatchAt(i)
						if err != nil {
							t.Fatal(err)
						}
						rr = append(rr, r)
					}
					back, err := s.ScanArrow(rr...)
					for _, r := range rr {
						r.Release()
					}
					rd.Close()
					in.Close()
					if err != nil {
						t.Fatal(err)
					}
					df, err = back.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					nativeStringListCheck(t, df, l, e, false)
					df.Close()
					scalar, err := scan.SelectExprs(Col("id").Alias("id")).Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					sc, _ := scalar.Column("id")
					sv, err := sc.ToAnySlice()
					if err != nil {
						t.Fatal(err)
					}
					if len(sv) != 130 {
						t.Fatal("scalar rows")
					}
					for row, v := range sv {
						if v != int64(row) {
							t.Fatal("scalar projection")
						}
					}
					scalar.Close()
					stream, err := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 1 << 20})
					if err != nil {
						t.Fatal(err)
					}
					row, batches := 0, 0
					for stream.Next(context.Background()) {
						b := stream.Batch()
						c, err := b.Column("items")
						if err != nil {
							t.Fatal(err)
						}
						v, err := c.ToAnySlice()
						if err != nil {
							t.Fatal(err)
						}
						for _, got := range v {
							if !reflect.DeepEqual(got, nativeStringListWant(row, l, e)) {
								t.Fatal("batch exact values", row)
							}
							row++
						}
						batches++
					}
					err = stream.Err()
					stream.Close()
					if err != nil {
						t.Fatal(err)
					}
					if row != 130 || batches < 2 {
						t.Fatal(row, batches)
					}
					t.Logf("rows=%d batches=%d", row, batches)
					early, err := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 1 << 20})
					if err != nil {
						t.Fatal(err)
					}
					if !early.Next(context.Background()) {
						t.Fatal(early.Err())
					}
					early.Close()
					denied, err := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 32768})
					if err == nil {
						for denied.Next(context.Background()) {
						}
						err = denied.Err()
						denied.Close()
					}
					if !errors.Is(err, memory.ErrBudgetExceeded) {
						t.Fatal("oversize row", err)
					}
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					df, err = q.Collect(ctx)
					if err == nil {
						df.Close()
					}
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
					tinyOpts := opts
					tinyOpts.MemoryLimitBytes = 1
					tiny := NewSession(tinyOpts)
					tq, err := tiny.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
					if err != nil {
						t.Fatal(err)
					}
					df, err = tq.Collect(context.Background())
					if err == nil {
						df.Close()
					}
					if !errors.Is(err, memory.ErrBudgetExceeded) {
						tiny.Close()
						t.Fatal(err)
					}
					tiny.Close()
					if tiny.Engine().Memory().Used() != 0 {
						t.Fatal("denial leak")
					}
					df, err = q.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					s.Close()
					nativeStringListCheck(t, df, l, e, false)
					df.Close()
				})
			}
		}
	}
}
