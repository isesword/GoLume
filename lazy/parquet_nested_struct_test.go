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
)

var forestWords = []string{"", "中🙂", "abc", "a\x00b", "é", strings.Repeat("x", 16)}

func forestList(row, seed int, text, nullable, elementNull bool) any {
	if nullable && (row+seed)%9 == 0 && row != 21 {
		return nil
	}
	n := (row + seed) % 4
	if (row+seed)%7 == 0 {
		n = 0
	}
	if row == 21 {
		n = 129
	}
	v := make([]any, n)
	for i := range v {
		if elementNull && i%5 == 2 {
			continue
		}
		if text {
			v[i] = forestWords[(row+seed+i)%len(forestWords)]
		} else {
			v[i] = int64(seed*1000 + row*10 + i%7)
		}
	}
	return v
}
func forestInstant(row int) int64 {
	if row%2 == 0 {
		return 1710054000000000000
	}
	return 1730613600000000000
}
func forestDay(row int) int32 {
	if row%2 == 0 {
		return 19792
	}
	return 20030
}

// Unmasked scalar/String payloads deliberately exist under a NULL Arrow parent.
// Arrow Parquet writer rejects nonempty hidden Lists, so those are zero-length NULL.
// The independent logical oracle masks the parent, not each raw child bitmap.
func forestChildren(row, side int) map[string]any {
	if side == 0 {
		var label, ts any
		if row%5 != 0 {
			label = forestWords[row%6]
		}
		if row%7 != 0 {
			ts = forestInstant(row)
		}
		return map[string]any{"label": label, "ints": forestList(row, 1, false, true, true), "words": forestList(row, 2, true, true, true), "ts": ts, "n": int64(row)}
	}
	return map[string]any{"label": forestWords[(row+1)%6], "ints": forestList(row, 3, false, true, false), "words": forestList(row, 4, true, false, true), "ok": row%2 == 0, "date": forestDay(row)}
}
func forestRaw(row int) map[string]any {
	var left, right, label, ts any
	if row%11 != 0 {
		left = forestChildren(row, 0)
	}
	if row%13 != 0 {
		right = forestChildren(row, 1)
	}
	if row%3 != 0 {
		label = forestWords[(row+2)%6]
	}
	if row%17 != 0 {
		ts = forestInstant(row)
	}
	return map[string]any{"left": left, "right": right, "label": label, "ints": forestList(row, 5, false, true, false), "words": forestList(row, 6, true, false, true), "id": int64(row), "ts": ts}
}
func forestDerived(row int) (map[string]any, bool) {
	raw := forestRaw(row)
	var li, rw, ln, head, score, day, topday, groupkey any
	if raw["left"] != nil {
		m := raw["left"].(map[string]any)
		li = m["ints"]
		ln = m["n"]
		if m["ts"] != nil {
			day = forestDay(row)
		}
	}
	if raw["right"] != nil {
		m := raw["right"].(map[string]any)
		rw = m["words"]
		groupkey = m["label"]
	}
	if rw != nil && len(rw.([]any)) > 0 {
		head = rw.([]any)[0]
	}
	if li != nil && raw["ints"] != nil {
		score = int64(len(li.([]any)) + len(raw["ints"].([]any)))
	}
	if raw["ts"] != nil {
		topday = forestDay(row)
	}
	keep := false
	if row > 5 && raw["left"] != nil && rw != nil && len(rw.([]any)) > 0 {
		label := raw["left"].(map[string]any)["label"]
		keep = label != nil && strings.Contains(label.(string), "中")
	}
	raw["score"], raw["head"], raw["day"], raw["topday"], raw["groupkey"] = score, head, day, topday, groupkey
	raw["made"] = map[string]any{"ints": li, "words": rw, "n": ln, "day": day}
	return raw, keep
}
func forestAppendList(b array.Builder, v any, text bool) {
	lb := b.(*array.ListBuilder)
	lb.Append(v != nil)
	if v == nil {
		return
	}
	for _, x := range v.([]any) {
		if x == nil {
			lb.ValueBuilder().AppendNull()
		} else if text {
			lb.ValueBuilder().(*array.StringBuilder).Append(x.(string))
		} else {
			lb.ValueBuilder().(*array.Int64Builder).Append(x.(int64))
		}
	}
}
func forestAppendString(b array.Builder, v any) {
	if v == nil {
		b.AppendNull()
	} else {
		b.(*array.StringBuilder).Append(v.(string))
	}
}
func forestFixture(t *testing.T, writer string) string {
	t.Helper()
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	list := func(text, en bool) arrow.DataType {
		var dt arrow.DataType = arrow.PrimitiveTypes.Int64
		if text {
			dt = arrow.BinaryTypes.String
		}
		return arrow.ListOfField(arrow.Field{Name: "element", Type: dt, Nullable: en})
	}
	timestamp := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	left := arrow.StructOf(arrow.Field{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true}, arrow.Field{Name: "ints", Type: list(false, true), Nullable: true}, arrow.Field{Name: "words", Type: list(true, true), Nullable: true}, arrow.Field{Name: "ts", Type: timestamp, Nullable: true}, arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64})
	right := arrow.StructOf(arrow.Field{Name: "label", Type: arrow.BinaryTypes.String}, arrow.Field{Name: "ints", Type: list(false, false), Nullable: true}, arrow.Field{Name: "words", Type: list(true, true)}, arrow.Field{Name: "ok", Type: arrow.FixedWidthTypes.Boolean}, arrow.Field{Name: "date", Type: arrow.FixedWidthTypes.Date32})
	schema := arrow.NewSchema([]arrow.Field{{Name: "left", Type: left, Nullable: true}, {Name: "right", Type: right, Nullable: true}, {Name: "label", Type: arrow.BinaryTypes.String, Nullable: true}, {Name: "ints", Type: list(false, false), Nullable: true}, {Name: "words", Type: list(true, true)}, {Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "ts", Type: timestamp, Nullable: true}}, nil)
	rb := array.NewRecordBuilder(alloc, schema)
	for row := 0; row < 65; row++ {
		raw := forestRaw(row)
		for side := 0; side < 2; side++ {
			sb := rb.Field(side).(*array.StructBuilder)
			valid := row%11 != 0
			if side == 1 {
				valid = row%13 != 0
			}
			sb.AppendValues([]bool{valid})
			m := forestChildren(row, side)
			forestAppendString(sb.FieldBuilder(0), m["label"])
			childInts, childWords := m["ints"], m["words"]
			if !valid {
				childInts = nil
				childWords = nil
			}
			forestAppendList(sb.FieldBuilder(1), childInts, false)
			forestAppendList(sb.FieldBuilder(2), childWords, true)
			if side == 0 {
				if m["ts"] == nil {
					sb.FieldBuilder(3).AppendNull()
				} else {
					sb.FieldBuilder(3).(*array.TimestampBuilder).Append(arrow.Timestamp(m["ts"].(int64)))
				}
				sb.FieldBuilder(4).(*array.Int64Builder).Append(m["n"].(int64))
			} else {
				sb.FieldBuilder(3).(*array.BooleanBuilder).Append(m["ok"].(bool))
				sb.FieldBuilder(4).(*array.Date32Builder).Append(arrow.Date32(m["date"].(int32)))
			}
		}
		forestAppendString(rb.Field(2), raw["label"])
		forestAppendList(rb.Field(3), raw["ints"], false)
		forestAppendList(rb.Field(4), raw["words"], true)
		rb.Field(5).(*array.Int64Builder).Append(int64(row))
		if raw["ts"] == nil {
			rb.Field(6).AppendNull()
		} else {
			rb.Field(6).(*array.TimestampBuilder).Append(arrow.Timestamp(raw["ts"].(int64)))
		}
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	defer rec.Release()
	path := filepath.Join(t.TempDir(), "forest.parquet")
	if writer == "sink" {
		s := NewSession(engine.DefaultOptions())
		q, err := s.ScanArrow(rec)
		if err != nil {
			t.Fatal(err)
		}
		_, err = q.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 23})
		s.Close()
		if err != nil {
			t.Fatal(err)
		}
	} else {
		version := pq.DataPageV1
		if writer == "v2dict" {
			version = pq.DataPageV2
		}
		tab := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
		defer tab.Release()
		out, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		err = pqarrow.WriteTable(tab, out, 23, pq.NewWriterProperties(pq.WithDataPageVersion(version), pq.WithDictionaryDefault(writer == "v2dict"), pq.WithDataPageSize(64), pq.WithBatchSize(64)), pqarrow.DefaultWriterProps())
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
	if rd.NumRowGroups() != 3 {
		t.Fatal("groups", rd.NumRowGroups())
	}
	dictCount := 0
	for _, leaf := range []int{1, 2, 6, 7, 11, 12} {
		pr, err := rd.RowGroup(0).GetColumnPageReader(leaf)
		if err != nil {
			t.Fatal(err)
		}
		pages, maxLevels, dict := 0, int32(0), false
		for pr.Next() {
			pg := pr.Page()
			switch pg.Type() {
			case file.PageTypeDictionaryPage:
				dict = true
			case file.PageTypeDataPage, file.PageTypeDataPageV2:
				pages++
				if pg.NumValues() > maxLevels {
					maxLevels = pg.NumValues()
				}
				if writer != "sink" {
					want := file.PageTypeDataPage
					if writer == "v2dict" {
						want = file.PageTypeDataPageV2
					}
					if pg.Type() != want {
						t.Fatal("page version")
					}
				}
			}
		}
		err = pr.Err()
		pr.Close()
		if err != nil {
			t.Fatal(err)
		}
		if writer == "v1plain" && (pages < 2 || maxLevels >= 129 || dict) {
			t.Fatal("cross-page long list evidence", leaf, pages, maxLevels)
		}
		if dict {
			dictCount++
		}
		t.Logf("%s leaf%d pages=%d maxLevels=%d dictionary=%v", writer, leaf, pages, maxLevels, dict)
	}
	if writer == "v2dict" && dictCount == 0 {
		t.Fatal("no actual dictionary List page")
	}
	rd.Close()
	in.Close()
	return path
}
func forestSchema(t *testing.T, schema memory.Schema, made bool) {
	t.Helper()
	fields := map[string]memory.Field{}
	for _, f := range schema.Fields {
		fields[f.Name] = f
	}
	for _, name := range []string{"left", "right"} {
		f := fields[name]
		if f.Type != memory.TypeStruct || !f.Nullable || len(f.Children) != 5 {
			t.Fatal("parent metadata", f)
		}
		if f.Children[0].Type != memory.TypeString || f.Children[1].Type != memory.TypeList || len(f.Children[1].Children) != 1 || f.Children[1].Children[0].Type != memory.TypeInt64 || f.Children[2].Type != memory.TypeList || len(f.Children[2].Children) != 1 || f.Children[2].Children[0].Type != memory.TypeString {
			t.Fatal("child types", f)
		}
	}
	if !fields["left"].Children[0].Nullable || fields["right"].Children[0].Nullable || fields["right"].Children[2].Nullable {
		t.Fatal("child nullable schema widened by parent")
	}
	lf, rf := fields["left"], fields["right"]
	if lf.Children[3].Type != memory.TypeTimestamp || !lf.Children[3].Nullable || lf.Children[4].Type != memory.TypeInt64 || lf.Children[4].Nullable || rf.Children[3].Type != memory.TypeBool || rf.Children[3].Nullable || rf.Children[4].Type != memory.TypeDate32 || rf.Children[4].Nullable {
		t.Fatal("fixed child metadata")
	}
	if !lf.Children[1].Children[0].Nullable || !lf.Children[2].Children[0].Nullable || rf.Children[1].Children[0].Nullable || !rf.Children[2].Children[0].Nullable {
		t.Fatal("element nullable metadata")
	}
	if fields["label"].Type != memory.TypeString || fields["ts"].Type != memory.TypeTimestamp || fields["ints"].Type != memory.TypeList || !fields["ints"].Nullable || len(fields["ints"].Children) != 1 || fields["ints"].Children[0].Type != memory.TypeInt64 || fields["ints"].Children[0].Nullable || fields["words"].Type != memory.TypeList || fields["words"].Nullable || len(fields["words"].Children) != 1 || fields["words"].Children[0].Type != memory.TypeString || !fields["words"].Children[0].Nullable {
		t.Fatal("top leaf metadata")
	}
	if made {
		if fields["day"].Type != memory.TypeDate32 || fields["topday"].Type != memory.TypeDate32 {
			t.Fatal("derived Date32 metadata")
		}
		f := fields["made"]
		if f.Type != memory.TypeStruct || f.Nullable || len(f.Children) != 4 {
			t.Fatal("constructed metadata", f)
		}
	}
}
func forestCheck(t *testing.T, df DataFrame, filtered bool) {
	t.Helper()
	forestSchema(t, df.Schema(), true)
	cols := map[string][]any{}
	for _, n := range []string{"left", "right", "label", "ints", "words", "id", "ts", "score", "head", "day", "topday", "groupkey", "made"} {
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
	at := 0
	for row := 0; row < 65; row++ {
		want, keep := forestDerived(row)
		if filtered && !keep {
			continue
		}
		if at >= len(cols["id"]) {
			t.Fatal("missing row", row)
		}
		for n, v := range want {
			if !reflect.DeepEqual(cols[n][at], v) {
				t.Fatalf("forest %s row%d mismatch", n, row)
			}
		}
		at++
	}
	if at != len(cols["id"]) {
		t.Fatal("extra rows")
	}
}
func forestGroups(t *testing.T, df DataFrame) {
	t.Helper()
	type agg struct {
		n     uint64
		sum   int64
		valid bool
	}
	want := map[any]agg{}
	for row := 0; row < 65; row++ {
		m, _ := forestDerived(row)
		k := m["groupkey"]
		v := want[k]
		v.n++
		if m["score"] != nil {
			v.sum += m["score"].(int64)
			v.valid = true
		}
		want[k] = v
	}
	cols := map[string][]any{}
	for _, n := range []string{"groupkey", "count", "sum"} {
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
	if len(cols["groupkey"]) != len(want) {
		t.Fatal("group count")
	}
	seen := map[any]bool{}
	for i, k := range cols["groupkey"] {
		v, ok := want[k]
		var sum any
		if v.valid {
			sum = v.sum
		}
		if !ok || seen[k] || cols["count"][i] != v.n || cols["sum"][i] != sum {
			t.Fatal("group oracle", k)
		}
		seen[k] = true
	}
}
func TestParquetNestedStructForestComposition(t *testing.T) {
	for _, writer := range []string{"sink", "v1plain", "v2dict"} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("%s/%d", writer, mode), func(t *testing.T) {
				path := forestFixture(t, writer)
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
				scan, err := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
				if err != nil {
					t.Fatal(err)
				}
				schema, err := scan.Schema()
				if err != nil {
					t.Fatal(err)
				}
				forestSchema(t, schema, false)
				leftInts := StructGet(Col("left"), "ints")
				rightWords := StructGet(Col("right"), "words")
				leftN := StructGet(Col("left"), "n")
				day := DateInZone(StructGet(Col("left"), "ts"), zoneLazyHandle(t, "new_york"))
				q := scan.WithColumns(expr.Add(ListLength(leftInts), ListLength(Col("ints"))).Alias("score"), ListGet(rightWords, expr.I64(0)).Alias("head"), day.Alias("day"), DateInZone(Col("ts"), zoneLazyHandle(t, "new_york")).Alias("topday"), StructGet(Col("right"), "label").Alias("groupkey")).WithColumns(StructFrom(leftInts.Alias("ints"), rightWords.Alias("words"), leftN.Alias("n"), Col("day").Alias("day")).Alias("made"))
				for pass := 0; pass < 2; pass++ {
					df, err := q.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					forestCheck(t, df, false)
					df.Close()
				}
				filtered := q.Filter(expr.And(expr.And(expr.GT(Col("id"), expr.I64(5)), expr.Contains(StructGet(Col("left"), "label"), expr.Str("中"))), expr.GT(ListLength(rightWords), expr.I64(0))))
				df, err := filtered.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				forestCheck(t, df, true)
				df.Close()
				group := q.SelectExprs(Col("groupkey").Alias("groupkey"), Col("score").Alias("score")).GroupBy("groupkey").Agg(Count("count"), Sum("score", "sum"))
				df, err = group.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				forestGroups(t, df)
				df.Close()
				// Whole original parents plus an always-valid constructed parent, mixed top
				// Lists and scalar Strings all survive the new native forest readback.
				pp := filepath.Join(t.TempDir(), "rewrite.parquet")
				if _, err := q.SinkParquet(context.Background(), pp, ParquetSinkOptions{RowGroupRows: 17}); err != nil {
					t.Fatal(err)
				}
				re, err := s.ScanParquetWithOptions(pp, ParquetScanOptions{NativeNested: true})
				if err != nil {
					t.Fatal(err)
				}
				df, err = re.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				forestCheck(t, df, false)
				df.Close()
				subset, err := scan.SelectExprs(Col("right").Alias("right"), Col("ints").Alias("ints"), Col("id").Alias("id")).Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"right", "ints", "id"} {
					c, err := subset.Column(name)
					if err != nil {
						t.Fatal(err)
					}
					v, err := c.ToAnySlice()
					if err != nil {
						t.Fatal(err)
					}
					if len(v) != 65 {
						t.Fatal("projection rows")
					}
					for row, x := range v {
						if !reflect.DeepEqual(x, forestRaw(row)[name]) {
							t.Fatal("projection parent alignment", name, row)
						}
					}
				}
				subset.Close()
				ip := filepath.Join(t.TempDir(), "forest.arrow")
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
				forestCheck(t, df, false)
				df.Close()
				stream, err := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 2 << 20})
				if err != nil {
					t.Fatal(err)
				}
				row, batches := 0, 0
				for stream.Next(context.Background()) {
					b := stream.Batch()
					for _, name := range []string{"left", "right", "made"} {
						c, err := b.Column(name)
						if err != nil {
							t.Fatal(err)
						}
						v, err := c.ToAnySlice()
						if err != nil {
							t.Fatal(err)
						}
						for i, x := range v {
							want, _ := forestDerived(row + i)
							if !reflect.DeepEqual(x, want[name]) {
								t.Fatal("batch parent mask", name, row+i)
							}
						}
					}
					row += int(b.Len())
					batches++
				}
				err = stream.Err()
				stream.Close()
				if err != nil {
					t.Fatal(err)
				}
				if row != 65 || batches < 2 {
					t.Fatal(row, batches)
				}
				t.Logf("rows=%d batches=%d", row, batches)
				early, err := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 2 << 20})
				if err != nil {
					t.Fatal(err)
				}
				if !early.Next(context.Background()) {
					t.Fatal(early.Err())
				}
				early.Close()
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
				forestCheck(t, df, false)
				df.Close()
			})
		}
	}
}
