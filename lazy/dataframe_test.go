package lazy

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestDataFrameShapeViewsAndChunkedSeries(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "name", Type: memory.TypeString, Encoding: memory.EncodingView},
		memory.ListStringField("tags", true, true),
	)
	first := dataFrameBatch([]int64{1, 2}, []bool{true, false}, []string{"张三", "Alice"}, [][]*string{{strptr("go"), strptr("")}, {}}, []bool{true, true})
	second := dataFrameBatch([]int64{3, 4}, nil, []string{"Bob", "🙂"}, [][]*string{nil, {strptr("simd"), nil}}, []bool{false, true})
	src, err := inmemory.New(schema, first, second)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	df, err := session.FromSource(src).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rows, columns := df.Shape(); rows != 4 || columns != 3 {
		t.Fatalf("shape=(%d,%d)", rows, columns)
	}
	if got := df.Columns(); strings.Join(got, ",") != "id,name,tags" {
		t.Fatalf("columns=%v", got)
	}

	view, err := df.Slice(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	series, err := view.Column("id")
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := series.Int64Chunks()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 || chunks[0].Len() != 1 || chunks[1].Len() != 1 {
		t.Fatalf("chunk lengths=%v,%v", chunks[0].Len(), chunks[1].Len())
	}
	if value, valid, err := chunks[0].Value(0); err != nil || valid || value != 2 {
		t.Fatalf("first sliced value=(%d,%v,%v)", value, valid, err)
	}
	if value, valid, err := chunks[1].Value(0); err != nil || !valid || value != 3 {
		t.Fatalf("second sliced value=(%d,%v,%v)", value, valid, err)
	}
	if series.NullCount() != 1 {
		t.Fatalf("null count=%d", series.NullCount())
	}
	if _, err := series.StringChunks(); err == nil {
		t.Fatal("type mismatch succeeded")
	}

	names, err := df.Column("name")
	if err != nil {
		t.Fatal(err)
	}
	copied, validity, err := names.CopyStrings()
	if err != nil || validity != nil || strings.Join(copied, ",") != "张三,Alice,Bob,🙂" {
		t.Fatalf("copied=%v validity=%v err=%v", copied, validity, err)
	}
	tags, err := df.Column("tags")
	if err != nil {
		t.Fatal(err)
	}
	anyValues, err := tags.ToAnySlice()
	if err != nil || len(anyValues) != 4 || anyValues[2] != nil {
		t.Fatalf("tags=%v err=%v", anyValues, err)
	}

	if err := df.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := view.Column("name"); !errors.Is(err, ErrDataFrameClosed) {
		t.Fatalf("view-after-close=%v", err)
	}
	if _, _, err := chunks[0].Value(0); !errors.Is(err, ErrDataFrameClosed) {
		t.Fatalf("chunk-after-close=%v", err)
	}
	if got := df.String(); got != "<closed DataFrame>" {
		t.Fatalf("closed String=%q", got)
	}
	if err := df.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDataFrameDisplayIsBoundedAndDistinguishesNestedNulls(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "name", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
		memory.ListStringField("tags", true, true),
	)
	batch := dataFrameBatch([]int64{1, 2, 3}, nil, []string{"张三", "", "ignored"}, [][]*string{{strptr("go"), strptr("simd")}, {}, nil}, []bool{true, true, false})
	batch.Int64s = map[string]*memory.Int64Vector{}
	batch.Strings["name"].Validity = memory.EnsureBitmap(nil, 3)
	memory.Set(batch.Strings["name"].Validity, 0)
	memory.Set(batch.Strings["name"].Validity, 1)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	df, err := session.FromSource(src).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	text, err := df.Format(DisplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"shape: (3, 2)", "张三", `""`, "[go, simd]", "[]", "NULL"} {
		if !strings.Contains(text, want) {
			t.Fatalf("display missing %q:\n%s", want, text)
		}
	}
	for _, line := range strings.Split(text, "\n") {
		if width := displayWidthForTest(line); width > 120 {
			t.Fatalf("line width=%d:\n%s", width, line)
		}
	}
}

func TestDataFrameMaterializationHonorsAndReleasesBudget(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}}
	batch.Reset(4)
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	low := engine.DefaultOptions()
	low.MemoryLimitBytes = 16
	small := NewSession(low)
	_, err = small.FromSource(src).Collect(context.Background())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("budget error=%v", err)
	}
	if used := small.engine.Memory().Used(); used != 0 {
		t.Fatalf("failed materialization leaked=%d", used)
	}
	_ = small.Close()
	ok := engine.DefaultOptions()
	ok.MemoryLimitBytes = 1024
	session := NewSession(ok)
	defer session.Close()
	df, err := session.FromSource(src).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if used := session.engine.Memory().Used(); used != 32 {
		t.Fatalf("reserved=%d want=32", used)
	}
	if err := df.Close(); err != nil {
		t.Fatal(err)
	}
	if used := session.engine.Memory().Used(); used != 0 {
		t.Fatalf("close leaked=%d", used)
	}
}

func TestDisplaySelectionUsesHeadTailAndLeftRightBounds(t *testing.T) {
	rows := selectDisplayRows(0, 25, 20)
	if len(rows) != 21 || !rows[10].ellipsis || rows[0].row != 0 || rows[len(rows)-1].row != 24 {
		t.Fatalf("rows=%+v", rows)
	}
	fields := make([]memory.Field, 14)
	for i := range fields {
		fields[i] = memory.Field{Name: string(rune('a' + i)), Type: memory.TypeInt64}
	}
	columns := selectDisplayColumns(fields, 12)
	if len(columns) != 13 || !columns[6].ellipsis || columns[0].field.Name != "a" || columns[len(columns)-1].field.Name != "n" {
		t.Fatalf("columns=%+v", columns)
	}
}

func dataFrameBatch(ids []int64, idValid []bool, names []string, lists [][]*string, listValid []bool) *memory.Batch {
	b := memory.NewBatch()
	iv := &memory.Int64Vector{Values: append([]int64(nil), ids...)}
	if idValid != nil {
		iv.Validity = memory.EnsureBitmap(nil, len(ids))
		for i, ok := range idValid {
			if ok {
				memory.Set(iv.Validity, i)
			}
		}
	}
	b.Int64s["id"] = iv
	sv := &memory.StringViewVector{Offsets: make([]uint32, len(names)+1), Encoding: memory.EncodingView}
	for i, name := range names {
		sv.Data = append(sv.Data, name...)
		sv.Offsets[i+1] = uint32(len(sv.Data))
	}
	b.Strings["name"] = sv
	lv := &memory.StringListVector{Offsets: []uint32{0}, Values: &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
	for i, list := range lists {
		valid := listValid == nil || listValid[i]
		if !valid {
			_ = memory.AppendNullStringList(lv)
			continue
		}
		row := &memory.StringListVector{Offsets: []uint32{0, uint32(len(list))}, Values: &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
		for _, item := range list {
			child := len(row.Values.Offsets) - 1
			if item == nil {
				if row.Values.Validity == nil {
					row.Values.Validity = memory.EnsureBitmap(nil, len(list))
					for j := 0; j < child; j++ {
						memory.Set(row.Values.Validity, j)
					}
				}
			} else {
				if row.Values.Validity != nil {
					memory.Set(row.Values.Validity, child)
				}
				row.Values.Data = append(row.Values.Data, (*item)...)
			}
			row.Values.Offsets = append(row.Values.Offsets, uint32(len(row.Values.Data)))
		}
		_ = memory.AppendStringListRow(lv, row, 0)
	}
	b.StringLists["tags"] = lv
	b.Reset(len(ids))
	return b
}
func strptr(value string) *string          { return &value }
func displayWidthForTest(value string) int { return len([]rune(value)) }
