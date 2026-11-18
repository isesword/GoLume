package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Independent ordinary-values oracle. No engine expression generates expected data.
func nestedListBRaw(row int, text bool) any {
	switch row % 9 {
	case 0:
		return nil
	case 1:
		return []any{}
	case 2:
		return []any{nil}
	case 3:
		return []any{[]any{}}
	case 4:
		return []any{[]any{nil}}
	}
	scalar := func(i int) any {
		if text {
			return []string{"", "中🙂", "a\x00b"}[(row+i)%3]
		}
		return int64(row*10 + i)
	}
	return []any{[]any{scalar(0), nil, scalar(2)}, []any{}, nil, []any{scalar(3)}}
}
func nestedListBOuter(row, level, depth int) any {
	if level%2 == 1 && row%(level+4) == 0 {
		return nil
	}
	if level < depth {
		return map[string]any{"node.中": nestedListBOuter(row, level+1, depth)}
	}
	return map[string]any{"values.中": nestedListBRaw(row, true), "tag": int64(row + 100)}
}
func nestedListBArrow(t *testing.T, alloc am.Allocator, depth, rows int) arrow.RecordBatch {
	t.Helper()
	typ := func(leaf arrow.DataType) arrow.DataType {
		return arrow.ListOfField(arrow.Field{Name: "item", Nullable: true, Type: arrow.ListOfField(arrow.Field{Name: "item", Nullable: true, Type: leaf})})
	}
	ints, texts := typ(arrow.PrimitiveTypes.Int64), typ(arrow.BinaryTypes.String)
	var outer arrow.DataType = arrow.StructOf(arrow.Field{Name: "values.中", Type: texts, Nullable: true}, arrow.Field{Name: "tag", Type: arrow.PrimitiveTypes.Int64})
	for level := depth - 1; level >= 1; level-- {
		outer = arrow.StructOf(arrow.Field{Name: "node.中", Type: outer, Nullable: (level+1)%2 == 1})
	}
	fields := []arrow.Field{{Name: "ints", Type: ints, Nullable: true}, {Name: "texts", Type: texts, Nullable: true}, {Name: "outer", Type: outer, Nullable: true}, {Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "index", Type: arrow.PrimitiveTypes.Int64}}
	rb := array.NewRecordBuilder(alloc, arrow.NewSchema(fields, nil))
	for row := 0; row < rows; row++ {
		values := []any{nestedListBRaw(row, false), nestedListBRaw(row, true), nestedListBOuter(row, 1, depth), int64(row), int64(row%6 - 1)}
		for i, v := range values {
			nestedListBAppend(rb.Field(i), v)
		}
	}
	result := rb.NewRecordBatch()
	rb.Release()
	return result
}
func nestedListBAppend(b array.Builder, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch a := b.(type) {
	case *array.ListBuilder:
		a.Append(true)
		for _, item := range v.([]any) {
			nestedListBAppend(a.ValueBuilder(), item)
		}
	case *array.StructBuilder:
		a.Append(true)
		for i, f := range a.Type().(*arrow.StructType).Fields() {
			nestedListBAppend(a.FieldBuilder(i), v.(map[string]any)[f.Name])
		}
	case *array.Int64Builder:
		a.Append(v.(int64))
	case *array.StringBuilder:
		a.Append(v.(string))
	default:
		panic(fmt.Sprintf("fixture unsupported builder %T", a))
	}
}

func nestedListBPick(v any, index int64) any {
	if v == nil || index < 0 || index >= int64(len(v.([]any))) {
		return nil
	}
	return v.([]any)[index]
}
func nestedListBExpected(row, depth int) map[string]any {
	ints, texts := nestedListBRaw(row, false), nestedListBRaw(row, true)
	index := int64(row%6 - 1)
	inner := nestedListBPick(ints, index)
	var length, innerLength any
	if ints != nil {
		length = int64(len(ints.([]any)))
	}
	if inner != nil {
		innerLength = int64(len(inner.([]any)))
	}
	outer := nestedListBOuter(row, 1, depth)
	node := outer
	for i := 1; i < depth && node != nil; i++ {
		node = node.(map[string]any)["node.中"]
	}
	var events any
	if node != nil {
		events = node.(map[string]any)["values.中"]
	}
	return map[string]any{"ints": ints, "texts": texts, "outer": outer, "id": int64(row), "index": index, "len": length, "inner": inner, "innerlen": innerLength, "value": nestedListBPick(inner, 0), "events": events}
}
func nestedListBCheck(t *testing.T, df DataFrame, depth, start, n int, names []string, filtered bool) {
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
		want := make([]any, 0, n)
		for row := start; row < start+n; row++ {
			if filtered && row <= 65 {
				continue
			}
			want = append(want, nestedListBExpected(row, depth)[name])
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("nested list oracle %s got%v want%v", name, got, want)
		}
	}
}
func TestNestedListBLazy(t *testing.T) {
	for _, depth := range []int{1, 8} {
		t.Run(fmt.Sprintf("outer%d", depth), func(t *testing.T) {
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			record := nestedListBArrow(t, alloc, depth, 132)
			sliced := record.NewSlice(1, 131)
			record.Release()
			s := NewSession(engine.DefaultOptions())
			defer s.Close()
			legacy, le := s.ScanArrow(sliced)
			if le == nil {
				ld, err := legacy.SelectExprs(ListLength(Col("ints")).Alias("n")).Collect(context.Background())
				if err == nil {
					ld.Close()
					t.Fatal("default native scope widened")
				}
			}
			q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, sliced)
			sliced.Release()
			if e != nil {
				t.Fatal(e)
			}
			var events expr.Expr = Col("outer")
			for i := 1; i < depth; i++ {
				events = StructGet(events, "node.中")
			}
			events = StructGet(events, "values.中")
			inner := ListGet(Col("ints"), Col("index"))
			derived := q.WithColumns(ListLength(Col("ints")).Alias("len"), inner.Alias("inner"), ListLength(inner).Alias("innerlen"), ListGet(inner, expr.I64(0)).Alias("value"), Named(events, "events"))
			names := []string{"ints", "texts", "outer", "id", "index", "len", "inner", "innerlen", "value", "events"}
			df, e := derived.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			nestedListBCheck(t, df, depth, 1, 130, names, false)
			sc := df.Schema()
			f := sc.Fields[0]
			if f.Type != memory.TypeList || len(f.Children) != 1 || f.Children[0].Type != memory.TypeList || f.Children[0].Children[0].Type != memory.TypeInt64 {
				t.Fatal("nested metadata", f)
			}
			// Filter via a true source row predicate; full new payload must be retained.
			fd, e := derived.Filter(expr.GT(Col("id"), expr.I64(65))).Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			nestedListBCheck(t, fd, depth, 1, 130, names, true)
			fd.Close()
			ip := filepath.Join(t.TempDir(), "nested.arrow")
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
			aq, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, records...)
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
			nestedListBCheck(t, ad, depth, 1, 130, names, false)
			reader, e := derived.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 2 << 20})
			if e != nil {
				t.Fatal(e)
			}
			if !reader.Next(context.Background()) {
				t.Fatal(reader.Err())
			}
			view := reader.Batch()
			reader.Close()
			if view.Valid() {
				t.Fatal("expired generation retained")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			cd, ce := derived.Collect(ctx)
			if ce == nil {
				cd.Close()
			}
			if !errors.Is(ce, context.Canceled) {
				t.Fatal("cancel", ce)
			}
			// D explicitly closes two-level List Parquet transport; other B restrictions remain.
			parquetPath := filepath.Join(t.TempDir(), "supported.parquet")
			if _, e = derived.SinkParquet(context.Background(), parquetPath, ParquetSinkOptions{}); e != nil {
				t.Fatal(e)
			}
			pq, e := s.ScanParquetWithOptions(parquetPath, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			pdf, e := pq.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			nestedListBCheck(t, pdf, depth, 1, 130, names, false)
			pdf.Close()
			sorted, se := derived.Sort(Asc("id")).Collect(context.Background())
			if se != nil {
				t.Fatal(se)
			}
			nestedListBCheck(t, sorted, depth, 1, 130, names, false)
			sorted.Close()
			for _, bad := range []LazyFrame{derived.Sort(Asc("ints"))} {
				sd, se := bad.Collect(context.Background())
				if se == nil {
					sd.Close()
					t.Fatal("nested key accepted")
				}
			}
			top, te := derived.TopK(3, Asc("id")).Collect(context.Background())
			if te != nil {
				t.Fatal(te)
			}
			nestedListBCheck(t, top, depth, 1, 3, names, false)
			top.Close()
			s.Close()
			nestedListBCheck(t, df, depth, 1, 130, names, false)
			nestedListBCheck(t, ad, depth, 1, 130, names, false)
			df.Close()
			ad.Close()
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("owned leak")
			}
		})
	}
}

func nestedListBNativeNode(n int, text bool) *memory.ArrayNode {
	root := &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0}, Validity: make(memory.Bitmap, memory.WordsFor(n)), Encoding: memory.EncodingView}
	mid := &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0}, Encoding: memory.EncodingView}
	root.Child = mid
	var mv, lv []bool
	var nums []int64
	str := &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
	for row := 0; row < n; row++ {
		raw := nestedListBRaw(row, text)
		if raw != nil {
			memory.Set(root.Validity, row)
			for _, inner := range raw.([]any) {
				mv = append(mv, inner != nil)
				if inner != nil {
					for _, value := range inner.([]any) {
						lv = append(lv, value != nil)
						if text {
							if value != nil {
								str.Data = append(str.Data, []byte(value.(string))...)
							}
							str.Offsets = append(str.Offsets, uint32(len(str.Data)))
						} else {
							var x int64
							if value != nil {
								x = value.(int64)
							}
							nums = append(nums, x)
						}
					}
				}
				mid.Offsets = append(mid.Offsets, uint32(len(lv)))
			}
		}
		root.Offsets = append(root.Offsets, uint32(len(mv)))
	}
	mid.Validity = make(memory.Bitmap, memory.WordsFor(len(mv)))
	for i, v := range mv {
		if v {
			memory.Set(mid.Validity, i)
		}
	}
	bits := make(memory.Bitmap, memory.WordsFor(len(lv)))
	for i, v := range lv {
		if v {
			memory.Set(bits, i)
		}
	}
	if text {
		str.Validity = bits
		mid.Child = &memory.ArrayNode{Kind: memory.TypeString, Strings: str, Encoding: memory.EncodingView}
	} else {
		mid.Child = &memory.ArrayNode{Kind: memory.TypeInt64, Int64: &memory.Int64Vector{Values: nums, Validity: bits}, Encoding: memory.EncodingPlain}
	}
	return root
}
func TestNestedListBMemoryAndBudget(t *testing.T) {
	fi, e := memory.NestedListField("ints", true, memory.ListInt64Field("item", true, true))
	if e != nil {
		t.Fatal(e)
	}
	fs, e := memory.NestedListField("texts", true, memory.ListStringField("item", true, true))
	if e != nil {
		t.Fatal(e)
	}
	sc, e := memory.NewSchemaWithNestedListsChecked(fi, fs, memory.Field{Name: "id", Type: memory.TypeInt64})
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Rows = 18
	ids := make([]int64, 18)
	for i := range ids {
		ids[i] = int64(i)
	}
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	b.Arrays["ints"] = nestedListBNativeNode(18, false)
	b.Arrays["texts"] = nestedListBNativeNode(18, true)
	src, e := inmemory.New(sc, b)
	if e != nil {
		t.Fatal(e)
	}
	s := NewSession(engine.DefaultOptions())
	q := s.FromSource(src)
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	nestedListBCheck(t, df, 1, 0, 18, []string{"ints", "texts"}, false)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("memory owner")
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1
	tiny := NewSession(opts)
	ts, e := inmemory.New(sc, b)
	if e != nil {
		t.Fatal(e)
	}
	td, te := tiny.FromSource(ts).Collect(context.Background())
	if te == nil {
		td.Close()
	}
	tiny.Close()
	if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
		t.Fatal("budget", te)
	}
	boundedOpts := engine.DefaultOptions()
	boundedOpts.ExecutionPolicy = engine.ExecutionBounded
	boundedOpts.MemoryLimitBytes = 8 << 20
	dir := t.TempDir()
	boundedOpts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 16 << 20}
	bounded := NewSession(boundedOpts)
	defer bounded.Close()
	bs, e := inmemory.New(sc, b)
	if e != nil {
		t.Fatal(e)
	}
	sd, se := bounded.FromSource(bs).Sort(Asc("ints")).Collect(context.Background())
	if se == nil {
		sd.Close()
		t.Fatal("nested key accepted")
	}
	// C explicitly opens bounded scalar-key payload transport only.
	sd, se = bounded.FromSource(bs).Sort(Asc("id")).Collect(context.Background())
	if se != nil {
		t.Fatal(se)
	}
	nestedListBCheck(t, sd, 1, 0, 18, []string{"ints", "texts", "id"}, false)
	sd.Close()
	recursiveCNoFiles(t, dir)
}

func TestNestedListBScanLadder(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	r := nestedListBArrow(t, am.DefaultAllocator, 1, 18)
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, r)
	r.Release()
	if e != nil {
		t.Fatal(e)
	}
	for _, stage := range []string{"scan", "project", "derived"} {
		t.Run(stage, func(t *testing.T) {
			p := q
			if stage == "project" {
				p = q.Select("ints", "texts", "id")
			}
			if stage == "derived" {
				p = q.WithColumns(ListLength(Col("ints")).Alias("len"))
			}
			df, e := p.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			names := []string{"ints", "texts", "id"}
			if stage == "derived" {
				names = append(names, "len")
			}
			nestedListBCheck(t, df, 1, 0, 18, names, false)
		})
	}
}

func TestNestedListBArrowBatchBoundaries(t *testing.T) {
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	rec := nestedListBArrow(t, alloc, 8, 65)
	records := []arrow.RecordBatch{rec.NewSlice(0, 1), rec.NewSlice(1, 64), rec.NewSlice(64, 65)}
	rec.Release()
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, records...)
	for _, r := range records {
		r.Release()
	}
	if e != nil {
		t.Fatal(e)
	}
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	nestedListBCheck(t, df, 8, 0, 65, []string{"ints", "texts", "outer", "id", "index"}, false)
	empty, e := q.Filter(expr.GT(Col("id"), expr.I64(100))).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	c, e := empty.Column("ints")
	if e != nil {
		t.Fatal(e)
	}
	v, e := c.ToAnySlice()
	if e != nil || len(v) != 0 {
		t.Fatal("empty result", v, e)
	}
	empty.Close()
	s.Close()
	nestedListBCheck(t, df, 8, 0, 65, []string{"ints", "texts", "outer"}, false)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("multiple record owner leak")
	}
}
