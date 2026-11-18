package lazy

// B fixture preparation only. No native Batch API or new Arrow scan option is
// assumed here. Consumer tests will be added after A approval and B API freeze.
import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func listStructBFields() []arrow.Field {
	return []arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8}, {Name: "i16", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32}, {Name: "code.中", Type: arrow.PrimitiveTypes.Int64},
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8}, {Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32}, {Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32}, {Name: "f64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ok", Type: arrow.FixedWidthTypes.Boolean}, {Name: "date", Type: arrow.FixedWidthTypes.Date32},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	}
}
func listStructBElement(row, seed, item int) map[string]any {
	n := int64(seed*10000 + row*10 + item)
	m := map[string]any{"i8": int8(-7), "i16": int16(-301), "i32": int32(-70001), "code.中": n, "u8": uint8(201), "u16": uint16(60001), "u32": uint32(4000000001), "u64": uint64(18446744073709551615), "f32": float32(1.25), "f64": float64(-1.125), "ok": item%2 == 0, "date": int32(19792 + row%2), "ts": int64(1710054000000000000 + row), "label": []string{"", "中🙂", "a\x00b"}[(row+seed+item)%3]}
	if (row+item+seed)%5 == 0 {
		m["i16"] = nil
		m["label"] = nil
	}
	return m
}
func listStructBList(row, seed int) any {
	if (row+seed)%11 == 0 {
		return nil
	}
	n := (row + seed) % 4
	if row == 64 {
		n = 5
	}
	out := make([]any, n)
	for i := range out {
		if (row+seed+i)%3 != 1 {
			out[i] = listStructBElement(row, seed, i)
		}
	}
	return out
}
func listStructBOuter(row, level, depth int) any {
	if level%2 == 1 && row%(level+4) == 0 {
		return nil
	}
	if level < depth {
		return map[string]any{"node.中": listStructBOuter(row, level+1, depth)}
	}
	return map[string]any{"events": listStructBList(row, 3), "tag": int64(row + 100)}
}
func listStructBRaw(row, ancestors int) map[string]any {
	return map[string]any{"left": listStructBList(row, 1), "right": listStructBList(row, 2), "outer": listStructBOuter(row, 1, ancestors), "id": int64(row), "index": int64(row%5 - 1)}
}
func listStructBAppend(b array.Builder, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch a := b.(type) {
	case *array.StructBuilder:
		a.Append(true)
		m := v.(map[string]any)
		for i, f := range a.Type().(*arrow.StructType).Fields() {
			listStructBAppend(a.FieldBuilder(i), m[f.Name])
		}
	case *array.ListBuilder:
		a.Append(true)
		for _, e := range v.([]any) {
			listStructBAppend(a.ValueBuilder(), e)
		}
	case *array.Int8Builder:
		a.Append(v.(int8))
	case *array.Int16Builder:
		a.Append(v.(int16))
	case *array.Int32Builder:
		a.Append(v.(int32))
	case *array.Int64Builder:
		a.Append(v.(int64))
	case *array.Uint8Builder:
		a.Append(v.(uint8))
	case *array.Uint16Builder:
		a.Append(v.(uint16))
	case *array.Uint32Builder:
		a.Append(v.(uint32))
	case *array.Uint64Builder:
		a.Append(v.(uint64))
	case *array.Float32Builder:
		a.Append(v.(float32))
	case *array.Float64Builder:
		a.Append(v.(float64))
	case *array.BooleanBuilder:
		a.Append(v.(bool))
	case *array.Date32Builder:
		a.Append(arrow.Date32(v.(int32)))
	case *array.TimestampBuilder:
		a.Append(arrow.Timestamp(v.(int64)))
	case *array.StringBuilder:
		a.Append(v.(string))
	default:
		panic(fmt.Sprintf("unplanned fixture builder %T", b))
	}
}

// listStructBArrow returns an independently constructed Arrow record. Caller
// owns it and must Release. ancestors=1 or7 gives Struct depth2 or8 including
// the element Struct; top-level lists retain independent validity domains.
func listStructBArrow(t *testing.T, alloc am.Allocator, ancestors, n int) arrow.RecordBatch {
	t.Helper()
	if ancestors < 1 || ancestors > 7 {
		t.Fatal("fixture ancestor bound")
	}
	item := arrow.StructOf(listStructBFields()...)
	list := arrow.ListOfField(arrow.Field{Name: "item", Type: item, Nullable: true})
	var outer arrow.DataType = arrow.StructOf(arrow.Field{Name: "events", Type: list, Nullable: true}, arrow.Field{Name: "tag", Type: arrow.PrimitiveTypes.Int64})
	for level := ancestors - 1; level >= 1; level-- {
		outer = arrow.StructOf(arrow.Field{Name: "node.中", Type: outer, Nullable: (level+1)%2 == 1})
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "left", Type: list, Nullable: true}, {Name: "right", Type: list, Nullable: true}, {Name: "outer", Type: outer, Nullable: true}, {Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "index", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rb := array.NewRecordBuilder(alloc, schema)
	for row := 0; row < n; row++ {
		m := listStructBRaw(row, ancestors)
		for i, f := range schema.Fields() {
			listStructBAppend(rb.Field(i), m[f.Name])
		}
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	return rec
}

func listStructBOuterExpr(ancestors int) expr.Expr {
	var e expr.Expr = Col("outer")
	for level := 1; level < ancestors; level++ {
		e = StructGet(e, "node.中")
	}
	return StructGet(e, "events")
}
func listStructBItem(v any, index int64) any {
	if v == nil || index < 0 || index >= int64(len(v.([]any))) {
		return nil
	}
	return v.([]any)[index]
}
func listStructBExpected(row, ancestors int) map[string]any {
	m := listStructBRaw(row, ancestors)
	v := m["left"]
	var length any
	if v != nil {
		length = int64(len(v.([]any)))
	}
	item := listStructBItem(v, m["index"].(int64))
	var code any
	if item != nil {
		code = item.(map[string]any)["code.中"]
	}
	outer := m["outer"]
	for level := 1; level < ancestors && outer != nil; level++ {
		outer = outer.(map[string]any)["node.中"]
	}
	var events any
	if outer != nil {
		events = outer.(map[string]any)["events"]
	}
	m["len"], m["item"], m["code"], m["events"] = length, item, code, events
	return m
}
func listStructBCheck(t *testing.T, df DataFrame, ancestors, start, count int, filtered bool) {
	t.Helper()
	for _, name := range []string{"left", "right", "outer", "id", "index", "len", "item", "code", "events"} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, 0, count)
		for i := start; i < start+count; i++ {
			m := listStructBExpected(i, ancestors)
			if filtered && (m["code"] == nil || m["code"].(int64) <= 10000) {
				continue
			}
			want = append(want, m[name])
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s oracle got%v want%v", name, got, want)
		}
	}
}
func TestListStructBLazy(t *testing.T) {
	for _, ancestors := range []int{1, 7} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("a%d/m%d", ancestors, mode), func(t *testing.T) {
				alloc := am.NewCheckedAllocator(am.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				rec := listStructBArrow(t, alloc, ancestors, 66)
				part := rec.NewSlice(1, 66)
				defer func() {
					if part != nil {
						part.Release()
					}
				}()
				rec.Release()
				opts := engine.DefaultOptions()
				opts.KernelMode = mode
				opts.PreparedCacheBytes = 0
				s := NewSession(opts)
				defer s.Close()
				// Preserve legacy top-level opaque List<Struct>, not blanket constructor rejection.
				legacy := array.NewRecordBatch(arrow.NewSchema(part.Schema().Fields()[:2], nil), part.Columns()[:2], 65)
				lq, le := s.ScanArrow(legacy)
				legacy.Release()
				if le != nil {
					t.Fatal(le)
				}
				ls, le := lq.Schema()
				if le != nil {
					t.Fatal(le)
				}
				if ls.Fields[0].Type != memory.TypeArrow {
					t.Fatal("default native widening", ls.Fields[0])
				}
				if _, le = lq.SelectExprs(ListLength(Col("left")).Alias("n")).Schema(); le == nil {
					t.Fatal("legacy opaque allowed native length")
				}
				q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeStructList: true}, part)
				part.Release()
				part = nil
				if e != nil {
					t.Fatal(e)
				}
				q = q.WithColumns(ListLength(Col("left")).Alias("len"), ListGet(Col("left"), Col("index")).Alias("item"), StructGet(ListGet(Col("left"), Col("index")), "code.中").Alias("code"), Named(listStructBOuterExpr(ancestors), "events"))
				df, e := q.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				listStructBCheck(t, df, ancestors, 1, 65, false)
				df.Close()
				filtered := q.Filter(expr.GT(Col("code"), expr.I64(10000)))
				df, e = filtered.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				listStructBCheck(t, df, ancestors, 1, 65, true)
				df.Close()
				path := filepath.Join(t.TempDir(), "struct-list.arrow")
				if _, e = q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
					t.Fatal(e)
				}
				in, e := os.Open(path)
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
				iq, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeStructList: true, NativeRecursiveStruct: true}, records...)
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
				listStructBCheck(t, df, ancestors, 1, 65, false)
				df.Close()
				sorted, se := q.Sort(Asc("id")).Collect(context.Background())
				if se != nil {
					t.Fatal(se)
				}
				listStructBCheck(t, sorted, ancestors, 1, 65, false)
				sorted.Close()
				for _, query := range []LazyFrame{q.Sort(Asc("left"))} {
					bad, be := query.Collect(context.Background())
					if be == nil {
						bad.Close()
						t.Fatal("nested key accepted")
					}
				}
				top, te := q.TopK(3, Asc("id")).Collect(context.Background())
				if te != nil {
					t.Fatal(te)
				}
				listStructBCheck(t, top, ancestors, 1, 3, false)
				top.Close()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				cd, ce := q.Collect(ctx)
				if ce == nil {
					cd.Close()
				}
				if !errors.Is(ce, context.Canceled) {
					t.Fatal(ce)
				}
				tinyOpts := opts
				tinyOpts.MemoryLimitBytes = 1
				tiny := NewSession(tinyOpts)
				tr := listStructBArrow(t, alloc, ancestors, 2)
				tq, te := tiny.ScanArrowWithOptions(ArrowScanOptions{NativeStructList: true}, tr)
				tr.Release()
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
				df, e = iq.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				s.Close()
				listStructBCheck(t, df, ancestors, 1, 65, false)
				df.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("owned leak")
				}
			})
		}
	}
}

func TestListStructBMemoryTruthTable(t *testing.T) {
	f, e := memory.ListStructField("xs", true, true, memory.Field{Name: "n", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	if e != nil {
		t.Fatal(e)
	}
	schema, e := memory.NewSchemaWithStructListsChecked(f, memory.Field{Name: "i", Type: memory.TypeInt64})
	if e != nil {
		t.Fatal(e)
	}
	values := memory.NewBatch()
	values.Rows = 4
	values.Int64s["n"] = &memory.Int64Vector{Values: []int64{999, 999, 7, 9}, Validity: memory.Bitmap{12}}
	values.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 2, 5}, Data: []byte("xx中"), Validity: memory.Bitmap{12}}
	b := memory.NewBatch()
	b.Rows = 5
	b.StructLists["xs"] = &memory.StructListVector{Offsets: []uint32{0, 0, 0, 1, 2, 4}, Validity: memory.Bitmap{30}, Values: &memory.StructVector{Rows: 4, Fields: f.Children[0].Children, Values: values, Validity: memory.Bitmap{14}, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{0, 0, 0, 0, 1}}
	src, e := inmemory.New(schema, b)
	if e != nil {
		t.Fatal(e)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := s.FromSource(src).WithColumns(ListLength(Col("xs")).Alias("len"), ListGet(Col("xs"), Col("i")).Alias("item"), StructGet(ListGet(Col("xs"), Col("i")), "n").Alias("n"))
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	s.Close()
	allNull := map[string]any{"n": nil, "s": nil}
	seven := map[string]any{"n": int64(7), "s": ""}
	nine := map[string]any{"n": int64(9), "s": "中"}
	expected := map[string][]any{"xs": {nil, []any{}, []any{nil}, []any{allNull}, []any{seven, nine}}, "len": {nil, int64(0), int64(1), int64(1), int64(2)}, "item": {nil, nil, nil, allNull, nine}, "n": {nil, nil, nil, nil, int64(9)}}
	for name, want := range expected {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("truth table", name, got, want)
		}
	}
}

func TestListStructBArrowBatchBoundaries(t *testing.T) {
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	rec := listStructBArrow(t, alloc, 7, 65)
	parts := []arrow.RecordBatch{rec.NewSlice(0, 1), rec.NewSlice(1, 64), rec.NewSlice(64, 65)}
	rec.Release()
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeStructList: true}, parts...)
	for _, p := range parts {
		p.Release()
	}
	if e != nil {
		t.Fatal(e)
	}
	q = q.WithColumns(ListLength(Col("left")).Alias("len"), ListGet(Col("left"), Col("index")).Alias("item"), StructGet(ListGet(Col("left"), Col("index")), "code.中").Alias("code"), Named(listStructBOuterExpr(7), "events"))
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	listStructBCheck(t, df, 7, 0, 65, false)
	df.Close()
	empty, e := q.Filter(expr.GT(Col("id"), expr.I64(1000))).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	c, e := empty.Column("left")
	if e != nil {
		t.Fatal(e)
	}
	if c.Len() != 0 {
		t.Fatal("all false retained rows")
	}
	empty.Close()
	reader, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	if !reader.Next(context.Background()) {
		t.Fatal(reader.Err())
	}
	reader.Close()
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("earlyclose leak")
	}
}

func TestListStructBNullableProfiles(t *testing.T) {
	for _, ln := range []bool{false, true} {
		for _, en := range []bool{false, true} {
			t.Run(fmt.Sprintf("L%v/E%v", ln, en), func(t *testing.T) {
				f, e := memory.ListStructField("xs", ln, en, memory.Field{Name: "n", Type: memory.TypeInt64, Nullable: true})
				if e != nil {
					t.Fatal(e)
				}
				schema, e := memory.NewSchemaWithStructListsChecked(f)
				if e != nil {
					t.Fatal(e)
				}
				child := memory.NewBatch()
				child.Rows = 2
				child.Int64s["n"] = &memory.Int64Vector{Values: []int64{7, 999}, Validity: memory.Bitmap{1}}
				var lv, ev memory.Bitmap
				if ln {
					lv = memory.Bitmap{6}
				}
				if en {
					ev = memory.Bitmap{2}
				}
				b := memory.NewBatch()
				b.Rows = 3
				b.StructLists["xs"] = &memory.StructListVector{Offsets: []uint32{0, 0, 1, 2}, Validity: lv, Values: &memory.StructVector{Rows: 2, Fields: f.Children[0].Children, Values: child, Validity: ev, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
				src, e := inmemory.New(schema, b)
				if e != nil {
					t.Fatal(e)
				}
				s := NewSession(engine.DefaultOptions())
				defer s.Close()
				q := s.FromSource(src).WithColumns(ListGet(Col("xs"), expr.I64(0)).Alias("item"))
				qs, e := q.Schema()
				if e != nil {
					t.Fatal(e)
				}
				list, _ := qs.Field("xs")
				item, _ := qs.Field("item")
				if list.Nullable != ln || list.Children[0].Nullable != en || !item.Nullable || !item.Children[0].Nullable {
					t.Fatal("profile metadata")
				}
				df, e := q.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer df.Close()
				c, e := df.Column("xs")
				if e != nil {
					t.Fatal(e)
				}
				got, e := c.ToAnySlice()
				if e != nil {
					t.Fatal(e)
				}
				first := any([]any{})
				if ln {
					first = nil
				}
				middle := any(map[string]any{"n": int64(7)})
				if en {
					middle = nil
				}
				want := []any{first, []any{middle}, []any{map[string]any{"n": nil}}}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("nullable profile", got, want)
				}
			})
		}
	}
}

func TestListStructBArrowOptionsCoexist(t *testing.T) {
	rec := listStructBArrow(t, am.DefaultAllocator, 7, 2)
	defer rec.Release()
	dt := &arrow.Decimal128Type{Precision: 20, Scale: 2}
	db := array.NewDecimal128Builder(am.DefaultAllocator, dt)
	db.Append(decimal128.FromI64(12345))
	db.Append(decimal128.FromI64(-12345))
	dec := db.NewArray()
	db.Release()
	defer dec.Release()
	fields := append(append([]arrow.Field(nil), rec.Schema().Fields()...), arrow.Field{Name: "dec", Type: dt})
	cols := append(append([]arrow.Array(nil), rec.Columns()...), dec)
	mixed := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 2)
	defer mixed.Release()
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeStructList: true, NativeRecursiveStruct: true, NativeDecimal128: true}, mixed)
	if e != nil {
		t.Fatal(e)
	}
	q = q.SelectExprs(Col("left").Alias("left"), expr.RoundDecimal128(Col("dec"), 20, 0, expr.DecimalRoundTowardZero).Alias("rounded"))
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	c, e := df.Column("rounded")
	if e != nil {
		t.Fatal(e)
	}
	chunks, e := c.Decimal128Chunks()
	if e != nil {
		t.Fatal(e)
	}
	row := 0
	for _, chunk := range chunks {
		if chunk.Precision() != 20 || chunk.Scale() != 0 {
			t.Fatal("decimal metadata")
		}
		for i := 0; i < chunk.Len(); i++ {
			v, ok, e := chunk.Value(i)
			want := int64(123)
			if row == 1 {
				want = -123
			}
			if e != nil || !ok || v.BigInt().Int64() != want {
				t.Fatal("parallel Decimal oracle", row, e)
			}
			row++
		}
	}
	if row != 2 {
		t.Fatal("row count")
	}
}

func TestListStructBBoundedNestedKeyRejectedBeforeSpill(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.PreparedCacheBytes = 0
	opts.MemoryLimitBytes = 1 << 20
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 16 << 20}
	s := NewSession(opts)
	defer s.Close()
	rec := listStructBArrow(t, am.DefaultAllocator, 7, 65)
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeStructList: true}, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	sinkPath := filepath.Join(t.TempDir(), "supported.parquet")
	if _, e = q.SinkParquet(context.Background(), sinkPath, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	readerSession := NewSession(engine.DefaultOptions())
	defer readerSession.Close()
	rq, e := readerSession.ScanParquetWithOptions(sinkPath, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	rd, e := rq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	listStructDCheck(t, rd, 7, []string{"left", "right", "outer", "id", "index"}, false, false)
	rd.Close()
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	df, e := q.Sort(Asc("left")).Collect(memory.WithQueryBudget(context.Background(), budget))
	if e == nil {
		df.Close()
		t.Fatal("bounded List key unexpectedly accepted")
	}
	if budget.Snapshot().SpilledBytes != 0 {
		t.Fatal("unsupported sort wrote spill before rejection")
	}
	recursiveCNoFiles(t, dir)
	budget.Close()
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("rejected sort leak")
	}
}
