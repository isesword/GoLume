package parquet_test

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	ps "github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/engine"
	"gocolumn/kernel"
	"gocolumn/lazy"
	"gocolumn/memory"
	"gocolumn/source"
	native "gocolumn/source/parquet"
	"math"
	"path/filepath"
	"reflect"
	"testing"
)

func nestedSinkValue(row int, text, O, I, E bool) any {
	value := func(i int) any {
		if text {
			return []string{"", "界🙂", "a\x00b"}[i%3]
		}
		return []int64{math.MinInt64, math.MaxInt64, -7}[i%3]
	}
	switch row % 8 {
	case 0:
		if O {
			return nil
		}
		return []any{}
	case 1:
		return []any{}
	case 2:
		if I {
			return []any{nil}
		}
		return []any{[]any{}}
	case 3:
		return []any{[]any{}}
	case 4:
		if E {
			return []any{[]any{nil}}
		}
		return []any{[]any{value(1)}}
	case 5:
		return []any{[]any{value(0), value(1)}, []any{}, []any{value(2)}}
	case 6:
		if E {
			return []any{[]any{value(0), nil, value(2)}}
		}
		return []any{[]any{value(0), value(1), value(2)}}
	default:
		return []any{[]any{value(2)}, []any{value(0)}}
	}
}
func nestedSinkWrapped(row, depth int, text, O, I, E bool) any {
	v := nestedSinkValue(row+1, text, O, I, E)
	for d := depth - 1; d >= 0; d-- {
		if row%19 == d {
			v = nil
		} else {
			v = map[string]any{"node": v}
		}
	}
	return v
}
func nestedSinkRecord(t *testing.T, alloc am.Allocator, n, depth int, text, O, I, E bool) arrow.RecordBatch {
	makeType := func(text bool) arrow.DataType {
		var dt arrow.DataType = arrow.PrimitiveTypes.Int64
		if text {
			dt = arrow.BinaryTypes.String
		}
		return arrow.ListOfField(arrow.Field{Name: "item", Nullable: I, Type: arrow.ListOfField(arrow.Field{Name: "item", Nullable: E, Type: dt})})
	}
	first := makeType(text)
	var second arrow.DataType = makeType(!text)
	for d := depth - 1; d >= 0; d-- {
		nullable := true
		if d == depth-1 {
			nullable = O
		}
		second = arrow.StructOf(arrow.Field{Name: "node", Type: second, Nullable: nullable})
	}
	nullable := O
	if depth > 0 {
		nullable = true
	}
	sc := arrow.NewSchema([]arrow.Field{{Name: "first", Type: first, Nullable: O}, {Name: "second.literal", Type: second, Nullable: nullable}}, nil)
	b := array.NewRecordBuilder(alloc, sc)
	defer b.Release()
	for r := 0; r < n; r++ {
		sinkInteropAppend(b.Field(0), nestedSinkValue(r, text, O, I, E))
		sinkInteropAppend(b.Field(1), nestedSinkWrapped(r, depth, !text, O, I, E))
	}
	return b.NewRecordBatch()
}
func nestedSinkCheckField(t *testing.T, f arrow.Field, depth int, text, O, I, E bool) {
	t.Helper()
	for d := 0; d < depth; d++ {
		if !f.Nullable {
			t.Fatal("ancestor nullable")
		}
		st, ok := f.Type.(*arrow.StructType)
		if !ok || st.NumFields() != 1 {
			t.Fatal("ancestor schema")
		}
		f = st.Field(0)
	}
	if f.Nullable != O {
		t.Fatal("outer nullable")
	}
	outer, ok := f.Type.(*arrow.ListType)
	if !ok || outer.ElemField().Nullable != I {
		t.Fatal("inner list metadata", f)
	}
	inner, ok := outer.Elem().(*arrow.ListType)
	if !ok || inner.ElemField().Nullable != E {
		t.Fatal("terminal nullable")
	}
	want := arrow.INT64
	if text {
		want = arrow.STRING
	}
	if inner.Elem().ID() != want {
		t.Fatal("terminal type")
	}
}
func nestedSinkNativeValue(v *memory.ArrayNode, row int) any {
	if !v.IsValid(row) {
		return nil
	}
	if v.Kind == memory.TypeList {
		a := make([]any, v.Offsets[row+1]-v.Offsets[row])
		for i := v.Offsets[row]; i < v.Offsets[row+1]; i++ {
			a[i-v.Offsets[row]] = nestedSinkNativeValue(v.Child, int(i))
		}
		return a
	}
	if v.Kind == memory.TypeInt64 {
		return v.Int64.Values[row]
	}
	return string(v.Strings.ValueBytes(row))
}
func nestedSinkVerify(t *testing.T, path string, alloc am.Allocator, start, n, depth int, text, O, I, E bool) {
	t.Helper()
	rd, e := file.OpenParquetFile(path, false)
	if e != nil {
		t.Fatal(e)
	}
	defer rd.Close()
	if rd.NumRows() != int64(n) || rd.MetaData().Schema.NumColumns() != 2 {
		t.Fatal("physical rows/leafcount")
	}
	if n > 0 && rd.NumRowGroups() < 2 {
		t.Fatal("fixture did not produce multiple row groups")
	}
	for group := 0; group < rd.NumRowGroups(); group++ {
		if rd.RowGroup(group).NumRows() > 4 {
			t.Fatal("row group bound")
		}
	}
	for col := 0; col < 2; col++ {
		var node ps.Node = rd.MetaData().Schema.Root().Field(col)
		ancestors := 0
		if col == 1 {
			ancestors = depth
		}
		for d := 0; d < ancestors; d++ {
			g, ok := node.(*ps.GroupNode)
			if !ok || g.NumFields() != 1 || node.RepetitionType() != pq.Repetitions.Optional {
				t.Fatal("physical ancestor")
			}
			node = g.Field(0)
		}
		for _, nullable := range []bool{O, I} {
			g, ok := node.(*ps.GroupNode)
			if !ok || g.NumFields() != 1 {
				t.Fatal("physical LIST group")
			}
			if _, ok := g.LogicalType().(ps.ListLogicalType); !ok {
				t.Fatal("physical LIST annotation")
			}
			want := pq.Repetitions.Required
			if nullable {
				want = pq.Repetitions.Optional
			}
			if node.RepetitionType() != want {
				t.Fatal("physical LIST nullable")
			}
			wrapper, ok := g.Field(0).(*ps.GroupNode)
			if !ok || wrapper.RepetitionType() != pq.Repetitions.Repeated || wrapper.NumFields() != 1 {
				t.Fatal("standard repeated wrapper")
			}
			node = wrapper.Field(0)
		}
		want := pq.Repetitions.Required
		if E {
			want = pq.Repetitions.Optional
		}
		if node.RepetitionType() != want {
			t.Fatal("physical terminal nullable")
		}
	}
	sum := int16(2)
	for _, b := range []bool{O, I, E} {
		if b {
			sum++
		}
	}
	for j := 0; j < 2; j++ {
		c := rd.MetaData().Schema.Column(j)
		wantDef := sum
		if j == 1 {
			wantDef += int16(depth)
		}
		if c.MaxRepetitionLevel() != 2 || c.MaxDefinitionLevel() != wantDef {
			t.Fatal("physical levels", j, c.MaxRepetitionLevel(), c.MaxDefinitionLevel(), wantDef)
		}
		isText := text
		if j == 1 {
			isText = !text
		}
		if isText {
			if c.PhysicalType() != pq.Types.ByteArray {
				t.Fatal("String physical")
			}
			if _, ok := c.LogicalType().(ps.StringLogicalType); !ok {
				t.Fatal("String annotation")
			}
		} else {
			if c.PhysicalType() != pq.Types.Int64 {
				t.Fatal("Int64 physical")
			}
		}
	}
	var gotDefs, gotReps []int16
	for group := 0; group < rd.NumRowGroups(); group++ {
		cr, err := rd.RowGroup(group).Column(0)
		if err != nil {
			t.Fatal(err)
		}
		var defs, reps [128]int16
		var got int64
		if text {
			var values [128]pq.ByteArray
			got, _, err = cr.(*file.ByteArrayColumnChunkReader).ReadBatch(128, values[:], defs[:], reps[:])
		} else {
			var values [128]int64
			got, _, err = cr.(*file.Int64ColumnChunkReader).ReadBatch(128, values[:], defs[:], reps[:])
		}
		if err != nil {
			t.Fatal(err)
		}
		gotDefs = append(gotDefs, defs[:got]...)
		gotReps = append(gotReps, reps[:got]...)
	}
	var wantDefs, wantReps []int16
	emit := func(d, r int16) { wantDefs = append(wantDefs, d); wantReps = append(wantReps, r) }
	for row := start; row < start+n; row++ {
		outer := nestedSinkValue(row, text, O, I, E)
		d := int16(0)
		if outer == nil {
			emit(d, 0)
			continue
		}
		if O {
			d++
		}
		inners := outer.([]any)
		if len(inners) == 0 {
			emit(d, 0)
			continue
		}
		d++
		for i, inner := range inners {
			r := int16(0)
			if i > 0 {
				r = 1
			}
			if inner == nil {
				emit(d, r)
				continue
			}
			dd := d
			if I {
				dd++
			}
			leaves := inner.([]any)
			if len(leaves) == 0 {
				emit(dd, r)
				continue
			}
			dd++
			for j, v := range leaves {
				rr := r
				if j > 0 {
					rr = 2
				}
				ddd := dd
				if v != nil && E {
					ddd++
				}
				emit(ddd, rr)
			}
		}
	}
	if !reflect.DeepEqual(gotDefs, wantDefs) || !reflect.DeepEqual(gotReps, wantReps) {
		t.Fatal("independent physical rep/def stream", gotDefs, gotReps, wantDefs, wantReps)
	}
	external, e := pqarrow.NewFileReader(rd, pqarrow.ArrowReadProperties{}, alloc)
	if e != nil {
		t.Fatal(e)
	}
	tab, e := external.ReadTable(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer tab.Release()
	nestedSinkCheckField(t, tab.Schema().Field(0), 0, text, O, I, E)
	nestedSinkCheckField(t, tab.Schema().Field(1), depth, !text, O, I, E)
	tr := array.NewTableReader(tab, 3)
	defer tr.Release()
	row := start
	for tr.Next() {
		r := tr.RecordBatch()
		for i := 0; i < int(r.NumRows()); i++ {
			for j, want := range []any{nestedSinkValue(row, text, O, I, E), nestedSinkWrapped(row, depth, !text, O, I, E)} {
				if got := sinkInteropValue(r.Column(j), i); !reflect.DeepEqual(got, want) {
					t.Fatalf("external row%d col%d got%#v want%#v", row, j, got, want)
				}
			}
			row++
		}
	}
	if tr.Err() != nil || row != start+n {
		t.Fatal("external row count", row, tr.Err())
	}
	src, e := native.OpenFileWithOptions(path, native.Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	opts := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 3, BatchBytes: 2 << 20, MemoryManager: m}
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	row = start
	for {
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		if e = memory.ValidateNestedListsBatch(b, src.Schema(), nil); e != nil {
			t.Fatal(e)
		}
		for i := 0; i < b.Rows; i++ {
			if got := nestedSinkNativeValue(b.Arrays["first"], i); !reflect.DeepEqual(got, nestedSinkValue(row, text, O, I, E)) {
				t.Fatal("native first", row, got)
			}
			cur := b
			visible := true
			name := "second.literal"
			for d := 0; d < depth; d++ {
				v := cur.Structs[name]
				if !v.IsValid(i) {
					visible = false
				}
				cur = v.Values
				name = "node"
			}
			got := nestedSinkNativeValue(cur.Arrays[name], i)
			want := nestedSinkValue(row+1, !text, O, I, E)
			if !visible {
				want = nil
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("native second", row, got, want)
			}
			row++
		}
	}
	st.Close()
	if row != start+n || m.Used() != 0 {
		t.Fatal("native row/lease", row, m.Used())
	}
}
func TestNestedListSinkIndependentInterop(t *testing.T) {
	for _, text := range []bool{false, true} {
		for mask := 0; mask < 8; mask++ {
			for _, depth := range []int{0, 8} {
				for _, n := range []int{0, 19} {
					t.Run(fmt.Sprint(text, mask, depth, n), func(t *testing.T) {
						O, I, E := mask&1 != 0, mask&2 != 0, mask&4 != 0
						alloc := am.NewCheckedAllocator(am.DefaultAllocator)
						defer alloc.AssertSize(t, 0)
						rec := nestedSinkRecord(t, alloc, n, depth, text, O, I, E)
						defer rec.Release()
						session := lazy.NewSession(engine.DefaultOptions())
						defer session.Close()
						q, e := session.ScanArrowWithOptions(lazy.ArrowScanOptions{NativeNested: true}, rec)
						if e != nil {
							t.Fatal(e)
						}
						path := filepath.Join(t.TempDir(), "interop.parquet")
						if _, e = q.SinkParquet(context.Background(), path, lazy.ParquetSinkOptions{RowGroupRows: 4}); e != nil {
							t.Fatal(e)
						}
						nestedSinkVerify(t, path, alloc, 0, n, depth, text, O, I, E)
					})
				}
			}
		}
	}
}

func TestNestedListSinkIndependentShards(t *testing.T) {
	for _, n := range []int{0, 19} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			rec := nestedSinkRecord(t, alloc, n, 8, true, true, true, true)
			defer rec.Release()
			session := lazy.NewSession(engine.DefaultOptions())
			defer session.Close()
			q, e := session.ScanArrowWithOptions(lazy.ArrowScanOptions{NativeNested: true}, rec)
			if e != nil {
				t.Fatal(e)
			}
			result, e := q.SinkParquet(context.Background(), filepath.Join(t.TempDir(), "parts"), lazy.ParquetSinkOptions{SinkOptions: lazy.SinkOptions{MaxRowsPerFile: 7}, RowGroupRows: 3})
			if e != nil {
				t.Fatal(e)
			}
			want := []int{7, 7, 5}
			if n == 0 {
				want = []int{0}
			}
			if len(result.Files) != len(want) {
				t.Fatal("part count", len(result.Files))
			}
			start := 0
			for i, path := range result.Files {
				nestedSinkVerify(t, path, alloc, start, want[i], 8, true, true, true, true)
				start += want[i]
			}
			if start != n {
				t.Fatal("shard total")
			}
		})
	}
}
