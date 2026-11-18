package arrowrecord

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func nestedArrowExpected(row int, text bool) any {
	var a, b any = int64(-7), int64(9223372036854775807)
	if text {
		a, b = "界\x00", ""
	}
	switch row % 7 {
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
	case 5:
		return []any{[]any{a, nil}, []any{}, nil, []any{b}}
	default:
		return []any{[]any{b, a}}
	}
}
func nestedArrowAppend(b array.Builder, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch b := b.(type) {
	case *array.ListBuilder:
		b.Append(true)
		for _, e := range v.([]any) {
			nestedArrowAppend(b.ValueBuilder(), e)
		}
	case *array.Int64Builder:
		b.Append(v.(int64))
	case *array.StringBuilder:
		b.Append(v.(string))
	default:
		panic(fmt.Sprintf("unexpected builder %T", b))
	}
}
func nestedArrowRecord(t *testing.T, alloc am.Allocator, depth, n int) arrow.RecordBatch {
	t.Helper()
	arrays := make([]arrow.Array, 2)
	fields := make([]arrow.Field, 2)
	for col := 0; col < 2; col++ {
		var terminal arrow.DataType = arrow.PrimitiveTypes.Int64
		if col == 1 {
			terminal = arrow.BinaryTypes.String
		}
		dt := arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: terminal, Nullable: true}), Nullable: true})
		b := array.NewBuilder(alloc, dt)
		for row := 0; row < n; row++ {
			nestedArrowAppend(b, nestedArrowExpected(row, col == 1))
		}
		arr := b.NewArray()
		b.Release()
		if col == 0 {
			for d := 0; d < depth; d++ {
				var bitmap *am.Buffer
				nulls := 0
				if d == depth-1 {
					bits := make([]byte, (n+7)/8)
					for r := 0; r < n; r++ {
						if r%11 != 6 {
							bits[r/8] |= 1 << uint(r%8)
						} else {
							nulls++
						}
					}
					bitmap = am.NewBufferBytes(bits)
				}
				name := "inner"
				if d == 0 {
					name = "items"
				}
				wrapped, e := array.NewStructArrayWithFieldsAndNulls([]arrow.Array{arr}, []arrow.Field{{Name: name, Type: arr.DataType(), Nullable: true}}, bitmap, nulls, 0)
				if bitmap != nil {
					bitmap.Release()
				}
				arr.Release()
				if e != nil {
					t.Fatal(e)
				}
				arr = wrapped
			}
		}
		arrays[col] = arr
		name := "other"
		if col == 0 {
			name = "items"
			if depth > 0 {
				name = "root"
			}
		}
		fields[col] = arrow.Field{Name: name, Type: arr.DataType(), Nullable: true}
	}
	defer arrays[0].Release()
	defer arrays[1].Release()
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), arrays, int64(n))
}
func nestedArrowRead(v *memory.ArrayNode, row int) any {
	if !v.IsValid(row) {
		return nil
	}
	switch v.Kind {
	case memory.TypeList:
		lo, hi := v.Offsets[row], v.Offsets[row+1]
		out := make([]any, hi-lo)
		for i := lo; i < hi; i++ {
			out[i-lo] = nestedArrowRead(v.Child, int(i))
		}
		return out
	case memory.TypeInt64:
		return v.Int64.Values[row]
	case memory.TypeString:
		return string(v.Strings.Data[v.Strings.Offsets[row]:v.Strings.Offsets[row+1]])
	}
	panic("unexpected memory kind")
}
func nestedArrowCheck(t *testing.T, b *memory.Batch, depth, start int) {
	t.Helper()
	current := b
	if depth > 0 {
		node := b.Structs["root"]
		for d := 0; d < depth; d++ {
			if node == nil || node.Rows != b.Rows {
				t.Fatal("outer shape")
			}
			if d == 0 {
				for r := 0; r < b.Rows; r++ {
					if node.IsValid(r) != ((start+r)%11 != 6) {
						t.Fatal("ancestor validity")
					}
				}
			}
			current = node.Values
			if d+1 < depth {
				node = current.Structs["inner"]
			}
		}
	}
	for row := 0; row < b.Rows; row++ {
		if depth == 0 || (start+row)%11 != 6 {
			if g := nestedArrowRead(current.Arrays["items"], row); !reflect.DeepEqual(g, nestedArrowExpected(start+row, false)) {
				t.Fatalf("int row%d got%#v", start+row, g)
			}
		}
		if g := nestedArrowRead(b.Arrays["other"], row); !reflect.DeepEqual(g, nestedArrowExpected(start+row, true)) {
			t.Fatalf("text row%d got%#v", start+row, g)
		}
	}
}
func TestNestedListArrowSlicedThreeAxes(t *testing.T) {
	for _, depth := range []int{0, 1, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			rec := nestedArrowRecord(t, alloc, depth, 132)
			slice := rec.NewSlice(63, 130)
			rec.Release()
			src, e := NewSourceWithOptions(Options{NativeNested: true}, slice)
			slice.Release()
			if e != nil {
				t.Fatal(e)
			}
			defer src.Close()
			schema := src.Schema()
			if len(schema.Fields) != 2 {
				t.Fatal("schema")
			}
			schema.Fields[0].Children[0].Name = "poison"
			if src.Schema().Fields[0].Children[0].Name == "poison" {
				t.Fatal("Schema alias")
			}
			m := memory.NewBudgetManager(0)
			opts := testReadOptions()
			opts.BatchRows = 1
			opts.MemoryManager = m
			stream, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				t.Fatal(e)
			}
			src.Close()
			defer stream.Close()
			for row := 63; row < 130; row++ {
				out := memory.NewBatch()
				ok, e := stream.Next(context.Background(), out)
				if e != nil || !ok {
					t.Fatal(row, ok, e)
				}
				nestedArrowCheck(t, out, depth, row)
			}
			ok, e := stream.Next(context.Background(), memory.NewBatch())
			if ok || e != nil {
				t.Fatal("EOF", ok, e)
			}
			stream.Close()
			if m.Used() != 0 {
				t.Fatal("stream charge leak")
			}
		})
	}
}

func TestNestedListArrowOwnedAfterClose(t *testing.T) {
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	rec := nestedArrowRecord(t, alloc, 8, 14)
	src, e := NewSourceWithOptions(Options{NativeNested: true}, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	schema := src.Schema()
	m := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.BatchRows = 3
	opts.MemoryManager = m
	stream, e := src.NewBatchStream(context.Background(), opts)
	if e != nil {
		src.Close()
		t.Fatal(e)
	}
	src.Close()
	ownedM := memory.NewBudgetManager(0)
	var batches []*memory.Batch
	var leases []memory.Reservation
	var starts []int
	row := 0
	for {
		out := memory.NewBatch()
		ok, e := stream.Next(context.Background(), out)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		copy, lease, e := memory.CloneCollectBatchNestedLists(context.Background(), ownedM, out, schema)
		if e != nil {
			t.Fatal(e)
		}
		starts = append(starts, row)
		row += out.Rows
		batches = append(batches, copy)
		leases = append(leases, lease)
	}
	stream.Close()
	if m.Used() != 0 || row != 14 {
		t.Fatal("view close/count", m.Used(), row)
	}
	for i, b := range batches {
		nestedArrowCheck(t, b, 8, starts[i])
		b.ReleaseOwned()
		leases[i].Release()
	}
	if ownedM.Used() != 0 {
		t.Fatal("owned leak")
	}
}
func TestNestedListArrowDefaultAndBudget(t *testing.T) {
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	rec := nestedArrowRecord(t, alloc, 0, 7)
	defer rec.Release()
	for _, options := range []Options{{}, {NativeRecursiveStruct: true}, {NativeStructList: true}} {
		src, e := NewSourceWithOptions(options, rec)
		if e != nil {
			continue
		}
		st, e := src.NewBatchStream(context.Background(), testReadOptions())
		if e != nil {
			src.Close()
			continue
		}
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e != nil || !ok || len(b.Arrays) != 0 {
			t.Fatal("old options expanded", options, ok, e)
		}
		st.Close()
		src.Close()
	}
	src, e := NewSourceWithOptions(Options{NativeNested: true}, rec)
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	m := memory.NewBudgetManager(1)
	opts := testReadOptions()
	opts.MemoryManager = m
	st, e := src.NewBatchStream(context.Background(), opts)
	if e == nil {
		_, e = st.Next(context.Background(), memory.NewBatch())
		st.Close()
	}
	if e == nil || m.Used() != 0 {
		t.Fatal("tiny budget cleanup", e, m.Used())
	}
}

type nestedArrowCancelContext struct {
	context.Context
	target   string
	checks   int
	stop     int
	canceled bool
	manager  memory.Manager
	charge   int64
}

func (c *nestedArrowCancelContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	var pcs [32]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, c.target) {
			c.checks++
			stop := c.stop
			if stop == 0 {
				stop = 2
			}
			if c.checks == stop {
				c.canceled = true
				c.charge = c.manager.Used()
				return context.Canceled
			}
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}
func TestNestedListArrowRebaseRunningCancel(t *testing.T) {
	for _, target := range []string{".arrayOffsets", ".arrayValidity"} {
		t.Run(target, func(t *testing.T) {
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			rec := nestedArrowRecord(t, alloc, 0, 21)
			src, e := NewSourceWithOptions(Options{NativeNested: true}, rec)
			rec.Release()
			if e != nil {
				t.Fatal(e)
			}
			defer src.Close()
			m := memory.NewBudgetManager(0)
			opts := testReadOptions()
			opts.MemoryManager = m
			opts.BatchRows = 1
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			for row := 0; row < 6; row++ {
				b := memory.NewBatch()
				ok, e := st.Next(context.Background(), b)
				if e != nil || !ok {
					t.Fatal(row, e)
				}
			}
			ctx := &nestedArrowCancelContext{Context: context.Background(), target: target, manager: m}
			ok, e := st.Next(ctx, memory.NewBatch())
			if ok || !errors.Is(e, context.Canceled) || ctx.checks != 2 || ctx.charge <= 0 {
				t.Fatalf("rebase cancel %v %v checks%d charge%d", ok, e, ctx.checks, ctx.charge)
			}
			st.Close()
			if m.Used() != 0 {
				t.Fatal("view cancel leak")
			}
		})
	}
}
func TestNestedListArrowTypedNilAndDepth(t *testing.T) {
	f := memory.Field{Name: "x", Type: memory.TypeList, Children: []memory.Field{{Name: "item", Type: memory.TypeList, Children: []memory.Field{{Name: "item", Type: memory.TypeInt64}}}}}
	var list *array.List
	var integer *array.Int64
	var str *array.String
	for _, tc := range []struct {
		a     arrow.Array
		field memory.Field
		depth int
	}{{list, f, 0}, {integer, f.Children[0].Children[0], 2}, {str, memory.Field{Name: "item", Type: memory.TypeString}, 2}} {
		if _, e := arrowArrayNode(tc.a, tc.field, tc.depth); e == nil {
			t.Fatal("typed nil accepted")
		}
	}
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	rec := nestedArrowRecord(t, alloc, 9, 7)
	defer rec.Release()
	src, e := NewSourceWithOptions(Options{NativeNested: true}, rec)
	if e == nil {
		src.Close()
		t.Fatal("depth9 accepted")
	}
}

func TestNestedListArrowHiddenOffsetsReject(t *testing.T) {
	for _, axis := range []string{"outer", "inner"} {
		t.Run(axis, func(t *testing.T) {
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			rec := nestedArrowRecord(t, alloc, 0, 7)
			defer rec.Release()
			outer := rec.Column(0).(*array.List)
			victim := outer
			if axis == "inner" {
				victim = outer.ListValues().(*array.List)
			}
			offsets := victim.Offsets()
			if len(offsets) < 3 {
				t.Fatal("fixture too small")
			}
			old := offsets[1]
			offsets[1] = offsets[2] + 1
			defer func() { offsets[1] = old }()
			if offsets[1] <= offsets[2] {
				t.Fatal("fixture not decreasing")
			}
			src, e := NewSourceWithOptions(Options{NativeNested: true}, rec)
			if e == nil {
				src.Close()
				t.Fatal("hidden malformed offsets accepted")
			}
		})
	}
}

func nestedArrowWrapList(child arrow.Array, offsets []int32) arrow.Array {
	dt := arrow.ListOfField(arrow.Field{Name: "item", Type: child.DataType(), Nullable: true})
	buf := am.NewBufferBytes(arrow.Int32Traits.CastToBytes(offsets))
	defer buf.Release()
	data := array.NewData(dt, len(offsets)-1, []*am.Buffer{nil, buf}, []arrow.ArrayData{child.Data()}, 0, 0)
	defer data.Release()
	return array.NewListData(data)
}
func TestNestedListArrowDictionary(t *testing.T) {
	for _, nullMode := range []string{"none", "referenced", "unreferenced"} {
		t.Run(nullMode, func(t *testing.T) {
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			sb := array.NewStringBuilder(alloc)
			sb.Append("界")
			if nullMode == "referenced" {
				sb.AppendNull()
			} else {
				sb.Append("")
			}
			if nullMode == "unreferenced" {
				sb.AppendNull()
			}
			table := sb.NewArray()
			sb.Release()
			defer table.Release()
			ib := array.NewInt8Builder(alloc)
			ib.AppendValues([]int8{0, 1}, nil)
			indices := ib.NewArray()
			ib.Release()
			defer indices.Release()
			dict := array.NewDictionaryArray(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}, indices, table)
			defer dict.Release()
			inner := nestedArrowWrapList(dict, []int32{0, 1, 2})
			defer inner.Release()
			outer := nestedArrowWrapList(inner, []int32{0, 2})
			defer outer.Release()
			rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "x", Type: outer.DataType(), Nullable: true}}, nil), []arrow.Array{outer}, 1)
			defer rec.Release()
			src, e := NewSourceWithOptions(Options{NativeNested: true}, rec)
			if nullMode != "none" {
				if e == nil {
					src.Close()
					t.Fatal("NULL dictionary table accepted", nullMode)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			defer src.Close()
			f := src.Schema().Fields[0].Children[0].Children[0]
			if f.Type != memory.TypeString || f.Encoding != memory.EncodingView || f.DictionaryDomain != "" {
				t.Fatal("dictionary metadata not normalized", f)
			}
			st, e := src.NewBatchStream(context.Background(), testReadOptions())
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e != nil || !ok {
				t.Fatal(ok, e)
			}
			if got := nestedArrowRead(b.Arrays["x"], 0); !reflect.DeepEqual(got, []any{[]any{"界"}, []any{""}}) {
				t.Fatalf("dictionary value %#v", got)
			}
		})
	}
}

func TestNestedListArrowPayloadPreflightCancel(t *testing.T) {
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	dt := arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))
	lb := array.NewBuilder(alloc, dt).(*array.ListBuilder)
	lb.Append(true)
	inner := lb.ValueBuilder().(*array.ListBuilder)
	for i := 0; i < 4097; i++ {
		inner.Append(true)
	}
	a := lb.NewArray()
	lb.Release()
	defer a.Release()
	r := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "x", Type: dt, Nullable: true}}, nil), []arrow.Array{a}, 1)
	defer r.Release()
	src, e := NewSourceWithOptions(Options{NativeNested: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	m := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.MemoryManager = m
	opts.BatchPayloadBytes = 1 << 20
	st, e := src.NewBatchStream(context.Background(), opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	before := m.Used()
	ctx := &nestedArrowCancelContext{Context: context.Background(), target: ".estimateArrayRangeBytes", manager: m, stop: 100}
	ok, e := st.Next(ctx, memory.NewBatch())
	if ok || !errors.Is(e, context.Canceled) || ctx.checks != 100 || ctx.charge != before {
		t.Fatalf("payload preflight %v checks%d charge%d before%d", e, ctx.checks, ctx.charge, before)
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("payload preflight cleanup")
	}
}

func TestNestedListArrowRequiredThreeDomains(t *testing.T) {
	for axis := 0; axis < 3; axis++ {
		for _, invalid := range []bool{false, true} {
			t.Run(fmt.Sprintf("axis%d/invalid%v", axis, invalid), func(t *testing.T) {
				alloc := am.NewCheckedAllocator(am.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				leaf := arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: axis != 2}
				inner := arrow.Field{Name: "item", Type: arrow.ListOfField(leaf), Nullable: axis != 1}
				dt := arrow.ListOfField(inner)
				b := array.NewBuilder(alloc, dt)
				value := nestedArrowExpected(6, false)
				if invalid {
					switch axis {
					case 0:
						value = nil
					case 1:
						value = []any{nil}
					case 2:
						value = []any{[]any{nil}}
					}
				}
				nestedArrowAppend(b, value)
				a := b.NewArray()
				b.Release()
				defer a.Release()
				r := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "x", Type: dt, Nullable: axis != 0}}, nil), []arrow.Array{a}, 1)
				defer r.Release()
				src, e := NewSourceWithOptions(Options{NativeNested: true}, r)
				if invalid {
					if e == nil {
						src.Close()
						t.Fatal("required domain NULL accepted")
					}
					return
				}
				if e != nil {
					t.Fatal(e)
				}
				src.Close()
			})
		}
	}
}
