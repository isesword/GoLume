package arrowrecord

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func structListArrowTypes() []arrow.Field {
	return []arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true}, {Name: "i16", Type: arrow.PrimitiveTypes.Int16, Nullable: true}, {Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true}, {Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8, Nullable: true}, {Name: "u16", Type: arrow.PrimitiveTypes.Uint16, Nullable: true}, {Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true}, {Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true}, {Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, {Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true}, {Name: "date", Type: arrow.FixedWidthTypes.Date32, Nullable: true}, {Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true}, {Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
	}
}
func structListArrowArray(t *testing.T, alloc arrowmem.Allocator) arrow.Array {
	t.Helper()
	fields := structListArrowTypes()
	lb := array.NewListBuilderWithField(alloc, arrow.Field{Name: "item", Type: arrow.StructOf(fields...), Nullable: true})
	defer lb.Release()
	sb := lb.ValueBuilder().(*array.StructBuilder)
	appendValue := func(which int) {
		sb.Append(true)
		for j := range fields {
			child := sb.FieldBuilder(j)
			if which < 0 {
				child.AppendNull()
				continue
			}
			n := int64(7 + which*2)
			switch b := child.(type) {
			case *array.Int8Builder:
				b.Append(int8(-n))
			case *array.Int16Builder:
				b.Append(int16(-n))
			case *array.Int32Builder:
				b.Append(int32(-n))
			case *array.Int64Builder:
				b.Append(-n)
			case *array.Uint8Builder:
				b.Append(uint8(n))
			case *array.Uint16Builder:
				b.Append(uint16(n))
			case *array.Uint32Builder:
				b.Append(uint32(n))
			case *array.Uint64Builder:
				b.Append(uint64(n))
			case *array.Float32Builder:
				b.Append(float32(n) + .25)
			case *array.Float64Builder:
				b.Append(float64(n) + .25)
			case *array.BooleanBuilder:
				b.Append(which == 0)
			case *array.Date32Builder:
				b.Append(arrow.Date32(-n))
			case *array.TimestampBuilder:
				b.Append(arrow.Timestamp(-n))
			case *array.StringBuilder:
				if which == 0 {
					b.Append("界")
				} else {
					b.Append("")
				}
			default:
				t.Fatalf("unexpected Arrow builder %T", b)
			}
		}
	}
	lb.AppendNull()
	lb.Append(true)
	lb.Append(true)
	sb.AppendNull()
	lb.Append(true)
	appendValue(-1)
	appendValue(0)
	lb.Append(true)
	appendValue(1)
	return lb.NewArray()
}
func structListArrowRecord(t *testing.T, alloc arrowmem.Allocator, depth int) arrow.RecordBatch {
	t.Helper()
	list := structListArrowArray(t, alloc)
	defer list.Release()
	var current arrow.Array = list
	current.Retain()
	for i := 0; i < depth; i++ {
		var bitmap *arrowmem.Buffer
		nulls := 0
		if i == depth-1 {
			bitmap = arrowmem.NewBufferBytes([]byte{15})
			nulls = 1
		}
		name := "inner"
		nullable := false
		if i == 0 {
			name = "items"
			nullable = true
		}
		next, e := array.NewStructArrayWithFieldsAndNulls([]arrow.Array{current}, []arrow.Field{{Name: name, Type: current.DataType(), Nullable: nullable}}, bitmap, nulls, 0)
		if bitmap != nil {
			bitmap.Release()
		}
		current.Release()
		if e != nil {
			t.Fatal(e)
		}
		current = next
	}
	defer current.Release()
	name := "items"
	if depth > 0 {
		name = "root"
	}
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: name, Type: current.DataType(), Nullable: true}, {Name: "other", Type: list.DataType(), Nullable: true}}, nil), []arrow.Array{current, list}, 5)
}
func structListArrowCheckList(t *testing.T, v *memory.StructListVector, rows []int, hiddenLast, canonical bool) {
	t.Helper()
	var elements []int
	var elementVisible []bool
	offset := 0
	spans := [][]int{nil, nil, {0}, {1, 2}, {3}}
	if len(v.Offsets) != len(rows)+1 || v.Offsets[0] != 0 {
		t.Fatal("List offsets")
	}
	for i, row := range rows {
		valid := row != 0 && !(hiddenLast && row == 4)
		if v.IsValid(i) != valid {
			t.Fatal("parent NULL", row, hiddenLast)
		}
		if valid || (!canonical && hiddenLast && row == 4) {
			elements = append(elements, spans[row]...)
			for range spans[row] {
				elementVisible = append(elementVisible, valid)
			}
			offset += len(spans[row])
		}
		if v.Offsets[i+1] != uint32(offset) {
			t.Fatal("List offset", row)
		}
	}
	if v.Values.Rows != len(elements) || len(v.Values.Fields) != 14 {
		t.Fatal("Struct element cardinality/fields")
	}
	for row, e := range elements {
		if !canonical && !elementVisible[row] {
			continue
		} // raw hidden payload may remain borrowed
		if v.Values.IsValid(row) != (e != 0 && elementVisible[row]) {
			t.Fatal("NULL Struct element")
		}
		valid := e >= 2 && elementVisible[row]
		which := e - 2
		n := int64(7 + which*2)
		for _, f := range v.Values.Fields {
			var ptr any
			switch f.Type {
			case memory.TypeInt8:
				ptr = v.Values.Values.Int8s[f.Name]
			case memory.TypeInt16:
				ptr = v.Values.Values.Int16s[f.Name]
			case memory.TypeInt32, memory.TypeDate32:
				ptr = v.Values.Values.Int32s[f.Name]
			case memory.TypeInt64, memory.TypeTimestamp:
				ptr = v.Values.Values.Int64s[f.Name]
			case memory.TypeUint8:
				ptr = v.Values.Values.Uint8s[f.Name]
			case memory.TypeUint16:
				ptr = v.Values.Values.Uint16s[f.Name]
			case memory.TypeUint32, memory.TypeBool:
				ptr = v.Values.Values.Uint32s[f.Name]
			case memory.TypeUint64:
				ptr = v.Values.Values.Uint64s[f.Name]
			case memory.TypeFloat32:
				ptr = v.Values.Values.Float32s[f.Name]
			case memory.TypeFloat64:
				ptr = v.Values.Values.Float64s[f.Name]
			case memory.TypeString:
				ptr = v.Values.Values.Strings[f.Name]
			}
			p := reflect.ValueOf(ptr)
			if !p.IsValid() || p.IsNil() {
				t.Fatal("missing field", f.Name)
			}
			if p.MethodByName("IsValid").Call([]reflect.Value{reflect.ValueOf(row)})[0].Bool() != valid {
				t.Fatal("child NULL", f.Name, row)
			}
			if !valid {
				continue
			}
			if f.Type == memory.TypeString {
				want := ""
				if which == 0 {
					want = "界"
				}
				if string(v.Values.Values.Strings[f.Name].ValueBytes(row)) != want {
					t.Fatal("String value")
				}
				continue
			}
			x := p.Elem().FieldByName("Values").Index(row)
			switch f.Type {
			case memory.TypeFloat32, memory.TypeFloat64:
				if x.Float() != float64(n)+.25 {
					t.Fatal("float value")
				}
			case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeBool:
				want := uint64(n)
				if f.Type == memory.TypeBool {
					want = 0
					if which == 0 {
						want = 1
					}
				}
				if x.Uint() != want {
					t.Fatal("unsigned/bool value")
				}
			default:
				if x.Int() != -n {
					t.Fatal("signed/date/timestamp value")
				}
			}
		}
	}
}
func structListArrowCheckBatch(t *testing.T, b *memory.Batch, depth int, rows []int, canonical bool) {
	t.Helper()
	current := b
	if depth > 0 {
		node := b.Structs["root"]
		for i := 0; i < depth; i++ {
			if node == nil || node.Rows != len(rows) {
				t.Fatal("ancestor shape")
			}
			if i == 0 {
				for j, row := range rows {
					if node.IsValid(j) != (row != 4) {
						t.Fatal("outer ancestor NULL")
					}
				}
			}
			current = node.Values
			if i+1 < depth {
				node = current.Structs["inner"]
			}
		}
	}
	structListArrowCheckList(t, current.StructLists["items"], rows, depth > 0, canonical)
	structListArrowCheckList(t, b.StructLists["other"], rows, false, canonical)
}

func TestStructListArrowAllFieldsSlicedOwned(t *testing.T) {
	for _, depth := range []int{0, 1, 7} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			rec := structListArrowRecord(t, alloc, depth)
			defer func() {
				if rec != nil {
					rec.Release()
				}
			}()
			for _, opts := range []Options{{}, {NativeRecursiveStruct: true}} {
				src, e := NewSourceWithOptions(opts, rec)
				if e == nil {
					if depth != 0 || opts.NativeRecursiveStruct {
						src.Close()
						t.Fatal("old recursive option admitted native Struct-list")
					}
					legacy, err := src.NewBatchStream(context.Background(), testReadOptions())
					if err != nil {
						src.Close()
						t.Fatal(err)
					}
					raw := memory.NewBatch()
					ok, err := legacy.Next(context.Background(), raw)
					if err != nil || !ok || raw.Opaque["items"] == nil || raw.Opaque["other"] == nil || len(raw.StructLists) != 0 {
						legacy.Close()
						src.Close()
						t.Fatal("default opaque transport changed", ok, err)
					}
					legacy.Close()
					src.Close()
				}
			}
			slice := rec.NewSlice(1, 5)
			rec.Release()
			rec = nil
			src, e := NewSourceWithOptions(Options{NativeStructList: true}, slice, slice)
			slice.Release()
			if e != nil {
				t.Fatal(e)
			}
			defer src.Close()
			schema := src.Schema()
			m := memory.NewBudgetManager(0)
			opts := testReadOptions()
			opts.BatchRows = 2
			opts.MemoryManager = m
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				src.Close()
				t.Fatal(e)
			}
			src.Close()
			defer st.Close()
			ownedM := memory.NewBudgetManager(0)
			var owned []*memory.Batch
			var leases []memory.Reservation
			var expected [][]int
			pos := 0
			for {
				b := memory.NewBatch()
				ok, e := st.Next(context.Background(), b)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				rows := make([]int, b.Rows)
				for i := range rows {
					rows[i] = 1 + (pos+i)%4
				}
				structListArrowCheckBatch(t, b, depth, rows, false)
				copy, lease, e := memory.CloneCollectBatchStructLists(context.Background(), ownedM, b, schema)
				if e != nil {
					t.Fatal(e)
				}
				owned = append(owned, copy)
				leases = append(leases, lease)
				expected = append(expected, rows)
				pos += b.Rows
			}
			st.Close()
			if pos != 8 || m.Used() != 0 {
				t.Fatal("stream rows/lifetime", pos, m.Used())
			}
			alloc.AssertSize(t, 0)
			for i, b := range owned {
				structListArrowCheckBatch(t, b, depth, expected[i], true)
				b.ReleaseOwned()
				leases[i].Release()
			}
			if ownedM.Used() != 0 {
				t.Fatal("owned release")
			}
		})
	}
}

func structListArrowDictionaryRecord(t *testing.T, alloc arrowmem.Allocator, nullTable, referenced bool) arrow.RecordBatch {
	t.Helper()
	sb := array.NewStringBuilder(alloc)
	sb.Append("界")
	if nullTable && referenced {
		sb.AppendNull()
	} else {
		sb.Append("")
	}
	if nullTable && !referenced {
		sb.AppendNull()
	}
	table := sb.NewArray()
	sb.Release()
	defer table.Release()
	ib := array.NewInt8Builder(alloc)
	ib.AppendValues([]int8{0, 0, 0, 1}, []bool{false, false, true, true})
	indices := ib.NewArray()
	ib.Release()
	defer indices.Release()
	dict := array.NewDictionaryArray(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}, indices, table)
	defer dict.Release()
	nb := array.NewInt64Builder(alloc)
	nb.AppendValues([]int64{0, 0, 7, 9}, []bool{false, false, true, true})
	n := nb.NewArray()
	nb.Release()
	defer n.Release()
	mask := arrowmem.NewBufferBytes([]byte{14})
	defer mask.Release()
	elements, e := array.NewStructArrayWithFieldsAndNulls([]arrow.Array{n, dict}, []arrow.Field{{Name: "n", Type: n.DataType(), Nullable: true}, {Name: "s", Type: dict.DataType(), Nullable: true}}, mask, 1, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer elements.Release()
	raw := make([]byte, 24)
	for i, v := range []uint32{0, 0, 0, 1, 3, 4} {
		binary.LittleEndian.PutUint32(raw[4*i:], v)
	}
	offsets := arrowmem.NewBufferBytes(raw)
	defer offsets.Release()
	valid := arrowmem.NewBufferBytes([]byte{30})
	defer valid.Release()
	data := array.NewData(arrow.ListOfField(arrow.Field{Name: "item", Type: elements.DataType(), Nullable: true}), 5, []*arrowmem.Buffer{valid, offsets}, []arrow.ArrayData{elements.Data()}, 1, 0)
	defer data.Release()
	list := array.NewListData(data)
	defer list.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "items", Type: list.DataType(), Nullable: true}}, nil), []arrow.Array{list}, 5)
}
func TestStructListArrowDictionaryNormalizationAndNullTable(t *testing.T) {
	for _, mode := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			rec := structListArrowDictionaryRecord(t, alloc, mode != 0, mode == 2)
			src, e := NewSourceWithOptions(Options{NativeStructList: true}, rec)
			rec.Release()
			if mode != 0 {
				if e == nil {
					src.Close()
					t.Fatal("NULL dictionary table accepted", mode)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			fields := src.Schema().Fields[0].Children[0].Children
			if fields[1].Encoding != memory.EncodingView || fields[1].DictionaryDomain != "" {
				t.Fatal("dictionary metadata not normalized")
			}
			m := memory.NewBudgetManager(0)
			opts := testReadOptions()
			opts.BatchRows = 5
			opts.MemoryManager = m
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				src.Close()
				t.Fatal(e)
			}
			src.Close()
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e != nil || !ok {
				t.Fatal(ok, e)
			}
			v := b.StructLists["items"]
			if !reflect.DeepEqual(v.Offsets, []uint32{0, 0, 0, 1, 3, 4}) || v.Values.IsValid(0) || !v.Values.IsValid(1) || v.Values.Values.Strings["s"].IsValid(1) || string(v.Values.Values.Strings["s"].ValueBytes(2)) != "界" || string(v.Values.Values.Strings["s"].ValueBytes(3)) != "" {
				t.Fatal("dictionary value/NULL oracle")
			}
			st.Close()
			if m.Used() != 0 {
				t.Fatal("dictionary stream charge")
			}
		})
	}
}

func TestStructListArrowIPCReentryAndSnapshot(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	rec := structListArrowRecord(t, alloc, 7)
	src, e := NewSourceWithOptions(Options{NativeStructList: true}, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	expectedSchema := src.Schema()
	mutated := src.Schema()
	f := &mutated.Fields[0]
	for i := 0; i < 7; i++ {
		f.Name = "poison"
		f = &f.Children[0]
	}
	f.Children[0].Children[0].Name = "poison"
	if !reflect.DeepEqual(src.Schema().Fields, expectedSchema.Fields) {
		t.Fatal("schema snapshot mutated source")
	}
	m := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.BatchRows = 5
	opts.MemoryManager = m
	st, e := src.NewBatchStream(context.Background(), opts)
	if e != nil {
		src.Close()
		t.Fatal(e)
	}
	src.Close()
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if e != nil || !ok {
		t.Fatal(ok, e)
	}
	ownedM := memory.NewBudgetManager(0)
	owned, lease, e := memory.CloneCollectBatchStructLists(context.Background(), ownedM, b, expectedSchema)
	if e != nil {
		t.Fatal(e)
	}
	st.Close()
	record, e := memory.ArrowRecord(alloc, expectedSchema, owned)
	if e != nil {
		t.Fatal(e)
	}
	var payload bytes.Buffer
	writer := ipc.NewWriter(&payload, ipc.WithSchema(record.Schema()), ipc.WithAllocator(alloc))
	if e = writer.Write(record); e != nil {
		t.Fatal(e)
	}
	if e = writer.Close(); e != nil {
		t.Fatal(e)
	}
	record.Release()
	owned.ReleaseOwned()
	lease.Release()
	reader, e := ipc.NewReader(bytes.NewReader(payload.Bytes()), ipc.WithAllocator(alloc))
	if e != nil {
		t.Fatal(e)
	}
	if !reader.Next() {
		t.Fatal("IPC record missing", reader.Err())
	}
	again, e := NewSourceWithOptions(Options{NativeStructList: true}, reader.RecordBatch())
	reader.Release()
	if e != nil {
		t.Fatal(e)
	}
	st, e = again.NewBatchStream(context.Background(), opts)
	if e != nil {
		again.Close()
		t.Fatal(e)
	}
	again.Close()
	b = memory.NewBatch()
	ok, e = st.Next(context.Background(), b)
	if e != nil || !ok {
		t.Fatal(ok, e)
	}
	structListArrowCheckBatch(t, b, 7, []int{0, 1, 2, 3, 4}, true)
	st.Close()
	if m.Used() != 0 || ownedM.Used() != 0 {
		t.Fatal("IPC/owner cleanup", m.Used(), ownedM.Used())
	}
}

func TestStructListArrowDepthBudgetAndCancellation(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	rec := structListArrowRecord(t, alloc, 8)
	src, e := NewSourceWithOptions(Options{NativeStructList: true}, rec)
	rec.Release()
	if e == nil {
		src.Close()
		t.Fatal("ninth total Struct accepted")
	}
	rec = structListArrowRecord(t, alloc, 7)
	src, e = NewSourceWithOptions(Options{NativeStructList: true, NativeRecursiveStruct: true, NativeDecimal128: true}, rec)
	rec.Release()
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
	if !errors.Is(e, memory.ErrBudgetExceeded) || m.Used() != 0 {
		t.Fatal("view budget refusal", e, m.Used())
	}
	opts.MemoryManager = memory.NewBudgetManager(0)
	ctx, cancel := context.WithCancel(context.Background())
	st, e = src.NewBatchStream(ctx, opts)
	if e != nil {
		t.Fatal(e)
	}
	cancel()
	ok, e := st.Next(ctx, memory.NewBatch())
	st.Close()
	if ok || !errors.Is(e, context.Canceled) || opts.MemoryManager.Used() != 0 {
		t.Fatal("cancellation cleanup", ok, e, opts.MemoryManager.Used())
	}
}

func TestStructListArrowHiddenStructureAndDictionaryCodes(t *testing.T) {
	for _, kind := range []string{"parent_offsets", "hidden_code", "visible_code", "dictionary_offsets"} {
		t.Run(kind, func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			rec := structListArrowDictionaryRecord(t, alloc, false, false)
			list := rec.Column(0).(*array.List)
			elements := list.ListValues().(*array.Struct)
			dict := elements.Field(1).(*array.Dictionary)
			switch kind {
			case "parent_offsets":
				list.Offsets()[1] = 2
				if list.Offsets()[1] <= list.Offsets()[2] {
					t.Fatal("fixture must really decrease")
				}
			case "hidden_code":
				idx := dict.Indices().(*array.Int8)
				idx.Int8Values()[0] = 99
				idx.NullBitmapBytes()[0] |= 1
			case "visible_code":
				dict.Indices().(*array.Int8).Int8Values()[2] = 99
			case "dictionary_offsets":
				table := dict.Dictionary().(*array.String)
				table.ValueOffsets()[1] = 4
				if table.ValueOffsets()[1] <= table.ValueOffsets()[2] {
					t.Fatal("dictionary fixture must decrease")
				}
			}
			src, e := NewSourceWithOptions(Options{NativeStructList: true}, rec)
			rec.Release()
			if kind == "hidden_code" {
				if e != nil {
					t.Fatal("hidden NULL element code evaluated", e)
				}
				src.Close()
			} else if e == nil {
				src.Close()
				t.Fatal("bad structure/visible value accepted", kind)
			}
		})
	}
}
func TestStructListArrowRejectElementProfileExpansion(t *testing.T) {
	for _, typ := range []arrow.DataType{arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Time64ns, &arrow.DurationType{Unit: arrow.Nanosecond}, &arrow.Decimal128Type{Precision: 9, Scale: 2}, arrow.BinaryTypes.Binary, arrow.ListOf(arrow.PrimitiveTypes.Int64), arrow.StructOf(arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64})} {
		t.Run(typ.String(), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
			defer alloc.AssertSize(t, 0)
			lb := array.NewListBuilderWithField(alloc, arrow.Field{Name: "item", Type: arrow.StructOf(arrow.Field{Name: "bad", Type: typ, Nullable: true}), Nullable: true})
			lb.Append(true)
			sb := lb.ValueBuilder().(*array.StructBuilder)
			sb.Append(true)
			sb.FieldBuilder(0).AppendEmptyValue()
			list := lb.NewArray()
			lb.Release()
			rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "items", Type: list.DataType(), Nullable: true}}, nil), []arrow.Array{list}, 1)
			list.Release()
			src, e := NewSourceWithOptions(Options{NativeStructList: true, NativeDecimal128: true}, rec)
			rec.Release()
			if e == nil {
				src.Close()
				t.Fatal("unsupported element type admitted")
			}
		})
	}
}

func TestStructListArrowTopDecimalCoexist(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	base := structListArrowRecord(t, alloc, 7)
	defer base.Release()
	dec := decimalRecord(t, alloc, 9, 2, []decimal128.Num{decimal128.FromI64(123), decimal128.FromI64(-456), decimal128.FromI64(0), decimal128.FromI64(999), decimal128.FromI64(1)}, []bool{true, true, false, true, true}, true)
	defer dec.Release()
	fields := append(append([]arrow.Field(nil), base.Schema().Fields()...), dec.Schema().Fields()[0])
	cols := append(append([]arrow.Array(nil), base.Columns()...), dec.Column(0))
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 5)
	defer rec.Release()
	src, e := NewSourceWithOptions(Options{NativeStructList: true, NativeRecursiveStruct: true, NativeDecimal128: true}, rec)
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	schema := src.Schema()
	f := schema.Fields[len(schema.Fields)-1]
	if f.DecimalPrecision != 9 || f.DecimalScale != 2 {
		t.Fatal(f)
	}
	opts := testReadOptions()
	opts.BatchRows = 5
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := src.NewBatchStream(context.Background(), opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if e != nil || !ok {
		t.Fatal(ok, e)
	}
	structListArrowCheckBatch(t, b, 7, []int{0, 1, 2, 3, 4}, false)
	d := b.Decimals["d"]
	if d == nil || d.Precision != 9 || d.Scale != 2 || d.IsValid(2) {
		t.Fatal("decimal metadata/NULL")
	}
	for i, n := range []int64{123, -456, 0, 999, 1} {
		if i != 2 && d.Values[i] != decimal128.FromI64(n) {
			t.Fatal("coefficient", i, d.Values[i])
		}
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("coexist view leak")
	}
}
