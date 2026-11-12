package arrowrecord

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"math"
	"testing"
)

func TestArrowInt64ListSlicedNullableStream(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	builder := array.NewListBuilderWithField(alloc, arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	values := builder.ValueBuilder().(*array.Int64Builder)
	builder.Append(true)
	values.Append(99) // discarded parent prefix
	builder.Append(true)
	values.Append(math.MinInt64)
	values.AppendNull()
	values.Append(math.MaxInt64)
	builder.AppendNull()
	builder.Append(true) // empty, distinct from NULL
	builder.Append(true)
	values.Append(0)
	full := builder.NewArray()
	builder.Release()
	sliced := array.NewSlice(full, 1, 5)
	full.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "nums", Type: sliced.DataType(), Nullable: true}}, nil), []arrow.Array{sliced}, 4)
	sliced.Release()
	src, err := NewSource(rec)
	rec.Release()
	if err != nil {
		t.Fatal(err)
	}
	if f := src.Schema().Fields[0]; f.Type != memory.TypeList || f.Children[0].Type != memory.TypeInt64 {
		t.Fatalf("field=%+v", f)
	}
	opts := testReadOptions()
	opts.BatchRows = 1
	stream, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	src.Close() // stream retains the owner independently
	defer stream.Close()
	b := memory.NewBatch()
	for row := 0; row < 4; row++ {
		ok, err := stream.Next(context.Background(), b)
		if !ok || err != nil {
			t.Fatalf("row %d ok=%v err=%v", row, ok, err)
		}
		v, err := b.Int64List("nums")
		if err != nil {
			t.Fatal(err)
		}
		if err := v.Validate(1); err != nil {
			t.Fatal(err)
		}
		if len(b.Opaque) != 0 {
			t.Fatal("native list left opaque")
		}
		switch row {
		case 0:
			if len(v.Values.Values) != 3 || v.Values.Values[0] != math.MinInt64 || v.Values.IsValid(1) || v.Values.Values[2] != math.MaxInt64 {
				t.Fatalf("values=%+v", v.Values)
			}
		case 1:
			if v.IsValid(0) {
				t.Fatal("NULL parent lost")
			}
		case 2:
			if !v.IsValid(0) || len(v.Values.Values) != 0 {
				t.Fatal("empty list lost")
			}
		case 3:
			if len(v.Values.Values) != 1 || v.Values.Values[0] != 0 {
				t.Fatal("zero lost")
			}
		}
	}
	if ok, err := stream.Next(context.Background(), b); ok || err != nil {
		t.Fatalf("EOF=%v,%v", ok, err)
	}
}

func TestArrowInt64ListRejectsNonNullableChild(t *testing.T) {
	builder := array.NewListBuilderWithField(arrowmem.NewGoAllocator(), arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: false})
	builder.Append(true)
	builder.ValueBuilder().(*array.Int64Builder).AppendNull()
	a := builder.NewArray()
	builder.Release()
	defer a.Release()
	r := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "nums", Type: a.DataType()}}, nil), []arrow.Array{a}, 1)
	defer r.Release()
	if src, err := NewSource(r); err == nil {
		src.Close()
		t.Fatal("NULL child accepted")
	}
}

func TestArrowInt64ListChildOffsetAndMalformedOffsets(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	b := array.NewInt64Builder(alloc)
	b.AppendValues([]int64{99, 42, 0, -7}, []bool{true, true, false, true})
	all := b.NewArray()
	b.Release()
	child := array.NewSlice(all, 1, 4)
	all.Release()
	defer child.Release()
	offsets := arrowmem.NewResizableBuffer(alloc)
	defer offsets.Release()
	offsets.Resize(12)
	copy(arrow.Int32Traits.CastFromBytes(offsets.Bytes()), []int32{0, 2, 3})
	dt := arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	data := array.NewData(dt, 2, []*arrowmem.Buffer{nil, offsets}, []arrow.ArrayData{child.Data()}, 0, 0)
	a := array.NewListData(data)
	data.Release()
	defer a.Release()
	v, err := arrowInt64ListVector(a, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Values.Values[0] != 42 || v.Values.IsValid(1) || v.Values.Values[2] != -7 {
		t.Fatalf("child offset lost: %+v", v.Values)
	}
	// Invalid terminal offsets must fail, never slice beyond child storage.
	arrow.Int32Traits.CastFromBytes(offsets.Bytes())[2] = 4
	if _, err := arrowInt64ListVector(a, nil); err == nil {
		t.Fatal("invalid terminal accepted")
	}
}

func TestArrowUnsupportedListShapesRemainOpaque(t *testing.T) {
	for _, dt := range []arrow.DataType{arrow.LargeListOf(arrow.PrimitiveTypes.Int64), arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int64), arrow.ListOf(arrow.PrimitiveTypes.Int32), arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)), arrow.ListOf(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.PrimitiveTypes.Int64})} {
		if f := memoryField(arrow.Field{Name: "x", Type: dt}); f.Type != memory.TypeArrow {
			t.Fatalf("unsupported %s became native: %+v", dt, f)
		}
	}
}
