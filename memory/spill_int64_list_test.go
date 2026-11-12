package memory

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"math"
	"reflect"
	"testing"
)

func int64ListSpillFixture() (*Batch, Schema) {
	b := NewBatch()
	b.Rows = 4
	b.Int64Lists["xs"] = &Int64ListVector{Offsets: []uint32{0, 2, 2, 3, 4}, Values: &Int64Vector{Values: []int64{math.MinInt64, 99, 111, math.MaxInt64}, Validity: Bitmap{9}}, Validity: Bitmap{11}}
	return b, NewSchema(ListInt64Field("xs", true, true))
}
func TestInt64ListSpillRoundtripAndArrow(t *testing.T) {
	b, s := int64ListSpillFixture()
	ctx := context.Background()
	manager := NewBudgetManager(0)
	encoded, err := EncodeColumnarRun(ctx, s, b, manager)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeColumnarRun(ctx, s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	v := decoded.Batch.Int64Lists["xs"]
	if !reflect.DeepEqual(v.Offsets, b.Int64Lists["xs"].Offsets) || !reflect.DeepEqual(v.Values.Values, b.Int64Lists["xs"].Values.Values) || !reflect.DeepEqual(v.Validity, Bitmap{11}) || !reflect.DeepEqual(v.Values.Validity, Bitmap{9}) {
		t.Fatalf("bad decoded vector: %+v", v)
	}
	record, err := ArrowRecord(arrowmem.DefaultAllocator, s, decoded.Batch)
	if err != nil {
		t.Fatal(err)
	}
	list := record.Column(0).(*array.List)
	values := list.ListValues().(*array.Int64)
	if !list.IsNull(2) || list.IsNull(1) || values.Value(0) != math.MinInt64 || !values.IsNull(1) || values.Value(2) != math.MaxInt64 {
		t.Fatal("Arrow lost nulls or values")
	}
	record.Release()
	if err := decoded.Close(); err != nil {
		t.Fatal(err)
	}
	encoded.Close()
	if manager.Used() != 0 {
		t.Fatal("reservation leaked", manager.Used())
	}
}
func TestInt64ListSpillTruncationAndBudget(t *testing.T) {
	b, s := int64ListSpillFixture()
	ctx := context.Background()
	encoded, err := EncodeColumnarRun(ctx, s, b, NewBudgetManager(0))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	for n := 0; n < len(encoded.Payload); n++ {
		m := NewBudgetManager(0)
		v, err := DecodeColumnarRun(ctx, s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload[:n]}}, m)
		if v != nil || !errors.Is(err, ErrSpillCorrupt) || m.Used() != 0 {
			t.Fatalf("truncation %d: %v %v %d", n, v, err, m.Used())
		}
	}
	m := NewBudgetManager(1)
	v, err := DecodeColumnarRun(ctx, s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}, m)
	if v != nil || !errors.Is(err, ErrBudgetExceeded) || m.Used() != 0 {
		t.Fatal(v, err, m.Used())
	}
}

func TestInt64ListSelectionSizeAndMixedMetadata(t *testing.T) {
	b, s := int64ListSpillFixture()
	ctx := context.Background()
	for _, rows := range [][]int{nil, {3, 0, 2, 1, 3}, {2}, {1}} {
		estimate, err := EstimateColumnarRunSelectionBytes(s, b, rows)
		if err != nil {
			t.Fatal(err)
		}
		out := NewBatch()
		out.Rows = len(rows)
		out.Int64Lists["xs"] = &Int64ListVector{}
		for _, row := range rows {
			if err := AppendInt64ListRow(out.Int64Lists["xs"], b.Int64Lists["xs"], row); err != nil {
				t.Fatal(err)
			}
		}
		if len(rows) == 0 {
			out.Int64Lists["xs"] = &Int64ListVector{Offsets: []uint32{0}, Values: &Int64Vector{}}
		}
		enc, err := EncodeColumnarRun(ctx, s, out, NewBudgetManager(0))
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(enc.Payload)) > estimate {
			t.Fatal("underestimate", rows, len(enc.Payload), estimate)
		}
		enc.Close()
	}
	b.StringLists["strings"] = &StringListVector{Offsets: []uint32{0, 0, 0, 0, 0}, Values: &StringViewVector{Offsets: []uint32{0}}}
	s = NewSchema(s.Fields[0], ListStringField("strings", true, true))
	m := NewBudgetManager(0)
	enc, err := EncodeColumnarRun(ctx, s, b, m)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := DecodeColumnarRun(ctx, s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: enc.Payload}}, m)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Batch.StringLists["strings"] == nil || dec.Batch.Int64Lists["xs"] == nil {
		t.Fatal("missing list maps")
	}
	dec.Close()
	enc.Close()
	if m.Used() != 0 {
		t.Fatal("leaked", m.Used())
	}
}

func TestInt64ListStructSelection(t *testing.T) {
	children, childSchema := int64ListSpillFixture()
	b := NewBatch()
	b.Rows = 4
	b.Structs["record"] = &StructVector{Rows: 4, Fields: childSchema.Fields, Values: children, Validity: Bitmap{13}}
	schema := NewSchema(StructField("record", true, childSchema.Fields...))
	estimate, err := EstimateColumnarRunSelectionBytes(schema, b, []int{3, 1, 0})
	if err != nil {
		t.Fatal(err)
	}
	out := NewBatch()
	out.Rows = 3
	out.Structs["record"] = &StructVector{}
	for _, row := range []int{3, 1, 0} {
		if err := AppendStructRow(out.Structs["record"], b.Structs["record"], row, b.Structs["record"].IsValid(row)); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := EncodeColumnarRun(context.Background(), schema, out, NewBudgetManager(0))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	if int64(len(encoded.Payload)) > estimate {
		t.Fatal("selection undercharged")
	}
	b.Structs["record"].Fields = []Field{ListStringField("xs", true, true)}
	if _, err := EstimateColumnarRunSelectionBytes(schema, b, []int{0}); err == nil {
		t.Fatal("accepted incompatible list child schema")
	}
}
