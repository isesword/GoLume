package memory

import (
	"math"
	"reflect"
	"testing"
)

func testInt64ListVector() *Int64ListVector {
	return &Int64ListVector{Offsets: []uint32{0, 3, 4, 4, 5}, Values: &Int64Vector{Values: []int64{math.MinInt64, 123, math.MaxInt64, 777, 0}, Validity: Bitmap{29}}, Validity: Bitmap{13}, Encoding: EncodingView}
}
func TestInt64ListLifecycle(t *testing.T) {
	src := testInt64ListVector()
	if err := src.Validate(4); err != nil {
		t.Fatal(err)
	}
	dst := &Int64ListVector{}
	for _, row := range []int{0, 1, 2, 3} {
		if err := AppendInt64ListRow(dst, src, row); err != nil {
			t.Fatal(err)
		}
	}
	if err := dst.Validate(4); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dst.Values.Values, []int64{math.MinInt64, 0, math.MaxInt64, 0}) {
		t.Fatal(dst.Values.Values)
	}
	if dst.IsValid(1) || !dst.IsValid(2) || dst.Values.IsValid(1) {
		t.Fatal("NULL semantics")
	}
	b := NewBatch()
	b.Rows = 4
	b.Int64Lists["x"] = src
	for _, r := range [][2]int{{0, 4}, {1, 3}, {2, 0}, {4, 0}} {
		c, err := CloneBatchRange(b, r[0], r[1])
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Int64Lists["x"].Validate(r[1]); err != nil {
			t.Fatal(err)
		}
		n, err := EstimateBatchRangeBytes(b, r[0], r[1])
		if err != nil || n != BatchBytes(c) {
			t.Fatalf("estimate %d actual %d: %v", n, BatchBytes(c), err)
		}
		if _, err := EstimateNativeOwnedBatchRangeBytes(b, r[0], r[1]); err != nil {
			t.Fatal(err)
		}
		view, err := BorrowBatchRange(b, r[0], r[1])
		if err != nil {
			t.Fatal(err)
		}
		if err := view.Int64Lists["x"].Validate(r[1]); err != nil {
			t.Fatal(err)
		}
		view.ClearColumns()
		if src.Len() != 4 {
			t.Fatal("source changed")
		}
	}
	c, err := CloneBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	c.Int64Lists["x"].Values.Values[0] = 42
	if src.Values.Values[0] != math.MinInt64 {
		t.Fatal("clone aliases")
	}
}
func TestInt64ListMalformed(t *testing.T) {
	for _, mutate := range []func(*Int64ListVector){func(v *Int64ListVector) { v.Offsets[2] = 2 }, func(v *Int64ListVector) { v.Offsets[4] = 99 }, func(v *Int64ListVector) { v.Validity = Bitmap{} }, func(v *Int64ListVector) { v.Values.Validity = Bitmap{} }, func(v *Int64ListVector) { v.Values = nil }} {
		v := testInt64ListVector()
		mutate(v)
		if v.Validate(4) == nil {
			t.Fatal("accepted malformed")
		}
		b := NewBatch()
		b.Rows = 4
		b.Int64Lists["x"] = v
		if _, err := BorrowBatchRange(b, 0, 4); err == nil {
			t.Fatal("borrow accepted malformed")
		}
	}
}
func TestInt64ListStructNulls(t *testing.T) {
	f := ListInt64Field("x", true, true)
	b := NewBatch()
	b.Rows = 4
	b.Int64Lists["x"] = testInt64ListVector()
	v := &StructVector{Rows: 4, Fields: []Field{f}, Values: b, Validity: Bitmap{14}}
	outer := StructField("s", true, f)
	if err := v.Validate(outer, 4); err != nil {
		t.Fatal(err)
	}
	out, err := CloneStructRange(v, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if out.Values.Int64Lists["x"].Offsets[1] != 0 {
		t.Fatal("hidden parent payload copied")
	}
	if err := out.Validate(outer, 4); err != nil {
		t.Fatal(err)
	}
}

func TestInt64ListLargeAppend(t *testing.T) {
	src := testInt64ListVector()
	out := &Int64ListVector{}
	for i := 0; i < 100000; i++ {
		if err := AppendInt64ListRow(out, src, i%4); err != nil {
			t.Fatal(err)
		}
	}
	if err := out.Validate(100000); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		if err := AppendNullInt64List(out); err != nil {
			t.Fatal(err)
		}
	}
	if err := out.Validate(200000); err != nil {
		t.Fatal(err)
	}
}
func BenchmarkInt64ListAppend(b *testing.B) {
	src := testInt64ListVector()
	for i := 0; i < b.N; i++ {
		out := &Int64ListVector{}
		for j := 0; j < 10000; j++ {
			if err := AppendInt64ListRow(out, src, j%4); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestInt64ListStructSchemaMismatch(t *testing.T) {
	f := ListInt64Field("x", true, false)
	b := NewBatch()
	b.Rows = 1
	b.Int64Lists["x"] = &Int64ListVector{Offsets: []uint32{0, 1}, Values: &Int64Vector{Values: []int64{7}}}
	b.StringLists["x"] = &StringListVector{Offsets: []uint32{0, 1}, Values: &StringViewVector{Offsets: []uint32{0, 0}, Validity: Bitmap{}}}
	v := &StructVector{Rows: 1, Fields: []Field{f}, Values: b}
	if err := v.Validate(StructField("s", true, f), 1); err != nil {
		t.Fatal(err)
	}
	v.Fields = []Field{ListStringField("x", true, false)}
	if err := v.Validate(StructField("s", true, f), 1); err == nil {
		t.Fatal("accepted mismatching list child type")
	}
}
