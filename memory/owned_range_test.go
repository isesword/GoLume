package memory

import (
	"math"
	"strings"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestOwnedRangeStructListGrowthAdmission(t *testing.T) {
	for _, rows := range []int{1, 255, 256, 257, 511, 512, 513, 4097} {
		child := NewBatch()
		child.Rows = rows
		offsets := make([]uint32, rows+1)
		for i := range offsets {
			offsets[i] = uint32(i)
		}
		child.StringLists["l"] = &StringListVector{Offsets: offsets, Values: &StringViewVector{Offsets: offsets, Data: make([]byte, rows)}}
		b := NewBatch()
		b.Rows = rows
		b.Structs["s"] = &StructVector{Rows: rows, Fields: []Field{ListStringField("l", true, true)}, Values: child}
		logical, err := EstimateBatchRangeBytes(b, 0, rows)
		if err != nil {
			t.Fatal(err)
		}
		admission, err := estimateNativeOwnedTestBytes(b, 0, rows)
		if err != nil {
			t.Fatal(err)
		}
		c, err := CloneBatchRange(b, 0, rows)
		if err != nil {
			t.Fatal(err)
		}
		defer c.ReleaseOwned()
		s := c.Structs["s"]
		l := s.Values.StringLists["l"]
		retained := int64(cap(l.Offsets)*4+cap(l.Values.Offsets)*4+cap(l.Values.Data)) + int64(cap(s.Fields))*int64(unsafe.Sizeof(Field{}))
		if admission <= retained || admission <= logical {
			t.Fatalf("rows=%d logical=%d cap=%d admission=%d", rows, logical, retained, admission)
		}
		if rows == 257 && retained <= logical {
			t.Fatal("fixture no longer exposes length/capacity gap")
		}
		t.Logf("rows=%d logical=%d retained payload/Field cap=%d admission=%d", rows, logical, retained, admission)
	}
}

func TestOwnedRangeIncludesEveryRuntimeMap(t *testing.T) {
	b := NewBatch()
	b.Rows = 1
	b.Int8s["a"] = &Int8Vector{Values: []int8{1}}
	b.Int16s["b"] = &Int16Vector{Values: []int16{1}}
	b.Int32s["c"] = &Int32Vector{Values: []int32{1}}
	b.Int64s["d"] = &Int64Vector{Values: []int64{1}}
	b.Uint8s["e"] = &Uint8Vector{Values: []uint8{1}}
	b.Uint16s["f"] = &Uint16Vector{Values: []uint16{1}}
	b.Uint32s["g"] = &Uint32Vector{Values: []uint32{1}}
	b.Uint64s["h"] = &Uint64Vector{Values: []uint64{1}}
	b.Float32s["i"] = &Float32Vector{Values: []float32{1}}
	b.Float64s["j"] = &Float64Vector{Values: []float64{1}}
	b.Decimals["k"] = &Decimal128Vector{Values: make([]Decimal128, 1), Precision: 10}
	b.Strings["l"] = &StringViewVector{Offsets: []uint32{0, 0}}
	b.DictStrings["m"] = &DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 0}, DictionaryDomain: strings.Repeat("d", 10000)}
	b.StringLists["n"] = &StringListVector{Offsets: []uint32{0, 0}, Values: &StringViewVector{}}
	b.Structs["o"] = &StructVector{Rows: 1, Values: &Batch{Rows: 1}}
	builder := array.NewInt64Builder(arrowmem.DefaultAllocator)
	builder.Append(7)
	a := builder.NewArray()
	builder.Release()
	defer a.Release()
	b.Opaque["p"] = &OpaqueVector{Rows: 1, Value: a}
	got, err := estimateNativeOwnedTestBytes(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got < 4096+15*1024+10000 {
		t.Fatalf("missing runtime metadata: %d", got)
	}
	before := got
	delete(b.Decimals, "k")
	after, err := estimateNativeOwnedTestBytes(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	decimalBulk, err := nativeBulkAdmission(16)
	if err != nil {
		t.Fatal(err)
	}
	if before-after != 1024+1+decimalBulk {
		t.Fatalf("decimal admission delta=%d", before-after)
	}
}

func TestOwnedRangeTopLevelFieldsAndSharedChildrenBoundary(t *testing.T) {
	child := NewBatch()
	child.Rows = 1
	child.Strings["v"] = &StringViewVector{Offsets: []uint32{0, 0}}
	fields := []Field{{Name: "v", Type: TypeString}}
	b := NewBatch()
	b.Rows = 1
	b.Structs["s"] = &StructVector{Rows: 1, Fields: fields, Values: child}
	before, err := estimateNativeOwnedTestBytes(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	backing := make([]Field, 2)
	backing[1].ArrowType = strings.Repeat("a", 10000)
	fields[0].Children = backing[:0]
	fields[0].DictionaryDomain = strings.Repeat("d", 5000)
	after, err := estimateNativeOwnedTestBytes(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if after-before != 5000 {
		t.Fatalf("direct domain must be charged, shared Children remain caller-owned: delta=%d", after-before)
	}
}

func TestOwnedRangeRejectsMalformedBeforeClone(t *testing.T) {
	t.Run("schema cycle", func(t *testing.T) {
		f := make([]Field, 1)
		f[0].Children = f
		if n, err := ownedRangeFields(f); err != nil || n <= 0 {
			t.Fatalf("shallow schema cycle failed: bytes=%d err=%v", n, err)
		}
	})
	t.Run("duplicate", func(t *testing.T) {
		c := NewBatch()
		c.Rows = 1
		c.Int8s["v"] = &Int8Vector{Values: []int8{1}}
		b := NewBatch()
		b.Rows = 1
		f := Field{Name: "v", Type: TypeInt8}
		b.Structs["s"] = &StructVector{Rows: 1, Fields: []Field{f, f}, Values: c}
		estimate, err := EstimateNativeOwnedBatchRangeBytes(b, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		clone, err := CloneBatchRange(b, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer clone.ReleaseOwned()
		if len(clone.Structs["s"].Values.Int8s["v"].Values) != 2 || estimate.NativeBytes <= 0 {
			t.Fatal("duplicate append behavior changed")
		}
	})
	t.Run("short validity", func(t *testing.T) {
		b := NewBatch()
		b.Rows = 65
		b.Int8s["v"] = &Int8Vector{Values: make([]int8, 65), Validity: Bitmap{1}}
		if _, err := estimateNativeOwnedTestBytes(b, 0, 65); err == nil {
			t.Fatal("short validity admitted")
		}
	})
	t.Run("nonmonotonic", func(t *testing.T) {
		b := NewBatch()
		b.Rows = 2
		b.Strings["v"] = &StringViewVector{Offsets: []uint32{0, 2, 1}, Data: []byte("ab")}
		if _, err := estimateNativeOwnedTestBytes(b, 0, 2); err != nil {
			t.Fatal(err)
		}
		if got, err := CloneBatchRange(b, 0, 2); err == nil {
			got.ReleaseOwned()
			t.Fatal("malformed offsets cloned")
		}
	})
	t.Run("overflow", func(t *testing.T) {
		for _, tc := range [][3]int64{{0, math.MaxInt64, 4}, {math.MaxInt64, 1, 1}, {0, -1, 1}} {
			if _, err := ownedRangeAdd(tc[0], tc[1], tc[2]); err == nil {
				t.Fatal("overflow admitted")
			}
		}
	})
}

func estimateNativeOwnedTestBytes(b *Batch, offset, length int) (int64, error) {
	estimate, err := EstimateNativeOwnedBatchRangeBytes(b, offset, length)
	return estimate.Bytes, err
}
