package memory

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestDecimal128BigEndianDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 29))
	cases := [][]byte{nil, {0}, {0xff}, {0x80}, {0, 0x80}, {0xff, 0x7f}, make([]byte, 16), append([]byte{1}, make([]byte, 16)...)}
	for n := 1; n <= 40; n++ {
		for i := 0; i < 20; i++ {
			b := make([]byte, n)
			for j := range b {
				b[j] = byte(rng.Uint32())
			}
			cases = append(cases, b)
		}
	}
	min128 := make([]byte, 16)
	min128[0] = 0x80
	cases = append(cases, min128)
	for _, b := range cases {
		for p := int32(1); p <= 38; p++ {
			want := new(big.Int).SetBytes(b)
			if len(b) > 0 && b[0]&128 != 0 {
				want.Sub(want, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
			}
			limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(p)), nil)
			fits := len(b) > 0 && new(big.Int).Abs(new(big.Int).Set(want)).Cmp(limit) < 0
			got, err := Decimal128FromBigEndian(b, p)
			if (err == nil) != fits {
				t.Fatalf("p=%d bytes=%x wantFits=%v got=%v error=%v", p, b, fits, got, err)
			}
			if err == nil && got.BigInt().Cmp(want) != 0 {
				t.Fatalf("p=%d bytes=%x got=%s want=%s", p, b, got.BigInt(), want)
			}
		}
	}
	for _, p := range []int32{-1, 0, 39, math.MaxInt32} {
		if _, err := Decimal128FromBigEndian([]byte{0}, p); err == nil {
			t.Fatalf("accepted precision %d", p)
		}
	}
	// Redundant sign extensions do not change the coefficient or retain input.
	for _, b := range [][]byte{append(make([]byte, 100), 1), append(bytesRepeat(0xff, 100), 0xfe)} {
		if _, err := Decimal128FromBigEndian(b, 1); err != nil {
			t.Fatal(err)
		}
	}
	if Decimal128FitsPrecision(decimal128.New(math.MinInt64, 0), 38) {
		t.Fatal("signed minimum incorrectly fits precision")
	}
}
func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestDecimal128MetadataAndOwnership(t *testing.T) {
	field, err := Decimal128Field("amount", true, 38, 18)
	if err != nil {
		t.Fatal(err)
	}
	for _, ps := range [][2]int32{{0, 0}, {39, 0}, {1, -1}, {1, 2}} {
		if _, err := Decimal128Field("x", false, ps[0], ps[1]); err == nil {
			t.Fatalf("accepted %v", ps)
		}
	}
	for _, mutate := range []func(*Field){func(f *Field) { f.Encoding = EncodingDictionary }, func(f *Field) { f.ArrowType = "decimal128(9, 2)" }, func(f *Field) { f.Children = []Field{{Name: "x", Type: TypeInt64}} }} {
		f := field
		mutate(&f)
		if f.ValidateDecimal128() == nil {
			t.Fatalf("accepted invalid field %+v", f)
		}
	}
	b := NewBatch()
	b.Rows = 66
	values := make([]Decimal128, 66)
	bits := EnsureBitmap(nil, 66)
	for i := range values {
		values[i] = decimal128.FromI64(int64(i))
		if i%3 != 0 {
			Set(bits, i)
		}
	}
	b.Decimals[field.Name] = &Decimal128Vector{Values: values, Validity: bits, Precision: 38, Scale: 18}
	if err := b.Decimals[field.Name].ValidateField(field, b.Rows); err != nil {
		t.Fatal(err)
	}
	clone, err := CloneBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	ranged, err := CloneBatchRange(b, 1, 65)
	if err != nil {
		t.Fatal(err)
	}
	borrowed, err := BorrowBatchRange(b, 1, 65)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 65; i++ {
		if ranged.Decimals[field.Name].Values[i] != values[i+1] || ranged.Decimals[field.Name].IsValid(i) != b.Decimals[field.Name].IsValid(i+1) || borrowed.Decimals[field.Name].IsValid(i) != b.Decimals[field.Name].IsValid(i+1) {
			t.Fatalf("row %d", i)
		}
	}
	values[1] = decimal128.FromI64(123)
	if clone.Decimals[field.Name].Values[1] == values[1] || ranged.Decimals[field.Name].Values[0] == values[1] {
		t.Fatal("owned clone aliases source")
	}
	if borrowed.Decimals[field.Name].Values[0] != values[1] {
		t.Fatal("borrowed range did not borrow")
	}
	estimate, err := EstimateBatchRangeBytes(b, 1, 65)
	if err != nil {
		t.Fatal(err)
	}
	if estimate != 65*16+16 || BatchBytes(ranged) != estimate {
		t.Fatalf("bytes=%d estimate=%d", BatchBytes(ranged), estimate)
	}
	meta, err := BorrowedRangeMetadataBytes(b, 1, 65)
	if err != nil || meta != 16 {
		t.Fatalf("metadata=%d error=%v", meta, err)
	}
	for _, f := range []Field{func() Field { x := field; x.DecimalScale--; return x }(), func() Field { x := field; x.DecimalPrecision--; return x }()} {
		if SchemaFingerprint(NewSchema(field)) == SchemaFingerprint(NewSchema(f)) {
			t.Fatal("decimal metadata lost from fingerprint")
		}
	}
	b.ClearColumns()
	if len(b.Decimals) != 0 || b.Rows != 0 {
		t.Fatal("clear retained decimal")
	}
}

func TestDecimal128SpillAndArrowRoundTrip(t *testing.T) {
	field, _ := Decimal128Field("amount", true, 38, 7)
	schema := NewSchema(field)
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	b := NewBatch()
	b.Rows = 4
	b.Decimals[field.Name] = &Decimal128Vector{Values: []Decimal128{max, max.Negate(), {}, decimal128.FromI64(1<<53 + 1)}, Validity: Bitmap{11}, Precision: 38, Scale: 7}
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	rec, err := ArrowRecord(alloc, schema, b)
	if err != nil {
		t.Fatal(err)
	}
	arr := rec.Column(0).(*array.Decimal128)
	typ := arr.DataType().(*arrow.Decimal128Type)
	if typ.Precision != 38 || typ.Scale != 7 || arr.Value(0) != max || arr.Value(1) != max.Negate() || !arr.IsNull(2) {
		t.Fatal("Arrow lost decimal type or coefficient")
	}
	rec.Release()
	alloc.AssertSize(t, 0)
	manager := NewBudgetManager(1 << 20)
	ctx := context.Background()
	encoded, err := EncodeColumnarRun(ctx, schema, b, manager)
	if err != nil {
		t.Fatal(err)
	}
	payload := append([]byte(nil), encoded.Payload...)
	encoded.Close()
	decoded, err := DecodeColumnarRun(ctx, schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: payload}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.Batch.Decimals[field.Name], b.Decimals[field.Name]) {
		t.Fatal("spill changed values/metadata")
	}
	decoded.Close()
	for _, mutate := range []func([]byte) []byte{
		func(p []byte) []byte { return p[:len(p)-1] },
		func(p []byte) []byte { binary.LittleEndian.PutUint32(p[columnarRunHeader+12:], 6); return p },
		func(p []byte) []byte { binary.LittleEndian.PutUint32(p[columnarRunHeader+16:], 8); return p },
		func(p []byte) []byte { binary.LittleEndian.PutUint64(p[columnarRunHeader+20+8:], 1<<63); return p },
	} {
		changed := mutate(append([]byte(nil), payload...))
		out, err := DecodeColumnarRun(ctx, schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: changed}}, manager)
		if err == nil {
			out.Close()
			t.Fatal("accepted corrupt decimal spill")
		}
		if !errors.Is(err, ErrSpillCorrupt) {
			t.Fatalf("unexpected error %v", err)
		}
		if manager.Used() != 0 {
			t.Fatal("failed decode leaked reservation")
		}
	}
	f := field
	f.DecimalScale++
	if _, err := DecodeColumnarRun(ctx, NewSchema(f), &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: payload}}, manager); !errors.Is(err, ErrSpillCorrupt) {
		t.Fatalf("scale mismatch %v", err)
	}
	tiny := NewBudgetManager(1)
	if _, err := EncodeColumnarRun(ctx, schema, b, tiny); !errors.Is(err, ErrBudgetExceeded) || tiny.Used() != 0 {
		t.Fatalf("budget rejection=%v used=%d", err, tiny.Used())
	}
}

func TestDecimal128InvalidVectorRejected(t *testing.T) {
	f, _ := Decimal128Field("x", true, 2, 1)
	b := NewBatch()
	b.Rows = 1
	for _, v := range []*Decimal128Vector{nil, {Values: []Decimal128{decimal128.FromI64(100)}, Precision: 2, Scale: 1}, {Values: []Decimal128{{}}, Validity: Bitmap{}, Precision: 2, Scale: 1}, {Values: []Decimal128{{}}, Precision: 2, Scale: 0}} {
		b.Decimals["x"] = v
		if _, err := EncodeColumnarRun(context.Background(), NewSchema(f), b, NewBudgetManager(1024)); err == nil {
			t.Fatal("invalid decimal spilled")
		}
		if rec, err := ArrowRecord(arrowmem.DefaultAllocator, NewSchema(f), b); err == nil {
			rec.Release()
			t.Fatal("invalid decimal exported")
		}
	}
}

func TestDecimal128CoefficientDecodeDoesNotAllocate(t *testing.T) {
	data := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}
	if n := testing.AllocsPerRun(100, func() {
		value, err := Decimal128FromBigEndian(data, 38)
		if err != nil || value != decimal128.FromI64(-129) {
			panic("incorrect decimal")
		}
	}); n != 0 {
		t.Fatalf("coefficient decode allocations=%v", n)
	}
}
