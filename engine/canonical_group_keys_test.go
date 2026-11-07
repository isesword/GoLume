package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"gocolumn/memory"
	"math"
	"testing"
)

func canonicalTestKey(t *testing.T, b canonicalGroupBinding, row int) []byte {
	t.Helper()
	n, e := b.KeySize(row)
	if e != nil {
		t.Fatal(e)
	}
	key, e := b.AppendKey(make([]byte, 0, n), row)
	if e != nil {
		t.Fatal(e)
	}
	hash, e := b.HashRow(row)
	if e != nil || hash != canonicalGroupKeyHash(key) {
		t.Fatalf("hash mismatch: %x %v", hash, e)
	}
	return key
}
func TestCanonicalGroupNativeTypes(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 1
	b.Int8s["x"] = &memory.Int8Vector{Values: []int8{-128}}
	b.Int16s["x"] = &memory.Int16Vector{Values: []int16{-32768}}
	b.Int32s["x"] = &memory.Int32Vector{Values: []int32{math.MinInt32}}
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{math.MinInt64}}
	b.Uint8s["x"] = &memory.Uint8Vector{Values: []uint8{255}}
	b.Uint16s["x"] = &memory.Uint16Vector{Values: []uint16{65535}}
	b.Uint32s["x"] = &memory.Uint32Vector{Values: []uint32{math.MaxUint32}}
	b.Uint32s["bool"] = &memory.Uint32Vector{Values: []uint32{1}}
	b.Uint64s["x"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64}}
	b.Float32s["x"] = &memory.Float32Vector{Values: []float32{float32(math.Inf(1))}}
	b.Float64s["x"] = &memory.Float64Vector{Values: []float64{math.Inf(-1)}}
	cases := []struct {
		typ  memory.DataType
		bits uint64
	}{
		{memory.TypeInt8, 128}, {memory.TypeInt16, 32768}, {memory.TypeInt32, 1 << 31}, {memory.TypeInt64, 1 << 63},
		{memory.TypeUint8, 255}, {memory.TypeUint16, 65535}, {memory.TypeUint32, math.MaxUint32}, {memory.TypeUint64, math.MaxUint64},
		{memory.TypeBool, 1}, {memory.TypeFloat32, 0x7f800000}, {memory.TypeFloat64, 0xfff0000000000000},
		{memory.TypeDate32, 1 << 31}, {memory.TypeTime32, 1 << 31}, {memory.TypeDate64, 1 << 63}, {memory.TypeTimestamp, 1 << 63}, {memory.TypeTime64, 1 << 63}, {memory.TypeDuration, 1 << 63},
	}
	fields := make([]memory.Field, len(cases))
	for i, c := range cases {
		fields[i] = memory.Field{Name: "x", Type: c.typ}
		if c.typ == memory.TypeBool {
			fields[i].Name = "bool"
		}
	}
	binding, e := bindCanonicalGroupKeys(fields, b)
	if e != nil {
		t.Fatal(e)
	}
	key := canonicalTestKey(t, binding, 0)
	seen := 0
	e = visitCanonicalGroupKey(key, fields, func(i int, typ memory.DataType, valid bool, bits uint64, data []byte) error {
		seen++
		if typ != cases[i].typ || !valid || bits != cases[i].bits || data != nil {
			t.Fatalf("component %d: %v %v %x", i, typ, valid, bits)
		}
		return nil
	})
	if e != nil || seen != len(cases) {
		t.Fatal(e, seen)
	}
}
func TestCanonicalGroupFloatEquivalence(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 6
	b.Float64s["f"] = &memory.Float64Vector{Values: []float64{0, math.Copysign(0, -1), math.Float64frombits(0x7ff0000000000001), math.Float64frombits(0xfff8123456789012), 0, 0}, Validity: memory.Bitmap{31}}
	b.Float32s["g"] = &memory.Float32Vector{Values: []float32{0, math.Float32frombits(0x80000000), math.Float32frombits(0x7f800001), math.Float32frombits(0xffc12345), 0, 0}, Validity: memory.Bitmap{31}}
	fields := []memory.Field{{Name: "f", Type: memory.TypeFloat64}, {Name: "g", Type: memory.TypeFloat32}}
	binding, e := bindCanonicalGroupKeys(fields, b)
	if e != nil {
		t.Fatal(e)
	}
	keys := make([][]byte, 6)
	for i := range keys {
		keys[i] = canonicalTestKey(t, binding, i)
	}
	if !bytes.Equal(keys[0], keys[1]) || !bytes.Equal(keys[2], keys[3]) || bytes.Equal(keys[4], keys[5]) {
		t.Fatal("float/null equivalence")
	}
}
func TestCanonicalGroupDictionaryAndComposite(t *testing.T) {
	fields := []memory.Field{{Name: "s", Type: memory.TypeString}, {Name: "tail", Type: memory.TypeString}, {Name: "s", Type: memory.TypeString}}
	a := memory.NewBatch()
	a.Rows = 3
	a.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 2}, Offsets: []uint32{0, 2, 3, 3}, Data: []byte("a\x00b")}
	a.Strings["tail"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 3, 3}, Data: []byte("cab")}
	b := memory.NewBatch()
	b.Rows = 3
	b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{2, 1, 0}, Offsets: []uint32{0, 0, 1, 3}, Data: []byte("ba\x00"), DictionaryDomain: "unrelated"}
	b.Strings["tail"] = a.Strings["tail"]
	ab, e := bindCanonicalGroupKeys(fields, a)
	if e != nil {
		t.Fatal(e)
	}
	bb, e := bindCanonicalGroupKeys(fields, b)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		if !bytes.Equal(canonicalTestKey(t, ab, i), canonicalTestKey(t, bb, i)) {
			t.Fatal("codebook-dependent key")
		}
	}
	b.DictStrings = nil
	b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 2, 3, 3}, Data: []byte("a\x00b")}
	bb, e = bindCanonicalGroupKeys(fields, b)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		if !bytes.Equal(canonicalTestKey(t, ab, i), canonicalTestKey(t, bb, i)) {
			t.Fatal("encoding-dependent key")
		}
	}
	a.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 2, 3, 3}, Data: []byte("aba")}
	a.Strings["tail"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 3, 3}, Data: []byte("cbc")}
	ab, e = bindCanonicalGroupKeys(fields[:2], a)
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Equal(canonicalTestKey(t, ab, 0), canonicalTestKey(t, ab, 1)) {
		t.Fatal("composite framing collision")
	}
}
func TestCanonicalGroupValidation(t *testing.T) {
	f := []memory.Field{{Name: "x", Type: memory.TypeUint64}}
	b := memory.NewBatch()
	b.Rows = 1
	b.Uint64s["x"] = &memory.Uint64Vector{Values: []uint64{42}}
	binding, e := bindCanonicalGroupKeys(f, b)
	if e != nil {
		t.Fatal(e)
	}
	key := canonicalTestKey(t, binding, 0)
	prefix := make([]byte, 3, 3+len(key))
	copy(prefix, "pre")
	got, e := binding.AppendKey(prefix, 0)
	if e != nil || string(got[:3]) != "pre" || !bytes.Equal(got[3:], key) {
		t.Fatal("prefix", e)
	}
	short := bytes.Repeat([]byte{99}, len(key)-1)
	before := bytes.Clone(short)
	if _, e = binding.AppendKey(short[:0], 0); e == nil || !bytes.Equal(before, short) {
		t.Fatal("capacity must reject without writing")
	}
	for _, row := range []int{-1, 1} {
		if _, e = binding.KeySize(row); e == nil {
			t.Fatal("row")
		}
		if _, e = binding.HashRow(row); e == nil {
			t.Fatal("hash row")
		}
	}
	visit := func(int, memory.DataType, bool, uint64, []byte) error { return nil }
	for n := 0; n < len(key); n++ {
		if e = visitCanonicalGroupKey(key[:n], f, visit); e == nil {
			t.Fatalf("truncation %d", n)
		}
	}
	invalid := [][]byte{append(bytes.Clone(key), 0), bytes.Clone(key), bytes.Clone(key), bytes.Clone(key), bytes.Clone(key)}
	invalid[1][0] = byte(memory.TypeInt64)
	invalid[2][1] = 2
	invalid[3][1] = 0
	binary.LittleEndian.PutUint32(invalid[4][2:6], 7)
	for i, k := range invalid {
		if e = visitCanonicalGroupKey(k, f, visit); e == nil {
			t.Fatal("malformed", i)
		}
	}
	sentinel := errors.New("visitor")
	if e = visitCanonicalGroupKey(key, f, func(int, memory.DataType, bool, uint64, []byte) error { return sentinel }); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
	if _, e = bindCanonicalGroupKeys(f, nil); e == nil {
		t.Fatal("nil")
	}
	b.Uint64s["x"].Validity = memory.Bitmap{}
	if _, e = bindCanonicalGroupKeys(f, b); e == nil {
		t.Fatal("short bitmap")
	}
	b.Uint64s["x"].Validity = nil
	b.Rows = 2
	if _, e = bindCanonicalGroupKeys(f, b); e == nil {
		t.Fatal("short vector")
	}
}
func TestCanonicalGroupMalformedSource(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 1
	f := []memory.Field{{Name: "s", Type: memory.TypeString}}
	for _, off := range [][]uint32{nil, {0}, {1, 0}, {0, 2}} {
		b.Strings["s"] = &memory.StringViewVector{Offsets: off, Data: []byte("a")}
		if _, e := bindCanonicalGroupKeys(f, b); e == nil {
			t.Fatalf("offsets %v", off)
		}
	}
	b.Strings = nil
	b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{math.MaxUint32}, Offsets: []uint32{0, 1}, Data: []byte("a")}
	if _, e := bindCanonicalGroupKeys(f, b); e == nil {
		t.Fatal("invalid code")
	}
	b.DictStrings["s"].Validity = memory.Bitmap{0}
	if _, e := bindCanonicalGroupKeys(f, b); e != nil {
		t.Fatal("null code should not be accessed", e)
	}
	b.Uint32s["s"] = &memory.Uint32Vector{Values: []uint32{2}}
	f[0].Type = memory.TypeBool
	if _, e := bindCanonicalGroupKeys(f, b); e == nil {
		t.Fatal("invalid bool")
	}
	f[0].Type = memory.TypeStruct
	if _, e := bindCanonicalGroupKeys(f, b); e == nil {
		t.Fatal("unsupported type")
	}
}
func TestCanonicalGroupZeroAllocations(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 1
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64}}
	b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 3}, Data: []byte("a\x00b")}
	f := []memory.Field{{Name: "u", Type: memory.TypeUint64}, {Name: "s", Type: memory.TypeString}}
	binding, e := bindCanonicalGroupKeys(f, b)
	if e != nil {
		t.Fatal(e)
	}
	key := canonicalTestKey(t, binding, 0)
	dst := make([]byte, 0, len(key))
	visit := func(int, memory.DataType, bool, uint64, []byte) error { return nil }
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, e := binding.KeySize(0); e != nil {
			panic(e)
		}
		if _, e := binding.AppendKey(dst, 0); e != nil {
			panic(e)
		}
		if _, e := binding.HashRow(0); e != nil {
			panic(e)
		}
		if e := visitCanonicalGroupKey(key, f, visit); e != nil {
			panic(e)
		}
	}); allocs != 0 {
		t.Fatalf("allocations %g", allocs)
	}
}

func TestCanonicalGroupDecoderRejectsNoncanonical(t *testing.T) {
	for _, c := range []struct {
		typ   memory.DataType
		bits  uint64
		width int
	}{
		{memory.TypeFloat32, 0x80000000, 4}, {memory.TypeFloat32, 0x7f800001, 4},
		{memory.TypeFloat64, 0x8000000000000000, 8}, {memory.TypeFloat64, 0xfff8000000000000, 8}, {memory.TypeBool, 2, 1},
	} {
		key := make([]byte, 6+c.width)
		key[0] = byte(c.typ)
		key[1] = 1
		binary.LittleEndian.PutUint32(key[2:6], uint32(c.width))
		for i := 0; i < c.width; i++ {
			key[6+i] = byte(c.bits >> (8 * i))
		}
		if e := visitCanonicalGroupKey(key, []memory.Field{{Type: c.typ}}, func(int, memory.DataType, bool, uint64, []byte) error { return nil }); e == nil {
			t.Fatalf("accepted noncanonical %s %x", c.typ, c.bits)
		}
	}
}
func TestCanonicalGroupNullEmptyBinary(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 0, 0, 2}, Data: []byte{0, 255}, Validity: memory.Bitmap{6}}
	f := []memory.Field{{Name: "s", Type: memory.TypeBinary}}
	binding, e := bindCanonicalGroupKeys(f, b)
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Equal(canonicalTestKey(t, binding, 0), canonicalTestKey(t, binding, 1)) {
		t.Fatal("null equals empty")
	}
	key := canonicalTestKey(t, binding, 2)
	e = visitCanonicalGroupKey(key, f, func(_ int, _ memory.DataType, valid bool, _ uint64, data []byte) error {
		if !valid || !bytes.Equal(data, []byte{0, 255}) {
			t.Fatal("binary payload")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	f[0].Type = memory.TypeString
	other, e := bindCanonicalGroupKeys(f, b)
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Equal(key, canonicalTestKey(t, other, 2)) {
		t.Fatal("missing type framing")
	}
}
func FuzzCanonicalGroupDecoder(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{byte(memory.TypeUint64), 1, 8, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	fields := []memory.Field{{Type: memory.TypeUint64}, {Type: memory.TypeString}, {Type: memory.TypeFloat64}}
	f.Fuzz(func(t *testing.T, key []byte) {
		_ = visitCanonicalGroupKey(key, fields, func(int, memory.DataType, bool, uint64, []byte) error { return nil })
	})
}
