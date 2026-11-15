package memory

import (
	"github.com/apache/arrow-go/v18/arrow"
	"math"
	"reflect"
	"testing"
	"unsafe"
)

type collectRetained struct{ refs int }

func (v *collectRetained) Retain()  { v.refs++ }
func (v *collectRetained) Release() { v.refs-- }

func TestCollectOwnedAdmissionMixedOpaquePreservesLegacyScope(t *testing.T) {
	b := NewBatch()
	b.Rows = 1
	b.Int64s["x"] = &Int64Vector{Values: []int64{7, 999}}
	native, err := EstimateCollectBatchBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := EstimateNativeOwnedBatchRangeBytes(b, 0, 1)
	// Collect-only layout constants are narrower; fan-out remains unchanged.
	if err != nil || native != expected.Bytes-(4096-collectContainerAdmission)-(1024-collectVectorAdmission) {
		t.Fatalf("native=%d expected=%+v err=%v", native, expected, err)
	}
	v := &collectRetained{refs: 1}
	b.Opaque["o"] = &OpaqueVector{Rows: 1, Value: v, Validity: Bitmap{1}}
	mixed, err := EstimateCollectBatchBytes(b)
	if err != nil || mixed != native+8 {
		t.Fatalf("mixed=%d native=%d err=%v", mixed, native, err)
	}
	c, err := CloneCollectBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Int64s["x"].Values) != 1 {
		t.Fatal("copied source tail")
	}
	b.Int64s["x"].Values[0] = 42
	if c.Int64s["x"].Values[0] != 7 || v.refs != 2 {
		t.Fatal("invalid owned clone")
	}
	c.ReleaseOwned()
	c.ReleaseOwned()
	if v.refs != 1 {
		t.Fatal("opaque lifetime")
	}
	delete(b.Int64s, "x")
	only, err := EstimateCollectBatchBytes(b)
	if err != nil || only != 8 {
		t.Fatalf("opaque=%d err=%v", only, err)
	}
}

func TestCollectOwnedAdmissionRejectsMalformedNative(t *testing.T) {
	if _, err := EstimateCollectBatchBytes(nil); err == nil {
		t.Fatal("nil accepted")
	}
	b := NewBatch()
	b.Rows = 2
	b.Int64s["x"] = &Int64Vector{Values: []int64{1}}
	if _, err := EstimateCollectBatchBytes(b); err == nil {
		t.Fatal("short vector accepted")
	}
	b.Int64s["x"].Values = []int64{1, 2}
	b.Rows = -1
	if _, err := EstimateCollectBatchBytes(b); err == nil {
		t.Fatal("negative rows accepted")
	}
}

type collectNoInspectArrow struct {
	arrow.Array
	refs int
}

func (v *collectNoInspectArrow) Retain()               { v.refs++ }
func (v *collectNoInspectArrow) Release()              { v.refs-- }
func (v *collectNoInspectArrow) Data() arrow.ArrayData { panic("Collect must not inspect Opaque Data") }
func TestCollectOwnedDoesNotInspectArrowOpaque(t *testing.T) {
	v := &collectNoInspectArrow{refs: 1}
	b := NewBatch()
	b.Rows = 2
	b.Opaque["o"] = &OpaqueVector{Rows: 2, Value: v}
	b.Int64s["x"] = &Int64Vector{Values: []int64{1, 2}}
	if _, err := EstimateCollectBatchBytes(b); err != nil {
		t.Fatal(err)
	}
	c, err := CloneCollectBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if c.Opaque["o"].Value != v || v.refs != 2 {
		t.Fatal("identity/refcount changed")
	}
	c.ReleaseOwned()
	if v.refs != 1 {
		t.Fatal("leak")
	}
}
func TestCollectOwnedAlignedValidityTail(t *testing.T) {
	for _, off := range []int{0, 64, 1} {
		for _, n := range []int{1, 63, 64, 65, 130} {
			src := Bitmap{^uint64(0), 0x5555555555555555, ^uint64(0), ^uint64(0)}
			c := cloneValidityRange(src, off, n)
			for i := 0; i < n; i++ {
				if IsSet(c, i) != IsSet(src, off+i) {
					t.Fatal("bit mismatch")
				}
			}
			if c[len(c)-1]&^TailMask(n) != 0 {
				t.Fatal("tail not cleared")
			}
			c[0] = 0
			if src[0] != ^uint64(0) {
				t.Fatal("alias")
			}
		}
	}
}

func TestCollectOwnedEmptyStructKeepsDeclaredChildSemantics(t *testing.T) {
	child := NewBatch()
	child.Int64s["x"] = &Int64Vector{}
	child.Int64s["hidden"] = &Int64Vector{}
	b := NewBatch()
	b.Structs["s"] = &StructVector{Fields: []Field{{Name: "x", Type: TypeInt64}}, Values: child}
	old, err := CloneBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	defer old.ReleaseOwned()
	c, err := CloneCollectBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	defer c.ReleaseOwned()
	if len(c.Structs["s"].Values.Int64s) != len(old.Structs["s"].Values.Int64s) || c.Structs["s"].Values.Int64s["hidden"] != nil {
		t.Fatal("hidden empty child cloned")
	}
	if _, err := EstimateCollectBatchBytes(b); err != nil {
		t.Fatal(err)
	}
}

func TestCollectOwnedEmptyRepeatedDictionary(t *testing.T) {
	child := NewBatch()
	child.DictStrings["s"] = &DictionaryStringVector{Offsets: []uint32{0, 1 << 20}, Data: make([]byte, 1<<20)}
	child.DictStrings["s"].Data[0] = 'a'
	b := NewBatch()
	b.Structs["r"] = &StructVector{Values: child, Fields: []Field{{Name: "s", Type: TypeString}, {Name: "s", Type: TypeString}, {Name: "s", Type: TypeString}}}
	b.Opaque["nil"] = nil
	if _, err := EstimateCollectBatchBytes(b); err != nil {
		t.Fatal(err)
	}
	c, err := CloneCollectBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	defer c.ReleaseOwned()
	if len(c.Structs["r"].Fields) != 3 || len(c.Structs["r"].Values.DictStrings) != 1 {
		t.Fatal("field contract")
	}
	child.DictStrings["s"].Data[0] = 'z'
	if c.Structs["r"].Values.DictStrings["s"].Data[0] != 'a' {
		t.Fatal("dictionary shared")
	}
}

func BenchmarkCollectV4Clone(b *testing.B) {
	src := NewBatch()
	src.Rows = 4
	src.Int64s["x"] = &Int64Vector{Values: []int64{1, 2, 3, 4}}
	for _, mode := range []string{"legacy", "v4"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var dst *Batch
				var err error
				if mode == "legacy" {
					dst, err = CloneBatch(src)
				} else {
					if _, err = EstimateCollectBatchBytes(src); err != nil {
						b.Fatal(err)
					}
					dst, err = CloneCollectBatch(src)
				}
				if err != nil {
					b.Fatal(err)
				}
				dst.ReleaseOwned()
			}
		})
	}
}

func TestCollectOwnedMetadataLayout(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("Collect native allowance targets supported 64-bit platforms")
	}
	if unsafe.Sizeof(Batch{}) > 160 {
		t.Fatal("Batch layout exceeds Collect container allowance")
	}
	for _, size := range []uintptr{unsafe.Sizeof(Int8Vector{}), unsafe.Sizeof(Int16Vector{}), unsafe.Sizeof(Int32Vector{}), unsafe.Sizeof(Int64Vector{}), unsafe.Sizeof(Uint8Vector{}), unsafe.Sizeof(Uint16Vector{}), unsafe.Sizeof(Uint32Vector{}), unsafe.Sizeof(Uint64Vector{}), unsafe.Sizeof(Float32Vector{}), unsafe.Sizeof(Float64Vector{}), unsafe.Sizeof(Decimal128Vector{}), unsafe.Sizeof(StringViewVector{}), unsafe.Sizeof(DictionaryStringVector{}), unsafe.Sizeof(StringListVector{}), unsafe.Sizeof(Int64ListVector{}), unsafe.Sizeof(StructVector{})} {
		if size > 128 {
			t.Fatalf("descriptor %d exceeds Collect allowance", size)
		}
	}
}

func TestCollectOwnedFixedWidthMatchesGeneral(t *testing.T) {
	for _, rows := range []int{0, 1, 63, 64, 65, 8192} {
		for _, nullable := range []bool{false, true} {
			b := NewBatch()
			b.Rows = rows
			var validity Bitmap
			if nullable {
				validity = make(Bitmap, WordsFor(rows))
			}
			b.Int8s["i8"] = &Int8Vector{Values: make([]int8, rows), Validity: validity}
			b.Int16s["i16"] = &Int16Vector{Values: make([]int16, rows), Validity: validity}
			b.Int32s["i32"] = &Int32Vector{Values: make([]int32, rows), Validity: validity}
			b.Int64s["i64"] = &Int64Vector{Values: make([]int64, rows), Validity: validity}
			b.Uint8s["u8"] = &Uint8Vector{Values: make([]uint8, rows), Validity: validity}
			b.Uint16s["u16"] = &Uint16Vector{Values: make([]uint16, rows), Validity: validity}
			b.Uint32s["u32"] = &Uint32Vector{Values: make([]uint32, rows), Validity: validity}
			b.Uint64s["u64"] = &Uint64Vector{Values: make([]uint64, rows), Validity: validity}
			b.Float32s["f32"] = &Float32Vector{Values: make([]float32, rows), Validity: validity}
			b.Float64s["f64"] = &Float64Vector{Values: make([]float64, rows), Validity: validity}
			variants := []*Batch{b}
			for i := 1; i < reflect.TypeOf(*b).NumField(); i++ {
				field := reflect.ValueOf(b).Elem().Field(i)
				if field.Len() == 0 {
					continue
				}
				single := NewBatch()
				single.Rows = rows
				reflect.ValueOf(single).Elem().Field(i).Set(field)
				variants = append(variants, single)
			}
			for _, b := range variants {
				got, err := EstimateCollectBatchBytes(b)
				if err != nil {
					t.Fatal(err)
				}
				state := nativeOwnedEstimateState{collect: true}
				metadata, err := ownedRangeMetadata(b, 0, rows, &state, false, false, true, true)
				if err != nil {
					t.Fatal(err)
				}
				want, err := nativeOwnedRangeFromMetadata(b, 0, rows, state, metadata)
				if err != nil {
					t.Fatal(err)
				}
				if got != want.Bytes {
					t.Fatalf("rows=%d nullable=%v got=%d want=%d", rows, nullable, got, want.Bytes)
				}
			}
		}
	}
}

func TestCollectOwnedFixedWidthEdgeCases(t *testing.T) {
	cases := []*Batch{{Rows: 0}, {Rows: -1}, {Rows: math.MaxInt64}, {Rows: 1, Int64s: map[string]*Int64Vector{"x": nil}}, {Rows: 2, Int64s: map[string]*Int64Vector{"x": {Values: []int64{1}}}}, {Rows: 65, Int64s: map[string]*Int64Vector{"x": {Values: make([]int64, 65), Validity: Bitmap{1}}}}}
	for i, b := range cases {
		got, gotErr := EstimateCollectBatchBytes(b)
		state := nativeOwnedEstimateState{collect: true}
		metadata, wantErr := ownedRangeMetadata(b, 0, b.Rows, &state, false, false, true, true)
		var want NativeOwnedRangeEstimate
		if wantErr == nil {
			want, wantErr = nativeOwnedRangeFromMetadata(b, 0, b.Rows, state, metadata)
		}
		if (gotErr == nil) != (wantErr == nil) || (gotErr == nil && got != want.Bytes) {
			t.Fatalf("case %d fast=%d,%v general=%d,%v", i, got, gotErr, want.Bytes, wantErr)
		}
	}
}
