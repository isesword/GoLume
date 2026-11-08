package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/rand"
	"testing"
)

func extremeBits[T extremeNumber](x T) uint64 {
	switch v := any(x).(type) {
	case float32:
		return uint64(math.Float32bits(v))
	case float64:
		return math.Float64bits(v)
	default:
		return uint64(x)
	}
}
func checkMaskedMinMax[T extremeNumber](t *testing.T, values []T, call func([]T, uint64) (T, T)) {
	t.Helper()
	rng := rand.New(rand.NewSource(999))
	for offset := 0; offset < 3; offset++ {
		for n := 0; n <= 64; n++ {
			v := values[offset : offset+n : offset+n]
			masks := []uint64{0, ^uint64(0), 0xaaaaaaaaaaaaaaaa, 0x5555555555555555, memory.TailMask(n), rng.Uint64()}
			for bit := 0; bit < 64; bit++ {
				masks = append(masks, uint64(1)<<uint(bit), ^(uint64(1) << uint(bit)))
			}
			for _, mask := range masks {
				var a, z T
				seen := false
				for i, x := range v {
					if mask&(uint64(1)<<uint(i)) == 0 {
						continue
					}
					if !seen || a != a {
						a, z = x, x
						seen = true
					} else if x == x {
						a = min(a, x)
						z = max(z, x)
					}
				}
				l, h := call(v, mask)
				if extremeBits(l) != extremeBits(a) || extremeBits(h) != extremeBits(z) {
					t.Fatalf("offset=%d n=%d mask=%x got=(%x,%x) want=(%x,%x)", offset, n, mask, extremeBits(l), extremeBits(h), extremeBits(a), extremeBits(z))
				}
			}
		}
	}
	for _, mask := range []uint64{0, ^uint64(0)} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("missing bound panic")
				}
			}()
			call(values[:65], mask)
		}()
	}
	for _, n := range []int{0, 1, 7, 31, 64} {
		for _, mask := range []uint64{0, 1, 0xaaaaaaaaaaaaaaaa, ^uint64(0)} {
			if a := testing.AllocsPerRun(100, func() { call(values[1:1+n], mask) }); a != 0 {
				t.Fatalf("allocations=%v", a)
			}
		}
	}
}
func TestMaskedMinMax(t *testing.T) {
	rng := rand.New(rand.NewSource(871))
	vInt8 := make([]int8, 67)
	for i := range vInt8 {
		vInt8[i] = int8(rng.Uint64())
	}
	vInt8[1], vInt8[2] = math.MinInt8, math.MaxInt8
	vInt16 := make([]int16, 67)
	for i := range vInt16 {
		vInt16[i] = int16(rng.Uint64())
	}
	vInt16[1], vInt16[2] = math.MinInt16, math.MaxInt16
	vUint8 := make([]uint8, 67)
	for i := range vUint8 {
		vUint8[i] = uint8(rng.Uint64())
	}
	vUint8[1], vUint8[2] = 0, math.MaxUint8
	vUint16 := make([]uint16, 67)
	for i := range vUint16 {
		vUint16[i] = uint16(rng.Uint64())
	}
	vUint16[1], vUint16[2] = 0, math.MaxUint16
	vInt32 := make([]int32, 67)
	for i := range vInt32 {
		vInt32[i] = int32(rng.Uint64())
	}
	vInt64 := make([]int64, 67)
	for i := range vInt64 {
		vInt64[i] = int64(rng.Uint64())
	}
	vUint32 := make([]uint32, 67)
	for i := range vUint32 {
		vUint32[i] = uint32(rng.Uint64())
	}
	vUint64 := make([]uint64, 67)
	for i := range vUint64 {
		vUint64[i] = uint64(rng.Uint64())
	}
	vFloat32 := make([]float32, 67)
	for i := range vFloat32 {
		vFloat32[i] = math.Float32frombits(uint32(rng.Uint64()))
	}
	vFloat64 := make([]float64, 67)
	for i := range vFloat64 {
		vFloat64[i] = math.Float64frombits(uint64(rng.Uint64()))
	}
	vInt32[1], vInt32[2] = math.MinInt32, math.MaxInt32
	vInt64[1], vInt64[2] = math.MinInt64, math.MaxInt64
	vUint32[1], vUint32[2] = 0, math.MaxUint32
	vUint64[1], vUint64[2] = 0, math.MaxUint64
	for i, b := range []uint32{0x7f800001, 0xff800007, 0x7fc12345, 0, 0x80000000, 0x7f800000, 0xff800000} {
		vFloat32[i+1] = math.Float32frombits(b)
	}
	for i, b := range []uint64{0x7ff0000000000001, 0xfff0000000000007, 0x7ff8123456789000, 0, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000} {
		vFloat64[i+1] = math.Float64frombits(b)
	}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		t.Run(mode.String()+"/Int8", func(t *testing.T) { checkMaskedMinMax(t, vInt8, set.MinMaxInt8Masked) })
		t.Run(mode.String()+"/Int16", func(t *testing.T) { checkMaskedMinMax(t, vInt16, set.MinMaxInt16Masked) })
		t.Run(mode.String()+"/Uint8", func(t *testing.T) { checkMaskedMinMax(t, vUint8, set.MinMaxUint8Masked) })
		t.Run(mode.String()+"/Uint16", func(t *testing.T) { checkMaskedMinMax(t, vUint16, set.MinMaxUint16Masked) })
		t.Run(mode.String()+"/Int32", func(t *testing.T) { checkMaskedMinMax(t, vInt32, set.MinMaxInt32Masked) })
		t.Run(mode.String()+"/Int64", func(t *testing.T) { checkMaskedMinMax(t, vInt64, set.MinMaxInt64Masked) })
		t.Run(mode.String()+"/Uint32", func(t *testing.T) { checkMaskedMinMax(t, vUint32, set.MinMaxUint32Masked) })
		t.Run(mode.String()+"/Uint64", func(t *testing.T) { checkMaskedMinMax(t, vUint64, set.MinMaxUint64Masked) })
		t.Run(mode.String()+"/Float32", func(t *testing.T) { checkMaskedMinMax(t, vFloat32, set.MinMaxFloat32Masked) })
		t.Run(mode.String()+"/Float64", func(t *testing.T) { checkMaskedMinMax(t, vFloat64, set.MinMaxFloat64Masked) })
	}
}
func TestMaskedMinMaxFloatPayloads(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		for n := 1; n <= 64; n++ {
			f32 := make([]float32, n)
			f64 := make([]float64, n)
			for i := range f32 {
				f32[i] = math.Float32frombits(0xff800001 + uint32(i))
				f64[i] = math.Float64frombits(0x7ff0000000000001 + uint64(i))
			}
			for _, mask := range []uint64{^uint64(0), 0xaaaaaaaaaaaaaaaa, 0x5555555555555555} {
				if mask&memory.TailMask(n) == 0 {
					continue
				}
				index := 0
				for i := range f32 {
					if mask&(uint64(1)<<uint(i)) != 0 {
						index = i
					}
				}
				a, b := s.MinMaxFloat32Masked(f32, mask)
				c, d := s.MinMaxFloat64Masked(f64, mask)
				if math.Float32bits(a) != math.Float32bits(f32[index]) || math.Float32bits(b) != math.Float32bits(f32[index]) || math.Float64bits(c) != math.Float64bits(f64[index]) || math.Float64bits(d) != math.Float64bits(f64[index]) {
					t.Fatalf("NaN payload mode=%s n=%d", mode, n)
				}
			}
			for i := range f32 {
				f32[i] = math.Float32frombits(uint32(i%2) << 31)
				f64[i] = math.Float64frombits(uint64(i%2) << 63)
			}
			a, b := s.MinMaxFloat32Masked(f32, ^uint64(0))
			c, d := s.MinMaxFloat64Masked(f64, ^uint64(0))
			neg := uint64(0)
			if n > 1 {
				neg = 1
			}
			if math.Float32bits(a) != uint32(neg<<31) || math.Float32bits(b) != 0 || math.Float64bits(c) != neg<<63 || math.Float64bits(d) != 0 {
				t.Fatalf("signed zeros mode=%s n=%d", mode, n)
			}
		}
	}
}
func TestMinMaxIDs(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		id, ok := MinMaxID(typ)
		if !ok || id.Kind() != KernelReduction || id.String() != fmt.Sprintf("minmax-%s-masked", typ) {
			t.Fatalf("bad id=%v", id)
		}
		count := 0
		for _, x := range KernelIDs() {
			if x == id {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("id registered %d times", count)
		}
		c, ok := New(ModeScalar).Capability(id)
		if !ok || c != (KernelCapability{Backend: "scalar"}) {
			t.Fatalf("capability=%+v", c)
		}
	}
	for _, typ := range []memory.DataType{memory.TypeBool, memory.TypeString} {
		if _, ok := MinMaxID(typ); ok {
			t.Fatal("unexpected supported type")
		}
	}
	for _, pair := range [][2]memory.DataType{{memory.TypeDate32, memory.TypeInt32}, {memory.TypeTime32, memory.TypeInt32}, {memory.TypeDate64, memory.TypeInt64}, {memory.TypeTimestamp, memory.TypeInt64}, {memory.TypeTime64, memory.TypeInt64}, {memory.TypeDuration, memory.TypeInt64}} {
		a, ok := MinMaxID(pair[0])
		b, _ := MinMaxID(pair[1])
		if !ok || a != b {
			t.Fatal("alias mismatch")
		}
	}
}
