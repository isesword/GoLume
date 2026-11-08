package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math/rand"
	"testing"
)

func checkMaskedSum[T sumInteger](t *testing.T, call func([]T, uint64) T) {
	t.Helper()
	rng := rand.New(rand.NewSource(9321))
	storage := make([]T, 67)
	for i := range storage {
		storage[i] = T(rng.Uint64())
	}
	// Include zero, extrema and overflow in both signed and unsigned domains.
	max, high64, high32 := ^uint64(0), uint64(1)<<63, uint64(1)<<31
	storage[1], storage[2], storage[3] = T(max), T(high64), T(high32)
	for offset := 0; offset < 3; offset++ {
		for n := 0; n <= 64; n++ {
			v := storage[offset : offset+n : offset+n]
			masks := []uint64{0, ^uint64(0), 0xaaaaaaaaaaaaaaaa, 0x5555555555555555, memory.TailMask(n), rng.Uint64()}
			for bit := 0; bit < 64; bit++ {
				masks = append(masks, uint64(1)<<uint(bit), ^(uint64(1) << uint(bit)))
			}
			for _, mask := range masks {
				var want T
				for i, x := range v {
					if mask&(uint64(1)<<uint(i)) != 0 {
						want += x
					}
				}
				if got := call(v, mask); got != want {
					t.Fatalf("offset=%d n=%d mask=%x got=%v want=%v", offset, n, mask, got, want)
				}
			}
		}
	}
	if got := call(nil, ^uint64(0)); got != 0 {
		t.Fatalf("nil sum=%v", got)
	}
	for _, mask := range []uint64{0, ^uint64(0)} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("len >64 did not panic")
				}
			}()
			call(storage[:65], mask)
		}()
	}
	for _, n := range []int{0, 1, 7, 31, 64} {
		for _, mask := range []uint64{0, 1, 0xaaaaaaaaaaaaaaaa, ^uint64(0)} {
			if got := testing.AllocsPerRun(100, func() { call(storage[1:1+n], mask) }); got != 0 {
				t.Fatalf("n=%d mask=%x allocations=%v", n, mask, got)
			}
		}
	}
}

func TestMaskedIntegerSums(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		t.Run(mode.String()+"/int32", func(t *testing.T) { checkMaskedSum(t, set.SumInt32Masked) })
		t.Run(mode.String()+"/uint32", func(t *testing.T) { checkMaskedSum(t, set.SumUint32Masked) })
		t.Run(mode.String()+"/int64", func(t *testing.T) { checkMaskedSum(t, set.SumInt64Masked) })
		t.Run(mode.String()+"/uint64", func(t *testing.T) { checkMaskedSum(t, set.SumUint64Masked) })
	}
}

func TestSumIDs(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
		id, ok := SumID(typ)
		if !ok || id.Kind() != KernelReduction || id.String() != fmt.Sprintf("sum-%s-masked", typ) {
			t.Fatalf("id=%v ok=%v", id, ok)
		}
		found := 0
		for _, registered := range KernelIDs() {
			if registered == id {
				found++
			}
		}
		if found != 1 {
			t.Fatalf("%v registered %d times", id, found)
		}
		cap, ok := New(ModeScalar).Capability(id)
		if !ok || cap != (KernelCapability{Backend: "scalar"}) {
			t.Fatalf("%v capability=%+v", id, cap)
		}
	}
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeUint16, memory.TypeFloat32, memory.TypeFloat64, memory.TypeString} {
		if _, ok := SumID(typ); ok {
			t.Fatalf("unexpected sum for %v", typ)
		}
	}
	for _, pair := range [][2]memory.DataType{{memory.TypeDate32, memory.TypeInt32}, {memory.TypeTime32, memory.TypeInt32}, {memory.TypeDate64, memory.TypeInt64}, {memory.TypeTimestamp, memory.TypeInt64}, {memory.TypeTime64, memory.TypeInt64}, {memory.TypeDuration, memory.TypeInt64}} {
		a, ok := SumID(pair[0])
		b, _ := SumID(pair[1])
		if !ok || a != b {
			t.Fatalf("bad alias %v", pair)
		}
	}
}

var maskedSumSink uint64

func benchmarkMaskedSum[T sumInteger](b *testing.B, call func([]T, uint64) T, mask uint64, n int) {
	var v [65]T
	for i := range v {
		v[i] = T(i * 137)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var result T
	for i := 0; i < b.N; i++ {
		result += call(v[1:n+1], mask)
	}
	maskedSumSink = uint64(result)
}
func BenchmarkMaskedIntegerSum(b *testing.B) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, n := range []int{7, 32, 64} {
			for _, m := range []struct {
				name string
				mask uint64
			}{{"dense", ^uint64(0)}, {"half", 0xaaaaaaaaaaaaaaaa}, {"sparse", 0x8000000000000001}, {"empty", 0}} {
				name := fmt.Sprintf("%s/n%d/%s/", mode, n, m.name)
				b.Run(name+"int32", func(b *testing.B) { benchmarkMaskedSum(b, set.SumInt32Masked, m.mask, n) })
				b.Run(name+"uint32", func(b *testing.B) { benchmarkMaskedSum(b, set.SumUint32Masked, m.mask, n) })
				b.Run(name+"int64", func(b *testing.B) { benchmarkMaskedSum(b, set.SumInt64Masked, m.mask, n) })
				b.Run(name+"uint64", func(b *testing.B) { benchmarkMaskedSum(b, set.SumUint64Masked, m.mask, n) })
			}
		}
	}
}
