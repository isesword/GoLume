package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math/rand"
	"reflect"
	"testing"
)

func checkNarrowSum[T narrowSumInteger](t *testing.T, call func([]T, uint64) int64, lo, hi T) {
	t.Helper()
	rng := rand.New(rand.NewSource(193286))
	var storage [80]T
	for pattern := 0; pattern < 4; pattern++ {
		for i := range storage {
			switch pattern {
			case 0:
				storage[i] = T(rng.Uint64())
			case 1:
				storage[i] = lo
			case 2:
				storage[i] = hi
			case 3:
				if i%2 == 0 {
					storage[i] = lo
				} else {
					storage[i] = hi
				}
			}
		}
		before := storage
		for offset := 0; offset < 16; offset++ {
			for n := 0; n <= 64; n++ {
				v := storage[offset : offset+n : offset+n]
				masks := []uint64{0, ^uint64(0), 0xaaaaaaaaaaaaaaaa, 0x5555555555555555, rng.Uint64(), rng.Uint64()}
				for bit := 0; bit < 64; bit++ {
					masks = append(masks, uint64(1)<<uint(bit), ^(uint64(1) << uint(bit)))
				}
				if n <= 8 {
					for mask := uint64(0); mask < uint64(1)<<uint(n); mask++ {
						masks = append(masks, mask)
					}
				}
				for _, mask := range masks {
					var want int64
					for i, x := range v {
						if mask&(uint64(1)<<uint(i)) != 0 {
							want += int64(x)
						}
					}
					if got := call(v, mask); got != want {
						t.Fatalf("pattern=%d offset=%d n=%d mask=%x got=%d want=%d", pattern, offset, n, mask, got, want)
					}
				}
			}
		}
		if storage != before {
			t.Fatal("sum mutated source")
		}
	}
	if call(nil, ^uint64(0)) != 0 {
		t.Fatal("nil sum")
	}
	for _, mask := range []uint64{0, ^uint64(0)} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("length >64 did not panic")
				}
			}()
			call(storage[:65], mask)
		}()
	}
	for _, n := range []int{0, 1, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64} {
		for _, mask := range []uint64{0, 1, 0xaaaaaaaaaaaaaaaa, ^uint64(0)} {
			if got := testing.AllocsPerRun(100, func() { call(storage[1:1+n], mask) }); got != 0 {
				t.Fatalf("n=%d mask=%x allocs=%v", n, mask, got)
			}
		}
	}
}

func TestNarrowMaskedSums(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		t.Run(mode.String()+"/int8", func(t *testing.T) { checkNarrowSum(t, set.SumInt8Masked, int8(-128), int8(127)) })
		t.Run(mode.String()+"/int16", func(t *testing.T) { checkNarrowSum(t, set.SumInt16Masked, int16(-32768), int16(32767)) })
		t.Run(mode.String()+"/uint8", func(t *testing.T) { checkNarrowSum(t, set.SumUint8Masked, uint8(0), uint8(255)) })
		t.Run(mode.String()+"/uint16", func(t *testing.T) { checkNarrowSum(t, set.SumUint16Masked, uint16(0), uint16(65535)) })
	}
}

func TestNarrowSumIDs(t *testing.T) {
	ids := KernelIDs()
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
		id, ok := SumID(typ)
		if !ok || id.Kind() != KernelReduction || id.String() != fmt.Sprintf("sum-%s-masked", typ) {
			t.Fatalf("bad ID %v", typ)
		}
		count := 0
		for _, candidate := range ids {
			if candidate == id {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("id=%v count=%d", id, count)
		}
		cap, ok := New(ModeScalar).Capability(id)
		if !ok || cap != (KernelCapability{Backend: "scalar"}) {
			t.Fatalf("scalar capability=%+v", cap)
		}
	}
	// Scalar and auto sets remain separately initialized and immutable.
	scalar := New(ModeScalar)
	before := reflect.ValueOf(scalar.d.sumInt8Masked).Pointer()
	_ = New(ModeAuto)
	if before != reflect.ValueOf(scalar.d.sumInt8Masked).Pointer() {
		t.Fatal("set mutated")
	}
}

var narrowSumSink int64

func benchmarkNarrowSum[T narrowSumInteger](b *testing.B, call func([]T, uint64) int64, n int, mask uint64) {
	var v [65]T
	for i := range v {
		v[i] = T(i*113 + 1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var sum int64
	for i := 0; i < b.N; i++ {
		sum += call(v[1:1+n], mask)
	}
	narrowSumSink = sum
}
func BenchmarkNarrowMaskedSum(b *testing.B) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, n := range []int{7, 32, 64} {
			for _, m := range []struct {
				name string
				mask uint64
			}{{"dense", ^uint64(0)}, {"half", 0xaaaaaaaaaaaaaaaa}, {"sparse", 0x8000000000000001}, {"empty", 0}} {
				name := fmt.Sprintf("%s/n%d/%s/", mode, n, m.name)
				b.Run(name+"int8", func(b *testing.B) { benchmarkNarrowSum(b, set.SumInt8Masked, n, m.mask) })
				b.Run(name+"int16", func(b *testing.B) { benchmarkNarrowSum(b, set.SumInt16Masked, n, m.mask) })
				b.Run(name+"uint8", func(b *testing.B) { benchmarkNarrowSum(b, set.SumUint8Masked, n, m.mask) })
				b.Run(name+"uint16", func(b *testing.B) { benchmarkNarrowSum(b, set.SumUint16Masked, n, m.mask) })
			}
		}
	}
}
