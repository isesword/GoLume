package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/rand/v2"
	"testing"
)

func compactEqual[T comparable](a, b T) bool {
	if f, ok := any(a).(float32); ok {
		return math.Float32bits(f) == math.Float32bits(any(b).(float32))
	}
	return a == b
}

func checkNativeCompact[T comparable](t *testing.T, compact func([]T, memory.Bitmap, []T) int, value func(int) T) {
	t.Helper()
	random := rand.New(rand.NewPCG(31, 79))
	for _, n := range []int{0, 1, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 1021, 1024, 1025} {
		for pattern := 0; pattern < 8; pattern++ {
			t.Run(fmt.Sprintf("n%d/p%d", n, pattern), func(t *testing.T) {
				storage := make([]T, n+7)
				for i := range storage {
					storage[i] = value(i)
				}
				values := storage[3 : 3+n]
				original := append([]T(nil), values...)
				var selection memory.Bitmap
				if pattern != 0 {
					selection = make(memory.Bitmap, memory.WordsFor(n))
					for row := 0; row < n; row++ {
						yes := false
						switch pattern {
						case 1:
							yes = true
						case 2:
							yes = false
						case 3:
							yes = row%2 == 0
						case 4:
							yes = row%67 == 0
						case 5:
							yes = row%64 >= 29 && row%64 < 61
						case 6:
							yes = row == n-1
						case 7:
							yes = random.Uint64()&1 != 0
						}
						if yes {
							memory.Set(selection, row)
						}
					}
					if n > 0 {
						selection[len(selection)-1] |= ^memory.TailMask(n)
					}
				}
				want := make([]T, 0, n)
				for row, v := range values {
					if selection == nil || memory.IsSet(selection, row) {
						want = append(want, v)
					}
				}
				for _, extra := range []int{0, 3, 67} {
					out := make([]T, len(want)+extra+5)
					for i := range out {
						out[i] = value(731 + i)
					}
					before := append([]T(nil), out...)
					got := compact(values, selection, out[2:2+len(want)+extra])
					if got != len(want) {
						t.Fatalf("length %d want %d", got, len(want))
					}
					for i := range out {
						expected := before[i]
						if i >= 2 && i < 2+len(want) {
							expected = want[i-2]
						}
						if !compactEqual(out[i], expected) {
							t.Fatalf("output[%d]=%v want %v", i, out[i], expected)
						}
					}
				}
				for i := range values {
					if !compactEqual(values[i], original[i]) {
						t.Fatal("input mutated")
					}
				}
				for _, offset := range []int{0, 3} {
					alias := make([]T, n+offset)
					copy(alias[offset:], original)
					before := append([]T(nil), alias...)
					got := compact(alias[offset:], selection, alias[:len(want)])
					if got != len(want) {
						t.Fatal("alias length")
					}
					for i, v := range want {
						if !compactEqual(alias[i], v) {
							t.Fatalf("alias[%d]", i)
						}
					}
					for i := len(want); i < len(alias); i++ {
						if !compactEqual(alias[i], before[i]) {
							t.Fatalf("alias suffix[%d] changed", i)
						}
					}
				}
				if len(want) > 0 {
					func() {
						defer func() {
							if recover() == nil {
								t.Fatal("short output must panic")
							}
						}()
						compact(values, selection, make([]T, len(want)-1))
					}()
				}
			})
		}
	}
}

func TestNativeCompactBoundariesAndBits(t *testing.T) {
	patterns := []uint32{0, 0x80000000, 0x7fc00001, 0x7f800001, 0xffc02345, 0x7f800000, 0xff800000, 1, 0x80000001, 0x007fffff, 0x00800000, 0x7f7fffff}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		t.Run(mode.String()+"/float32", func(t *testing.T) {
			checkNativeCompact(t, set.CompactFloat32, func(i int) float32 { return math.Float32frombits(patterns[i%len(patterns)]) })
		})
		t.Run(mode.String()+"/int8", func(t *testing.T) { checkNativeCompact(t, set.CompactInt8, func(i int) int8 { return int8(i * 113) }) })
		t.Run(mode.String()+"/uint8", func(t *testing.T) {
			checkNativeCompact(t, set.CompactUint8, func(i int) uint8 { return uint8(i * 113) })
		})
		t.Run(mode.String()+"/int16", func(t *testing.T) {
			checkNativeCompact(t, set.CompactInt16, func(i int) int16 { return int16(i * 14071) })
		})
		t.Run(mode.String()+"/uint16", func(t *testing.T) {
			checkNativeCompact(t, set.CompactUint16, func(i int) uint16 { return uint16(i * 14071) })
		})
	}
}

func TestNativeCompactExhaustiveFloat32Masks(t *testing.T) {
	set := New(ModeAuto)
	values, out := make([]float32, 16), make([]float32, 16)
	for i := range values {
		values[i] = math.Float32frombits(uint32(i) * 0x89171713)
	}
	selection := memory.Bitmap{0}
	for mask := 0; mask < 1<<16; mask++ {
		selection[0] = uint64(mask)
		got := set.CompactFloat32(values, selection, out)
		want := 0
		for row, v := range values {
			if mask&(1<<row) != 0 {
				if !compactEqual(out[want], v) {
					t.Fatalf("mask=%x row=%d", mask, row)
				}
				want++
			}
		}
		if got != want {
			t.Fatalf("mask=%x got=%d want=%d", mask, got, want)
		}
	}
}

func TestNativeCompactNoAllocations(t *testing.T) {
	set := New(ModeAuto)
	const n = 1025
	f32 := make([]float32, n)
	i8 := make([]int8, n)
	u8 := make([]uint8, n)
	i16 := make([]int16, n)
	u16 := make([]uint16, n)
	selection := make(memory.Bitmap, memory.WordsFor(n))
	for row := 0; row < n; row += 3 {
		memory.Set(selection, row)
	}
	if got := testing.AllocsPerRun(100, func() {
		set.CompactFloat32(f32, selection, f32)
		set.CompactInt8(i8, selection, i8)
		set.CompactUint8(u8, selection, u8)
		set.CompactInt16(i16, selection, i16)
		set.CompactUint16(u16, selection, u16)
	}); got != 0 {
		t.Fatalf("allocations=%v", got)
	}
}
