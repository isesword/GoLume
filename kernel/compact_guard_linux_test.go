//go:build linux

package kernel

import (
	"gocolumn/memory"
	"math"
	"syscall"
	"testing"
	"unsafe"
)

func reviewGuardedCompact[T any](t *testing.T, fn func([]T, memory.Bitmap, []T) int, val func(int) T, eq func(T, T) bool) {
	page := syscall.Getpagesize()
	sz := int(unsafe.Sizeof(*new(T)))
	src, err := syscall.Mmap(-1, 0, 2*page, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(src)
	dst, err := syscall.Mmap(-1, 0, 2*page, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(dst)
	if err = syscall.Mprotect(src[page:], syscall.PROT_NONE); err != nil {
		t.Fatal(err)
	}
	if err = syscall.Mprotect(dst[page:], syscall.PROT_NONE); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129} {
		for p := 0; p < 5; p++ {
			values := unsafe.Slice((*T)(unsafe.Pointer(&src[page-n*sz])), n)
			for i := range values {
				values[i] = val(i)
			}
			sel := make(memory.Bitmap, memory.WordsFor(n))
			want := []T{}
			for i, v := range values {
				if p == 0 || p == 1 && i == n-1 || p == 2 && i%3 == 1 || p == 3 && i%64 != 1 {
					memory.Set(sel, i)
					want = append(want, v)
				}
			}
			if n > 0 {
				sel[len(sel)-1] |= ^memory.TailMask(n)
			}
			out := unsafe.Slice((*T)(unsafe.Pointer(&dst[page-len(want)*sz])), len(want))
			if fn(values, sel, out) != len(want) {
				t.Fatal("guard count")
			}
			for i, v := range want {
				if !eq(out[i], v) {
					t.Fatalf("n%d p%d row%d", n, p, i)
				}
			}
		}
	}
}
func TestReviewCompactGuardPages(t *testing.T) {
	s := New(ModeAuto)
	reviewGuardedCompact(t, s.CompactInt8, func(i int) int8 { return int8(i * 97) }, func(a, b int8) bool { return a == b })
	reviewGuardedCompact(t, s.CompactUint8, func(i int) uint8 { return uint8(i * 97) }, func(a, b uint8) bool { return a == b })
	reviewGuardedCompact(t, s.CompactInt16, func(i int) int16 { return int16(i * 997) }, func(a, b int16) bool { return a == b })
	reviewGuardedCompact(t, s.CompactUint16, func(i int) uint16 { return uint16(i * 997) }, func(a, b uint16) bool { return a == b })
	reviewGuardedCompact(t, s.CompactFloat32, func(i int) float32 { return math.Float32frombits(0xff800001 + uint32(i)) }, func(a, b float32) bool { return math.Float32bits(a) == math.Float32bits(b) })
}
