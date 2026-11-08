//go:build linux

package kernel

import (
	"gocolumn/memory"
	"math"
	"syscall"
	"testing"
	"unsafe"
)

func testMathMaskedGuard[T mathFloat](t *testing.T, fn func([]T, []T, memory.Bitmap, MathUnaryOp)) {
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
	for n := 0; n <= 129; n++ {
		in := unsafe.Slice((*T)(unsafe.Pointer(&src[page-n*sz])), n)
		out := unsafe.Slice((*T)(unsafe.Pointer(&dst[page-n*sz])), n)
		for i := range in {
			in[i] = T(-float64(i) - .25)
		}
		for pattern := 0; pattern < 7; pattern++ {
			v := mathMaskedValidity(n, pattern)
			for _, op := range []MathUnaryOp{MathAbs, MathSqrt} {
				fn(in, out, v, op)
				for i, x := range out {
					if !validAt(v, i) {
						if x != 0 || math.Signbit(float64(x)) {
							t.Fatal("invalid lane")
						}
					} else if op == MathAbs {
						if x != -in[i] {
							t.Fatal("abs")
						}
					} else if !math.IsNaN(float64(x)) {
						t.Fatal("sqrt")
					}
				}
			}
		}
	}
	// An all-invalid word must not touch its input, even when that page is unreadable.
	in := unsafe.Slice((*T)(unsafe.Pointer(&src[page])), 64)
	out := unsafe.Slice((*T)(unsafe.Pointer(&dst[page-64*sz])), 64)
	for _, op := range []MathUnaryOp{MathAbs, MathSqrt} {
		fn(in, out, memory.Bitmap{0}, op)
	}
}

func TestMathMaskedGuardPages(t *testing.T) {
	for _, mode := range []Mode{ModeAuto, ModeSIMD} {
		s := New(mode)
		// Scalar iteration loads values before checking validity, so only use the
		// inaccessible-input assertion when the native masked backend is installed.
		id, _ := MathUnaryID(memory.TypeFloat32, MathAbs)
		cap, _ := s.Capability(id)
		if !cap.HardwareSIMD {
			continue
		}
		testMathMaskedGuard(t, s.MathUnaryFloat32)
		testMathMaskedGuard(t, s.MathUnaryFloat64)
	}
}
