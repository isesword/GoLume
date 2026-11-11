//go:build linux

package kernel

import (
	"gocolumn/memory"
	"syscall"
	"testing"
	"unsafe"
)

func testBitwiseGuard[T bitwiseInteger](t *testing.T, fn func([]T, []T, []T, memory.Bitmap, BitwiseOp)) {
	page := syscall.Getpagesize()
	size := int(unsafe.Sizeof(*new(T)))
	var pages [3][]byte
	for i := range pages {
		p, err := syscall.Mmap(-1, 0, 2*page, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
		if err != nil {
			t.Fatal(err)
		}
		defer syscall.Munmap(p)
		if err := syscall.Mprotect(p[page:], syscall.PROT_NONE); err != nil {
			t.Fatal(err)
		}
		pages[i] = p
	}
	for n := 0; n <= 129; n++ {
		a := unsafe.Slice((*T)(unsafe.Pointer(&pages[0][page-n*size])), n)
		b := unsafe.Slice((*T)(unsafe.Pointer(&pages[1][page-n*size])), n)
		o := unsafe.Slice((*T)(unsafe.Pointer(&pages[2][page-n*size])), n)
		for i := range a {
			a[i] = T(i * 313)
			b[i] = ^T(i * 517)
		}
		for pattern := 0; pattern < 7; pattern++ {
			v := bitwiseValidity(n, pattern)
			for op := BitwiseAnd; op <= BitwiseNot; op++ {
				fn(a, b, o, v, op)
				for i, x := range o {
					var want T
					if validAt(v, i) {
						want = bitwiseReference(a[i], b[i], op)
					}
					if x != want {
						t.Fatalf("n=%d row=%d %s", n, i, op)
					}
				}
			}
		}
	}
	// All-invalid inputs, including a partial final word, are never accessed.
	for _, n := range []int{1, 63, 64, 65, 127, 128, 129} {
		a := unsafe.Slice((*T)(unsafe.Pointer(&pages[0][page])), n)
		b := unsafe.Slice((*T)(unsafe.Pointer(&pages[1][page])), n)
		o := unsafe.Slice((*T)(unsafe.Pointer(&pages[2][page-n*size])), n)
		for op := BitwiseAnd; op <= BitwiseNot; op++ {
			fn(a, b, o, make(memory.Bitmap, (n+63)/64), op)
		}
	}
	// A valid unary call must ignore even an unreadable right slice.
	n := 129
	a := unsafe.Slice((*T)(unsafe.Pointer(&pages[0][page-n*size])), n)
	b := unsafe.Slice((*T)(unsafe.Pointer(&pages[1][page])), n)
	o := unsafe.Slice((*T)(unsafe.Pointer(&pages[2][page-n*size])), n)
	fn(a, b, o, nil, BitwiseNot)
	for i, x := range o {
		if x != ^a[i] {
			t.Fatal("not read right")
		}
	}
}
func TestBitwiseGuardPages(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		t.Run(mode.String()+"/Int32", func(t *testing.T) { testBitwiseGuard(t, s.BitwiseInt32) })
		t.Run(mode.String()+"/Uint32", func(t *testing.T) { testBitwiseGuard(t, s.BitwiseUint32) })
		t.Run(mode.String()+"/Int64", func(t *testing.T) { testBitwiseGuard(t, s.BitwiseInt64) })
		t.Run(mode.String()+"/Uint64", func(t *testing.T) { testBitwiseGuard(t, s.BitwiseUint64) })
	}
}
