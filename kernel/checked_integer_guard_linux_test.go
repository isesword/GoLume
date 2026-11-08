//go:build linux

package kernel

import (
	"gocolumn/memory"
	"syscall"
	"testing"
	"unsafe"
)

func reviewCheckedPages[T reviewInteger](t *testing.T, signed bool, run func([]T, []T, []T, memory.Bitmap, memory.Bitmap, ArithmeticOp) int, native func(ArithmeticOp) bool) {
	p := syscall.Getpagesize()
	maps := make([][]byte, 3)
	for i := range maps {
		m, e := syscall.Mmap(-1, 0, 2*p, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
		if e != nil {
			t.Fatal(e)
		}
		defer syscall.Munmap(m)
		if e = syscall.Mprotect(m[p:], syscall.PROT_NONE); e != nil {
			t.Fatal(e)
		}
		maps[i] = m
	}
	for n := 0; n <= 129; n++ {
		a := unsafe.Slice((*T)(unsafe.Pointer(&maps[0][p-n*int(unsafe.Sizeof(*new(T)))])), n)
		b := unsafe.Slice((*T)(unsafe.Pointer(&maps[1][p-n*int(unsafe.Sizeof(*new(T)))])), n)
		out := unsafe.Slice((*T)(unsafe.Pointer(&maps[2][p-n*int(unsafe.Sizeof(*new(T)))])), n)
		for i := range a {
			a[i] = T(uint64(i+1) * 0xf0f0f0f0f0f0f0f1)
			b[i] = T(i%5 + 1)
		}
		for pattern := 0; pattern < 4; pattern++ {
			valid := make(memory.Bitmap, memory.WordsFor(n))
			for i := 0; i < n; i++ {
				if pattern == 0 || pattern == 1 && i%3 == 0 {
					memory.Set(valid, i)
				}
			}
			if pattern == 3 {
				valid = nil
			} else if n > 0 {
				valid[len(valid)-1] |= ^memory.TailMask(n)
			}
			for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
				over := make(memory.Bitmap, memory.WordsFor(n))
				for i := range over {
					over[i] = ^uint64(0)
				}
				got := run(a, b, out, valid, over, op)
				first := -1
				for i := range a {
					want := T(0)
					bad := false
					if valid == nil || memory.IsSet(valid, i) {
						want, bad = reviewIntegerOp(a[i], b[i], op, signed)
					}
					if out[i] != want || memory.IsSet(over, i) != bad {
						t.Fatalf("guard n%d p%d op%s row%d", n, pattern, op, i)
					}
					if bad && first < 0 {
						first = i
					}
				}
				if got != first {
					t.Fatal("guard first row")
				}
			}
		}
	}
	for _, n := range []int{1, 2, 7, 8, 9, 63, 64, 65, 127, 128, 129} {
		a := unsafe.Slice((*T)(unsafe.Pointer(&maps[0][p])), n)
		b := unsafe.Slice((*T)(unsafe.Pointer(&maps[1][p])), n)
		out := make([]T, n)
		v := make(memory.Bitmap, memory.WordsFor(n))
		over := make(memory.Bitmap, len(v))
		for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
			if !native(op) {
				continue
			}
			for i := range out {
				out[i] = 123
			}
			for i := range over {
				over[i] = ^uint64(0)
			}
			if run(a, b, out, v, over, op) != -1 {
				t.Fatal("invalid unread first")
			}
			for _, z := range out {
				if z != 0 {
					t.Fatal("invalid unread value")
				}
			}
			for _, w := range over {
				if w != 0 {
					t.Fatal("invalid unread mask")
				}
			}
		}
	}
}
func TestIndependentCheckedGuardPages(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		reviewCheckedPages(t, true, s.Int64Arithmetic, func(op ArithmeticOp) bool {
			id, _ := ArithmeticID(memory.TypeInt64, op)
			c, _ := s.Capability(id)
			return c.HardwareSIMD
		})
		reviewCheckedPages(t, false, s.Uint64Arithmetic, func(op ArithmeticOp) bool {
			id, _ := ArithmeticID(memory.TypeUint64, op)
			c, _ := s.Capability(id)
			return c.HardwareSIMD
		})
	}
}
