package kernel

import (
	"gocolumn/memory"
	"testing"
)

func TestCheckedInteger32AllocationAndAllAlias(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		for _, n := range []int{1, 7, 8, 9, 63, 64, 65, 129, 4097} {
			a, b, o := make([]int32, n), make([]int32, n), make([]int32, n)
			u, v, w := make([]uint32, n), make([]uint32, n), make([]uint32, n)
			valid, over := make(memory.Bitmap, memory.WordsFor(n)), make(memory.Bitmap, memory.WordsFor(n))
			for i := range valid {
				valid[i] = 0xa55a55aa0ff00ff0
			}
			for i := range a {
				a[i] = int32(i) * 0x43ff00ff
				b[i] = int32(i%7) - 3
				u[i] = uint32(a[i])
				v[i] = uint32(b[i])
			}
			for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
				for _, mask := range []memory.Bitmap{nil, valid} {
					if z := testing.AllocsPerRun(20, func() { s.Int32Arithmetic(a, b, o, mask, over, op); s.Uint32Arithmetic(u, v, w, mask, over, op) }); z != 0 {
						t.Fatalf("%s %s n%d allocations%f", mode, op, n, z)
					}
					for i := range o {
						o[i] = a[i]
						w[i] = u[i]
					}
					firstI, firstU := -1, -1
					for i := range o {
						if mask != nil && !memory.IsSet(mask, i) {
							continue
						}
						_, bi := reviewIntegerOp(o[i], o[i], op, true)
						_, bu := reviewIntegerOp(w[i], w[i], op, false)
						if bi && firstI < 0 {
							firstI = i
						}
						if bu && firstU < 0 {
							firstU = i
						}
					}
					if got := s.Int32Arithmetic(o, o, o, mask, over, op); got != firstI {
						t.Fatalf("same-all signed first%d want%d", got, firstI)
					}
					if got := s.Uint32Arithmetic(w, w, w, mask, over, op); got != firstU {
						t.Fatalf("same-all unsigned first%d want%d", got, firstU)
					}
					for i := range o {
						var wi int32
						var wu uint32
						if mask == nil || memory.IsSet(mask, i) {
							wi, _ = reviewIntegerOp(a[i], a[i], op, true)
							wu, _ = reviewIntegerOp(u[i], u[i], op, false)
						}
						if o[i] != wi || w[i] != wu {
							t.Fatalf("same-all n%d lane%d", n, i)
						}
					}
				}
			}
		}
	}
}
