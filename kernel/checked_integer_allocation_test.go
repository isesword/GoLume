package kernel

import (
	"gocolumn/memory"
	"testing"
)

func TestIndependentCheckedAllocationAndAllAlias(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		for _, n := range []int{1, 7, 8, 9, 63, 64, 65, 129, 4097} {
			a, b, o := make([]int64, n), make([]int64, n), make([]int64, n)
			u, v, w := make([]uint64, n), make([]uint64, n), make([]uint64, n)
			valid, over := make(memory.Bitmap, memory.WordsFor(n)), make(memory.Bitmap, memory.WordsFor(n))
			for i := range valid {
				valid[i] = 0xa55a55aa0ff00ff0
			}
			for i := range a {
				a[i] = int64(i) * 0x43ff00ff00ff00ff
				b[i] = int64(i%7) - 3
				u[i] = uint64(a[i])
				v[i] = uint64(b[i])
			}
			for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
				for _, mask := range []memory.Bitmap{nil, valid} {
					if z := testing.AllocsPerRun(20, func() { s.Int64Arithmetic(a, b, o, mask, over, op); s.Uint64Arithmetic(u, v, w, mask, over, op) }); z != 0 {
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
					if got := s.Int64Arithmetic(o, o, o, mask, over, op); got != firstI {
						t.Fatalf("same-all signed first%d want%d", got, firstI)
					}
					if got := s.Uint64Arithmetic(w, w, w, mask, over, op); got != firstU {
						t.Fatalf("same-all unsigned first%d want%d", got, firstU)
					}
					for i := range o {
						var wi int64
						var wu uint64
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
