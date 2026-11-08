package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand/v2"
	"slices"
	"testing"
)

type reviewInteger interface{ ~int64 | ~uint64 }

func reviewIntegerOp[T reviewInteger](a, b T, op ArithmeticOp, signed bool) (T, bool) {
	var x, y big.Int
	if signed {
		x.SetInt64(int64(a))
		y.SetInt64(int64(b))
	} else {
		x.SetUint64(uint64(a))
		y.SetUint64(uint64(b))
	}
	var z T
	switch op {
	case ArithmeticAdd:
		x.Add(&x, &y)
		z = a + b
	case ArithmeticSub:
		x.Sub(&x, &y)
		z = a - b
	case ArithmeticMul:
		x.Mul(&x, &y)
		z = a * b
	}
	if signed {
		return z, !x.IsInt64()
	}
	return z, !x.IsUint64()
}
func reviewIntegerCases[T reviewInteger](t *testing.T, signed bool, run func([]T, []T, []T, memory.Bitmap, memory.Bitmap, ArithmeticOp) int) {
	r := rand.New(rand.NewPCG(0x3427, 0x9876))
	edges := []uint64{0, 1, 2, 3, 0x7fffffff, 0x80000000, 0xfffffffe, 0xffffffff, 0x100000000, 0x100000001, 3037000499, 3037000500, 0x7ffffffffffffffe, 0x7fffffffffffffff, 0x8000000000000000, 0x8000000000000001, 0xffffffff00000000, 0xfffffffffffffffe, 0xffffffffffffffff}
	lengths := []int{}
	for n := 0; n <= 193; n++ {
		lengths = append(lengths, n)
	}
	lengths = append(lengths, 1025, 2051)
	for _, n := range lengths {
		a, b := make([]T, n+3), make([]T, n+5)
		a = a[1 : 1+n : 1+n]
		b = b[3 : 3+n : 3+n]
		for i := range a {
			a[i] = T(r.Uint64())
			b[i] = T(r.Uint64())
			switch i % 5 {
			case 0:
				a[i] = T(edges[i%len(edges)])
				b[i] = T(edges[(i/len(edges))%len(edges)])
			case 1:
				a[i] = T(int64(int32(r.Uint32())))
				b[i] = T(int64(int32(r.Uint32())))
			case 2:
				b[i] = T(int64(i%5) - 2)
			}
		}
		ac, bc := slices.Clone(a), slices.Clone(b)
		for kind := 0; kind < 5; kind++ {
			v := make(memory.Bitmap, memory.WordsFor(n))
			for i := range v {
				switch kind {
				case 1:
					v[i] = ^uint64(0)
				case 2:
					v[i] = r.Uint64()
				case 3:
					v[i] = 1 << uint((i*17)%64)
				}
			}
			if kind == 0 {
				v = nil
			}
			vc := slices.Clone(v)
			for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
				want := make([]T, n)
				fail := make(memory.Bitmap, memory.WordsFor(n))
				first := -1
				for i := range a {
					if v != nil && (v[i/64]>>uint(i%64))&1 == 0 {
						continue
					}
					value, bad := reviewIntegerOp(a[i], b[i], op, signed)
					want[i] = value
					if bad {
						memory.Set(fail, i)
						if first < 0 {
							first = i
						}
					}
				}
				for withMask := 0; withMask < 2; withMask++ {
					out := make([]T, n+2)
					out[0] = 123
					out[n+1] = 117
					var over memory.Bitmap
					if withMask != 0 {
						over = make(memory.Bitmap, len(fail)+2)
						for i := range over {
							over[i] = ^uint64(0)
						}
					}
					var mask memory.Bitmap
					if over != nil {
						mask = over[1 : len(over)-1]
					}
					got := run(a, b, out[1:n+1:n+1], v, mask, op)
					if got != first || !slices.Equal(out[1:n+1], want) || out[0] != 123 || out[n+1] != 117 {
						t.Fatalf("n%d kind%d op%s signed%t first%d want%d", n, kind, op, signed, got, first)
					}
					if over != nil && (!slices.Equal(mask, fail) || over[0] != ^uint64(0) || over[len(over)-1] != ^uint64(0)) {
						t.Fatalf("overflow n%d kind%d op%s", n, kind, op)
					}
				}
				for side := 0; side < 2; side++ {
					la, rb := slices.Clone(a), slices.Clone(b)
					out := la
					if side != 0 {
						out = rb
					}
					if got := run(la, rb, out, v, nil, op); got != first || !slices.Equal(out, want) {
						t.Fatalf("exact alias n%d side%d kind%d op%s", n, side, kind, op)
					}
				}
			}
			if !slices.Equal(v, vc) || !slices.Equal(a, ac) || !slices.Equal(b, bc) {
				t.Fatal("input mutated")
			}
		}
	}
}
func TestIndependentCheckedIntegerReference(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		t.Run(fmt.Sprint(mode)+"/signed", func(t *testing.T) { reviewIntegerCases(t, true, s.Int64Arithmetic) })
		t.Run(fmt.Sprint(mode)+"/unsigned", func(t *testing.T) { reviewIntegerCases(t, false, s.Uint64Arithmetic) })
	}
}
func reviewSequential[T reviewInteger](a, b, out []T, v memory.Bitmap, op ArithmeticOp, signed bool) int {
	first := -1
	for base := 0; base < len(a); base += 64 {
		end := min(base+64, len(a))
		for i := base; i < end; i++ {
			z, bad := reviewIntegerOp(a[i], b[i], op, signed)
			out[i] = z
			if bad && (v == nil || memory.IsSet(v, i)) && first < 0 {
				first = i
			}
		}
		for i := base; i < end; i++ {
			if v != nil && !memory.IsSet(v, i) {
				out[i] = 0
			}
		}
	}
	return first
}
func reviewPartial[T reviewInteger](t *testing.T, signed bool, run func([]T, []T, []T, memory.Bitmap, memory.Bitmap, ArithmeticOp) int) {
	const n = 131
	for _, shift := range []int{-64, -63, -7, -2, -1, 1, 2, 7, 63, 64} {
		for side := 0; side < 3; side++ {
			for masked := 0; masked < 2; masked++ {
				for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
					back := make([]T, n+192)
					for i := range back {
						back[i] = T(uint64(i+13) * 0xa28f64e581d5f7c1)
					}
					wantBack := slices.Clone(back)
					other := make([]T, n)
					for i := range other {
						other[i] = T(i%5 + 1)
					}
					a, b := back[64:64+n], other
					wa, wb := wantBack[64:64+n], slices.Clone(other)
					if side == 1 {
						a, b = b, a
						wa, wb = wb, wa
					}
					if side == 2 {
						b = back[65 : 65+n]
						wb = wantBack[65 : 65+n]
					}
					out, wo := back[64+shift:64+shift+n], wantBack[64+shift:64+shift+n]
					var v memory.Bitmap
					if masked != 0 {
						v = memory.Bitmap{0xaaaaaaaaaaaaaaaa, 0xf0f0f0f0f0f0f0f0, 7}
					}
					want := reviewSequential(wa, wb, wo, v, op, signed)
					got := run(a, b, out, v, nil, op)
					if got != want || !slices.Equal(back, wantBack) {
						t.Fatalf("partial shift%d side%d mask%d op%s first%d want%d", shift, side, masked, op, got, want)
					}
				}
			}
		}
	}
}
func TestIndependentCheckedIntegerPartialAlias(t *testing.T) {
	for _, mode := range []Mode{ModeAuto, ModeSIMD} {
		s := New(mode)
		reviewPartial(t, true, s.Int64Arithmetic)
		reviewPartial(t, false, s.Uint64Arithmetic)
	}
}
func TestIndependentCheckedIntegerEarliest(t *testing.T) {
	const n = 193
	for _, mode := range []Mode{ModeAuto, ModeSIMD} {
		s := New(mode)
		for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
			for first := 0; first < n; first++ {
				a, b, o := make([]int64, n), make([]int64, n), make([]int64, n)
				u, v, w := make([]uint64, n), make([]uint64, n), make([]uint64, n)
				valid := make(memory.Bitmap, memory.WordsFor(n))
				for i := range a {
					a[i], b[i], u[i], v[i] = math.MaxInt64, 2, math.MaxUint64, 2
					if op == ArithmeticSub {
						a[i], b[i], u[i], v[i] = math.MinInt64, 1, 0, 1
					}
					if i >= first {
						memory.Set(valid, i)
					}
				}
				over := make(memory.Bitmap, memory.WordsFor(n))
				if got := s.Int64Arithmetic(a, b, o, valid, over, op); got != first {
					t.Fatalf("signed %s first%d got%d", op, first, got)
				}
				for i := range a {
					if memory.IsSet(over, i) != (i >= first) || i < first && o[i] != 0 {
						t.Fatal("signed bitmap")
					}
				}
				if got := s.Uint64Arithmetic(u, v, w, valid, over, op); got != first {
					t.Fatalf("unsigned %s first%d got%d", op, first, got)
				}
			}
		}
	}
}
