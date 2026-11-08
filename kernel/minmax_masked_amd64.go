//go:build goexperiment.simd && amd64

package kernel

import (
	"simd"
	"simd/archsimd"
)

// finishMinMaxAVX forces BOTH scalar extractions to finish before VZEROUPPER.
// Baseline Go callers may immediately copy aggregate state with legacy SSE.
//
//go:noinline
func finishMinMaxAVX(lo, hi uint64) (uint64, uint64) {
	archsimd.ClearAVXUpperBits()
	return lo, hi
}

func reduceMinMaxUint32(lo, hi simd.Uint32s) (uint32, uint32) {
	var a, z archsimd.Uint32x4
	switch x := lo.ToArch().(type) {
	case archsimd.Uint32x16:
		y := hi.ToArch().(archsimd.Uint32x16)
		p, q := x.GetLo(), x.GetHi()
		r, t := y.GetLo(), y.GetHi()
		u := p.IfElse(p.Less(q), q)
		v := r.IfElse(r.Greater(t), t)
		p2, q2 := u.GetLo(), u.GetHi()
		r2, t2 := v.GetLo(), v.GetHi()
		a = p2.IfElse(p2.Less(q2), q2)
		z = r2.IfElse(r2.Greater(t2), t2)
	case archsimd.Uint32x8:
		y := hi.ToArch().(archsimd.Uint32x8)
		p, q := x.GetLo(), x.GetHi()
		r, t := y.GetLo(), y.GetHi()
		a = p.IfElse(p.Less(q), q)
		z = r.IfElse(r.Greater(t), t)
	case archsimd.Uint32x4:
		y := hi.ToArch().(archsimd.Uint32x4)
		a, z = x, y
	default:
		panic("kernel: unsupported native SIMD min/max width")
	}
	b := a.PermuteScalars(2, 3, 0, 1)
	c := z.PermuteScalars(2, 3, 0, 1)
	a = a.IfElse(a.Less(b), b)
	z = z.IfElse(z.Greater(c), c)
	b = a.PermuteScalars(1, 0, 3, 2)
	c = z.PermuteScalars(1, 0, 3, 2)
	a = a.IfElse(a.Less(b), b)
	z = z.IfElse(z.Greater(c), c)
	l, h := finishMinMaxAVX(uint64(a.GetElem(0)), uint64(z.GetElem(0)))
	return uint32(l), uint32(h)
}

func reduceMinMaxUint64(lo, hi simd.Uint64s) (uint64, uint64) {
	var a, z archsimd.Uint64x2
	switch x := lo.ToArch().(type) {
	case archsimd.Uint64x8:
		y := hi.ToArch().(archsimd.Uint64x8)
		p, q := x.GetLo(), x.GetHi()
		r, t := y.GetLo(), y.GetHi()
		u := p.IfElse(p.Less(q), q)
		v := r.IfElse(r.Greater(t), t)
		p2, q2 := u.GetLo(), u.GetHi()
		r2, t2 := v.GetLo(), v.GetHi()
		a = p2.IfElse(p2.Less(q2), q2)
		z = r2.IfElse(r2.Greater(t2), t2)
	case archsimd.Uint64x4:
		y := hi.ToArch().(archsimd.Uint64x4)
		p, q := x.GetLo(), x.GetHi()
		r, t := y.GetLo(), y.GetHi()
		a = p.IfElse(p.Less(q), q)
		z = r.IfElse(r.Greater(t), t)
	case archsimd.Uint64x2:
		y := hi.ToArch().(archsimd.Uint64x2)
		a, z = x, y
	default:
		panic("kernel: unsupported native SIMD min/max width")
	}
	b := a.ConcatPermuteScalars(1, 0, a)
	c := z.ConcatPermuteScalars(1, 0, z)
	a = a.IfElse(a.Less(b), b)
	z = z.IfElse(z.Greater(c), c)
	l, h := finishMinMaxAVX(uint64(a.GetElem(0)), uint64(z.GetElem(0)))
	return uint64(l), uint64(h)
}
