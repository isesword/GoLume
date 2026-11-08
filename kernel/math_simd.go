//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"simd"
)

func installMathSIMD(set *Set, mode Mode, vectorBits int) {
	// Prefer native dense SIMD in Auto and explicit SIMD mode. Nullable
	// inputs retain the reference path below.
	if mode == ModeScalar || simd.Emulated() {
		return
	}
	for _, op := range []MathUnaryOp{MathAbs, MathSqrt} {
		set.d.math.unaryFloat32[op] = func(a, o []float32, v memory.Bitmap) { simdMathFloat32Unary(a, o, v, op) }
		set.d.math.unaryFloat64[op] = func(a, o []float64, v memory.Bitmap) { simdMathFloat64Unary(a, o, v, op) }
		for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
			id, _ := MathUnaryID(typ, op)
			set.d.caps.byID[id] = KernelCapability{HardwareSIMD: true, VectorBits: vectorBits, Backend: "portable-simd"}
		}
	}
}

func simdMathFloat32Unary(in, out []float32, validity memory.Bitmap, op MathUnaryOp) {
	if len(out) < len(in) {
		panic("kernel: math output too small")
	}
	// Keep sparse/nullable lanes on the reference path so inactive input is
	// never evaluated. This is a dense backend, not a claim of masked SIMD.
	if validity != nil {
		scalarMathFloatUnary(in, out, validity, op)
		return
	}
	lanes := simd.Float32s{}.Len()
	row := 0
	for ; row+lanes <= len(in); row += lanes {
		x := simd.LoadFloat32s(in[row:])
		if op == MathAbs {
			x.Abs().Store(out[row:])
		} else {
			x.Sqrt().Store(out[row:])
		}
	}
	scalarMathFloatUnary(in[row:], out[row:], nil, op)
}
func simdMathFloat64Unary(in, out []float64, validity memory.Bitmap, op MathUnaryOp) {
	if len(out) < len(in) {
		panic("kernel: math output too small")
	}
	if validity != nil {
		scalarMathFloatUnary(in, out, validity, op)
		return
	}
	lanes := simd.Float64s{}.Len()
	row := 0
	for ; row+lanes <= len(in); row += lanes {
		x := simd.LoadFloat64s(in[row:])
		if op == MathAbs {
			x.Abs().Store(out[row:])
		} else {
			x.Sqrt().Store(out[row:])
		}
	}
	scalarMathFloatUnary(in[row:], out[row:], nil, op)
}
