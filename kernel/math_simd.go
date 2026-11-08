//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"simd"
	"unsafe"
)

func installMathSIMD(set *Set, mode Mode, vectorBits int) {
	// Prefer native SIMD in Auto and explicit SIMD mode. Packed validity
	// masks are expanded in registers before any floating operation.
	if mode == ModeScalar || !nativeSIMDAvailable() {
		return
	}
	// The fixed lane-weight tables cover the supported portable widths.
	if vectorBits != 128 && vectorBits != 256 && vectorBits != 512 {
		return
	}
	for _, op := range []MathUnaryOp{MathAbs, MathSqrt} {
		set.d.math.unaryFloat32[op] = func(a, o []float32, v memory.Bitmap) { simdMathFloat32UnaryDispatch(a, o, v, op) }
		set.d.math.unaryFloat64[op] = func(a, o []float64, v memory.Bitmap) { simdMathFloat64UnaryDispatch(a, o, v, op) }
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

// Separate mask dispatch from the established dense loops. Changing their cold
// validity branch also changes compiler block layout in the hot dense loops.
// Keep dense code stable while native nullable batches use the masked backend.
func simdMathFloat32UnaryDispatch(in, out []float32, validity memory.Bitmap, op MathUnaryOp) {
	if validity == nil {
		simdMathFloat32Unary(in, out, nil, op)
		return
	}
	if len(out) < len(in) {
		panic("kernel: math output too small")
	}
	simdMathFloat32UnaryMasked(in, out, validity, op)
}
func simdMathFloat64UnaryDispatch(in, out []float64, validity memory.Bitmap, op MathUnaryOp) {
	if validity == nil {
		simdMathFloat64Unary(in, out, nil, op)
		return
	}
	if len(out) < len(in) {
		panic("kernel: math output too small")
	}
	simdMathFloat64UnaryMasked(in, out, validity, op)
}

// simdMathFloat32UnaryMasked replaces inactive operands with positive zero
// before Abs/Sqrt. In particular, an invalid negative or signaling NaN input
// never reaches a floating operation. The all-invalid word path does not read
// input, and full valid words reuse the dense loop. Tails use a fixed stack
// buffer; no heap or row-sized scratch is allocated.
func simdMathFloat32UnaryMasked(in, out []float32, validity memory.Bitmap, op MathUnaryOp) {
	lanes := simd.Float32s{}.Len()
	weights := simd.LoadUint32s(sumWeights32[:])
	for base := 0; base < len(in); base += 64 {
		rows := min(64, len(in)-base)
		word := validity[base>>6] & memory.TailMask(rows)
		if word == 0 {
			clear(out[base : base+rows])
			continue
		}
		off := 0
		if word == memory.TailMask(rows) {
			// Keep the dense loop local: re-entering the dispatched entry point
			// per word adds substantial overhead to short nullable batches.
			for ; off+lanes <= rows; off += lanes {
				x := simd.LoadFloat32s(in[base+off:])
				if op == MathAbs {
					x.Abs().Store(out[base+off:])
				} else {
					x.Sqrt().Store(out[base+off:])
				}
			}
		} else {
			for ; off+lanes <= rows; off += lanes {
				selected := simd.BroadcastUint32s(uint32(word >> uint(off))).And(weights).NotEqual(simd.Uint32s{})
				x := simd.LoadFloat32s(in[base+off:]).Masked(selected)
				if op == MathAbs {
					x.Abs().Store(out[base+off:])
				} else {
					x.Sqrt().Store(out[base+off:])
				}
			}
		}
		if off < rows {
			// Portable partial loads currently overread at a page boundary on
			// amd64. Stage only the real tail in a fixed 64-byte stack buffer.
			// Integer bit copies keep conversion and arithmetic in SIMD,
			// avoiding the separate scalar floating tail evaluation.
			var tail [16]float32
			tailBits := unsafe.Slice((*uint32)(unsafe.Pointer(&tail[0])), len(tail))
			srcBits := unsafe.Slice((*uint32)(unsafe.Pointer(&in[base+off])), rows-off)
			dstBits := unsafe.Slice((*uint32)(unsafe.Pointer(&out[base+off])), rows-off)
			for i := range srcBits {
				tailBits[i] = srcBits[i]
			}
			selected := simd.BroadcastUint32s(uint32(word >> uint(off))).And(weights).NotEqual(simd.Uint32s{})
			x := simd.LoadFloat32s(tail[:]).Masked(selected)
			if op == MathAbs {
				x.Abs().Store(tail[:])
			} else {
				x.Sqrt().Store(tail[:])
			}
			for i := range dstBits {
				dstBits[i] = tailBits[i]
			}
		}
	}
}

// simdMathFloat64UnaryMasked replaces inactive operands with positive zero
// before Abs/Sqrt. In particular, an invalid negative or signaling NaN input
// never reaches a floating operation. The all-invalid word path does not read
// input, and full valid words reuse the dense loop. Tails use a fixed stack
// buffer; no heap or row-sized scratch is allocated.
func simdMathFloat64UnaryMasked(in, out []float64, validity memory.Bitmap, op MathUnaryOp) {
	lanes := simd.Float64s{}.Len()
	weights := simd.LoadUint64s(sumWeights64[:])
	for base := 0; base < len(in); base += 64 {
		rows := min(64, len(in)-base)
		word := validity[base>>6] & memory.TailMask(rows)
		if word == 0 {
			clear(out[base : base+rows])
			continue
		}
		off := 0
		if word == memory.TailMask(rows) {
			// Keep the dense loop local: re-entering the dispatched entry point
			// per word adds substantial overhead to short nullable batches.
			for ; off+lanes <= rows; off += lanes {
				x := simd.LoadFloat64s(in[base+off:])
				if op == MathAbs {
					x.Abs().Store(out[base+off:])
				} else {
					x.Sqrt().Store(out[base+off:])
				}
			}
		} else {
			for ; off+lanes <= rows; off += lanes {
				selected := simd.BroadcastUint64s(uint64(word >> uint(off))).And(weights).NotEqual(simd.Uint64s{})
				x := simd.LoadFloat64s(in[base+off:]).Masked(selected)
				if op == MathAbs {
					x.Abs().Store(out[base+off:])
				} else {
					x.Sqrt().Store(out[base+off:])
				}
			}
		}
		if off < rows {
			// Portable partial loads currently overread at a page boundary on
			// amd64. Stage only the real tail in a fixed 64-byte stack buffer.
			// Integer bit copies keep conversion and arithmetic in SIMD,
			// avoiding the separate scalar floating tail evaluation.
			var tail [8]float64
			tailBits := unsafe.Slice((*uint64)(unsafe.Pointer(&tail[0])), len(tail))
			srcBits := unsafe.Slice((*uint64)(unsafe.Pointer(&in[base+off])), rows-off)
			dstBits := unsafe.Slice((*uint64)(unsafe.Pointer(&out[base+off])), rows-off)
			for i := range srcBits {
				tailBits[i] = srcBits[i]
			}
			selected := simd.BroadcastUint64s(uint64(word >> uint(off))).And(weights).NotEqual(simd.Uint64s{})
			x := simd.LoadFloat64s(tail[:]).Masked(selected)
			if op == MathAbs {
				x.Abs().Store(tail[:])
			} else {
				x.Sqrt().Store(tail[:])
			}
			for i := range dstBits {
				dstBits[i] = tailBits[i]
			}
		}
	}
}
