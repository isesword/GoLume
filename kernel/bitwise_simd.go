//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"simd"
)

func installBitwiseSIMD(set *Set, mode Mode, vectorBits int) {
	if mode == ModeScalar || !nativeSIMDAvailable() {
		return
	}
	if vectorBits != 128 && vectorBits != 256 && vectorBits != 512 {
		return
	}
	set.d.bitwise.int32 = simdBitwiseInt32
	set.d.bitwise.uint32 = simdBitwiseUint32
	set.d.bitwise.int64 = simdBitwiseInt64
	set.d.bitwise.uint64 = simdBitwiseUint64
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
		for op := BitwiseAnd; op <= BitwiseNot; op++ {
			id, _ := BitwiseID(typ, op)
			set.d.caps.byID[id] = KernelCapability{HardwareSIMD: true, VectorBits: vectorBits, Backend: "portable-simd"}
		}
	}
}

// simdBitwiseInt32 never reads an all-null word. Mixed vectors use packed
// validity expanded to native masks; tails stay scalar and in bounds.
func simdBitwiseInt32(left, right, out []int32, validity memory.Bitmap, op BitwiseOp) {
	lanes := simd.Int32s{}.Len()
	if len(left) < lanes {
		scalarBitwise(left, right, out, validity, op)
		return
	}
	prepareBitwise(len(left), len(right), len(out), validity, op)
	weights := simd.LoadUint32s(sumWeights32[:])
	for base := 0; base < len(left); base += 64 {
		rows := min(64, len(left)-base)
		word := bitmapWord(validity, base>>6) & memory.TailMask(rows)
		if word == 0 {
			clear(out[base : base+rows])
			continue
		}
		off := 0
		for ; off+lanes <= rows; off += lanes {
			row := base + off
			x := simd.LoadInt32s(left[row:])
			switch op {
			case BitwiseAnd:
				x = x.And(simd.LoadInt32s(right[row:]))
			case BitwiseOr:
				x = x.Or(simd.LoadInt32s(right[row:]))
			case BitwiseXor:
				x = x.Xor(simd.LoadInt32s(right[row:]))
			case BitwiseNot:
				x = x.Not()
			}
			if word != memory.TailMask(rows) {
				selected := simd.BroadcastUint32s(uint32(word >> uint(off))).And(weights).NotEqual(simd.Uint32s{})
				x = x.Masked(selected)
			}
			x.Store(out[row:])
		}
		for ; off < rows; off++ {
			row := base + off
			if word&(uint64(1)<<uint(off)) == 0 {
				out[row] = 0
				continue
			}
			switch op {
			case BitwiseAnd:
				out[row] = left[row] & right[row]
			case BitwiseOr:
				out[row] = left[row] | right[row]
			case BitwiseXor:
				out[row] = left[row] ^ right[row]
			case BitwiseNot:
				out[row] = ^left[row]
			}
		}
	}
}

// simdBitwiseUint32 never reads an all-null word. Mixed vectors use packed
// validity expanded to native masks; tails stay scalar and in bounds.
func simdBitwiseUint32(left, right, out []uint32, validity memory.Bitmap, op BitwiseOp) {
	lanes := simd.Uint32s{}.Len()
	if len(left) < lanes {
		scalarBitwise(left, right, out, validity, op)
		return
	}
	prepareBitwise(len(left), len(right), len(out), validity, op)
	weights := simd.LoadUint32s(sumWeights32[:])
	for base := 0; base < len(left); base += 64 {
		rows := min(64, len(left)-base)
		word := bitmapWord(validity, base>>6) & memory.TailMask(rows)
		if word == 0 {
			clear(out[base : base+rows])
			continue
		}
		off := 0
		for ; off+lanes <= rows; off += lanes {
			row := base + off
			x := simd.LoadUint32s(left[row:])
			switch op {
			case BitwiseAnd:
				x = x.And(simd.LoadUint32s(right[row:]))
			case BitwiseOr:
				x = x.Or(simd.LoadUint32s(right[row:]))
			case BitwiseXor:
				x = x.Xor(simd.LoadUint32s(right[row:]))
			case BitwiseNot:
				x = x.Not()
			}
			if word != memory.TailMask(rows) {
				selected := simd.BroadcastUint32s(uint32(word >> uint(off))).And(weights).NotEqual(simd.Uint32s{})
				x = x.Masked(selected)
			}
			x.Store(out[row:])
		}
		for ; off < rows; off++ {
			row := base + off
			if word&(uint64(1)<<uint(off)) == 0 {
				out[row] = 0
				continue
			}
			switch op {
			case BitwiseAnd:
				out[row] = left[row] & right[row]
			case BitwiseOr:
				out[row] = left[row] | right[row]
			case BitwiseXor:
				out[row] = left[row] ^ right[row]
			case BitwiseNot:
				out[row] = ^left[row]
			}
		}
	}
}

// simdBitwiseInt64 never reads an all-null word. Mixed vectors use packed
// validity expanded to native masks; tails stay scalar and in bounds.
func simdBitwiseInt64(left, right, out []int64, validity memory.Bitmap, op BitwiseOp) {
	lanes := simd.Int64s{}.Len()
	if len(left) < lanes {
		scalarBitwise(left, right, out, validity, op)
		return
	}
	prepareBitwise(len(left), len(right), len(out), validity, op)
	weights := simd.LoadUint64s(sumWeights64[:])
	for base := 0; base < len(left); base += 64 {
		rows := min(64, len(left)-base)
		word := bitmapWord(validity, base>>6) & memory.TailMask(rows)
		if word == 0 {
			clear(out[base : base+rows])
			continue
		}
		off := 0
		for ; off+lanes <= rows; off += lanes {
			row := base + off
			x := simd.LoadInt64s(left[row:])
			switch op {
			case BitwiseAnd:
				x = x.And(simd.LoadInt64s(right[row:]))
			case BitwiseOr:
				x = x.Or(simd.LoadInt64s(right[row:]))
			case BitwiseXor:
				x = x.Xor(simd.LoadInt64s(right[row:]))
			case BitwiseNot:
				x = x.Not()
			}
			if word != memory.TailMask(rows) {
				selected := simd.BroadcastUint64s(uint64(word >> uint(off))).And(weights).NotEqual(simd.Uint64s{})
				x = x.Masked(selected)
			}
			x.Store(out[row:])
		}
		for ; off < rows; off++ {
			row := base + off
			if word&(uint64(1)<<uint(off)) == 0 {
				out[row] = 0
				continue
			}
			switch op {
			case BitwiseAnd:
				out[row] = left[row] & right[row]
			case BitwiseOr:
				out[row] = left[row] | right[row]
			case BitwiseXor:
				out[row] = left[row] ^ right[row]
			case BitwiseNot:
				out[row] = ^left[row]
			}
		}
	}
}

// simdBitwiseUint64 never reads an all-null word. Mixed vectors use packed
// validity expanded to native masks; tails stay scalar and in bounds.
func simdBitwiseUint64(left, right, out []uint64, validity memory.Bitmap, op BitwiseOp) {
	lanes := simd.Uint64s{}.Len()
	if len(left) < lanes {
		scalarBitwise(left, right, out, validity, op)
		return
	}
	prepareBitwise(len(left), len(right), len(out), validity, op)
	weights := simd.LoadUint64s(sumWeights64[:])
	for base := 0; base < len(left); base += 64 {
		rows := min(64, len(left)-base)
		word := bitmapWord(validity, base>>6) & memory.TailMask(rows)
		if word == 0 {
			clear(out[base : base+rows])
			continue
		}
		off := 0
		for ; off+lanes <= rows; off += lanes {
			row := base + off
			x := simd.LoadUint64s(left[row:])
			switch op {
			case BitwiseAnd:
				x = x.And(simd.LoadUint64s(right[row:]))
			case BitwiseOr:
				x = x.Or(simd.LoadUint64s(right[row:]))
			case BitwiseXor:
				x = x.Xor(simd.LoadUint64s(right[row:]))
			case BitwiseNot:
				x = x.Not()
			}
			if word != memory.TailMask(rows) {
				selected := simd.BroadcastUint64s(uint64(word >> uint(off))).And(weights).NotEqual(simd.Uint64s{})
				x = x.Masked(selected)
			}
			x.Store(out[row:])
		}
		for ; off < rows; off++ {
			row := base + off
			if word&(uint64(1)<<uint(off)) == 0 {
				out[row] = 0
				continue
			}
			switch op {
			case BitwiseAnd:
				out[row] = left[row] & right[row]
			case BitwiseOr:
				out[row] = left[row] | right[row]
			case BitwiseXor:
				out[row] = left[row] ^ right[row]
			case BitwiseNot:
				out[row] = ^left[row]
			}
		}
	}
}
