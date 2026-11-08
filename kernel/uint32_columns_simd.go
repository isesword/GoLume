//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"math/bits"
	"simd"
)

func installUint32ColumnSIMD(set *Set, mode Mode, vectorBits int) {
	if mode == ModeScalar || simd.Emulated() {
		return
	}
	set.d.compareUint32Columns[CompareGT] = simdUint32ColumnsGT
	set.d.compareUint32Columns[CompareGE] = simdUint32ColumnsGE
	set.d.compareUint32Columns[CompareLT] = simdUint32ColumnsLT
	set.d.compareUint32Columns[CompareLE] = simdUint32ColumnsLE
	set.d.compareUint32Columns[CompareEQ] = simdUint32ColumnsEQ
	for op := CompareGT; op <= CompareEQ; op++ {
		id, _ := ComparisonID(memory.TypeUint32, op)
		set.d.caps.columns[id] = KernelCapability{HardwareSIMD: true, VectorBits: vectorBits, Backend: "portable-simd"}
	}
}

func simdUint32Columns(left, right []uint32, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: uint32 column comparison length mismatch")
	}
	words := prepareComparison(len(left), out)
	lanes := simd.Uint32s{}.Len()
	var laneMaskStorage [64]int32
	laneMask := laneMaskStorage[:lanes]
	selected := 0
	for wi := 0; wi < words; wi++ {
		base := wi * 64
		rows := min(64, len(left)-base)
		validWord := bitmapWord(leftValid, wi) & bitmapWord(rightValid, wi) & memory.TailMask(rows)
		// prepareComparison cleared the logical words, including skipped words.
		if validWord == 0 {
			continue
		}
		var word uint64
		off := 0
		for ; off+lanes <= rows; off += lanes {
			lv := simd.LoadUint32s(left[base+off:])
			rv := simd.LoadUint32s(right[base+off:])
			var mask simd.Mask32s
			switch op {
			case CompareGT:
				mask = lv.Greater(rv)
			case CompareGE:
				mask = lv.GreaterEqual(rv)
			case CompareLT:
				mask = lv.Less(rv)
			case CompareLE:
				mask = lv.LessEqual(rv)
			case CompareEQ:
				mask = lv.Equal(rv)
			}
			mask.ToInt32s().Store(laneMask)
			for lane := 0; lane < lanes; lane++ {
				if uint32(laneMask[lane])>>31 != 0 {
					word |= uint64(1) << uint(off+lane)
				}
			}
		}
		for ; off < rows; off++ {
			row := base + off
			if scalarUint32CompareValue(left[row], right[row], op) {
				word |= uint64(1) << uint(off)
			}
		}
		word &= validWord
		out[wi] = word
		selected += bits.OnesCount64(word)
	}
	return selected
}

func scalarUint32CompareValue(a, b uint32, op CompareOp) bool {
	switch op {
	case CompareGT:
		return a > b
	case CompareGE:
		return a >= b
	case CompareLT:
		return a < b
	case CompareLE:
		return a <= b
	case CompareEQ:
		return a == b
	default:
		return false
	}
}

func simdUint32ColumnsGT(a, b []uint32, av, bv, out memory.Bitmap) int {
	return simdUint32Columns(a, b, av, bv, out, CompareGT)
}
func simdUint32ColumnsGE(a, b []uint32, av, bv, out memory.Bitmap) int {
	return simdUint32Columns(a, b, av, bv, out, CompareGE)
}
func simdUint32ColumnsLT(a, b []uint32, av, bv, out memory.Bitmap) int {
	return simdUint32Columns(a, b, av, bv, out, CompareLT)
}
func simdUint32ColumnsLE(a, b []uint32, av, bv, out memory.Bitmap) int {
	return simdUint32Columns(a, b, av, bv, out, CompareLE)
}
func simdUint32ColumnsEQ(a, b []uint32, av, bv, out memory.Bitmap) int {
	return simdUint32Columns(a, b, av, bv, out, CompareEQ)
}
