//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"math/bits"
	"simd"
)

func installInt32ColumnSIMD(set *Set, mode Mode, vectorBits int) {
	if mode != ModeSIMD || simd.Emulated() {
		return
	}
	_ = vectorBits
	set.d.compareInt32Columns[CompareGT] = simdInt32ColumnsGT
	set.d.compareInt32Columns[CompareGE] = simdInt32ColumnsGE
	set.d.compareInt32Columns[CompareLT] = simdInt32ColumnsLT
	set.d.compareInt32Columns[CompareLE] = simdInt32ColumnsLE
	set.d.compareInt32Columns[CompareEQ] = simdInt32ColumnsEQ
}

func simdInt32Columns(left, right []int32, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: int32 column comparison length mismatch")
	}
	words := prepareComparison(len(left), out)
	lanes := simd.Int32s{}.Len()
	var laneMaskStorage [64]int32
	laneMask := laneMaskStorage[:lanes]
	selected := 0
	for wi := 0; wi < words; wi++ {
		base := wi * 64
		rows := min(64, len(left)-base)
		var word uint64
		off := 0
		for ; off+lanes <= rows; off += lanes {
			lv := simd.LoadInt32s(left[base+off:])
			rv := simd.LoadInt32s(right[base+off:])
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
				row := base + off + lane
				if uint32(laneMask[lane])>>31 != 0 && validAt(leftValid, row) && validAt(rightValid, row) {
					word |= uint64(1) << uint(off+lane)
				}
			}
		}
		for ; off < rows; off++ {
			row := base + off
			if validAt(leftValid, row) && validAt(rightValid, row) && scalarInt32CompareValue(left[row], right[row], op) {
				word |= uint64(1) << uint(off)
			}
		}
		word &= memory.TailMask(rows)
		out[wi] = word
		selected += bits.OnesCount64(word)
	}
	return selected
}

func scalarInt32CompareValue(a, b int32, op CompareOp) bool {
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

func simdInt32ColumnsGT(a, b []int32, av, bv, out memory.Bitmap) int {
	return simdInt32Columns(a, b, av, bv, out, CompareGT)
}
func simdInt32ColumnsGE(a, b []int32, av, bv, out memory.Bitmap) int {
	return simdInt32Columns(a, b, av, bv, out, CompareGE)
}
func simdInt32ColumnsLT(a, b []int32, av, bv, out memory.Bitmap) int {
	return simdInt32Columns(a, b, av, bv, out, CompareLT)
}
func simdInt32ColumnsLE(a, b []int32, av, bv, out memory.Bitmap) int {
	return simdInt32Columns(a, b, av, bv, out, CompareLE)
}
func simdInt32ColumnsEQ(a, b []int32, av, bv, out memory.Bitmap) int {
	return simdInt32Columns(a, b, av, bv, out, CompareEQ)
}
