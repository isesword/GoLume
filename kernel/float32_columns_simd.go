//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"math/bits"
	"simd"
)

func installFloat32ColumnSIMD(set *Set, mode Mode, vectorBits int) {
	if mode == ModeScalar || simd.Emulated() {
		return
	}
	set.d.compareFloat32Columns[CompareGT] = simdFloat32ColumnsGT
	set.d.compareFloat32Columns[CompareGE] = simdFloat32ColumnsGE
	set.d.compareFloat32Columns[CompareLT] = simdFloat32ColumnsLT
	set.d.compareFloat32Columns[CompareLE] = simdFloat32ColumnsLE
	set.d.compareFloat32Columns[CompareEQ] = simdFloat32ColumnsEQ
	for op := CompareGT; op <= CompareEQ; op++ {
		id, _ := ComparisonID(memory.TypeFloat32, op)
		set.d.caps.columns[id] = KernelCapability{HardwareSIMD: true, VectorBits: vectorBits, Backend: "portable-simd"}
	}
}

func simdFloat32Columns(left, right []float32, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: float32 column comparison length mismatch")
	}
	words := prepareComparison(len(left), out)
	lanes := simd.Float32s{}.Len()
	var laneMaskStorage [64]int32
	laneMask := laneMaskStorage[:lanes]
	selected := 0
	for wi := 0; wi < words; wi++ {
		base := wi * 64
		rows := min(64, len(left)-base)
		var word uint64
		off := 0
		for ; off+lanes <= rows; off += lanes {
			lv := simd.LoadFloat32s(left[base+off:])
			rv := simd.LoadFloat32s(right[base+off:])
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
			if validAt(leftValid, row) && validAt(rightValid, row) && compareFloat32(left[row], right[row], op) {
				word |= uint64(1) << uint(off)
			}
		}
		word &= memory.TailMask(rows)
		out[wi] = word
		selected += bits.OnesCount64(word)
	}
	return selected
}

func simdFloat32ColumnsGT(a, b []float32, av, bv, out memory.Bitmap) int {
	return simdFloat32Columns(a, b, av, bv, out, CompareGT)
}
func simdFloat32ColumnsGE(a, b []float32, av, bv, out memory.Bitmap) int {
	return simdFloat32Columns(a, b, av, bv, out, CompareGE)
}
func simdFloat32ColumnsLT(a, b []float32, av, bv, out memory.Bitmap) int {
	return simdFloat32Columns(a, b, av, bv, out, CompareLT)
}
func simdFloat32ColumnsLE(a, b []float32, av, bv, out memory.Bitmap) int {
	return simdFloat32Columns(a, b, av, bv, out, CompareLE)
}
func simdFloat32ColumnsEQ(a, b []float32, av, bv, out memory.Bitmap) int {
	return simdFloat32Columns(a, b, av, bv, out, CompareEQ)
}

func compareFloat32(a, b float32, op CompareOp) bool {
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
