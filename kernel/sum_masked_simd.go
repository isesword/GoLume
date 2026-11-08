//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"simd"
)

func installSumSIMD(set *Set, mode Mode, vectorBits int) {
	if mode == ModeScalar || !nativeSIMDAvailable() {
		return
	}
	// The supported portable vector widths fit these fixed lane-weight tables.
	if vectorBits != 128 && vectorBits != 256 && vectorBits != 512 {
		return
	}
	installNarrowSumSIMD(set, vectorBits)
	set.d.sumInt32Masked = simdSumInt32Masked
	set.d.sumUint32Masked = simdSumUint32Masked
	set.d.sumInt64Masked = simdSumInt64Masked
	set.d.sumUint64Masked = simdSumUint64Masked
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
		id, _ := SumID(typ)
		set.setCapability(id, KernelCapability{HardwareSIMD: true, VectorBits: vectorBits, Backend: "portable-simd"})
	}
}

// Lane weights expand the packed selection using vector AND and comparison,
// without materializing a row-sized selection or value buffer.
var sumWeights32 = [16]uint32{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768}
var sumWeights64 = [8]uint64{1, 2, 4, 8, 16, 32, 64, 128}

func simdSumInt32Masked(v []int32, mask uint64) int32 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	lanes := simd.Int32s{}.Len()
	if len(v) < lanes {
		return scalarSumMasked(v, mask)
	}
	var acc simd.Int32s
	off := 0
	if mask == memory.TailMask(len(v)) {
		// Dense words avoid mask expansion altogether.
		for ; off+lanes <= len(v); off += lanes {
			acc = acc.Add(simd.LoadInt32s(v[off:]))
		}
	} else {
		weights := simd.LoadUint32s(sumWeights32[:])
		for ; off+lanes <= len(v); off += lanes {
			selected := simd.BroadcastUint32s(uint32(mask >> uint(off))).And(weights).NotEqual(simd.Uint32s{})
			acc = acc.Add(simd.LoadInt32s(v[off:]).Masked(selected))
		}
	}
	sum := reduceSumInt32(acc)
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += v[off]
		}
	}
	return sum
}

func simdSumUint32Masked(v []uint32, mask uint64) uint32 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	lanes := simd.Uint32s{}.Len()
	if len(v) < lanes {
		return scalarSumMasked(v, mask)
	}
	var acc simd.Uint32s
	off := 0
	if mask == memory.TailMask(len(v)) {
		// Dense words avoid mask expansion altogether.
		for ; off+lanes <= len(v); off += lanes {
			acc = acc.Add(simd.LoadUint32s(v[off:]))
		}
	} else {
		weights := simd.LoadUint32s(sumWeights32[:])
		for ; off+lanes <= len(v); off += lanes {
			selected := simd.BroadcastUint32s(uint32(mask >> uint(off))).And(weights).NotEqual(simd.Uint32s{})
			acc = acc.Add(simd.LoadUint32s(v[off:]).Masked(selected))
		}
	}
	sum := reduceSumUint32(acc)
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += v[off]
		}
	}
	return sum
}

func simdSumInt64Masked(v []int64, mask uint64) int64 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	lanes := simd.Int64s{}.Len()
	if len(v) < lanes {
		return scalarSumMasked(v, mask)
	}
	var acc simd.Int64s
	off := 0
	if mask == memory.TailMask(len(v)) {
		// Dense words avoid mask expansion altogether.
		for ; off+lanes <= len(v); off += lanes {
			acc = acc.Add(simd.LoadInt64s(v[off:]))
		}
	} else {
		weights := simd.LoadUint64s(sumWeights64[:])
		for ; off+lanes <= len(v); off += lanes {
			selected := simd.BroadcastUint64s(uint64(mask >> uint(off))).And(weights).NotEqual(simd.Uint64s{})
			acc = acc.Add(simd.LoadInt64s(v[off:]).Masked(selected))
		}
	}
	sum := reduceSumInt64(acc)
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += v[off]
		}
	}
	return sum
}

func simdSumUint64Masked(v []uint64, mask uint64) uint64 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	lanes := simd.Uint64s{}.Len()
	if len(v) < lanes {
		return scalarSumMasked(v, mask)
	}
	var acc simd.Uint64s
	off := 0
	if mask == memory.TailMask(len(v)) {
		// Dense words avoid mask expansion altogether.
		for ; off+lanes <= len(v); off += lanes {
			acc = acc.Add(simd.LoadUint64s(v[off:]))
		}
	} else {
		weights := simd.LoadUint64s(sumWeights64[:])
		for ; off+lanes <= len(v); off += lanes {
			selected := simd.BroadcastUint64s(uint64(mask >> uint(off))).And(weights).NotEqual(simd.Uint64s{})
			acc = acc.Add(simd.LoadUint64s(v[off:]).Masked(selected))
		}
	}
	sum := reduceSumUint64(acc)
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += v[off]
		}
	}
	return sum
}
