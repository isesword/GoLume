//go:build goexperiment.simd && arm64

package kernel

import (
	"gocolumn/memory"
	"simd/archsimd"
	"unsafe"
)

// installNarrowSumSIMD is called only behind the native CPU and width guards.
func installNarrowSumSIMD(set *Set, vectorBits int) {
	switch vectorBits {
	case 128:
		set.d.sumInt8Masked = sumInt8MaskedNative128
		set.d.sumInt16Masked = sumInt16MaskedNative128
		set.d.sumUint8Masked = sumUint8MaskedNative128
		set.d.sumUint16Masked = sumUint16MaskedNative128
	default:
		return
	}
	backend := "archsimd-widen-sum"
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
		id, _ := SumID(typ)
		set.setCapability(id, KernelCapability{HardwareSIMD: true, VectorBits: vectorBits, Backend: backend})
	}
}

// sumInt8MaskedNative128 widens each input before addition. At most
// 64 inputs bound even the largest unsigned sum to 64*65535, fitting int32.
// Packed loads read exactly 4 elements; the loop bound guards the entire word.
func sumInt8MaskedNative128(v []int8, mask uint64) int64 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	if len(v) < 4 {
		return scalarNarrowSumMasked(v, mask)
	}
	var acc archsimd.Int32x4
	off := 0
	if mask == memory.TailMask(len(v)) {
		for ; off+4 <= len(v); off += 4 {
			acc = acc.Add((archsimd.Uint32x4{}).SetElem(0, *(*uint32)(unsafe.Pointer(&v[off]))).ReshapeToUint8s().BitsToInt8().ExtendLo8ToInt16().ExtendLo4ToInt32())
		}
	} else {
		weights := archsimd.LoadUint32x4(sumWeights32[:])
		for ; off+4 <= len(v); off += 4 {
			selected := archsimd.BroadcastUint32x4(uint32(mask >> uint(off))).And(weights).NotEqual(archsimd.Uint32x4{})
			acc = acc.Add((archsimd.Uint32x4{}).SetElem(0, *(*uint32)(unsafe.Pointer(&v[off]))).ReshapeToUint8s().BitsToInt8().ExtendLo8ToInt16().ExtendLo4ToInt32().Masked(selected))
		}
	}
	sum := int64(acc.ReduceSum())
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += int64(v[off])
		}
	}
	return sum
}

// sumInt16MaskedNative128 widens each input before addition. At most
// 64 inputs bound even the largest unsigned sum to 64*65535, fitting int32.
// Packed loads read exactly 4 elements; the loop bound guards the entire word.
func sumInt16MaskedNative128(v []int16, mask uint64) int64 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	if len(v) < 4 {
		return scalarNarrowSumMasked(v, mask)
	}
	var acc archsimd.Int32x4
	off := 0
	if mask == memory.TailMask(len(v)) {
		for ; off+4 <= len(v); off += 4 {
			acc = acc.Add((archsimd.Uint64x2{}).SetElem(0, *(*uint64)(unsafe.Pointer(&v[off]))).ReshapeToUint16s().BitsToInt16().ExtendLo4ToInt32())
		}
	} else {
		weights := archsimd.LoadUint32x4(sumWeights32[:])
		for ; off+4 <= len(v); off += 4 {
			selected := archsimd.BroadcastUint32x4(uint32(mask >> uint(off))).And(weights).NotEqual(archsimd.Uint32x4{})
			acc = acc.Add((archsimd.Uint64x2{}).SetElem(0, *(*uint64)(unsafe.Pointer(&v[off]))).ReshapeToUint16s().BitsToInt16().ExtendLo4ToInt32().Masked(selected))
		}
	}
	sum := int64(acc.ReduceSum())
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += int64(v[off])
		}
	}
	return sum
}

// sumUint8MaskedNative128 widens each input before addition. At most
// 64 inputs bound even the largest unsigned sum to 64*65535, fitting int32.
// Packed loads read exactly 4 elements; the loop bound guards the entire word.
func sumUint8MaskedNative128(v []uint8, mask uint64) int64 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	if len(v) < 4 {
		return scalarNarrowSumMasked(v, mask)
	}
	var acc archsimd.Uint32x4
	off := 0
	if mask == memory.TailMask(len(v)) {
		for ; off+4 <= len(v); off += 4 {
			acc = acc.Add((archsimd.Uint32x4{}).SetElem(0, *(*uint32)(unsafe.Pointer(&v[off]))).ReshapeToUint8s().ExtendLo8ToUint16().ExtendLo4ToUint32())
		}
	} else {
		weights := archsimd.LoadUint32x4(sumWeights32[:])
		for ; off+4 <= len(v); off += 4 {
			selected := archsimd.BroadcastUint32x4(uint32(mask >> uint(off))).And(weights).NotEqual(archsimd.Uint32x4{})
			acc = acc.Add((archsimd.Uint32x4{}).SetElem(0, *(*uint32)(unsafe.Pointer(&v[off]))).ReshapeToUint8s().ExtendLo8ToUint16().ExtendLo4ToUint32().Masked(selected))
		}
	}
	sum := int64(acc.ReduceSum())
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += int64(v[off])
		}
	}
	return sum
}

// sumUint16MaskedNative128 widens each input before addition. At most
// 64 inputs bound even the largest unsigned sum to 64*65535, fitting int32.
// Packed loads read exactly 4 elements; the loop bound guards the entire word.
func sumUint16MaskedNative128(v []uint16, mask uint64) int64 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	if len(v) < 4 {
		return scalarNarrowSumMasked(v, mask)
	}
	var acc archsimd.Uint32x4
	off := 0
	if mask == memory.TailMask(len(v)) {
		for ; off+4 <= len(v); off += 4 {
			acc = acc.Add((archsimd.Uint64x2{}).SetElem(0, *(*uint64)(unsafe.Pointer(&v[off]))).ReshapeToUint16s().ExtendLo4ToUint32())
		}
	} else {
		weights := archsimd.LoadUint32x4(sumWeights32[:])
		for ; off+4 <= len(v); off += 4 {
			selected := archsimd.BroadcastUint32x4(uint32(mask >> uint(off))).And(weights).NotEqual(archsimd.Uint32x4{})
			acc = acc.Add((archsimd.Uint64x2{}).SetElem(0, *(*uint64)(unsafe.Pointer(&v[off]))).ReshapeToUint16s().ExtendLo4ToUint32().Masked(selected))
		}
	}
	sum := int64(acc.ReduceSum())
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += int64(v[off])
		}
	}
	return sum
}
