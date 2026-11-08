//go:build goexperiment.simd && amd64

package kernel

import (
	"gocolumn/memory"
	"simd"
	"simd/archsimd"
)

func installArchSumSIMD(set *Set, mode Mode, vectorBits int) {
	if mode == ModeScalar || !nativeSIMDAvailable() || vectorBits != 512 || !archsimd.X86.AVX512() {
		return
	}
	set.d.sumInt32Masked = sumInt32MaskedAVX512
	set.d.sumUint32Masked = sumUint32MaskedAVX512
	set.d.sumInt64Masked = sumInt64MaskedAVX512
	set.d.sumUint64Masked = sumUint64MaskedAVX512
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
		id, _ := SumID(typ)
		set.setCapability(id, KernelCapability{HardwareSIMD: true, VectorBits: 512, Backend: "archsimd-avx512-sum"})
	}
}

// finishSumAVX is a scalar-only boundary between native SIMD reduction and
// baseline Go callers, which may immediately copy aggregate state using SSE.
// A direct ClearAVXUpperBits in the reduction can be scheduled before pure
// vector extraction by the compiler. The non-inlined scalar argument forces
// extraction to complete before this boundary clears upper lanes. Callers are
// installed only behind nativeSIMDAvailable's AVX2 guard.
//
//go:noinline
func finishSumAVX(value uint64) uint64 {
	archsimd.ClearAVXUpperBits()
	return value
}

func reduceSumInt32(x simd.Int32s) int32 {
	var sum int32
	// ToArch is lowered at compile time for the selected portable vector width.
	switch a := x.ToArch().(type) {
	case archsimd.Int32x16:
		b := a.GetLo().Add(a.GetHi())
		c := b.GetLo().Add(b.GetHi())
		c = c.ConcatAddPairs(c)
		c = c.ConcatAddPairs(c)
		sum = c.GetElem(0)
	case archsimd.Int32x8:
		c := a.GetLo().Add(a.GetHi())
		c = c.ConcatAddPairs(c)
		c = c.ConcatAddPairs(c)
		sum = c.GetElem(0)
	case archsimd.Int32x4:
		a = a.ConcatAddPairs(a)
		a = a.ConcatAddPairs(a)
		sum = a.GetElem(0)
	default:
		panic("kernel: unsupported native SIMD sum width")
	}
	return int32(finishSumAVX(uint64(sum)))
}

func sumInt32MaskedAVX512(v []int32, mask uint64) int32 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	if len(v) < 16 {
		return scalarSumMasked(v, mask)
	}
	var acc archsimd.Int32x16
	off := 0
	if mask == memory.TailMask(len(v)) {
		for ; off+16 <= len(v); off += 16 {
			acc = acc.Add(archsimd.LoadInt32x16(v[off:]))
		}
	} else {
		for ; off+16 <= len(v); off += 16 {
			acc = acc.Add(archsimd.LoadInt32x16(v[off:]).Masked(archsimd.Mask32x16FromBits(uint16(mask >> uint(off)))))
		}
	}
	sum := reduceSumInt32(simd.Int32sFromArch(acc))
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += v[off]
		}
	}
	return sum
}

func reduceSumUint32(x simd.Uint32s) uint32 {
	var sum uint32
	// ToArch is lowered at compile time for the selected portable vector width.
	switch a := x.ToArch().(type) {
	case archsimd.Uint32x16:
		b := a.GetLo().Add(a.GetHi())
		c := b.GetLo().Add(b.GetHi())
		c = c.ConcatAddPairs(c)
		c = c.ConcatAddPairs(c)
		sum = c.GetElem(0)
	case archsimd.Uint32x8:
		c := a.GetLo().Add(a.GetHi())
		c = c.ConcatAddPairs(c)
		c = c.ConcatAddPairs(c)
		sum = c.GetElem(0)
	case archsimd.Uint32x4:
		a = a.ConcatAddPairs(a)
		a = a.ConcatAddPairs(a)
		sum = a.GetElem(0)
	default:
		panic("kernel: unsupported native SIMD sum width")
	}
	return uint32(finishSumAVX(uint64(sum)))
}

func sumUint32MaskedAVX512(v []uint32, mask uint64) uint32 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	if len(v) < 16 {
		return scalarSumMasked(v, mask)
	}
	var acc archsimd.Uint32x16
	off := 0
	if mask == memory.TailMask(len(v)) {
		for ; off+16 <= len(v); off += 16 {
			acc = acc.Add(archsimd.LoadUint32x16(v[off:]))
		}
	} else {
		for ; off+16 <= len(v); off += 16 {
			acc = acc.Add(archsimd.LoadUint32x16(v[off:]).Masked(archsimd.Mask32x16FromBits(uint16(mask >> uint(off)))))
		}
	}
	sum := reduceSumUint32(simd.Uint32sFromArch(acc))
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += v[off]
		}
	}
	return sum
}

func reduceSumInt64(x simd.Int64s) int64 {
	var sum int64
	// ToArch is lowered at compile time for the selected portable vector width.
	switch a := x.ToArch().(type) {
	case archsimd.Int64x8:
		b := a.GetLo().Add(a.GetHi())
		c := b.GetLo().Add(b.GetHi())
		sum = c.GetElem(0) + c.GetElem(1)
	case archsimd.Int64x4:
		c := a.GetLo().Add(a.GetHi())
		sum = c.GetElem(0) + c.GetElem(1)
	case archsimd.Int64x2:
		sum = a.GetElem(0) + a.GetElem(1)
	default:
		panic("kernel: unsupported native SIMD sum width")
	}
	return int64(finishSumAVX(uint64(sum)))
}

func sumInt64MaskedAVX512(v []int64, mask uint64) int64 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	if len(v) < 8 {
		return scalarSumMasked(v, mask)
	}
	var acc archsimd.Int64x8
	off := 0
	if mask == memory.TailMask(len(v)) {
		for ; off+8 <= len(v); off += 8 {
			acc = acc.Add(archsimd.LoadInt64x8(v[off:]))
		}
	} else {
		for ; off+8 <= len(v); off += 8 {
			acc = acc.Add(archsimd.LoadInt64x8(v[off:]).Masked(archsimd.Mask64x8FromBits(uint8(mask >> uint(off)))))
		}
	}
	sum := reduceSumInt64(simd.Int64sFromArch(acc))
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += v[off]
		}
	}
	return sum
}

func reduceSumUint64(x simd.Uint64s) uint64 {
	var sum uint64
	// ToArch is lowered at compile time for the selected portable vector width.
	switch a := x.ToArch().(type) {
	case archsimd.Uint64x8:
		b := a.GetLo().Add(a.GetHi())
		c := b.GetLo().Add(b.GetHi())
		sum = c.GetElem(0) + c.GetElem(1)
	case archsimd.Uint64x4:
		c := a.GetLo().Add(a.GetHi())
		sum = c.GetElem(0) + c.GetElem(1)
	case archsimd.Uint64x2:
		sum = a.GetElem(0) + a.GetElem(1)
	default:
		panic("kernel: unsupported native SIMD sum width")
	}
	return uint64(finishSumAVX(uint64(sum)))
}

func sumUint64MaskedAVX512(v []uint64, mask uint64) uint64 {
	mask = checkedSumMask(len(v), mask)
	if mask == 0 {
		return 0
	}
	if len(v) < 8 {
		return scalarSumMasked(v, mask)
	}
	var acc archsimd.Uint64x8
	off := 0
	if mask == memory.TailMask(len(v)) {
		for ; off+8 <= len(v); off += 8 {
			acc = acc.Add(archsimd.LoadUint64x8(v[off:]))
		}
	} else {
		for ; off+8 <= len(v); off += 8 {
			acc = acc.Add(archsimd.LoadUint64x8(v[off:]).Masked(archsimd.Mask64x8FromBits(uint8(mask >> uint(off)))))
		}
	}
	sum := reduceSumUint64(simd.Uint64sFromArch(acc))
	for ; off < len(v); off++ {
		if mask&(uint64(1)<<uint(off)) != 0 {
			sum += v[off]
		}
	}
	return sum
}
