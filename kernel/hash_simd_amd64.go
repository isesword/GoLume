//go:build goexperiment.simd && amd64

package kernel

import (
	"gocolumn/internal/hashmix"
	"gocolumn/memory"
	"math/bits"
	"simd/archsimd"
)

func installArchSIMDHash(set *Set, mode Mode, vectorBits int) {
	if mode != ModeSIMD || vectorBits < 512 || !archsimd.X86.AVX512() {
		return
	}
	set.d.hash64 = hash64BatchX8
	set.setCapability(KernelHash64Batch, KernelCapability{HardwareSIMD: true, VectorBits: 512, Backend: "archsimd-avx512-hash"})
}

func installArchSIMDCompact(set *Set, mode Mode, vectorBits int) {
	if mode != ModeSIMD || vectorBits < 512 || !archsimd.X86.AVX512() {
		return
	}
	set.d.compactInt32 = compactInt32X16
	set.d.compactInt64 = compactInt64X8
	set.d.compactUint32 = compactUint32X16
	set.d.compactUint64 = compactUint64X8
	set.d.compactFloat64 = compactFloat64X8
	capability := KernelCapability{HardwareSIMD: true, VectorBits: 512, Backend: "archsimd-avx512-compress"}
	for _, id := range []KernelID{KernelCompactInt32, KernelCompactInt64, KernelCompactUint32, KernelCompactUint64, KernelCompactFloat64} {
		set.setCapability(id, capability)
	}
}

func selectionBits(selection memory.Bitmap, row, lanes int) uint64 {
	if selection == nil {
		return memory.TailMask(lanes)
	}
	return (selection[row/64] >> uint(row%64)) & memory.TailMask(lanes)
}

func compactInt32X16(values []int32, selection memory.Bitmap, out []int32) int {
	written, row := 0, 0
	for ; row+16 <= len(values); row += 16 {
		maskBits := uint16(selectionBits(selection, row, 16))
		count := bits.OnesCount16(maskBits)
		compressed := archsimd.LoadInt32x16(values[row:]).Compress(archsimd.Mask32x16FromBits(maskBits))
		if len(out)-written >= 16 {
			compressed.Store(out[written:])
		} else {
			var tmp [16]int32
			compressed.Store(tmp[:])
			copy(out[written:], tmp[:count])
		}
		written += count
	}
	return written + scalarCompactFrom(values, selection, out[written:], row)
}

func compactUint32X16(values []uint32, selection memory.Bitmap, out []uint32) int {
	written, row := 0, 0
	for ; row+16 <= len(values); row += 16 {
		maskBits := uint16(selectionBits(selection, row, 16))
		count := bits.OnesCount16(maskBits)
		compressed := archsimd.LoadUint32x16(values[row:]).Compress(archsimd.Mask32x16FromBits(maskBits))
		if len(out)-written >= 16 {
			compressed.Store(out[written:])
		} else {
			var tmp [16]uint32
			compressed.Store(tmp[:])
			copy(out[written:], tmp[:count])
		}
		written += count
	}
	return written + scalarCompactFrom(values, selection, out[written:], row)
}

func compactInt64X8(values []int64, selection memory.Bitmap, out []int64) int {
	written, row := 0, 0
	for ; row+8 <= len(values); row += 8 {
		maskBits := uint8(selectionBits(selection, row, 8))
		count := bits.OnesCount8(maskBits)
		compressed := archsimd.LoadInt64x8(values[row:]).Compress(archsimd.Mask64x8FromBits(maskBits))
		if len(out)-written >= 8 {
			compressed.Store(out[written:])
		} else {
			var tmp [8]int64
			compressed.Store(tmp[:])
			copy(out[written:], tmp[:count])
		}
		written += count
	}
	return written + scalarCompactFrom(values, selection, out[written:], row)
}

func compactUint64X8(values []uint64, selection memory.Bitmap, out []uint64) int {
	written, row := 0, 0
	for ; row+8 <= len(values); row += 8 {
		maskBits := uint8(selectionBits(selection, row, 8))
		count := bits.OnesCount8(maskBits)
		compressed := archsimd.LoadUint64x8(values[row:]).Compress(archsimd.Mask64x8FromBits(maskBits))
		if len(out)-written >= 8 {
			compressed.Store(out[written:])
		} else {
			var tmp [8]uint64
			compressed.Store(tmp[:])
			copy(out[written:], tmp[:count])
		}
		written += count
	}
	return written + scalarCompactFrom(values, selection, out[written:], row)
}

func compactFloat64X8(values []float64, selection memory.Bitmap, out []float64) int {
	written, row := 0, 0
	for ; row+8 <= len(values); row += 8 {
		maskBits := uint8(selectionBits(selection, row, 8))
		count := bits.OnesCount8(maskBits)
		compressed := archsimd.LoadFloat64x8(values[row:]).Compress(archsimd.Mask64x8FromBits(maskBits))
		if len(out)-written >= 8 {
			compressed.Store(out[written:])
		} else {
			var tmp [8]float64
			compressed.Store(tmp[:])
			copy(out[written:], tmp[:count])
		}
		written += count
	}
	return written + scalarCompactFrom(values, selection, out[written:], row)
}

func hash64BatchX8(keys, out []uint64) {
	if len(out) < len(keys) {
		panic("kernel: hash output too small")
	}
	add := archsimd.BroadcastUint64x8(hashmix.Gamma)
	mul1 := archsimd.BroadcastUint64x8(hashmix.Mul1)
	mul2 := archsimd.BroadcastUint64x8(hashmix.Mul2)
	i := 0
	for ; i+8 <= len(keys); i += 8 {
		x := archsimd.LoadUint64x8(keys[i:]).Add(add)
		x = x.Xor(x.ShiftAllRight(30)).Mul(mul1)
		x = x.Xor(x.ShiftAllRight(27)).Mul(mul2)
		x.Xor(x.ShiftAllRight(31)).Store(out[i:])
	}
	scalarHash64Batch(keys[i:], out[i:])
}
