//go:build goexperiment.simd && amd64

package kernel

import (
	"gocolumn/memory"
	"math/bits"
	"simd/archsimd"
)

// installNarrowSIMDCompact is called only after the native AVX-512 and selected
// vector-width guards in installArchSIMDCompact. Byte/word compression needs
// VBMI2 in addition to the base AVX-512 feature bundle.
func installNarrowSIMDCompact(set *Set, hasVBMI2 bool) {
	set.d.compactFloat32 = compactFloat32X16
	set.setCapability(KernelCompactFloat32, KernelCapability{HardwareSIMD: true, VectorBits: 512, Backend: "archsimd-avx512-compress"})
	if !hasVBMI2 {
		return
	}
	set.d.compactInt8 = compactInt8X64
	set.d.compactUint8 = compactUint8X64
	set.d.compactInt16 = compactInt16X32
	set.d.compactUint16 = compactUint16X32
	capability := KernelCapability{HardwareSIMD: true, VectorBits: 512, Backend: "archsimd-avx512vbmi2-compress"}
	for _, id := range []KernelID{KernelCompactInt8, KernelCompactUint8, KernelCompactInt16, KernelCompactUint16} {
		set.setCapability(id, capability)
	}
}

func compactFloat32X16(values []float32, selection memory.Bitmap, out []float32) int {
	if selection == nil || len(values) < 16 {
		return scalarCompact(values, selection, out)
	}
	written, row := 0, 0
	for ; row+16 <= len(values); row += 16 {
		maskBits := uint16(selectionBits(selection, row, 16))
		if maskBits == 0 {
			continue
		}
		count := bits.OnesCount16(maskBits)
		if len(out)-written < count {
			panic("kernel: compact output too small")
		}
		compressed := archsimd.LoadFloat32x16(values[row:]).Compress(archsimd.Mask32x16FromBits(maskBits))
		// Store only selected lanes, including for exact-length outputs. This keeps
		// the unwritten suffix intact and permits forward in-place compaction.
		compressed.StorePart(out[written : written+count])
		written += count
	}
	archsimd.ClearAVXUpperBits()
	return written + scalarCompactFrom(values, selection, out[written:], row)
}

func compactInt8X64(values []int8, selection memory.Bitmap, out []int8) int {
	if selection == nil || len(values) < 64 {
		return scalarCompact(values, selection, out)
	}
	written, row := 0, 0
	for ; row+64 <= len(values); row += 64 {
		maskBits := uint64(selectionBits(selection, row, 64))
		if maskBits == 0 {
			continue
		}
		count := bits.OnesCount64(maskBits)
		if len(out)-written < count {
			panic("kernel: compact output too small")
		}
		compressed := archsimd.LoadInt8x64(values[row:]).Compress(archsimd.Mask8x64FromBits(maskBits))
		// Store only selected lanes, including for exact-length outputs. This keeps
		// the unwritten suffix intact and permits forward in-place compaction.
		compressed.StorePart(out[written : written+count])
		written += count
	}
	archsimd.ClearAVXUpperBits()
	return written + scalarCompactFrom(values, selection, out[written:], row)
}

func compactUint8X64(values []uint8, selection memory.Bitmap, out []uint8) int {
	if selection == nil || len(values) < 64 {
		return scalarCompact(values, selection, out)
	}
	written, row := 0, 0
	for ; row+64 <= len(values); row += 64 {
		maskBits := uint64(selectionBits(selection, row, 64))
		if maskBits == 0 {
			continue
		}
		count := bits.OnesCount64(maskBits)
		if len(out)-written < count {
			panic("kernel: compact output too small")
		}
		compressed := archsimd.LoadUint8x64(values[row:]).Compress(archsimd.Mask8x64FromBits(maskBits))
		// Store only selected lanes, including for exact-length outputs. This keeps
		// the unwritten suffix intact and permits forward in-place compaction.
		compressed.StorePart(out[written : written+count])
		written += count
	}
	archsimd.ClearAVXUpperBits()
	return written + scalarCompactFrom(values, selection, out[written:], row)
}

func compactInt16X32(values []int16, selection memory.Bitmap, out []int16) int {
	if selection == nil || len(values) < 32 {
		return scalarCompact(values, selection, out)
	}
	written, row := 0, 0
	for ; row+32 <= len(values); row += 32 {
		maskBits := uint32(selectionBits(selection, row, 32))
		if maskBits == 0 {
			continue
		}
		count := bits.OnesCount32(maskBits)
		if len(out)-written < count {
			panic("kernel: compact output too small")
		}
		compressed := archsimd.LoadInt16x32(values[row:]).Compress(archsimd.Mask16x32FromBits(maskBits))
		// Store only selected lanes, including for exact-length outputs. This keeps
		// the unwritten suffix intact and permits forward in-place compaction.
		compressed.StorePart(out[written : written+count])
		written += count
	}
	archsimd.ClearAVXUpperBits()
	return written + scalarCompactFrom(values, selection, out[written:], row)
}

func compactUint16X32(values []uint16, selection memory.Bitmap, out []uint16) int {
	if selection == nil || len(values) < 32 {
		return scalarCompact(values, selection, out)
	}
	written, row := 0, 0
	for ; row+32 <= len(values); row += 32 {
		maskBits := uint32(selectionBits(selection, row, 32))
		if maskBits == 0 {
			continue
		}
		count := bits.OnesCount32(maskBits)
		if len(out)-written < count {
			panic("kernel: compact output too small")
		}
		compressed := archsimd.LoadUint16x32(values[row:]).Compress(archsimd.Mask16x32FromBits(maskBits))
		// Store only selected lanes, including for exact-length outputs. This keeps
		// the unwritten suffix intact and permits forward in-place compaction.
		compressed.StorePart(out[written : written+count])
		written += count
	}
	archsimd.ClearAVXUpperBits()
	return written + scalarCompactFrom(values, selection, out[written:], row)
}
