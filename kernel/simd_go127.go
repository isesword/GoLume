//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"math/bits"
	"simd"
	"simd/archsimd"
)

func newSIMDInt64GT(mode Mode) (Int64GTFunc, Capabilities, bool) {
	caps := Capabilities{
		Mode:         mode,
		Arch:         runtimeArch(),
		HardwareSIMD: !simd.Emulated(),
		Emulated:     simd.Emulated(),
		VectorBits:   simd.Int64s{}.Len() * 64,
		Backend:      "portable-simd",
	}
	if simd.Emulated() && mode == ModeAuto {
		return nil, caps, false
	}
	if simd.Emulated() && mode == ModeSIMD {
		// Forced SIMD remains available for diagnostics, but callers can see Emulated=true.
		return simdInt64GT, caps, true
	}
	return simdInt64GT, caps, true
}

func mask64Bits(m simd.Mask64s) uint64 {
	switch v := m.ToArch().(type) {
	case archsimd.Mask64x2:
		return uint64(v.ToBits())
	case archsimd.Mask64x4:
		return uint64(v.ToBits())
	case archsimd.Mask64x8:
		return uint64(v.ToBits())
	default:
		lanes := simd.Int64s{}.Len()
		var tmp [8]int64
		m.ToInt64s().Store(tmp[:lanes])
		var z uint64
		for i := 0; i < lanes; i++ {
			if tmp[i] != 0 {
				z |= uint64(1) << uint(i)
			}
		}
		return z
	}
}

func simdInt64GT(values []int64, validity memory.Bitmap, out memory.Bitmap, threshold int64) int {
	rows := len(values)
	words := memory.WordsFor(rows)
	if len(out) < words {
		panic("kernel: output bitmap too small")
	}
	lanes := simd.Int64s{}.Len()
	if lanes <= 0 || lanes > 64 {
		return scalarInt64GT(values, validity, out, threshold)
	}
	laneMask := uint64(1<<uint(lanes)) - 1
	tv := simd.BroadcastInt64s(threshold)
	selected := 0

	for wi := 0; wi < words; wi++ {
		base := wi << 6
		wordRows := 64
		if remain := rows - base; remain < wordRows {
			wordRows = remain
		}
		valid := ^uint64(0)
		if validity != nil {
			valid = validity[wi]
		}
		if wordRows < 64 {
			valid &= memory.TailMask(wordRows)
		}
		var sel uint64
		off := 0
		for ; off+lanes <= wordRows; off += lanes {
			cmp := mask64Bits(simd.LoadInt64s(values[base+off:]).Greater(tv))
			sel |= (cmp & ((valid >> uint(off)) & laneMask)) << uint(off)
		}
		for ; off < wordRows; off++ {
			if valid&(uint64(1)<<uint(off)) != 0 && values[base+off] > threshold {
				sel |= uint64(1) << uint(off)
			}
		}
		out[wi] = sel
		selected += bits.OnesCount64(sel)
	}
	return selected
}
