package kernel

import (
	"gocolumn/memory"
	"math/bits"
)

func scalarInt64GT(values []int64, validity memory.Bitmap, out memory.Bitmap, threshold int64) int {
	rows := len(values)
	words := memory.WordsFor(rows)
	if len(out) < words {
		panic("kernel: output bitmap too small")
	}
	selected := 0
	for wi := 0; wi < words; wi++ {
		base := wi << 6
		limit := base + 64
		if limit > rows {
			limit = rows
		}
		valid := ^uint64(0)
		if validity != nil {
			valid = validity[wi]
		}
		if wi == words-1 {
			valid &= memory.TailMask(rows)
		}
		var sel uint64
		for i := base; i < limit; i++ {
			bit := uint(i - base)
			if valid&(uint64(1)<<bit) != 0 && values[i] > threshold {
				sel |= uint64(1) << bit
			}
		}
		out[wi] = sel
		selected += bits.OnesCount64(sel)
	}
	return selected
}
