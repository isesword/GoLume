package kernel

import (
	"bytes"
	"gocolumn/memory"
)

func installStringMatchScalar(d *dispatch) {
	d.stringMatch[StringEqual] = scalarStringEqual
	d.stringMatch[StringPrefix] = scalarStringPrefix
	d.stringMatch[StringSuffix] = scalarStringSuffix
	d.stringMatch[StringContains] = scalarStringContains
}

func scalarStringMatch(offsets []uint32, data []byte, validity, out memory.Bitmap, literal []byte, match func([]byte) bool) int {
	if len(offsets) == 0 {
		prepareComparison(0, out)
		return 0
	}
	rows := len(offsets) - 1
	prepareComparison(rows, out)
	selected := 0
	for row := 0; row < rows; row++ {
		if !validAt(validity, row) {
			continue
		}
		lo, hi := offsets[row], offsets[row+1]
		if lo > hi || int(hi) > len(data) {
			panic("kernel: invalid string offsets")
		}
		if match(data[lo:hi]) {
			setSelected(out, row, &selected)
		}
	}
	return selected
}

func scalarStringEqual(offsets []uint32, data []byte, valid, out memory.Bitmap, literal []byte) int {
	return scalarStringMatch(offsets, data, valid, out, literal, func(v []byte) bool { return bytes.Equal(v, literal) })
}
func scalarStringPrefix(offsets []uint32, data []byte, valid, out memory.Bitmap, literal []byte) int {
	return scalarStringMatch(offsets, data, valid, out, literal, func(v []byte) bool { return bytes.HasPrefix(v, literal) })
}
func scalarStringSuffix(offsets []uint32, data []byte, valid, out memory.Bitmap, literal []byte) int {
	return scalarStringMatch(offsets, data, valid, out, literal, func(v []byte) bool { return bytes.HasSuffix(v, literal) })
}
func scalarStringContains(offsets []uint32, data []byte, valid, out memory.Bitmap, literal []byte) int {
	return scalarStringMatch(offsets, data, valid, out, literal, func(v []byte) bool { return bytes.Contains(v, literal) })
}
