package kernel

import "gocolumn/memory"

// Native-width comparison entry points use the scalar reference backend until
// each architecture has an independent performance gate. Keeping them on Set
// prevents physical operators from bypassing dispatch while storage narrows.

// CompareInt16 compares nullable int16 values and writes a packed selection bitmap.
func (s Set) CompareInt16(v []int16, valid, out memory.Bitmap, lit int16, op CompareOp) int {
	return nativeCompare(v, valid, out, lit, op)
}

// CompareUint8 compares nullable uint8 values and writes a packed selection bitmap.
func (s Set) CompareUint8(v []uint8, valid, out memory.Bitmap, lit uint8, op CompareOp) int {
	return nativeCompare(v, valid, out, lit, op)
}

// CompareUint16 compares nullable uint16 values and writes a packed selection bitmap.
func (s Set) CompareUint16(v []uint16, valid, out memory.Bitmap, lit uint16, op CompareOp) int {
	return nativeCompare(v, valid, out, lit, op)
}

func nativeCompare[T ~int16 | ~uint8 | ~uint16](values []T, valid, out memory.Bitmap, lit T, op CompareOp) int {
	prepareComparison(len(values), out)
	selected := 0
	for row, value := range values {
		if !validAt(valid, row) {
			continue
		}
		match := false
		switch op {
		case CompareGT:
			match = value > lit
		case CompareGE:
			match = value >= lit
		case CompareLT:
			match = value < lit
		case CompareLE:
			match = value <= lit
		case CompareEQ:
			match = value == lit
		}
		if match {
			setSelected(out, row, &selected)
		}
	}
	return selected
}
