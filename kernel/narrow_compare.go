package kernel

import "gocolumn/memory"

// Narrow literal comparisons use the same operation-specialized dispatch as
// every other numeric width. Implementations are generated from one type table.

// CompareInt16 compares nullable int16 values and writes a packed selection bitmap.
func (s Set) CompareInt16(v []int16, valid, out memory.Bitmap, lit int16, op CompareOp) int {
	return s.d.compareInt16[op](v, valid, out, lit)
}

// CompareUint8 compares nullable uint8 values and writes a packed selection bitmap.
func (s Set) CompareUint8(v []uint8, valid, out memory.Bitmap, lit uint8, op CompareOp) int {
	return s.d.compareUint8[op](v, valid, out, lit)
}

// CompareUint16 compares nullable uint16 values and writes a packed selection bitmap.
func (s Set) CompareUint16(v []uint16, valid, out memory.Bitmap, lit uint16, op CompareOp) int {
	return s.d.compareUint16[op](v, valid, out, lit)
}
