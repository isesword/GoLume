package kernel

import "gocolumn/memory"

// The native-width compaction entry points intentionally remain scalar until
// per-architecture measurements admit a SIMD variant. They are dispatched
// through Set so capability reporting and future admission use the same path
// as every other physical compact kernel.

// CompactInt8 compacts selected int8 values into out and returns the output length.
func (s Set) CompactInt8(values []int8, selection memory.Bitmap, out []int8) int {
	return s.d.compactInt8(values, selection, out)
}

// CompactInt16 compacts selected int16 values into out and returns the output length.
func (s Set) CompactInt16(values []int16, selection memory.Bitmap, out []int16) int {
	return s.d.compactInt16(values, selection, out)
}

// CompactUint8 compacts selected uint8 values into out and returns the output length.
func (s Set) CompactUint8(values []uint8, selection memory.Bitmap, out []uint8) int {
	return s.d.compactUint8(values, selection, out)
}

// CompactUint16 compacts selected uint16 values into out and returns the output length.
func (s Set) CompactUint16(values []uint16, selection memory.Bitmap, out []uint16) int {
	return s.d.compactUint16(values, selection, out)
}

// CompactFloat32 compacts selected float32 values into out and returns the output length.
func (s Set) CompactFloat32(values []float32, selection memory.Bitmap, out []float32) int {
	return s.d.compactFloat32(values, selection, out)
}
