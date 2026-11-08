//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"simd"
	"testing"
)

// Invoke portable kernels directly so amd64 overrides cannot hide regressions.
func TestPortableNarrowLiteralComparisons(t *testing.T) {
	if !simd.Emulated() && !expectedNativeSIMD() {
		t.Skip("experimental native lowering requires supported ISA")
	}
	t.Run("int16", func(t *testing.T) {
		funcs := [...]func([]int16, memory.Bitmap, memory.Bitmap, int16) int{nil, simdInt16GT, simdInt16GE, simdInt16LT, simdInt16LE, simdInt16EQ}
		checkNumericLiterals(t, []int16{-32768, -32767, -1, 0, 1, 32766, 32767}, func(Set) literalCompare[int16] {
			return func(v []int16, valid, out memory.Bitmap, lit int16, op CompareOp) int {
				return funcs[op](v, valid, out, lit)
			}
		})
	})
	t.Run("uint8", func(t *testing.T) {
		funcs := [...]func([]uint8, memory.Bitmap, memory.Bitmap, uint8) int{nil, simdUint8GT, simdUint8GE, simdUint8LT, simdUint8LE, simdUint8EQ}
		checkNumericLiterals(t, []uint8{0, 1, 127, 128, 254, 255}, func(Set) literalCompare[uint8] {
			return func(v []uint8, valid, out memory.Bitmap, lit uint8, op CompareOp) int {
				return funcs[op](v, valid, out, lit)
			}
		})
	})
	t.Run("uint16", func(t *testing.T) {
		funcs := [...]func([]uint16, memory.Bitmap, memory.Bitmap, uint16) int{nil, simdUint16GT, simdUint16GE, simdUint16LT, simdUint16LE, simdUint16EQ}
		checkNumericLiterals(t, []uint16{0, 1, 32767, 32768, 65534, 65535}, func(Set) literalCompare[uint16] {
			return func(v []uint16, valid, out memory.Bitmap, lit uint16, op CompareOp) int {
				return funcs[op](v, valid, out, lit)
			}
		})
	})
}
