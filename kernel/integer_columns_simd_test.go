//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"reflect"
	"simd"
	"testing"
)

func TestPortableIntegerColumnComparisons(t *testing.T) {
	if !simd.Emulated() && !expectedNativeSIMD() {
		t.Skip("experimental native lowering requires supported ISA")
	}
	t.Run("int8", func(t *testing.T) {
		funcs := [...]func([]int8, []int8, memory.Bitmap, memory.Bitmap, memory.Bitmap) int{nil, simdInt8ColumnsGT, simdInt8ColumnsGE, simdInt8ColumnsLT, simdInt8ColumnsLE, simdInt8ColumnsEQ}
		integerColumnCheck(t, []int8{-128, -127, -1, 0, 1, 126, 127}, []integerColumnRunner[int8]{{"portable", func(a, b []int8, av, bv, out memory.Bitmap, op CompareOp) int { return funcs[op](a, b, av, bv, out) }}})
	})
	t.Run("int16", func(t *testing.T) {
		funcs := [...]func([]int16, []int16, memory.Bitmap, memory.Bitmap, memory.Bitmap) int{nil, simdInt16ColumnsGT, simdInt16ColumnsGE, simdInt16ColumnsLT, simdInt16ColumnsLE, simdInt16ColumnsEQ}
		integerColumnCheck(t, []int16{-32768, -32767, -1, 0, 1, 32766, 32767}, []integerColumnRunner[int16]{{"portable", func(a, b []int16, av, bv, out memory.Bitmap, op CompareOp) int { return funcs[op](a, b, av, bv, out) }}})
	})
	t.Run("int64", func(t *testing.T) {
		funcs := [...]func([]int64, []int64, memory.Bitmap, memory.Bitmap, memory.Bitmap) int{nil, simdInt64ColumnsGT, simdInt64ColumnsGE, simdInt64ColumnsLT, simdInt64ColumnsLE, simdInt64ColumnsEQ}
		integerColumnCheck(t, []int64{-1 << 63, -1<<63 + 1, -1<<53 - 1, -1, 0, 1, 1<<53 + 1, 1<<63 - 2, 1<<63 - 1}, []integerColumnRunner[int64]{{"portable", func(a, b []int64, av, bv, out memory.Bitmap, op CompareOp) int { return funcs[op](a, b, av, bv, out) }}})
	})
	t.Run("uint8", func(t *testing.T) {
		funcs := [...]func([]uint8, []uint8, memory.Bitmap, memory.Bitmap, memory.Bitmap) int{nil, simdUint8ColumnsGT, simdUint8ColumnsGE, simdUint8ColumnsLT, simdUint8ColumnsLE, simdUint8ColumnsEQ}
		integerColumnCheck(t, []uint8{0, 1, 127, 128, 129, 254, 255}, []integerColumnRunner[uint8]{{"portable", func(a, b []uint8, av, bv, out memory.Bitmap, op CompareOp) int { return funcs[op](a, b, av, bv, out) }}})
	})
	t.Run("uint16", func(t *testing.T) {
		funcs := [...]func([]uint16, []uint16, memory.Bitmap, memory.Bitmap, memory.Bitmap) int{nil, simdUint16ColumnsGT, simdUint16ColumnsGE, simdUint16ColumnsLT, simdUint16ColumnsLE, simdUint16ColumnsEQ}
		integerColumnCheck(t, []uint16{0, 1, 32767, 32768, 32769, 65534, 65535}, []integerColumnRunner[uint16]{{"portable", func(a, b []uint16, av, bv, out memory.Bitmap, op CompareOp) int { return funcs[op](a, b, av, bv, out) }}})
	})
	t.Run("uint64", func(t *testing.T) {
		funcs := [...]func([]uint64, []uint64, memory.Bitmap, memory.Bitmap, memory.Bitmap) int{nil, simdUint64ColumnsGT, simdUint64ColumnsGE, simdUint64ColumnsLT, simdUint64ColumnsLE, simdUint64ColumnsEQ}
		integerColumnCheck(t, []uint64{0, 1, 1<<53 - 1, 1 << 53, 1<<53 + 1, 1<<63 - 1, 1 << 63, 1<<63 + 1, 1<<64 - 2, 1<<64 - 1}, []integerColumnRunner[uint64]{{"portable", func(a, b []uint64, av, bv, out memory.Bitmap, op CompareOp) int { return funcs[op](a, b, av, bv, out) }}})
	})
}
func TestIntegerColumnNativeDispatch(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		scalar := New(ModeScalar)
		native := mode != ModeScalar && expectedNativeSIMD()
		portable := newScalarSet(mode)
		width := simd.Int64s{}.Len() * 64
		installGeneratedPortableSIMD(&portable, mode, width)
		installGeneratedArchSIMD(&portable, mode, width)
		check := func(typ memory.DataType, op CompareOp, got, installed, fallback any) {
			t.Helper()
			want := fallback
			if native {
				want = installed
			}
			if reflect.ValueOf(got).Pointer() != reflect.ValueOf(want).Pointer() {
				t.Fatalf("%s/%s/%s wrong dispatch", mode, typ, op)
			}
			if native && reflect.ValueOf(got).Pointer() == reflect.ValueOf(fallback).Pointer() {
				t.Fatalf("%s/%s/%s scalar dispatched as native", mode, typ, op)
			}
			cap, ok := s.ColumnComparisonCapability(typ, op)
			backend := "scalar"
			bits := 0
			if native {
				backend = expectedComparisonBackend()
				bits = width
			}
			if !ok || cap.HardwareSIMD != native || cap.Emulated || cap.Backend != backend || cap.VectorBits != bits {
				t.Fatalf("%s/%s/%s cap=%+v ok=%t want=%s/%d", mode, typ, op, cap, ok, backend, bits)
			}
		}
		for op := CompareGT; op <= CompareEQ; op++ {
			check(memory.TypeInt8, op, s.d.compareInt8Columns[op], portable.d.compareInt8Columns[op], scalar.d.compareInt8Columns[op])
			check(memory.TypeInt16, op, s.d.compareInt16Columns[op], portable.d.compareInt16Columns[op], scalar.d.compareInt16Columns[op])
			check(memory.TypeInt64, op, s.d.compareInt64Columns[op], portable.d.compareInt64Columns[op], scalar.d.compareInt64Columns[op])
			check(memory.TypeUint8, op, s.d.compareUint8Columns[op], portable.d.compareUint8Columns[op], scalar.d.compareUint8Columns[op])
			check(memory.TypeUint16, op, s.d.compareUint16Columns[op], portable.d.compareUint16Columns[op], scalar.d.compareUint16Columns[op])
			check(memory.TypeUint64, op, s.d.compareUint64Columns[op], portable.d.compareUint64Columns[op], scalar.d.compareUint64Columns[op])
		}
	}
}
