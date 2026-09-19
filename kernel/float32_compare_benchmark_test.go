package kernel

import (
	"gocolumn/memory"
	"testing"
)

func BenchmarkFloat32Compare(b *testing.B) {
	values := make([]float32, 1<<20)
	valid := make(memory.Bitmap, (len(values)+63)/64)
	for i := range values {
		values[i] = float32(i%97) - 48
		valid[i/64] |= uint64(1) << uint(i%64)
	}
	out := make(memory.Bitmap, len(valid))
	for _, tc := range []struct {
		name string
		mode Mode
	}{
		{"scalar", ModeScalar},
		{"auto", ModeAuto},
		{"simd", ModeSIMD},
	} {
		b.Run(tc.name, func(b *testing.B) {
			set := New(tc.mode)
			b.SetBytes(int64(len(values) * 4))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.CompareFloat32(values, valid, out, 0, CompareGT)
			}
		})
	}
}

func BenchmarkFloat32Arithmetic(b *testing.B) {
	a := make([]float32, 1<<20)
	c := make([]float32, 1<<20)
	out := make([]float32, 1<<20)
	valid := make(memory.Bitmap, (len(a)+63)/64)
	for i := range a {
		a[i] = float32(i%97) - 48
		c[i] = float32(i%13) + 1
		valid[i/64] |= uint64(1) << uint(i%64)
	}
	for _, tc := range []struct {
		name string
		mode Mode
	}{
		{"scalar", ModeScalar},
		{"auto", ModeAuto},
		{"simd", ModeSIMD},
	} {
		b.Run(tc.name, func(b *testing.B) {
			set := New(tc.mode)
			b.SetBytes(int64(len(a) * 4))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.Float32Arithmetic(a, c, out, valid, ArithmeticMul)
			}
		})
	}
}

func BenchmarkFloat32ColumnCompare(b *testing.B) {
	left := make([]float32, 1<<20)
	right := make([]float32, 1<<20)
	valid := make(memory.Bitmap, (len(left)+63)/64)
	for i := range left {
		left[i] = float32(i % 97)
		right[i] = float32(i % 89)
		valid[i/64] |= uint64(1) << uint(i%64)
	}
	out := make(memory.Bitmap, len(valid))
	for _, tc := range []struct {
		name string
		mode Mode
	}{
		{"scalar", ModeScalar},
		{"auto", ModeAuto},
		{"simd", ModeSIMD},
	} {
		b.Run(tc.name, func(b *testing.B) {
			set := New(tc.mode)
			b.SetBytes(int64(len(left) * 8))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.CompareFloat32Columns(left, right, valid, valid, out, CompareGT)
			}
		})
	}
}

func BenchmarkInt32ColumnCompare(b *testing.B) {
	left := make([]int32, 1<<20)
	right := make([]int32, 1<<20)
	valid := make(memory.Bitmap, (len(left)+63)/64)
	for i := range left {
		left[i] = int32(i % 97)
		right[i] = int32(i % 89)
		valid[i/64] |= uint64(1) << uint(i%64)
	}
	out := make(memory.Bitmap, len(valid))
	for _, tc := range []struct {
		name string
		mode Mode
	}{
		{"scalar", ModeScalar},
		{"auto", ModeAuto},
		{"simd", ModeSIMD},
	} {
		b.Run(tc.name, func(b *testing.B) {
			set := New(tc.mode)
			b.SetBytes(int64(len(left) * 8))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.CompareInt32Columns(left, right, valid, valid, out, CompareGT)
			}
		})
	}
}

func BenchmarkUint32ColumnCompare(b *testing.B) {
	left := make([]uint32, 1<<20)
	right := make([]uint32, 1<<20)
	valid := make(memory.Bitmap, (len(left)+63)/64)
	for i := range left {
		left[i] = uint32(i % 97)
		right[i] = uint32(i % 89)
		valid[i/64] |= uint64(1) << uint(i%64)
	}
	out := make(memory.Bitmap, len(valid))
	for _, tc := range []struct {
		name string
		mode Mode
	}{
		{"scalar", ModeScalar},
		{"auto", ModeAuto},
		{"simd", ModeSIMD},
	} {
		b.Run(tc.name, func(b *testing.B) {
			set := New(tc.mode)
			b.SetBytes(int64(len(left) * 8))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.CompareUint32Columns(left, right, valid, valid, out, CompareGT)
			}
		})
	}
}

func BenchmarkInt64ColumnCompare(b *testing.B) {
	left := make([]int64, 1<<20)
	right := make([]int64, 1<<20)
	valid := make(memory.Bitmap, (len(left)+63)/64)
	for i := range left {
		left[i] = int64(i % 97)
		right[i] = int64(i % 89)
		valid[i/64] |= uint64(1) << uint(i%64)
	}
	out := make(memory.Bitmap, len(valid))
	for _, tc := range []struct {
		name string
		mode Mode
	}{{"scalar", ModeScalar}, {"auto", ModeAuto}, {"simd", ModeSIMD}} {
		b.Run(tc.name, func(b *testing.B) {
			set := New(tc.mode)
			b.SetBytes(int64(len(left) * 16))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.CompareInt64Columns(left, right, valid, valid, out, CompareGT)
			}
		})
	}
}

func BenchmarkFloat64ColumnCompare(b *testing.B) {
	left := make([]float64, 1<<20)
	right := make([]float64, 1<<20)
	valid := make(memory.Bitmap, (len(left)+63)/64)
	for i := range left {
		left[i] = float64(i % 97)
		right[i] = float64(i % 89)
		valid[i/64] |= uint64(1) << uint(i%64)
	}
	out := make(memory.Bitmap, len(valid))
	for _, tc := range []struct {
		name string
		mode Mode
	}{{"scalar", ModeScalar}, {"auto", ModeAuto}, {"simd", ModeSIMD}} {
		b.Run(tc.name, func(b *testing.B) {
			set := New(tc.mode)
			b.SetBytes(int64(len(left) * 16))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.CompareFloat64Columns(left, right, valid, valid, out, CompareGT)
			}
		})
	}
}
