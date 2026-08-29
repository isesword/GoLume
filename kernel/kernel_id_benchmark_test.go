package kernel

import (
	"gocolumn/memory"
	"testing"
)

// BenchmarkKernelID is the exhaustive operation-level gate input. Use
// scripts/bench_simd.sh for the shorter candidate suite and run this benchmark
// before approving any previously unmeasured KernelID.
func BenchmarkKernelID(b *testing.B) {
	const rows = 1 << 16
	i32a, i32b, i32out := make([]int32, rows), make([]int32, rows), make([]int32, rows)
	i64a, i64b, i64out, i64base := make([]int64, rows), make([]int64, rows), make([]int64, rows), make([]int64, rows)
	u32a, u32b, u32out := make([]uint32, rows), make([]uint32, rows), make([]uint32, rows)
	u64a, u64b, u64out := make([]uint64, rows), make([]uint64, rows), make([]uint64, rows)
	f64a, f64b, f64out := make([]float64, rows), make([]float64, rows), make([]float64, rows)
	validMasks := make([]uint8, rows)
	selection := memory.EnsureBitmap(nil, rows)
	truthA, truthB, validA, validB := memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows)
	bitmapOut, bitmapValidity := memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows)
	offsets := make([]uint32, rows+1)
	data := make([]byte, 0, rows*8)
	for row := 0; row < rows; row++ {
		i32a[row], i32b[row] = int32(row), 2
		i64a[row], i64b[row], i64base[row] = int64(row), 2, int64(row+1)
		u32a[row], u32b[row] = uint32(row), 2
		u64a[row], u64b[row] = uint64(row), 2
		f64a[row], f64b[row] = float64(row+1), 2
		validMasks[row] = uint8(row & 3)
		if row&1 == 0 {
			memory.Set(selection, row)
			memory.Set(truthA, row)
		}
		if row%3 == 0 {
			memory.Set(truthB, row)
		}
		if row%5 != 0 {
			memory.Set(validA, row)
		}
		if row%7 != 0 {
			memory.Set(validB, row)
		}
		data = append(data, "Go语言"...)
		offsets[row+1] = uint32(len(data))
	}

	for _, id := range KernelIDs() {
		id := id
		b.Run(id.String(), func(b *testing.B) {
			for _, choice := range []struct {
				name string
				mode Mode
			}{{"mode=scalar", ModeScalar}, {"mode=simd", ModeSIMD}} {
				set := New(choice.mode)
				b.Run(choice.name, func(b *testing.B) {
					b.ReportAllocs()
					for iteration := 0; iteration < b.N; iteration++ {
						switch id.kind {
						case KernelComparison:
							switch id.typ {
							case memory.TypeInt32:
								set.CompareInt32(i32a, nil, bitmapOut, rows/2, CompareOp(id.op))
							case memory.TypeInt64:
								set.CompareInt64(i64a, nil, bitmapOut, rows/2, CompareOp(id.op))
							case memory.TypeUint32:
								set.CompareUint32(u32a, nil, bitmapOut, rows/2, CompareOp(id.op))
							case memory.TypeUint64:
								set.CompareUint64(u64a, nil, bitmapOut, rows/2, CompareOp(id.op))
							case memory.TypeFloat64:
								set.CompareFloat64(f64a, nil, bitmapOut, rows/2, CompareOp(id.op))
							}
						case KernelArithmetic:
							switch id.typ {
							case memory.TypeInt32:
								set.Int32Arithmetic(i32a, i32b, i32out, nil, nil, ArithmeticOp(id.op))
							case memory.TypeInt64:
								set.Int64Arithmetic(i64a, i64b, i64out, nil, nil, ArithmeticOp(id.op))
							case memory.TypeUint32:
								set.Uint32Arithmetic(u32a, u32b, u32out, nil, nil, ArithmeticOp(id.op))
							case memory.TypeUint64:
								set.Uint64Arithmetic(u64a, u64b, u64out, nil, nil, ArithmeticOp(id.op))
							case memory.TypeFloat64:
								set.Float64Arithmetic(f64a, f64b, f64out, nil, ArithmeticOp(id.op))
							}
						case KernelBoolean:
							switch id {
							case KernelBoolAnd:
								set.BoolAnd(truthA, validA, truthB, validB, bitmapOut, bitmapValidity, rows)
							case KernelBoolOr:
								set.BoolOr(truthA, validA, truthB, validB, bitmapOut, bitmapValidity, rows)
							case KernelBoolNot:
								set.BoolNot(truthA, validA, bitmapOut, bitmapValidity, rows)
							}
						case KernelHash:
							if id == KernelHash64Batch {
								set.Hash64Batch(u64a, u64out)
							} else {
								set.Hash128Batch(u64a, u64b, validMasks, u64out)
							}
						case KernelScale:
							copy(i64out, i64base)
							set.ScaleInt64(i64out, nil, 3)
						case KernelStringLength:
							set.CompareStringLength(offsets, data, nil, bitmapOut, 3, CompareGT, id == KernelStringUTF8Length)
						case KernelCompact:
							switch id.typ {
							case memory.TypeInt32:
								set.CompactInt32(i32a, selection, i32out)
							case memory.TypeInt64:
								set.CompactInt64(i64a, selection, i64out)
							case memory.TypeUint32:
								set.CompactUint32(u32a, selection, u32out)
							case memory.TypeUint64:
								set.CompactUint64(u64a, selection, u64out)
							case memory.TypeFloat64:
								set.CompactFloat64(f64a, selection, f64out)
							}
						}
					}
				})
			}
		})
	}
}
