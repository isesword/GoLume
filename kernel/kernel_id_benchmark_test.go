package kernel

import (
	"fmt"
	"slices"
	"testing"

	"gocolumn/memory"
)

// kernelBenchOutput holds every output buffer one kernel invocation can
// write. The self-check runs each kernel through the scalar and forced-SIMD
// sets into two slots and requires identical results before any timing
// sample is accepted.
type kernelBenchOutput struct {
	bitmap   memory.Bitmap
	validity memory.Bitmap
	i8       []int8
	i16      []int16
	i32      []int32
	i64      []int64
	u8       []uint8
	u16      []uint16
	u32      []uint32
	u64      []uint64
	f32      []float32
	f64      []float64
	count    int
}

func newKernelBenchOutput(rows int) kernelBenchOutput {
	return kernelBenchOutput{
		bitmap:   memory.EnsureBitmap(nil, rows),
		validity: memory.EnsureBitmap(nil, rows),
		i8:       make([]int8, rows),
		i16:      make([]int16, rows),
		i32:      make([]int32, rows),
		i64:      make([]int64, rows),
		u8:       make([]uint8, rows),
		u16:      make([]uint16, rows),
		u32:      make([]uint32, rows),
		u64:      make([]uint64, rows),
		f32:      make([]float32, rows),
		f64:      make([]float64, rows),
	}
}

func (o *kernelBenchOutput) clear() {
	clear(o.bitmap)
	clear(o.validity)
	clear(o.i8)
	clear(o.i16)
	clear(o.i32)
	clear(o.i64)
	clear(o.u8)
	clear(o.u16)
	clear(o.u32)
	clear(o.u64)
	clear(o.f32)
	clear(o.f64)
	o.count = 0
}

// diff reports the first difference between two output snapshots.
func (o kernelBenchOutput) diff(other kernelBenchOutput) error {
	if o.count != other.count {
		return fmt.Errorf("returned count %d != %d", o.count, other.count)
	}
	if !slices.Equal(o.bitmap, other.bitmap) {
		return fmt.Errorf("selection bitmaps differ")
	}
	if !slices.Equal(o.validity, other.validity) {
		return fmt.Errorf("validity/overflow bitmaps differ")
	}
	if !slices.Equal(o.i8, other.i8) {
		return fmt.Errorf("int8 outputs differ")
	}
	if !slices.Equal(o.i16, other.i16) {
		return fmt.Errorf("int16 outputs differ")
	}
	if !slices.Equal(o.i32, other.i32) {
		return fmt.Errorf("int32 outputs differ")
	}
	if !slices.Equal(o.i64, other.i64) {
		return fmt.Errorf("int64 outputs differ")
	}
	if !slices.Equal(o.u8, other.u8) {
		return fmt.Errorf("uint8 outputs differ")
	}
	if !slices.Equal(o.u16, other.u16) {
		return fmt.Errorf("uint16 outputs differ")
	}
	if !slices.Equal(o.u32, other.u32) {
		return fmt.Errorf("uint32 outputs differ")
	}
	if !slices.Equal(o.u64, other.u64) {
		return fmt.Errorf("uint64 outputs differ")
	}
	if !slices.Equal(o.f32, other.f32) {
		return fmt.Errorf("float32 outputs differ")
	}
	if !slices.Equal(o.f64, other.f64) {
		return fmt.Errorf("float64 outputs differ")
	}
	return nil
}

// kernelBenchFixture holds the shared inputs for BenchmarkKernelID. Data
// shapes follow the established per-family conventions so new measurements
// stay comparable with the evidence that approved the earlier kernels:
// 1<<16 rows, ascending values with the comparison threshold at the median
// of the rendered sequence, every-other-row compaction selection, and the
// mixed ASCII/Unicode string layout of BenchmarkStringMatchKernel.
type kernelBenchFixture struct {
	rows                           int
	i8a, i8b                       []int8
	i16a, i16b                     []int16
	i32a, i32b                     []int32
	i64a, i64b, i64base            []int64
	u8a, u8b                       []uint8
	u16a, u16b                     []uint16
	u32a, u32b                     []uint32
	u64a, u64b                     []uint64
	f32a, f32b                     []float32
	f64a, f64b                     []float64
	validMasks                     []uint8
	selection                      memory.Bitmap
	truthA, truthB, validA, validB memory.Bitmap
	lengthOffsets                  []uint32
	lengthData                     []byte
	matchOffsets                   []uint32
	matchData                      []byte
	matchValid                     memory.Bitmap
	outputs                        [2]kernelBenchOutput
}

func newKernelBenchFixture(rows int) *kernelBenchFixture {
	f := &kernelBenchFixture{rows: rows}
	f.i8a, f.i8b = make([]int8, rows), make([]int8, rows)
	f.i16a, f.i16b = make([]int16, rows), make([]int16, rows)
	f.i32a, f.i32b = make([]int32, rows), make([]int32, rows)
	f.i64a, f.i64b, f.i64base = make([]int64, rows), make([]int64, rows), make([]int64, rows)
	f.u8a, f.u8b = make([]uint8, rows), make([]uint8, rows)
	f.u16a, f.u16b = make([]uint16, rows), make([]uint16, rows)
	f.u32a, f.u32b = make([]uint32, rows), make([]uint32, rows)
	f.u64a, f.u64b = make([]uint64, rows), make([]uint64, rows)
	f.f32a, f.f32b = make([]float32, rows), make([]float32, rows)
	f.f64a, f.f64b = make([]float64, rows), make([]float64, rows)
	f.validMasks = make([]uint8, rows)
	f.selection = memory.EnsureBitmap(nil, rows)
	f.truthA, f.truthB = memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows)
	f.validA, f.validB = memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows)
	f.lengthOffsets = make([]uint32, rows+1)
	f.matchOffsets = make([]uint32, rows+1)
	f.matchValid = memory.EnsureBitmap(nil, rows)
	f.outputs[0] = newKernelBenchOutput(rows)
	f.outputs[1] = newKernelBenchOutput(rows)
	for row := 0; row < rows; row++ {
		f.i8a[row], f.i8b[row] = int8(row), 2
		f.i16a[row], f.i16b[row] = int16(row), 2
		f.i32a[row], f.i32b[row] = int32(row), 2
		f.i64a[row], f.i64b[row], f.i64base[row] = int64(row), 2, int64(row+1)
		f.u8a[row], f.u8b[row] = uint8(row), 2
		f.u16a[row], f.u16b[row] = uint16(row), 2
		f.u32a[row], f.u32b[row] = uint32(row), 2
		f.u64a[row], f.u64b[row] = uint64(row), 2
		f.f32a[row], f.f32b[row] = float32(row+1), 2
		f.f64a[row], f.f64b[row] = float64(row+1), 2
		f.validMasks[row] = uint8(row & 3)
		if row&1 == 0 {
			memory.Set(f.selection, row)
			memory.Set(f.truthA, row)
		}
		if row%3 == 0 {
			memory.Set(f.truthB, row)
		}
		if row%5 != 0 {
			memory.Set(f.validA, row)
		}
		if row%7 != 0 {
			memory.Set(f.validB, row)
		}
		f.lengthData = append(f.lengthData, "Go语言"...)
		f.lengthOffsets[row+1] = uint32(len(f.lengthData))
		value := []byte("alpha-")
		if row%7 == 0 {
			value = []byte("中文-😀-alpha")
		} else if row%11 == 0 {
			value = []byte("omega")
		}
		f.matchData = append(f.matchData, value...)
		f.matchOffsets[row+1] = uint32(len(f.matchData))
		if row%13 != 0 {
			memory.Set(f.matchValid, row)
		}
	}
	return f
}

// matchLiteral returns the per-operation literal of the string-match family
// benchmark so equal, prefix, suffix, and contains all have realistic hit
// rates over the fixture data.
func (f *kernelBenchFixture) matchLiteral(op StringMatchOp) []byte {
	switch op {
	case StringEqual:
		return []byte("alpha-")
	case StringContains:
		return []byte("😀")
	default:
		return []byte("alpha")
	}
}

// invoke runs one kernel once, writing into the given output slot.
func (f *kernelBenchFixture) invoke(set Set, id KernelID, slot int) {
	out := &f.outputs[slot]
	switch id.kind {
	case KernelMathUnary:
		switch id.typ {
		case memory.TypeInt8:
			set.MathUnaryInt8(f.i8a, out.i8, nil, MathUnaryOp(id.op))
		case memory.TypeInt16:
			set.MathUnaryInt16(f.i16a, out.i16, nil, MathUnaryOp(id.op))
		case memory.TypeInt32:
			set.MathUnaryInt32(f.i32a, out.i32, nil, MathUnaryOp(id.op))
		case memory.TypeInt64:
			set.MathUnaryInt64(f.i64a, out.i64, nil, MathUnaryOp(id.op))
		case memory.TypeUint8:
			set.MathUnaryUint8(f.u8a, out.u8, nil, MathUnaryOp(id.op))
		case memory.TypeUint16:
			set.MathUnaryUint16(f.u16a, out.u16, nil, MathUnaryOp(id.op))
		case memory.TypeUint32:
			set.MathUnaryUint32(f.u32a, out.u32, nil, MathUnaryOp(id.op))
		case memory.TypeUint64:
			set.MathUnaryUint64(f.u64a, out.u64, nil, MathUnaryOp(id.op))
		case memory.TypeFloat32:
			set.MathUnaryFloat32(f.f32a, out.f32, nil, MathUnaryOp(id.op))
		case memory.TypeFloat64:
			set.MathUnaryFloat64(f.f64a, out.f64, nil, MathUnaryOp(id.op))
		}
	case KernelMathBinary:
		switch id.typ {
		case memory.TypeInt8:
			out.count = set.MathBinaryInt8(f.i8a, f.i8b, out.i8, nil, out.validity, MathBinaryOp(id.op))
		case memory.TypeInt16:
			out.count = set.MathBinaryInt16(f.i16a, f.i16b, out.i16, nil, out.validity, MathBinaryOp(id.op))
		case memory.TypeInt32:
			out.count = set.MathBinaryInt32(f.i32a, f.i32b, out.i32, nil, out.validity, MathBinaryOp(id.op))
		case memory.TypeInt64:
			out.count = set.MathBinaryInt64(f.i64a, f.i64b, out.i64, nil, out.validity, MathBinaryOp(id.op))
		case memory.TypeUint8:
			out.count = set.MathBinaryUint8(f.u8a, f.u8b, out.u8, nil, out.validity, MathBinaryOp(id.op))
		case memory.TypeUint16:
			out.count = set.MathBinaryUint16(f.u16a, f.u16b, out.u16, nil, out.validity, MathBinaryOp(id.op))
		case memory.TypeUint32:
			out.count = set.MathBinaryUint32(f.u32a, f.u32b, out.u32, nil, out.validity, MathBinaryOp(id.op))
		case memory.TypeUint64:
			out.count = set.MathBinaryUint64(f.u64a, f.u64b, out.u64, nil, out.validity, MathBinaryOp(id.op))
		case memory.TypeFloat32:
			out.count = set.MathBinaryFloat32(f.f32a, f.f32b, out.f32, nil, out.validity, MathBinaryOp(id.op))
		case memory.TypeFloat64:
			out.count = set.MathBinaryFloat64(f.f64a, f.f64b, out.f64, nil, out.validity, MathBinaryOp(id.op))
		}

	case KernelComparison:
		switch id.typ {
		case memory.TypeInt8:
			out.count = set.CompareInt8(f.i8a, nil, out.bitmap, 0, CompareOp(id.op))
		case memory.TypeInt16:
			out.count = set.CompareInt16(f.i16a, nil, out.bitmap, 0, CompareOp(id.op))
		case memory.TypeInt32:
			out.count = set.CompareInt32(f.i32a, nil, out.bitmap, int32(f.rows/2), CompareOp(id.op))
		case memory.TypeInt64:
			out.count = set.CompareInt64(f.i64a, nil, out.bitmap, int64(f.rows/2), CompareOp(id.op))
		case memory.TypeUint8:
			out.count = set.CompareUint8(f.u8a, nil, out.bitmap, 128, CompareOp(id.op))
		case memory.TypeUint16:
			out.count = set.CompareUint16(f.u16a, nil, out.bitmap, 32768, CompareOp(id.op))
		case memory.TypeUint32:
			out.count = set.CompareUint32(f.u32a, nil, out.bitmap, uint32(f.rows/2), CompareOp(id.op))
		case memory.TypeUint64:
			out.count = set.CompareUint64(f.u64a, nil, out.bitmap, uint64(f.rows/2), CompareOp(id.op))
		case memory.TypeFloat32:
			out.count = set.CompareFloat32(f.f32a, nil, out.bitmap, float32(f.rows/2), CompareOp(id.op))
		case memory.TypeFloat64:
			out.count = set.CompareFloat64(f.f64a, nil, out.bitmap, float64(f.rows/2), CompareOp(id.op))
		}
	case KernelArithmetic:
		switch id.typ {
		case memory.TypeInt32:
			out.count = set.Int32Arithmetic(f.i32a, f.i32b, out.i32, nil, out.validity, ArithmeticOp(id.op))
		case memory.TypeInt64:
			out.count = set.Int64Arithmetic(f.i64a, f.i64b, out.i64, nil, out.validity, ArithmeticOp(id.op))
		case memory.TypeUint32:
			out.count = set.Uint32Arithmetic(f.u32a, f.u32b, out.u32, nil, out.validity, ArithmeticOp(id.op))
		case memory.TypeUint64:
			out.count = set.Uint64Arithmetic(f.u64a, f.u64b, out.u64, nil, out.validity, ArithmeticOp(id.op))
		case memory.TypeFloat32:
			set.Float32Arithmetic(f.f32a, f.f32b, out.f32, nil, ArithmeticOp(id.op))
		case memory.TypeFloat64:
			set.Float64Arithmetic(f.f64a, f.f64b, out.f64, nil, ArithmeticOp(id.op))
		}
	case KernelBoolean:
		switch id {
		case KernelBoolAnd:
			set.BoolAnd(f.truthA, f.validA, f.truthB, f.validB, out.bitmap, out.validity, f.rows)
		case KernelBoolOr:
			set.BoolOr(f.truthA, f.validA, f.truthB, f.validB, out.bitmap, out.validity, f.rows)
		case KernelBoolNot:
			set.BoolNot(f.truthA, f.validA, out.bitmap, out.validity, f.rows)
		}
	case KernelHash:
		if id == KernelHash64Batch {
			set.Hash64Batch(f.u64a, out.u64)
		} else {
			set.Hash128Batch(f.u64a, f.u64b, f.validMasks, out.u64)
		}
	case KernelScale:
		copy(out.i64, f.i64base)
		out.count = set.ScaleInt64(out.i64, nil, 3)
	case KernelStringLength:
		out.count = set.CompareStringLength(f.lengthOffsets, f.lengthData, nil, out.bitmap, 3, CompareGT, id == KernelStringUTF8Length)
	case KernelStringMatch:
		op := StringMatchOp(id.op)
		out.count = set.CompareStringMatch(f.matchOffsets, f.matchData, f.matchValid, out.bitmap, f.matchLiteral(op), op)
	case KernelCompact:
		switch id.typ {
		case memory.TypeInt8:
			out.count = set.CompactInt8(f.i8a, f.selection, out.i8)
		case memory.TypeInt16:
			out.count = set.CompactInt16(f.i16a, f.selection, out.i16)
		case memory.TypeInt32:
			out.count = set.CompactInt32(f.i32a, f.selection, out.i32)
		case memory.TypeInt64:
			out.count = set.CompactInt64(f.i64a, f.selection, out.i64)
		case memory.TypeUint8:
			out.count = set.CompactUint8(f.u8a, f.selection, out.u8)
		case memory.TypeUint16:
			out.count = set.CompactUint16(f.u16a, f.selection, out.u16)
		case memory.TypeUint32:
			out.count = set.CompactUint32(f.u32a, f.selection, out.u32)
		case memory.TypeUint64:
			out.count = set.CompactUint64(f.u64a, f.selection, out.u64)
		case memory.TypeFloat32:
			out.count = set.CompactFloat32(f.f32a, f.selection, out.f32)
		case memory.TypeFloat64:
			out.count = set.CompactFloat64(f.f64a, f.selection, out.f64)
		}
	}
}

// BenchmarkKernelID is the exhaustive operation-level gate input. Use
// scripts/bench_simd.sh for the shorter candidate suite and run this benchmark
// before approving any previously unmeasured KernelID.
//
// Every kernel first goes through a correctness self-check: the scalar and
// forced-SIMD sets must produce byte-identical outputs, otherwise the
// benchmark fails and no timing sample is recorded.
func BenchmarkKernelID(b *testing.B) {
	const rows = 1 << 16
	fixture := newKernelBenchFixture(rows)

	scalarSet := New(ModeScalar)
	simdSet := New(ModeSIMD)
	for _, id := range KernelIDs() {
		fixture.outputs[0].clear()
		fixture.outputs[1].clear()
		fixture.invoke(scalarSet, id, 0)
		fixture.invoke(simdSet, id, 1)
		if err := fixture.outputs[0].diff(fixture.outputs[1]); err != nil {
			b.Fatalf("kernel %s: scalar and SIMD results differ: %v", id, err)
		}
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
						fixture.invoke(set, id, 0)
					}
				})
			}
		})
	}
}
