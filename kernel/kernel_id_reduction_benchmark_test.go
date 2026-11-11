package kernel

import (
	"fmt"
	"math"

	"gocolumn/memory"
)

// invokeReduction runs one bounded 64-row block. Keep every return value in the
// output snapshot, preserving floating-point payloads and signed zero exactly.
// The generic benchmark's ID/mode naming remains compatible with kernelgate.
func (f *kernelBenchFixture) invokeReduction(set Set, id KernelID, out *kernelBenchOutput) {
	n := min(f.rows, 64)
	mask := f.reductionMask
	switch id.op {
	case 1:
		var sum uint64
		switch id.typ {
		case memory.TypeInt8:
			sum = uint64(set.SumInt8Masked(f.i8a[:n], mask))
		case memory.TypeInt16:
			sum = uint64(set.SumInt16Masked(f.i16a[:n], mask))
		case memory.TypeUint8:
			sum = uint64(set.SumUint8Masked(f.u8a[:n], mask))
		case memory.TypeUint16:
			sum = uint64(set.SumUint16Masked(f.u16a[:n], mask))
		case memory.TypeInt32:
			sum = uint64(set.SumInt32Masked(f.i32a[:n], mask))
		case memory.TypeInt64:
			sum = uint64(set.SumInt64Masked(f.i64a[:n], mask))
		case memory.TypeUint32:
			sum = uint64(set.SumUint32Masked(f.u32a[:n], mask))
		case memory.TypeUint64:
			sum = uint64(set.SumUint64Masked(f.u64a[:n], mask))
		default:
			panic("kernel benchmark: unsupported sum type")
		}
		out.reduction = [2]uint64{sum, 0}
	case 2:
		switch id.typ {
		case memory.TypeInt8:
			lo, hi := set.MinMaxInt8Masked(f.i8a[:n], mask)
			out.reduction = [2]uint64{uint64(lo), uint64(hi)}
		case memory.TypeInt16:
			lo, hi := set.MinMaxInt16Masked(f.i16a[:n], mask)
			out.reduction = [2]uint64{uint64(lo), uint64(hi)}
		case memory.TypeUint8:
			lo, hi := set.MinMaxUint8Masked(f.u8a[:n], mask)
			out.reduction = [2]uint64{uint64(lo), uint64(hi)}
		case memory.TypeUint16:
			lo, hi := set.MinMaxUint16Masked(f.u16a[:n], mask)
			out.reduction = [2]uint64{uint64(lo), uint64(hi)}
		case memory.TypeInt32:
			lo, hi := set.MinMaxInt32Masked(f.i32a[:n], mask)
			out.reduction = [2]uint64{uint64(lo), uint64(hi)}
		case memory.TypeInt64:
			lo, hi := set.MinMaxInt64Masked(f.i64a[:n], mask)
			out.reduction = [2]uint64{uint64(lo), uint64(hi)}
		case memory.TypeUint32:
			lo, hi := set.MinMaxUint32Masked(f.u32a[:n], mask)
			out.reduction = [2]uint64{uint64(lo), uint64(hi)}
		case memory.TypeUint64:
			lo, hi := set.MinMaxUint64Masked(f.u64a[:n], mask)
			out.reduction = [2]uint64{uint64(lo), uint64(hi)}
		case memory.TypeFloat32:
			lo, hi := set.MinMaxFloat32Masked(f.f32a[:n], mask)
			out.reduction = [2]uint64{uint64(math.Float32bits(lo)), uint64(math.Float32bits(hi))}
		case memory.TypeFloat64:
			lo, hi := set.MinMaxFloat64Masked(f.f64a[:n], mask)
			out.reduction = [2]uint64{math.Float64bits(lo), math.Float64bits(hi)}
		default:
			panic("kernel benchmark: unsupported min/max type")
		}
	default:
		panic("kernel benchmark: unsupported reduction operation")
	}
}

func (f *kernelBenchFixture) checkReduction(id KernelID, slot int) error {
	want := f.reductionOracle(id)
	got := f.outputs[slot].reduction
	if got != want {
		return fmt.Errorf("reduction result bits %x != independent oracle %x", got, want)
	}
	return nil
}

// reductionOracle deliberately does not call Set, dispatch, or scalar kernel
// helpers. Linear row-by-row evaluation checks the mask and the public result
// contract independently from both implementations being timed.
func (f *kernelBenchFixture) reductionOracle(id KernelID) [2]uint64 {
	n := min(f.rows, 64)
	mask := f.reductionMask
	switch id.op {
	case 1:
		var sum uint64
		switch id.typ {
		case memory.TypeInt8:
			sum = kernelBenchNarrowSumOracle(f.i8a[:n], mask)
		case memory.TypeInt16:
			sum = kernelBenchNarrowSumOracle(f.i16a[:n], mask)
		case memory.TypeUint8:
			sum = kernelBenchNarrowSumOracle(f.u8a[:n], mask)
		case memory.TypeUint16:
			sum = kernelBenchNarrowSumOracle(f.u16a[:n], mask)
		case memory.TypeInt32:
			sum = kernelBenchSumOracle(f.i32a[:n], mask)
		case memory.TypeInt64:
			sum = kernelBenchSumOracle(f.i64a[:n], mask)
		case memory.TypeUint32:
			sum = kernelBenchSumOracle(f.u32a[:n], mask)
		case memory.TypeUint64:
			sum = kernelBenchSumOracle(f.u64a[:n], mask)
		default:
			panic("kernel benchmark oracle: unsupported sum type")
		}
		return [2]uint64{sum, 0}
	case 2:
		switch id.typ {
		case memory.TypeInt8:
			return kernelBenchMinMaxOracle(f.i8a[:n], mask)
		case memory.TypeInt16:
			return kernelBenchMinMaxOracle(f.i16a[:n], mask)
		case memory.TypeUint8:
			return kernelBenchMinMaxOracle(f.u8a[:n], mask)
		case memory.TypeUint16:
			return kernelBenchMinMaxOracle(f.u16a[:n], mask)
		case memory.TypeInt32:
			return kernelBenchMinMaxOracle(f.i32a[:n], mask)
		case memory.TypeInt64:
			return kernelBenchMinMaxOracle(f.i64a[:n], mask)
		case memory.TypeUint32:
			return kernelBenchMinMaxOracle(f.u32a[:n], mask)
		case memory.TypeUint64:
			return kernelBenchMinMaxOracle(f.u64a[:n], mask)
		case memory.TypeFloat32:
			return kernelBenchMinMaxOracle(f.f32a[:n], mask)
		case memory.TypeFloat64:
			return kernelBenchMinMaxOracle(f.f64a[:n], mask)
		default:
			panic("kernel benchmark oracle: unsupported min/max type")
		}
	default:
		panic("kernel benchmark oracle: unsupported reduction operation")
	}
}

func kernelBenchNarrowSumOracle[T int8 | int16 | uint8 | uint16](v []T, mask uint64) uint64 {
	var sum int64
	for row, value := range v {
		if mask&(uint64(1)<<row) != 0 {
			sum += int64(value)
		}
	}
	return uint64(sum)
}

func kernelBenchSumOracle[T int32 | int64 | uint32 | uint64](v []T, mask uint64) uint64 {
	var sum T
	for row, value := range v {
		if mask&(uint64(1)<<row) != 0 {
			sum += value
		}
	}
	return uint64(sum)
}

type kernelBenchNumber interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

func kernelBenchResultBits[T kernelBenchNumber](value T) uint64 {
	switch x := any(value).(type) {
	case float32:
		return uint64(math.Float32bits(x))
	case float64:
		return math.Float64bits(x)
	default:
		return uint64(value)
	}
}

func kernelBenchMinMaxOracle[T kernelBenchNumber](v []T, mask uint64) [2]uint64 {
	var lo, hi, lastNaN T
	haveNumber, haveNaN := false, false
	for row, value := range v {
		if mask&(uint64(1)<<row) == 0 {
			continue
		}
		if value != value {
			lastNaN, haveNaN = value, true
			continue
		}
		if !haveNumber {
			lo, hi, haveNumber = value, value, true
			continue
		}
		if value < lo || value == 0 && lo == 0 && math.Signbit(float64(value)) {
			lo = value
		}
		if value > hi || value == 0 && hi == 0 && !math.Signbit(float64(value)) {
			hi = value
		}
	}
	if !haveNumber && haveNaN {
		lo, hi = lastNaN, lastNaN
	}
	return [2]uint64{kernelBenchResultBits(lo), kernelBenchResultBits(hi)}
}
