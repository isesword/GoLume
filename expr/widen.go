package expr

import (
	"gocolumn/kernel"
	"gocolumn/memory"
)

// evalWidenBinary operates directly on native inputs and the existing Float64
// destination. It never allocates conversion arrays. Neither promotion nor
// arithmetic is performed at Float32 precision, and operations stay separate.
func evalWidenBinary(out, a, b *scalarRegister, rows int, op kernel.ArithmeticOp, kernels kernel.Set) bool {
	if a.constant || b.constant || !IsNumeric(a.typ) || !IsNumeric(b.typ) {
		return false
	}
	values := out.f64[:rows]
	if a.typ == memory.TypeFloat32 && b.typ == memory.TypeFloat32 {
		kernels.Float32WidenArithmetic(a.f32[:rows], b.f32[:rows], values, out.validity, op)
		return true
	}
	// A Float64 operand can be consumed without materializing a conversion.
	// In particular, keep the original operand order for subtraction/division.
	left, right, reverse := a, b, false
	if a.typ != memory.TypeFloat64 && b.typ == memory.TypeFloat64 {
		left, right, reverse = b, a, true
	}
	var dense []float64
	if left.typ == memory.TypeFloat64 {
		dense = left.f64[:rows]
	} else {
		switch left.typ {
		case memory.TypeInt8:
			kernels.WidenInt8ToFloat64(left.i8[:rows], values, out.validity)
		case memory.TypeInt16:
			kernels.WidenInt16ToFloat64(left.i16[:rows], values, out.validity)
		case memory.TypeInt32:
			kernels.WidenInt32ToFloat64(left.i32[:rows], values, out.validity)
		case memory.TypeInt64:
			kernels.WidenInt64ToFloat64(left.i64[:rows], values, out.validity)
		case memory.TypeUint8:
			kernels.WidenUint8ToFloat64(left.u8[:rows], values, out.validity)
		case memory.TypeUint16:
			kernels.WidenUint16ToFloat64(left.u16[:rows], values, out.validity)
		case memory.TypeUint32:
			kernels.WidenUint32ToFloat64(left.u32[:rows], values, out.validity)
		case memory.TypeUint64:
			kernels.WidenUint64ToFloat64(left.u64[:rows], values, out.validity)
		case memory.TypeFloat32:
			kernels.WidenFloat32ToFloat64(left.f32[:rows], values, out.validity)
		default:
			return false
		}
		dense = values
	}
	switch right.typ {
	case memory.TypeInt8:
		kernels.Float64Int8Arithmetic(dense, right.i8[:rows], values, out.validity, op, reverse)
	case memory.TypeInt16:
		kernels.Float64Int16Arithmetic(dense, right.i16[:rows], values, out.validity, op, reverse)
	case memory.TypeInt32:
		kernels.Float64Int32Arithmetic(dense, right.i32[:rows], values, out.validity, op, reverse)
	case memory.TypeInt64:
		kernels.Float64Int64Arithmetic(dense, right.i64[:rows], values, out.validity, op, reverse)
	case memory.TypeUint8:
		kernels.Float64Uint8Arithmetic(dense, right.u8[:rows], values, out.validity, op, reverse)
	case memory.TypeUint16:
		kernels.Float64Uint16Arithmetic(dense, right.u16[:rows], values, out.validity, op, reverse)
	case memory.TypeUint32:
		kernels.Float64Uint32Arithmetic(dense, right.u32[:rows], values, out.validity, op, reverse)
	case memory.TypeUint64:
		kernels.Float64Uint64Arithmetic(dense, right.u64[:rows], values, out.validity, op, reverse)
	case memory.TypeFloat32:
		kernels.Float64Float32Arithmetic(dense, right.f32[:rows], values, out.validity, op, reverse)
	default:
		return false
	}
	return true
}
