package kernel

import (
	"fmt"
	"gocolumn/memory"
)

// MathUnaryOp identifies a type-preserving numeric unary operation.
type MathUnaryOp uint8

const (
	// MathAbs computes the absolute value; signed minimum values wrap.
	MathAbs MathUnaryOp = iota + 1
	// MathRound rounds floating values to the nearest integer, with ties to even.
	MathRound
	// MathFloor rounds toward negative infinity.
	MathFloor
	// MathCeil rounds toward positive infinity.
	MathCeil
	// MathSqrt computes the floating square root with IEEE domain semantics.
	MathSqrt
	// MathLog computes the natural logarithm with IEEE domain semantics.
	MathLog
)

// String returns a stable unary operation name.
func (op MathUnaryOp) String() string {
	switch op {
	case MathAbs:
		return "abs"
	case MathRound:
		return "round"
	case MathFloor:
		return "floor"
	case MathCeil:
		return "ceil"
	case MathSqrt:
		return "sqrt"
	case MathLog:
		return "log"
	}
	return fmt.Sprintf("math-unary(%d)", op)
}

// MathBinaryOp identifies a type-preserving numeric binary operation.
type MathBinaryOp uint8

const (
	// MathMod computes remainder with the sign of the divisor.
	MathMod MathBinaryOp = iota + 1
	// MathFloorDiv computes division rounded toward negative infinity.
	MathFloorDiv
	// MathPow raises the left value to the right power.
	MathPow
)

// String returns a stable binary operation name.
func (op MathBinaryOp) String() string {
	switch op {
	case MathMod:
		return "mod"
	case MathFloorDiv:
		return "floor-div"
	case MathPow:
		return "pow"
	}
	return fmt.Sprintf("math-binary(%d)", op)
}

// MathUnaryID returns the exact native-width unary math identifier.
func MathUnaryID(typ memory.DataType, op MathUnaryOp) (KernelID, bool) {
	if op < MathAbs || op > MathLog {
		return KernelID{}, false
	}
	switch typ {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64:
		if op > MathCeil {
			return KernelID{}, false
		}
	case memory.TypeFloat32, memory.TypeFloat64:
	default:
		return KernelID{}, false
	}
	return KernelID{kind: KernelMathUnary, typ: typ, op: uint8(op)}, true
}

// MathBinaryID returns the exact native-width binary math identifier.
func MathBinaryID(typ memory.DataType, op MathBinaryOp) (KernelID, bool) {
	if op < MathMod || op > MathPow {
		return KernelID{}, false
	}
	if _, ok := MathUnaryID(typ, MathAbs); !ok {
		return KernelID{}, false
	}
	return KernelID{kind: KernelMathBinary, typ: typ, op: uint8(op)}, true
}

type mathUnaryFunc[T mathSigned | mathUnsigned | mathFloat] func([]T, []T, memory.Bitmap)
type mathBinaryFunc[T mathSigned | mathUnsigned | mathFloat] func([]T, []T, []T, memory.Bitmap, memory.Bitmap) int

type mathDispatch struct {
	unaryInt8     [MathLog + 1]mathUnaryFunc[int8]
	binaryInt8    [MathPow + 1]mathBinaryFunc[int8]
	unaryInt16    [MathLog + 1]mathUnaryFunc[int16]
	binaryInt16   [MathPow + 1]mathBinaryFunc[int16]
	unaryInt32    [MathLog + 1]mathUnaryFunc[int32]
	binaryInt32   [MathPow + 1]mathBinaryFunc[int32]
	unaryInt64    [MathLog + 1]mathUnaryFunc[int64]
	binaryInt64   [MathPow + 1]mathBinaryFunc[int64]
	unaryUint8    [MathLog + 1]mathUnaryFunc[uint8]
	binaryUint8   [MathPow + 1]mathBinaryFunc[uint8]
	unaryUint16   [MathLog + 1]mathUnaryFunc[uint16]
	binaryUint16  [MathPow + 1]mathBinaryFunc[uint16]
	unaryUint32   [MathLog + 1]mathUnaryFunc[uint32]
	binaryUint32  [MathPow + 1]mathBinaryFunc[uint32]
	unaryUint64   [MathLog + 1]mathUnaryFunc[uint64]
	binaryUint64  [MathPow + 1]mathBinaryFunc[uint64]
	unaryFloat32  [MathLog + 1]mathUnaryFunc[float32]
	binaryFloat32 [MathPow + 1]mathBinaryFunc[float32]
	unaryFloat64  [MathLog + 1]mathUnaryFunc[float64]
	binaryFloat64 [MathPow + 1]mathBinaryFunc[float64]
}

func installMathScalar(d *dispatch) {
	d.math.unaryInt8[MathAbs] = func(a, o []int8, v memory.Bitmap) { scalarMathSignedAbs(a, o, v, nil) }
	d.math.unaryInt8[MathRound] = func(a, o []int8, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryInt8[MathFloor] = func(a, o []int8, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryInt8[MathCeil] = func(a, o []int8, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.binaryInt8[MathMod] = func(a, b, o []int8, v, e memory.Bitmap) int { return scalarMathSignedBinary(a, b, o, v, e, MathMod) }
	d.math.binaryInt8[MathFloorDiv] = func(a, b, o []int8, v, e memory.Bitmap) int {
		return scalarMathSignedBinary(a, b, o, v, e, MathFloorDiv)
	}
	d.math.binaryInt8[MathPow] = func(a, b, o []int8, v, e memory.Bitmap) int { return scalarMathSignedBinary(a, b, o, v, e, MathPow) }
	d.math.unaryInt16[MathAbs] = func(a, o []int16, v memory.Bitmap) { scalarMathSignedAbs(a, o, v, nil) }
	d.math.unaryInt16[MathRound] = func(a, o []int16, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryInt16[MathFloor] = func(a, o []int16, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryInt16[MathCeil] = func(a, o []int16, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.binaryInt16[MathMod] = func(a, b, o []int16, v, e memory.Bitmap) int { return scalarMathSignedBinary(a, b, o, v, e, MathMod) }
	d.math.binaryInt16[MathFloorDiv] = func(a, b, o []int16, v, e memory.Bitmap) int {
		return scalarMathSignedBinary(a, b, o, v, e, MathFloorDiv)
	}
	d.math.binaryInt16[MathPow] = func(a, b, o []int16, v, e memory.Bitmap) int { return scalarMathSignedBinary(a, b, o, v, e, MathPow) }
	d.math.unaryInt32[MathAbs] = func(a, o []int32, v memory.Bitmap) { scalarMathSignedAbs(a, o, v, nil) }
	d.math.unaryInt32[MathRound] = func(a, o []int32, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryInt32[MathFloor] = func(a, o []int32, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryInt32[MathCeil] = func(a, o []int32, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.binaryInt32[MathMod] = func(a, b, o []int32, v, e memory.Bitmap) int { return scalarMathSignedBinary(a, b, o, v, e, MathMod) }
	d.math.binaryInt32[MathFloorDiv] = func(a, b, o []int32, v, e memory.Bitmap) int {
		return scalarMathSignedBinary(a, b, o, v, e, MathFloorDiv)
	}
	d.math.binaryInt32[MathPow] = func(a, b, o []int32, v, e memory.Bitmap) int { return scalarMathSignedBinary(a, b, o, v, e, MathPow) }
	d.math.unaryInt64[MathAbs] = func(a, o []int64, v memory.Bitmap) { scalarMathSignedAbs(a, o, v, nil) }
	d.math.unaryInt64[MathRound] = func(a, o []int64, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryInt64[MathFloor] = func(a, o []int64, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryInt64[MathCeil] = func(a, o []int64, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.binaryInt64[MathMod] = func(a, b, o []int64, v, e memory.Bitmap) int { return scalarMathSignedBinary(a, b, o, v, e, MathMod) }
	d.math.binaryInt64[MathFloorDiv] = func(a, b, o []int64, v, e memory.Bitmap) int {
		return scalarMathSignedBinary(a, b, o, v, e, MathFloorDiv)
	}
	d.math.binaryInt64[MathPow] = func(a, b, o []int64, v, e memory.Bitmap) int { return scalarMathSignedBinary(a, b, o, v, e, MathPow) }
	d.math.unaryUint8[MathAbs] = func(a, o []uint8, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint8[MathRound] = func(a, o []uint8, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint8[MathFloor] = func(a, o []uint8, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint8[MathCeil] = func(a, o []uint8, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.binaryUint8[MathMod] = func(a, b, o []uint8, v, e memory.Bitmap) int { return scalarMathUnsignedBinary(a, b, o, v, e, MathMod) }
	d.math.binaryUint8[MathFloorDiv] = func(a, b, o []uint8, v, e memory.Bitmap) int {
		return scalarMathUnsignedBinary(a, b, o, v, e, MathFloorDiv)
	}
	d.math.binaryUint8[MathPow] = func(a, b, o []uint8, v, e memory.Bitmap) int { return scalarMathUnsignedBinary(a, b, o, v, e, MathPow) }
	d.math.unaryUint16[MathAbs] = func(a, o []uint16, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint16[MathRound] = func(a, o []uint16, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint16[MathFloor] = func(a, o []uint16, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint16[MathCeil] = func(a, o []uint16, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.binaryUint16[MathMod] = func(a, b, o []uint16, v, e memory.Bitmap) int {
		return scalarMathUnsignedBinary(a, b, o, v, e, MathMod)
	}
	d.math.binaryUint16[MathFloorDiv] = func(a, b, o []uint16, v, e memory.Bitmap) int {
		return scalarMathUnsignedBinary(a, b, o, v, e, MathFloorDiv)
	}
	d.math.binaryUint16[MathPow] = func(a, b, o []uint16, v, e memory.Bitmap) int {
		return scalarMathUnsignedBinary(a, b, o, v, e, MathPow)
	}
	d.math.unaryUint32[MathAbs] = func(a, o []uint32, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint32[MathRound] = func(a, o []uint32, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint32[MathFloor] = func(a, o []uint32, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint32[MathCeil] = func(a, o []uint32, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.binaryUint32[MathMod] = func(a, b, o []uint32, v, e memory.Bitmap) int {
		return scalarMathUnsignedBinary(a, b, o, v, e, MathMod)
	}
	d.math.binaryUint32[MathFloorDiv] = func(a, b, o []uint32, v, e memory.Bitmap) int {
		return scalarMathUnsignedBinary(a, b, o, v, e, MathFloorDiv)
	}
	d.math.binaryUint32[MathPow] = func(a, b, o []uint32, v, e memory.Bitmap) int {
		return scalarMathUnsignedBinary(a, b, o, v, e, MathPow)
	}
	d.math.unaryUint64[MathAbs] = func(a, o []uint64, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint64[MathRound] = func(a, o []uint64, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint64[MathFloor] = func(a, o []uint64, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.unaryUint64[MathCeil] = func(a, o []uint64, v memory.Bitmap) { scalarMathIdentity(a, o, v) }
	d.math.binaryUint64[MathMod] = func(a, b, o []uint64, v, e memory.Bitmap) int {
		return scalarMathUnsignedBinary(a, b, o, v, e, MathMod)
	}
	d.math.binaryUint64[MathFloorDiv] = func(a, b, o []uint64, v, e memory.Bitmap) int {
		return scalarMathUnsignedBinary(a, b, o, v, e, MathFloorDiv)
	}
	d.math.binaryUint64[MathPow] = func(a, b, o []uint64, v, e memory.Bitmap) int {
		return scalarMathUnsignedBinary(a, b, o, v, e, MathPow)
	}
	d.math.unaryFloat32[MathAbs] = func(a, o []float32, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathAbs) }
	d.math.unaryFloat32[MathRound] = func(a, o []float32, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathRound) }
	d.math.unaryFloat32[MathFloor] = func(a, o []float32, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathFloor) }
	d.math.unaryFloat32[MathCeil] = func(a, o []float32, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathCeil) }
	d.math.unaryFloat32[MathSqrt] = func(a, o []float32, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathSqrt) }
	d.math.unaryFloat32[MathLog] = func(a, o []float32, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathLog) }
	d.math.binaryFloat32[MathMod] = func(a, b, o []float32, v, e memory.Bitmap) int {
		prepareIntegerArithmetic(len(a), len(a), len(b), len(o), e)
		scalarMathFloatBinary(a, b, o, v, MathMod)
		return 0
	}
	d.math.binaryFloat32[MathFloorDiv] = func(a, b, o []float32, v, e memory.Bitmap) int {
		prepareIntegerArithmetic(len(a), len(a), len(b), len(o), e)
		scalarMathFloatBinary(a, b, o, v, MathFloorDiv)
		return 0
	}
	d.math.binaryFloat32[MathPow] = func(a, b, o []float32, v, e memory.Bitmap) int {
		prepareIntegerArithmetic(len(a), len(a), len(b), len(o), e)
		scalarMathFloatBinary(a, b, o, v, MathPow)
		return 0
	}
	d.math.unaryFloat64[MathAbs] = func(a, o []float64, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathAbs) }
	d.math.unaryFloat64[MathRound] = func(a, o []float64, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathRound) }
	d.math.unaryFloat64[MathFloor] = func(a, o []float64, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathFloor) }
	d.math.unaryFloat64[MathCeil] = func(a, o []float64, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathCeil) }
	d.math.unaryFloat64[MathSqrt] = func(a, o []float64, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathSqrt) }
	d.math.unaryFloat64[MathLog] = func(a, o []float64, v memory.Bitmap) { scalarMathFloatUnary(a, o, v, MathLog) }
	d.math.binaryFloat64[MathMod] = func(a, b, o []float64, v, e memory.Bitmap) int {
		prepareIntegerArithmetic(len(a), len(a), len(b), len(o), e)
		scalarMathFloatBinary(a, b, o, v, MathMod)
		return 0
	}
	d.math.binaryFloat64[MathFloorDiv] = func(a, b, o []float64, v, e memory.Bitmap) int {
		prepareIntegerArithmetic(len(a), len(a), len(b), len(o), e)
		scalarMathFloatBinary(a, b, o, v, MathFloorDiv)
		return 0
	}
	d.math.binaryFloat64[MathPow] = func(a, b, o []float64, v, e memory.Bitmap) int {
		prepareIntegerArithmetic(len(a), len(a), len(b), len(o), e)
		scalarMathFloatBinary(a, b, o, v, MathPow)
		return 0
	}
}

// MathUnaryInt8 evaluates a native-width unary batch. Invalid lanes become zero.
// op must be MathAbs, MathRound, MathFloor, or MathCeil; check MathUnaryID when dynamic.
func (s Set) MathUnaryInt8(in, out []int8, validity memory.Bitmap, op MathUnaryOp) {
	s.d.math.unaryInt8[op](in, out, validity)
}

// MathBinaryInt8 evaluates a native-width binary batch and reports invalid integral operations.
// failures, when non-nil, is cleared then marks failing active rows; the return value counts them.
// Floating domain errors produce IEEE NaN or infinity and do not mark failures.
func (s Set) MathBinaryInt8(left, right, out []int8, validity, failures memory.Bitmap, op MathBinaryOp) int {
	return s.d.math.binaryInt8[op](left, right, out, validity, failures)
}

// MathUnaryInt16 evaluates a native-width unary batch. Invalid lanes become zero.
// op must be MathAbs, MathRound, MathFloor, or MathCeil; check MathUnaryID when dynamic.
func (s Set) MathUnaryInt16(in, out []int16, validity memory.Bitmap, op MathUnaryOp) {
	s.d.math.unaryInt16[op](in, out, validity)
}

// MathBinaryInt16 evaluates a native-width binary batch and reports invalid integral operations.
// failures, when non-nil, is cleared then marks failing active rows; the return value counts them.
// Floating domain errors produce IEEE NaN or infinity and do not mark failures.
func (s Set) MathBinaryInt16(left, right, out []int16, validity, failures memory.Bitmap, op MathBinaryOp) int {
	return s.d.math.binaryInt16[op](left, right, out, validity, failures)
}

// MathUnaryInt32 evaluates a native-width unary batch. Invalid lanes become zero.
// op must be MathAbs, MathRound, MathFloor, or MathCeil; check MathUnaryID when dynamic.
func (s Set) MathUnaryInt32(in, out []int32, validity memory.Bitmap, op MathUnaryOp) {
	s.d.math.unaryInt32[op](in, out, validity)
}

// MathBinaryInt32 evaluates a native-width binary batch and reports invalid integral operations.
// failures, when non-nil, is cleared then marks failing active rows; the return value counts them.
// Floating domain errors produce IEEE NaN or infinity and do not mark failures.
func (s Set) MathBinaryInt32(left, right, out []int32, validity, failures memory.Bitmap, op MathBinaryOp) int {
	return s.d.math.binaryInt32[op](left, right, out, validity, failures)
}

// MathUnaryInt64 evaluates a native-width unary batch. Invalid lanes become zero.
// op must be MathAbs, MathRound, MathFloor, or MathCeil; check MathUnaryID when dynamic.
func (s Set) MathUnaryInt64(in, out []int64, validity memory.Bitmap, op MathUnaryOp) {
	s.d.math.unaryInt64[op](in, out, validity)
}

// MathBinaryInt64 evaluates a native-width binary batch and reports invalid integral operations.
// failures, when non-nil, is cleared then marks failing active rows; the return value counts them.
// Floating domain errors produce IEEE NaN or infinity and do not mark failures.
func (s Set) MathBinaryInt64(left, right, out []int64, validity, failures memory.Bitmap, op MathBinaryOp) int {
	return s.d.math.binaryInt64[op](left, right, out, validity, failures)
}

// MathUnaryUint8 evaluates a native-width unary batch. Invalid lanes become zero.
// op must be MathAbs, MathRound, MathFloor, or MathCeil; check MathUnaryID when dynamic.
func (s Set) MathUnaryUint8(in, out []uint8, validity memory.Bitmap, op MathUnaryOp) {
	s.d.math.unaryUint8[op](in, out, validity)
}

// MathBinaryUint8 evaluates a native-width binary batch and reports invalid integral operations.
// failures, when non-nil, is cleared then marks failing active rows; the return value counts them.
// Floating domain errors produce IEEE NaN or infinity and do not mark failures.
func (s Set) MathBinaryUint8(left, right, out []uint8, validity, failures memory.Bitmap, op MathBinaryOp) int {
	return s.d.math.binaryUint8[op](left, right, out, validity, failures)
}

// MathUnaryUint16 evaluates a native-width unary batch. Invalid lanes become zero.
// op must be MathAbs, MathRound, MathFloor, or MathCeil; check MathUnaryID when dynamic.
func (s Set) MathUnaryUint16(in, out []uint16, validity memory.Bitmap, op MathUnaryOp) {
	s.d.math.unaryUint16[op](in, out, validity)
}

// MathBinaryUint16 evaluates a native-width binary batch and reports invalid integral operations.
// failures, when non-nil, is cleared then marks failing active rows; the return value counts them.
// Floating domain errors produce IEEE NaN or infinity and do not mark failures.
func (s Set) MathBinaryUint16(left, right, out []uint16, validity, failures memory.Bitmap, op MathBinaryOp) int {
	return s.d.math.binaryUint16[op](left, right, out, validity, failures)
}

// MathUnaryUint32 evaluates a native-width unary batch. Invalid lanes become zero.
// op must be MathAbs, MathRound, MathFloor, or MathCeil; check MathUnaryID when dynamic.
func (s Set) MathUnaryUint32(in, out []uint32, validity memory.Bitmap, op MathUnaryOp) {
	s.d.math.unaryUint32[op](in, out, validity)
}

// MathBinaryUint32 evaluates a native-width binary batch and reports invalid integral operations.
// failures, when non-nil, is cleared then marks failing active rows; the return value counts them.
// Floating domain errors produce IEEE NaN or infinity and do not mark failures.
func (s Set) MathBinaryUint32(left, right, out []uint32, validity, failures memory.Bitmap, op MathBinaryOp) int {
	return s.d.math.binaryUint32[op](left, right, out, validity, failures)
}

// MathUnaryUint64 evaluates a native-width unary batch. Invalid lanes become zero.
// op must be MathAbs, MathRound, MathFloor, or MathCeil; check MathUnaryID when dynamic.
func (s Set) MathUnaryUint64(in, out []uint64, validity memory.Bitmap, op MathUnaryOp) {
	s.d.math.unaryUint64[op](in, out, validity)
}

// MathBinaryUint64 evaluates a native-width binary batch and reports invalid integral operations.
// failures, when non-nil, is cleared then marks failing active rows; the return value counts them.
// Floating domain errors produce IEEE NaN or infinity and do not mark failures.
func (s Set) MathBinaryUint64(left, right, out []uint64, validity, failures memory.Bitmap, op MathBinaryOp) int {
	return s.d.math.binaryUint64[op](left, right, out, validity, failures)
}

// MathUnaryFloat32 evaluates a native-width unary batch. Invalid lanes become zero.
func (s Set) MathUnaryFloat32(in, out []float32, validity memory.Bitmap, op MathUnaryOp) {
	s.d.math.unaryFloat32[op](in, out, validity)
}

// MathBinaryFloat32 evaluates a native-width binary batch and reports invalid integral operations.
// failures, when non-nil, is cleared then marks failing active rows; the return value counts them.
// Floating domain errors produce IEEE NaN or infinity and do not mark failures.
func (s Set) MathBinaryFloat32(left, right, out []float32, validity, failures memory.Bitmap, op MathBinaryOp) int {
	return s.d.math.binaryFloat32[op](left, right, out, validity, failures)
}

// MathUnaryFloat64 evaluates a native-width unary batch. Invalid lanes become zero.
func (s Set) MathUnaryFloat64(in, out []float64, validity memory.Bitmap, op MathUnaryOp) {
	s.d.math.unaryFloat64[op](in, out, validity)
}

// MathBinaryFloat64 evaluates a native-width binary batch and reports invalid integral operations.
// failures, when non-nil, is cleared then marks failing active rows; the return value counts them.
// Floating domain errors produce IEEE NaN or infinity and do not mark failures.
func (s Set) MathBinaryFloat64(left, right, out []float64, validity, failures memory.Bitmap, op MathBinaryOp) int {
	return s.d.math.binaryFloat64[op](left, right, out, validity, failures)
}

func scalarMathIdentity[T mathSigned | mathUnsigned](in, out []T, validity memory.Bitmap) {
	if len(out) < len(in) {
		panic("kernel: math output too small")
	}
	for i, v := range in {
		if validAt(validity, i) {
			out[i] = v
		} else {
			out[i] = 0
		}
	}
}
