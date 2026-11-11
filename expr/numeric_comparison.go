package expr

import (
	"gocolumn/kernel"
	"gocolumn/memory"
)

// evalNumericRegisterComparison reuses the kernel set's guarded dispatch for
// computed values, not just source columns. Matching logical types are required:
// converting mixed signed/unsigned or integer/float operands would lose the
// exact comparison semantics of compareRegisters. Non-numeric domains retain
// that evaluator as well. The caller has already intersected input validity,
// including the demand masks of guarded regions, into out.validity.
func evalNumericRegisterComparison(out, a, b *scalarRegister, op Op, rows int, kernels kernel.Set) bool {
	if a.typ != b.typ || (a.constant && b.constant) {
		return false
	}
	var comparison kernel.CompareOp
	switch op {
	case OpGT:
		comparison = kernel.CompareGT
	case OpGE:
		comparison = kernel.CompareGE
	case OpLT:
		comparison = kernel.CompareLT
	case OpLE:
		comparison = kernel.CompareLE
	case OpEQ:
		comparison = kernel.CompareEQ
	default:
		return false
	}
	if a.constant {
		a, b = b, a
		switch comparison {
		case kernel.CompareGT:
			comparison = kernel.CompareLT
		case kernel.CompareGE:
			comparison = kernel.CompareLE
		case kernel.CompareLT:
			comparison = kernel.CompareGT
		case kernel.CompareLE:
			comparison = kernel.CompareGE
		}
	}
	if b.constant {
		// These are the numeric types with compact literal payloads. Do not
		// materialize a broadcast vector or narrow a literal to fit a column.
		switch a.typ {
		case memory.TypeInt64:
			kernels.CompareInt64(a.i64[:rows], out.validity, out.Truth, b.i64lit, comparison)
		case memory.TypeFloat64:
			kernels.CompareFloat64(a.f64[:rows], out.validity, out.Truth, b.f64lit, comparison)
		default:
			return false
		}
		return true
	}
	switch a.typ {
	case memory.TypeInt8:
		kernels.CompareInt8Columns(a.i8[:rows], b.i8[:rows], out.validity, nil, out.Truth, comparison)
	case memory.TypeInt16:
		kernels.CompareInt16Columns(a.i16[:rows], b.i16[:rows], out.validity, nil, out.Truth, comparison)
	case memory.TypeInt32:
		kernels.CompareInt32Columns(a.i32[:rows], b.i32[:rows], out.validity, nil, out.Truth, comparison)
	case memory.TypeInt64:
		kernels.CompareInt64Columns(a.i64[:rows], b.i64[:rows], out.validity, nil, out.Truth, comparison)
	case memory.TypeUint8:
		kernels.CompareUint8Columns(a.u8[:rows], b.u8[:rows], out.validity, nil, out.Truth, comparison)
	case memory.TypeUint16:
		kernels.CompareUint16Columns(a.u16[:rows], b.u16[:rows], out.validity, nil, out.Truth, comparison)
	case memory.TypeUint32:
		kernels.CompareUint32Columns(a.u32[:rows], b.u32[:rows], out.validity, nil, out.Truth, comparison)
	case memory.TypeUint64:
		kernels.CompareUint64Columns(a.u64[:rows], b.u64[:rows], out.validity, nil, out.Truth, comparison)
	case memory.TypeFloat32:
		kernels.CompareFloat32Columns(a.f32[:rows], b.f32[:rows], out.validity, nil, out.Truth, comparison)
	case memory.TypeFloat64:
		kernels.CompareFloat64Columns(a.f64[:rows], b.f64[:rows], out.validity, nil, out.Truth, comparison)
	default:
		return false
	}
	return true
}
