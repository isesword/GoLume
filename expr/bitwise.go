package expr

import (
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// BitAnd returns the native-width bitwise AND of two integers. Operands must
// have the same Int32, Uint32, Int64, or Uint64 type, except an Int64 literal
// may be converted losslessly to the other operand's type. NULL propagates.
func BitAnd(left, right Expr) Binary { return Binary{Op: OpBitAnd, Left: left, Right: right} }

// BitOr returns native-width bitwise OR with the same typing rules as BitAnd.
func BitOr(left, right Expr) Binary { return Binary{Op: OpBitOr, Left: left, Right: right} }

// BitXor returns native-width bitwise XOR with the same typing rules as BitAnd.
func BitXor(left, right Expr) Binary { return Binary{Op: OpBitXor, Left: left, Right: right} }

// BitNot complements every bit of an Int32, Uint32, Int64, or Uint64 value,
// preserving its type, width, and NULL validity.
func BitNot(value Expr) Unary { return Unary{Op: OpBitNot, Value: value} }

// BitAnd applies native-width bitwise AND to this expression.
func (v Column) BitAnd(right Expr) Binary { return BitAnd(v, right) }

// BitOr applies native-width bitwise OR to this expression.
func (v Column) BitOr(right Expr) Binary { return BitOr(v, right) }

// BitXor applies native-width bitwise XOR to this expression.
func (v Column) BitXor(right Expr) Binary { return BitXor(v, right) }

// BitNot complements every bit of this integer expression.
func (v Column) BitNot() Unary { return BitNot(v) }

// BitAnd applies native-width bitwise AND to this expression.
func (v Binary) BitAnd(right Expr) Binary { return BitAnd(v, right) }

// BitOr applies native-width bitwise OR to this expression.
func (v Binary) BitOr(right Expr) Binary { return BitOr(v, right) }

// BitXor applies native-width bitwise XOR to this expression.
func (v Binary) BitXor(right Expr) Binary { return BitXor(v, right) }

// BitNot complements every bit of this integer expression.
func (v Binary) BitNot() Unary { return BitNot(v) }

// BitAnd applies native-width bitwise AND to this expression.
func (v Unary) BitAnd(right Expr) Binary { return BitAnd(v, right) }

// BitOr applies native-width bitwise OR to this expression.
func (v Unary) BitOr(right Expr) Binary { return BitOr(v, right) }

// BitXor applies native-width bitwise XOR to this expression.
func (v Unary) BitXor(right Expr) Binary { return BitXor(v, right) }

// BitNot complements every bit of this integer expression.
func (v Unary) BitNot() Unary { return BitNot(v) }

// BitAnd applies native-width bitwise AND to this expression.
func (v Cast) BitAnd(right Expr) Binary { return BitAnd(v, right) }

// BitOr applies native-width bitwise OR to this expression.
func (v Cast) BitOr(right Expr) Binary { return BitOr(v, right) }

// BitXor applies native-width bitwise XOR to this expression.
func (v Cast) BitXor(right Expr) Binary { return BitXor(v, right) }

// BitNot complements every bit of this integer expression.
func (v Cast) BitNot() Unary { return BitNot(v) }

// BitAnd applies native-width bitwise AND to this expression.
func (v TryCastExpr) BitAnd(right Expr) Binary { return BitAnd(v, right) }

// BitOr applies native-width bitwise OR to this expression.
func (v TryCastExpr) BitOr(right Expr) Binary { return BitOr(v, right) }

// BitXor applies native-width bitwise XOR to this expression.
func (v TryCastExpr) BitXor(right Expr) Binary { return BitXor(v, right) }

// BitNot complements every bit of this integer expression.
func (v TryCastExpr) BitNot() Unary { return BitNot(v) }

// BitAnd applies native-width bitwise AND to this expression.
func (v Int64Literal) BitAnd(right Expr) Binary { return BitAnd(v, right) }

// BitOr applies native-width bitwise OR to this expression.
func (v Int64Literal) BitOr(right Expr) Binary { return BitOr(v, right) }

// BitXor applies native-width bitwise XOR to this expression.
func (v Int64Literal) BitXor(right Expr) Binary { return BitXor(v, right) }

// BitNot complements every bit of this integer expression.
func (v Int64Literal) BitNot() Unary { return BitNot(v) }

// BitAnd applies native-width bitwise AND to this expression.
func (v IfExpr) BitAnd(right Expr) Binary { return BitAnd(v, right) }

// BitOr applies native-width bitwise OR to this expression.
func (v IfExpr) BitOr(right Expr) Binary { return BitOr(v, right) }

// BitXor applies native-width bitwise XOR to this expression.
func (v IfExpr) BitXor(right Expr) Binary { return BitXor(v, right) }

// BitNot complements every bit of this integer expression.
func (v IfExpr) BitNot() Unary { return BitNot(v) }

// BitAnd applies native-width bitwise AND to this expression.
func (v CaseExpr) BitAnd(right Expr) Binary { return BitAnd(v, right) }

// BitOr applies native-width bitwise OR to this expression.
func (v CaseExpr) BitOr(right Expr) Binary { return BitOr(v, right) }

// BitXor applies native-width bitwise XOR to this expression.
func (v CaseExpr) BitXor(right Expr) Binary { return BitXor(v, right) }

// BitNot complements every bit of this integer expression.
func (v CaseExpr) BitNot() Unary { return BitNot(v) }

// BitAnd applies native-width bitwise AND to this expression.
func (v CoalesceExpr) BitAnd(right Expr) Binary { return BitAnd(v, right) }

// BitOr applies native-width bitwise OR to this expression.
func (v CoalesceExpr) BitOr(right Expr) Binary { return BitOr(v, right) }

// BitXor applies native-width bitwise XOR to this expression.
func (v CoalesceExpr) BitXor(right Expr) Binary { return BitXor(v, right) }

// BitNot complements every bit of this integer expression.
func (v CoalesceExpr) BitNot() Unary { return BitNot(v) }

func bitwiseType(typ memory.DataType) bool {
	switch typ {
	case memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64:
		return true
	}
	return false
}

func bitwiseOp(op Op) (kernel.BitwiseOp, bool) {
	switch op {
	case OpBitAnd:
		return kernel.BitwiseAnd, true
	case OpBitOr:
		return kernel.BitwiseOr, true
	case OpBitXor:
		return kernel.BitwiseXor, true
	}
	return 0, false
}

func bindBitwiseUnary(bound *BoundExpr) error {
	input := bound.Children[0].Field
	if !bitwiseType(input.Type) {
		return fmt.Errorf("BIT_NOT requires int32, uint32, int64, or uint64 input, got %s; cast explicitly to a supported integer type", input.Type)
	}
	bound.Field = computedField(input.Type, input.Nullable)
	return nil
}

func bindBitwiseBinary(bound *BoundExpr, n Binary) error {
	left, right := bound.Children[0], bound.Children[1]
	lt, rt := left.Field.Type, right.Field.Type
	if !bitwiseType(lt) || !bitwiseType(rt) {
		return fmt.Errorf("%s requires int32, uint32, int64, or uint64 operands, got %s and %s; cast explicitly to a supported integer type", n.Op, lt, rt)
	}
	typ := lt
	if lt != rt {
		// Unlike arithmetic coercion, bitwise operations never pass through
		// float64, change column signedness, or silently widen the bit domain.
		switch {
		case mathLiteralFits(right, lt):
		case mathLiteralFits(left, rt):
			typ = rt
		default:
			return fmt.Errorf("%s requires matching integer types, got %s and %s; cast explicitly", n.Op, lt, rt)
		}
	}
	bound.Children[0], bound.Children[1] = mathBoundCast(left, typ), mathBoundCast(right, typ)
	bound.Field = computedField(typ, left.Field.Nullable || right.Field.Nullable)
	return nil
}

type bitwiseInteger interface {
	~int32 | ~uint32 | ~int64 | ~uint64
}

func evalBitwiseBatch[T bitwiseInteger](out *scalarRegister, left, right, values []T, rows int, op kernel.BitwiseOp, fn func([]T, []T, []T, memory.Bitmap, kernel.BitwiseOp)) error {
	for start := 0; start < rows; start += 4096 {
		if out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		end := min(rows, start+4096)
		var b []T
		if op != kernel.BitwiseNot {
			b = right[start:end]
		}
		fn(left[start:end], b, values[start:end], mathMaskSlice(out.validity, start), op)
	}
	return nil
}

func evalBitwise(out, left, right *scalarRegister, op kernel.BitwiseOp, rows int, kernels kernel.Set) error {
	if op == kernel.BitwiseNot {
		copyRegisterValidity(out, left, rows)
	} else {
		combineValidity(out, left, right, rows)
	}
	// Literal planning keeps operands dense for these kernels. Each output
	// uses its admitted native-width buffer; no broadcast scratch is created.
	switch out.typ {
	case memory.TypeInt32:
		var b []int32
		if right != nil {
			b = right.i32
		}
		return evalBitwiseBatch(out, left.i32, b, out.i32, rows, op, kernels.BitwiseInt32)
	case memory.TypeUint32:
		var b []uint32
		if right != nil {
			b = right.u32
		}
		return evalBitwiseBatch(out, left.u32, b, out.u32, rows, op, kernels.BitwiseUint32)
	case memory.TypeInt64:
		var b []int64
		if right != nil {
			b = right.i64
		}
		return evalBitwiseBatch(out, left.i64, b, out.i64, rows, op, kernels.BitwiseInt64)
	case memory.TypeUint64:
		var b []uint64
		if right != nil {
			b = right.u64
		}
		return evalBitwiseBatch(out, left.u64, b, out.u64, rows, op, kernels.BitwiseUint64)
	default:
		return fmt.Errorf("unsupported bitwise output type %s", out.typ)
	}
}
