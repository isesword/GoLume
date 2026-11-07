package expr

import (
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// Abs returns the native-width absolute value; signed minimum values wrap.
func Abs(value Expr) Unary { return Unary{Op: OpAbs, Value: value} }

// Round rounds to zero decimal places with ties to even.
func Round(value Expr) Unary { return Unary{Op: OpRound, Value: value} }

// Floor rounds toward negative infinity and retains the numeric type.
func Floor(value Expr) Unary { return Unary{Op: OpFloor, Value: value} }

// Ceil rounds toward positive infinity and retains the numeric type.
func Ceil(value Expr) Unary { return Unary{Op: OpCeil, Value: value} }

// Sqrt computes the square root; integer inputs become float64.
func Sqrt(value Expr) Unary { return Unary{Op: OpSqrt, Value: value} }

// Log computes the natural logarithm; integer inputs become float64.
func Log(value Expr) Unary { return Unary{Op: OpLog, Value: value} }

// Mod computes divisor-signed remainder; an integer zero divisor produces NULL.
func Mod(left, right Expr) Binary { return Binary{Op: OpMod, Left: left, Right: right} }

// FloorDiv computes division rounded down; an integer zero divisor produces NULL.
func FloorDiv(left, right Expr) Binary { return Binary{Op: OpFloorDiv, Left: left, Right: right} }

// Pow computes exponentiation; negative integral exponents are errors.
func Pow(left, right Expr) Binary { return Binary{Op: OpPow, Left: left, Right: right} }

// Abs applies abs to this expression.
func (v Column) Abs() Unary { return Abs(v) }

// Round applies round to this expression.
func (v Column) Round() Unary { return Round(v) }

// Floor applies floor to this expression.
func (v Column) Floor() Unary { return Floor(v) }

// Ceil applies ceil to this expression.
func (v Column) Ceil() Unary { return Ceil(v) }

// Sqrt applies sqrt to this expression.
func (v Column) Sqrt() Unary { return Sqrt(v) }

// Log applies log to this expression.
func (v Column) Log() Unary { return Log(v) }

// Mod applies mod with right to this expression.
func (v Column) Mod(right Expr) Binary { return Mod(v, right) }

// FloorDiv applies floordiv with right to this expression.
func (v Column) FloorDiv(right Expr) Binary { return FloorDiv(v, right) }

// Pow applies pow with right to this expression.
func (v Column) Pow(right Expr) Binary { return Pow(v, right) }

// Abs applies abs to this expression.
func (v Binary) Abs() Unary { return Abs(v) }

// Round applies round to this expression.
func (v Binary) Round() Unary { return Round(v) }

// Floor applies floor to this expression.
func (v Binary) Floor() Unary { return Floor(v) }

// Ceil applies ceil to this expression.
func (v Binary) Ceil() Unary { return Ceil(v) }

// Sqrt applies sqrt to this expression.
func (v Binary) Sqrt() Unary { return Sqrt(v) }

// Log applies log to this expression.
func (v Binary) Log() Unary { return Log(v) }

// Mod applies mod with right to this expression.
func (v Binary) Mod(right Expr) Binary { return Mod(v, right) }

// FloorDiv applies floordiv with right to this expression.
func (v Binary) FloorDiv(right Expr) Binary { return FloorDiv(v, right) }

// Pow applies pow with right to this expression.
func (v Binary) Pow(right Expr) Binary { return Pow(v, right) }

// Abs applies abs to this expression.
func (v Unary) Abs() Unary { return Abs(v) }

// Round applies round to this expression.
func (v Unary) Round() Unary { return Round(v) }

// Floor applies floor to this expression.
func (v Unary) Floor() Unary { return Floor(v) }

// Ceil applies ceil to this expression.
func (v Unary) Ceil() Unary { return Ceil(v) }

// Sqrt applies sqrt to this expression.
func (v Unary) Sqrt() Unary { return Sqrt(v) }

// Log applies log to this expression.
func (v Unary) Log() Unary { return Log(v) }

// Mod applies mod with right to this expression.
func (v Unary) Mod(right Expr) Binary { return Mod(v, right) }

// FloorDiv applies floordiv with right to this expression.
func (v Unary) FloorDiv(right Expr) Binary { return FloorDiv(v, right) }

// Pow applies pow with right to this expression.
func (v Unary) Pow(right Expr) Binary { return Pow(v, right) }

// Abs applies abs to this expression.
func (v Cast) Abs() Unary { return Abs(v) }

// Round applies round to this expression.
func (v Cast) Round() Unary { return Round(v) }

// Floor applies floor to this expression.
func (v Cast) Floor() Unary { return Floor(v) }

// Ceil applies ceil to this expression.
func (v Cast) Ceil() Unary { return Ceil(v) }

// Sqrt applies sqrt to this expression.
func (v Cast) Sqrt() Unary { return Sqrt(v) }

// Log applies log to this expression.
func (v Cast) Log() Unary { return Log(v) }

// Mod applies mod with right to this expression.
func (v Cast) Mod(right Expr) Binary { return Mod(v, right) }

// FloorDiv applies floordiv with right to this expression.
func (v Cast) FloorDiv(right Expr) Binary { return FloorDiv(v, right) }

// Pow applies pow with right to this expression.
func (v Cast) Pow(right Expr) Binary { return Pow(v, right) }

// Abs applies abs to this expression.
func (v TryCastExpr) Abs() Unary { return Abs(v) }

// Round applies round to this expression.
func (v TryCastExpr) Round() Unary { return Round(v) }

// Floor applies floor to this expression.
func (v TryCastExpr) Floor() Unary { return Floor(v) }

// Ceil applies ceil to this expression.
func (v TryCastExpr) Ceil() Unary { return Ceil(v) }

// Sqrt applies sqrt to this expression.
func (v TryCastExpr) Sqrt() Unary { return Sqrt(v) }

// Log applies log to this expression.
func (v TryCastExpr) Log() Unary { return Log(v) }

// Mod applies mod with right to this expression.
func (v TryCastExpr) Mod(right Expr) Binary { return Mod(v, right) }

// FloorDiv applies floordiv with right to this expression.
func (v TryCastExpr) FloorDiv(right Expr) Binary { return FloorDiv(v, right) }

// Pow applies pow with right to this expression.
func (v TryCastExpr) Pow(right Expr) Binary { return Pow(v, right) }

// Abs applies abs to this expression.
func (v Int64Literal) Abs() Unary { return Abs(v) }

// Round applies round to this expression.
func (v Int64Literal) Round() Unary { return Round(v) }

// Floor applies floor to this expression.
func (v Int64Literal) Floor() Unary { return Floor(v) }

// Ceil applies ceil to this expression.
func (v Int64Literal) Ceil() Unary { return Ceil(v) }

// Sqrt applies sqrt to this expression.
func (v Int64Literal) Sqrt() Unary { return Sqrt(v) }

// Log applies log to this expression.
func (v Int64Literal) Log() Unary { return Log(v) }

// Mod applies mod with right to this expression.
func (v Int64Literal) Mod(right Expr) Binary { return Mod(v, right) }

// FloorDiv applies floordiv with right to this expression.
func (v Int64Literal) FloorDiv(right Expr) Binary { return FloorDiv(v, right) }

// Pow applies pow with right to this expression.
func (v Int64Literal) Pow(right Expr) Binary { return Pow(v, right) }

// Abs applies abs to this expression.
func (v Float64Literal) Abs() Unary { return Abs(v) }

// Round applies round to this expression.
func (v Float64Literal) Round() Unary { return Round(v) }

// Floor applies floor to this expression.
func (v Float64Literal) Floor() Unary { return Floor(v) }

// Ceil applies ceil to this expression.
func (v Float64Literal) Ceil() Unary { return Ceil(v) }

// Sqrt applies sqrt to this expression.
func (v Float64Literal) Sqrt() Unary { return Sqrt(v) }

// Log applies log to this expression.
func (v Float64Literal) Log() Unary { return Log(v) }

// Mod applies mod with right to this expression.
func (v Float64Literal) Mod(right Expr) Binary { return Mod(v, right) }

// FloorDiv applies floordiv with right to this expression.
func (v Float64Literal) FloorDiv(right Expr) Binary { return FloorDiv(v, right) }

// Pow applies pow with right to this expression.
func (v Float64Literal) Pow(right Expr) Binary { return Pow(v, right) }

// Abs applies abs to this expression.
func (v IfExpr) Abs() Unary { return Abs(v) }

// Round applies round to this expression.
func (v IfExpr) Round() Unary { return Round(v) }

// Floor applies floor to this expression.
func (v IfExpr) Floor() Unary { return Floor(v) }

// Ceil applies ceil to this expression.
func (v IfExpr) Ceil() Unary { return Ceil(v) }

// Sqrt applies sqrt to this expression.
func (v IfExpr) Sqrt() Unary { return Sqrt(v) }

// Log applies log to this expression.
func (v IfExpr) Log() Unary { return Log(v) }

// Mod applies mod with right to this expression.
func (v IfExpr) Mod(right Expr) Binary { return Mod(v, right) }

// FloorDiv applies floordiv with right to this expression.
func (v IfExpr) FloorDiv(right Expr) Binary { return FloorDiv(v, right) }

// Pow applies pow with right to this expression.
func (v IfExpr) Pow(right Expr) Binary { return Pow(v, right) }

// Abs applies abs to this expression.
func (v CaseExpr) Abs() Unary { return Abs(v) }

// Round applies round to this expression.
func (v CaseExpr) Round() Unary { return Round(v) }

// Floor applies floor to this expression.
func (v CaseExpr) Floor() Unary { return Floor(v) }

// Ceil applies ceil to this expression.
func (v CaseExpr) Ceil() Unary { return Ceil(v) }

// Sqrt applies sqrt to this expression.
func (v CaseExpr) Sqrt() Unary { return Sqrt(v) }

// Log applies log to this expression.
func (v CaseExpr) Log() Unary { return Log(v) }

// Mod applies mod with right to this expression.
func (v CaseExpr) Mod(right Expr) Binary { return Mod(v, right) }

// FloorDiv applies floordiv with right to this expression.
func (v CaseExpr) FloorDiv(right Expr) Binary { return FloorDiv(v, right) }

// Pow applies pow with right to this expression.
func (v CaseExpr) Pow(right Expr) Binary { return Pow(v, right) }

// Abs applies abs to this expression.
func (v CoalesceExpr) Abs() Unary { return Abs(v) }

// Round applies round to this expression.
func (v CoalesceExpr) Round() Unary { return Round(v) }

// Floor applies floor to this expression.
func (v CoalesceExpr) Floor() Unary { return Floor(v) }

// Ceil applies ceil to this expression.
func (v CoalesceExpr) Ceil() Unary { return Ceil(v) }

// Sqrt applies sqrt to this expression.
func (v CoalesceExpr) Sqrt() Unary { return Sqrt(v) }

// Log applies log to this expression.
func (v CoalesceExpr) Log() Unary { return Log(v) }

// Mod applies mod with right to this expression.
func (v CoalesceExpr) Mod(right Expr) Binary { return Mod(v, right) }

// FloorDiv applies floordiv with right to this expression.
func (v CoalesceExpr) FloorDiv(right Expr) Binary { return FloorDiv(v, right) }

// Pow applies pow with right to this expression.
func (v CoalesceExpr) Pow(right Expr) Binary { return Pow(v, right) }

func mathUnaryOp(op Op) (kernel.MathUnaryOp, bool) {
	switch op {
	case OpAbs:
		return kernel.MathAbs, true
	case OpRound:
		return kernel.MathRound, true
	case OpFloor:
		return kernel.MathFloor, true
	case OpCeil:
		return kernel.MathCeil, true
	case OpSqrt:
		return kernel.MathSqrt, true
	case OpLog:
		return kernel.MathLog, true
	}
	return 0, false
}
func mathBinaryOp(op Op) (kernel.MathBinaryOp, bool) {
	switch op {
	case OpMod:
		return kernel.MathMod, true
	case OpFloorDiv:
		return kernel.MathFloorDiv, true
	case OpPow:
		return kernel.MathPow, true
	}
	return 0, false
}
func mathBoundCast(child *BoundExpr, typ memory.DataType) *BoundExpr {
	if child.Field.Type == typ {
		return child
	}
	return &BoundExpr{Expr: CastTo(child.Expr, typ), Field: computedField(typ, child.Field.Nullable), MayError: child.MayError || (castMayError(child.Field.Type, typ) && !mathLiteralFits(child, typ)), Columns: child.Columns, Children: []*BoundExpr{child}}
}
func bindMathUnary(bound *BoundExpr, n Unary) error {
	child := bound.Children[0]
	typ := child.Field.Type
	if !IsNumeric(typ) {
		return fmt.Errorf("%s requires numeric input, got %s", n.Op, typ)
	}
	if (n.Op == OpSqrt || n.Op == OpLog) && typ != memory.TypeFloat32 && typ != memory.TypeFloat64 {
		typ = memory.TypeFloat64
	}
	bound.Children[0] = mathBoundCast(child, typ)
	bound.Field = computedField(typ, child.Field.Nullable)
	return nil
}

func mathLiteralFits(child *BoundExpr, typ memory.DataType) bool {
	lit, ok := child.Expr.(Int64Literal)
	if !ok {
		return false
	}
	if isSignedInteger(typ) {
		width := integerWidth(typ)
		return width == 64 || (lit.Value >= -(int64(1)<<(width-1)) && lit.Value <= (int64(1)<<(width-1))-1)
	}
	if unsignedInteger(typ) {
		width := integerWidth(typ)
		return lit.Value >= 0 && (width == 64 || uint64(lit.Value) <= (uint64(1)<<width)-1)
	}
	return false
}
func bindMathBinary(bound *BoundExpr, n Binary) error {
	left, right := bound.Children[0], bound.Children[1]
	lt, rt := left.Field.Type, right.Field.Type
	if !IsNumeric(lt) || !IsNumeric(rt) {
		return fmt.Errorf("%s requires numeric operands, got %s and %s", n.Op, lt, rt)
	}
	typ := lt
	if lt != rt {
		switch {
		case unsignedInteger(lt) && unsignedInteger(rt):
			if integerWidth(rt) > integerWidth(lt) {
				typ = rt
			}
		case mathLiteralFits(right, lt):
			typ = lt
		case mathLiteralFits(left, rt):
			typ = rt
		case (lt == memory.TypeUint64 && isSignedInteger(rt)) || (rt == memory.TypeUint64 && isSignedInteger(lt)):
			return fmt.Errorf("%s cannot losslessly combine uint64 and signed integer columns; cast explicitly", n.Op)
		default:
			var err error
			typ, err = CommonNumericType(lt, rt)
			if err != nil {
				return err
			}
		}
	}
	bound.Children[0], bound.Children[1] = mathBoundCast(left, typ), mathBoundCast(right, typ)
	nullable := left.Field.Nullable || right.Field.Nullable
	integral := isSignedInteger(typ) || unsignedInteger(typ)
	if integral && (n.Op == OpMod || n.Op == OpFloorDiv) {
		nullable = true
	}
	bound.Field = computedField(typ, nullable)
	bound.MayError = n.Op == OpPow && isSignedInteger(typ)
	return nil
}

type mathNumber interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

func mathMaskSlice(valid memory.Bitmap, row int) memory.Bitmap {
	if valid == nil {
		return nil
	}
	return valid[row>>6:]
}
func evalMathUnaryBatch[T mathNumber](r *scalarRegister, in, out []T, rows int, op kernel.MathUnaryOp, fn func([]T, []T, memory.Bitmap, kernel.MathUnaryOp)) error {
	for start := 0; start < rows; start += 4096 {
		if r.checkContext != nil {
			if err := r.checkContext(); err != nil {
				return err
			}
		}
		end := min(rows, start+4096)
		fn(in[start:end], out[start:end], mathMaskSlice(r.validity, start), op)
	}
	return nil
}
func evalMathBinaryBatch[T mathNumber](r *scalarRegister, left, right, out []T, rows int, op kernel.MathBinaryOp, fn func([]T, []T, []T, memory.Bitmap, memory.Bitmap, kernel.MathBinaryOp) int) error {
	integral := isSignedInteger(r.typ) || unsignedInteger(r.typ)
	if integral {
		for row, v := range right[:rows] {
			if row&255 == 0 && r.checkContext != nil {
				if err := r.checkContext(); err != nil {
					return err
				}
			}
			if !isValidRegister(r, row) {
				continue
			}
			if op == kernel.MathPow && v < 0 {
				return fmt.Errorf("POW negative integer exponent at row %d", row)
			}
			if op != kernel.MathPow && v == 0 {
				if r.validity == nil {
					v := ensureValidity(r, rows)
					for i := range v {
						v[i] = ^uint64(0)
					}
					if len(v) > 0 {
						v[len(v)-1] &= memory.TailMask(rows)
					}
				}
				r.validity[row>>6] &^= uint64(1) << uint(row&63)
			}
		}
	}
	for start := 0; start < rows; start += 4096 {
		if r.checkContext != nil {
			if err := r.checkContext(); err != nil {
				return err
			}
		}
		end := min(rows, start+4096)
		if count := fn(left[start:end], right[start:end], out[start:end], mathMaskSlice(r.validity, start), nil, op); count != 0 {
			return fmt.Errorf("%s unexpected invalid numeric operation", op)
		}
	}
	return nil
}
func evalMathUnary(out, in *scalarRegister, op kernel.MathUnaryOp, rows int, kernels kernel.Set) error {
	copyRegisterValidity(out, in, rows)
	switch out.typ {
	case memory.TypeInt8:
		return evalMathUnaryBatch(out, in.i8, out.i8, rows, op, kernels.MathUnaryInt8)
	case memory.TypeInt16:
		return evalMathUnaryBatch(out, in.i16, out.i16, rows, op, kernels.MathUnaryInt16)
	case memory.TypeInt32:
		return evalMathUnaryBatch(out, in.i32, out.i32, rows, op, kernels.MathUnaryInt32)
	case memory.TypeInt64:
		return evalMathUnaryBatch(out, in.i64, out.i64, rows, op, kernels.MathUnaryInt64)
	case memory.TypeUint8:
		return evalMathUnaryBatch(out, in.u8, out.u8, rows, op, kernels.MathUnaryUint8)
	case memory.TypeUint16:
		return evalMathUnaryBatch(out, in.u16, out.u16, rows, op, kernels.MathUnaryUint16)
	case memory.TypeUint32:
		return evalMathUnaryBatch(out, in.u32, out.u32, rows, op, kernels.MathUnaryUint32)
	case memory.TypeUint64:
		return evalMathUnaryBatch(out, in.u64, out.u64, rows, op, kernels.MathUnaryUint64)
	case memory.TypeFloat32:
		return evalMathUnaryBatch(out, in.f32, out.f32, rows, op, kernels.MathUnaryFloat32)
	case memory.TypeFloat64:
		return evalMathUnaryBatch(out, in.f64, out.f64, rows, op, kernels.MathUnaryFloat64)
	default:
		return fmt.Errorf("unsupported math output type %s", out.typ)
	}
}
func evalMathBinary(out, left, right *scalarRegister, op kernel.MathBinaryOp, rows int, kernels kernel.Set) error {
	combineValidity(out, left, right, rows)
	switch out.typ {
	case memory.TypeInt8:
		return evalMathBinaryBatch(out, left.i8, right.i8, out.i8, rows, op, kernels.MathBinaryInt8)
	case memory.TypeInt16:
		return evalMathBinaryBatch(out, left.i16, right.i16, out.i16, rows, op, kernels.MathBinaryInt16)
	case memory.TypeInt32:
		return evalMathBinaryBatch(out, left.i32, right.i32, out.i32, rows, op, kernels.MathBinaryInt32)
	case memory.TypeInt64:
		return evalMathBinaryBatch(out, left.i64, right.i64, out.i64, rows, op, kernels.MathBinaryInt64)
	case memory.TypeUint8:
		return evalMathBinaryBatch(out, left.u8, right.u8, out.u8, rows, op, kernels.MathBinaryUint8)
	case memory.TypeUint16:
		return evalMathBinaryBatch(out, left.u16, right.u16, out.u16, rows, op, kernels.MathBinaryUint16)
	case memory.TypeUint32:
		return evalMathBinaryBatch(out, left.u32, right.u32, out.u32, rows, op, kernels.MathBinaryUint32)
	case memory.TypeUint64:
		return evalMathBinaryBatch(out, left.u64, right.u64, out.u64, rows, op, kernels.MathBinaryUint64)
	case memory.TypeFloat32:
		return evalMathBinaryBatch(out, left.f32, right.f32, out.f32, rows, op, kernels.MathBinaryFloat32)
	case memory.TypeFloat64:
		return evalMathBinaryBatch(out, left.f64, right.f64, out.f64, rows, op, kernels.MathBinaryFloat64)
	default:
		return fmt.Errorf("unsupported math output type %s", out.typ)
	}
}
