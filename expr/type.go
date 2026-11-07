package expr

import (
	"fmt"
	"gocolumn/memory"
)

// TypeInfo is the inferred logical type and nullability of one scalar expression.
type TypeInfo struct {
	Type     memory.DataType
	Nullable bool
}

// InferScalarType returns the type and nullability from the shared expression
// binder. Predicates and projected values use the same validation rules.
func InferScalarType(schema memory.Schema, e Expr) (TypeInfo, error) {
	bound, err := Bind(schema, e)
	if err != nil {
		return TypeInfo{}, err
	}
	return TypeInfo{Type: bound.Field.Type, Nullable: bound.Field.Nullable}, nil
}

func isTemporalType(t memory.DataType) bool {
	return t == memory.TypeDate32 || t == memory.TypeDate64 || t == memory.TypeTimestamp
}

func isSignedInteger(t memory.DataType) bool {
	switch t {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64:
		return true
	default:
		return false
	}
}

// temporalArithmeticType returns the result type for valid temporal arithmetic.
// Dates retain their existing physical-unit integer deltas; timestamp and
// duration operations always use nanoseconds.
func temporalArithmeticType(op Op, left, right memory.DataType) (memory.DataType, bool) {
	if op == OpAdd {
		if left == memory.TypeTimestamp && right == memory.TypeDuration || left == memory.TypeDuration && right == memory.TypeTimestamp {
			return memory.TypeTimestamp, true
		}
		if left == memory.TypeDuration && right == memory.TypeDuration {
			return memory.TypeDuration, true
		}
		if isTemporalType(left) && isSignedInteger(right) {
			return left, true
		}
		if isSignedInteger(left) && isTemporalType(right) {
			return right, true
		}
		return memory.TypeInvalid, false
	}
	if op == OpSub {
		if left == memory.TypeTimestamp && right == memory.TypeDuration {
			return memory.TypeTimestamp, true
		}
		if left == memory.TypeTimestamp && right == memory.TypeTimestamp {
			return memory.TypeDuration, true
		}
		if left == memory.TypeDuration && right == memory.TypeDuration {
			return memory.TypeDuration, true
		}
		if isTemporalType(left) && isSignedInteger(right) {
			return left, true
		}
	}
	return memory.TypeInvalid, false
}

// IsNumeric reports whether t participates in scalar numeric coercion.
func IsNumeric(t memory.DataType) bool {
	switch t {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
		memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
		memory.TypeFloat32, memory.TypeFloat64:
		return true
	default:
		return false
	}
}

// CommonNumericType returns the physical result type used for a binary numeric
// expression. Mixed integer/float expressions use float64; integer expressions
// use int64 unless uint64 would make that conversion lossy, in which case the
// common type is float64.
func CommonNumericType(a, b memory.DataType) (memory.DataType, error) {
	if !IsNumeric(a) || !IsNumeric(b) {
		return memory.TypeInvalid, fmt.Errorf("numeric operands required, got %s and %s", a, b)
	}
	if a == memory.TypeFloat32 || a == memory.TypeFloat64 || b == memory.TypeFloat32 || b == memory.TypeFloat64 {
		return memory.TypeFloat64, nil
	}
	if a == memory.TypeUint64 || b == memory.TypeUint64 {
		if a == memory.TypeUint64 && b == memory.TypeUint64 {
			return memory.TypeUint64, nil
		}
		return memory.TypeFloat64, nil
	}
	return memory.TypeInt64, nil
}

// CanCast reports whether the scalar engine has an explicit implementation for
// from -> to. String timestamp casts use the documented RFC3339/UTC parser;
// date and integer temporal casts otherwise use physical epoch values.
func CanCast(from, to memory.DataType) bool {
	if from == to {
		return conditionalScalarType(from) || from == memory.TypeBinary
	}
	if IsNumeric(from) && IsNumeric(to) {
		return true
	}
	if from == memory.TypeDate32 && (to == memory.TypeInt32 || to == memory.TypeInt64) {
		return true
	}
	if (from == memory.TypeTimestamp || from == memory.TypeDuration) && to == memory.TypeInt64 {
		return true
	}
	if from == memory.TypeInt32 && to == memory.TypeDate32 {
		return true
	}
	if from == memory.TypeInt64 && (to == memory.TypeTimestamp || to == memory.TypeDate32 || to == memory.TypeDuration) {
		return true
	}
	if from == memory.TypeString && (IsNumeric(to) || to == memory.TypeTimestamp || to == memory.TypeDuration) {
		return true
	}
	if to == memory.TypeString && (IsNumeric(from) || from == memory.TypeDate32 || from == memory.TypeTimestamp) {
		return true
	}
	return false
}
