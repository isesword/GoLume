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

// InferScalarType validates e against schema and returns its result type. It
// centralizes Phase 2.6 numeric coercion and explicit cast rules so logical and
// physical compilation use identical semantics.
func InferScalarType(schema memory.Schema, e Expr) (TypeInfo, error) {
	switch n := e.(type) {
	case Column:
		f, ok := schema.Field(n.Name)
		if !ok {
			return TypeInfo{}, fmt.Errorf("column %q not found", n.Name)
		}
		return TypeInfo{Type: f.Type, Nullable: f.Nullable}, nil
	case Int64Literal:
		return TypeInfo{Type: memory.TypeInt64}, nil
	case Float64Literal:
		return TypeInfo{Type: memory.TypeFloat64}, nil
	case StringLiteral:
		return TypeInfo{Type: memory.TypeString}, nil
	case Date32Literal:
		return TypeInfo{Type: memory.TypeDate32}, nil
	case TimestampLiteral:
		return TypeInfo{Type: memory.TypeTimestamp}, nil
	case Cast:
		in, err := InferScalarType(schema, n.Value)
		if err != nil {
			return TypeInfo{}, err
		}
		if !CanCast(in.Type, n.Target) {
			return TypeInfo{}, fmt.Errorf("cannot cast %s to %s", in.Type, n.Target)
		}
		return TypeInfo{Type: n.Target, Nullable: in.Nullable}, nil
	case Binary:
		left, err := InferScalarType(schema, n.Left)
		if err != nil {
			return TypeInfo{}, err
		}
		right, err := InferScalarType(schema, n.Right)
		if err != nil {
			return TypeInfo{}, err
		}
		nullable := left.Nullable || right.Nullable
		switch n.Op {
		case OpAdd, OpSub, OpMul:
			t, err := CommonNumericType(left.Type, right.Type)
			if err != nil {
				return TypeInfo{}, fmt.Errorf("operator %s: %w", n.Op, err)
			}
			return TypeInfo{Type: t, Nullable: nullable}, nil
		case OpDiv:
			if !IsNumeric(left.Type) || !IsNumeric(right.Type) {
				return TypeInfo{}, fmt.Errorf("operator / requires numeric operands, got %s and %s", left.Type, right.Type)
			}
			return TypeInfo{Type: memory.TypeFloat64, Nullable: nullable}, nil
		case OpConcat:
			if left.Type != memory.TypeString || right.Type != memory.TypeString {
				return TypeInfo{}, fmt.Errorf("operator || requires string operands, got %s and %s", left.Type, right.Type)
			}
			return TypeInfo{Type: memory.TypeString, Nullable: nullable}, nil
		default:
			return TypeInfo{}, fmt.Errorf("operator %s is not a scalar projection operator", n.Op)
		}
	default:
		return TypeInfo{}, fmt.Errorf("unsupported scalar expression node %T", e)
	}
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

// CanCast reports whether Phase 2.6 has an explicit scalar cast implementation
// for from -> to. Date/timestamp casts deliberately use physical epoch values
// rather than timezone-aware calendar conversion.
func CanCast(from, to memory.DataType) bool {
	if from == to {
		return true
	}
	if IsNumeric(from) && IsNumeric(to) {
		return true
	}
	if from == memory.TypeDate32 && (to == memory.TypeInt32 || to == memory.TypeInt64) {
		return true
	}
	if from == memory.TypeTimestamp && to == memory.TypeInt64 {
		return true
	}
	if from == memory.TypeInt32 && to == memory.TypeDate32 {
		return true
	}
	if from == memory.TypeInt64 && (to == memory.TypeTimestamp || to == memory.TypeDate32) {
		return true
	}
	if to == memory.TypeString && (IsNumeric(from) || from == memory.TypeDate32 || from == memory.TypeTimestamp) {
		return true
	}
	return false
}
