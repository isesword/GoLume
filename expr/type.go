package expr

import (
	"fmt"
	"gocolumn/memory"
	"unicode/utf8"
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
		if !utf8.ValidString(n.Value) {
			return TypeInfo{}, fmt.Errorf("string literal is not valid UTF-8")
		}
		return TypeInfo{Type: memory.TypeString}, nil
	case Date32Literal:
		return TypeInfo{Type: memory.TypeDate32}, nil
	case TimestampLiteral:
		return TypeInfo{Type: memory.TypeTimestamp}, nil
	case DurationLiteral:
		return TypeInfo{Type: memory.TypeDuration}, nil
	case TimestampUnitLiteral:
		if _, err := n.Nanoseconds(); err != nil {
			return TypeInfo{}, err
		}
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
		case OpAdd, OpSub:
			if temporal, ok := temporalArithmeticType(n.Op, left.Type, right.Type); ok {
				return TypeInfo{Type: temporal, Nullable: nullable}, nil
			}
			if isTemporalType(left.Type) || isTemporalType(right.Type) || left.Type == memory.TypeDuration || right.Type == memory.TypeDuration {
				return TypeInfo{}, fmt.Errorf("operator %s does not support temporal operands %s and %s", n.Op, left.Type, right.Type)
			}
			t, err := CommonNumericType(left.Type, right.Type)
			if err != nil {
				return TypeInfo{}, fmt.Errorf("operator %s: %w", n.Op, err)
			}
			return TypeInfo{Type: t, Nullable: nullable}, nil
		case OpMul:
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
		case OpFind:
			if left.Type != memory.TypeString || right.Type != memory.TypeString {
				return TypeInfo{}, fmt.Errorf("FIND requires string operands, got %s and %s", left.Type, right.Type)
			}
			return TypeInfo{Type: memory.TypeInt64, Nullable: nullable}, nil
		case OpSplit:
			if left.Type != memory.TypeString || right.Type != memory.TypeString {
				return TypeInfo{}, fmt.Errorf("SPLIT requires string value and delimiter, got %s and %s", left.Type, right.Type)
			}
			return TypeInfo{Type: memory.TypeList, Nullable: nullable}, nil
		case OpContains, OpStartsWith, OpEndsWith, OpRegexMatch:
			return TypeInfo{}, fmt.Errorf("operator %s is a filter predicate and cannot be projected", n.Op)
		default:
			return TypeInfo{}, fmt.Errorf("operator %s is not a scalar projection operator", n.Op)
		}
	case Unary:
		in, err := InferScalarType(schema, n.Value)
		if err != nil {
			return TypeInfo{}, err
		}
		switch n.Op {
		case OpLength, OpByteLength:
			if in.Type != memory.TypeString {
				return TypeInfo{}, fmt.Errorf("%s requires string input, got %s", n.Op, in.Type)
			}
			return TypeInfo{Type: memory.TypeInt64, Nullable: in.Nullable}, nil
		case OpTrim, OpTrimStart, OpTrimEnd, OpLower, OpUpper:
			if in.Type != memory.TypeString {
				return TypeInfo{}, fmt.Errorf("%s requires string input, got %s", n.Op, in.Type)
			}
			return TypeInfo{Type: memory.TypeString, Nullable: in.Nullable}, nil
		case OpYear, OpMonth, OpDay:
			if in.Type != memory.TypeDate32 && in.Type != memory.TypeDate64 && in.Type != memory.TypeTimestamp {
				return TypeInfo{}, fmt.Errorf("%s requires date32, date64, or timestamp input, got %s", n.Op, in.Type)
			}
		case OpHour, OpMinute, OpSecond:
			if in.Type != memory.TypeTimestamp {
				return TypeInfo{}, fmt.Errorf("%s requires timestamp input, got %s", n.Op, in.Type)
			}
		default:
			return TypeInfo{}, fmt.Errorf("operator %s is not a scalar projection operator", n.Op)
		}
		return TypeInfo{Type: memory.TypeInt64, Nullable: in.Nullable}, nil
	case Ternary:
		value, err := InferScalarType(schema, n.Value)
		if err != nil {
			return TypeInfo{}, err
		}
		first, err := InferScalarType(schema, n.First)
		if err != nil {
			return TypeInfo{}, err
		}
		second, err := InferScalarType(schema, n.Second)
		if err != nil {
			return TypeInfo{}, err
		}
		nullable := value.Nullable || first.Nullable || second.Nullable
		switch n.Op {
		case OpReplace, OpReplaceAll:
			if value.Type != memory.TypeString || first.Type != memory.TypeString || second.Type != memory.TypeString {
				return TypeInfo{}, fmt.Errorf("%s requires string value, old text, and new text; got %s, %s, and %s", n.Op, value.Type, first.Type, second.Type)
			}
			return TypeInfo{Type: memory.TypeString, Nullable: nullable}, nil
		case OpSlice:
			if value.Type != memory.TypeString || !isSignedInteger(first.Type) || !isSignedInteger(second.Type) {
				return TypeInfo{}, fmt.Errorf("SLICE requires string value and signed integer offset/length, got %s, %s, and %s", value.Type, first.Type, second.Type)
			}
			return TypeInfo{Type: memory.TypeString, Nullable: nullable}, nil
		case OpRegexExtract, OpRegexExtractAll:
			if value.Type != memory.TypeString || first.Type != memory.TypeString || !isSignedInteger(second.Type) {
				return TypeInfo{}, fmt.Errorf("%s requires string value, string-literal pattern, and int64-literal group; got %s, %s, and %s", n.Op, value.Type, first.Type, second.Type)
			}
			pattern, ok := n.First.(StringLiteral)
			if !ok {
				return TypeInfo{}, fmt.Errorf("%s pattern must be a string literal", n.Op)
			}
			group, ok := n.Second.(Int64Literal)
			if !ok {
				return TypeInfo{}, fmt.Errorf("%s group must be an int64 literal", n.Op)
			}
			if _, _, err := compileRegexExtract(pattern.Value, group.Value); err != nil {
				return TypeInfo{}, err
			}
			if n.Op == OpRegexExtractAll {
				return TypeInfo{Type: memory.TypeList, Nullable: value.Nullable}, nil
			}
			return TypeInfo{Type: memory.TypeString, Nullable: true}, nil
		default:
			return TypeInfo{}, fmt.Errorf("operator %s is not a ternary scalar projection operator", n.Op)
		}
	default:
		return TypeInfo{}, fmt.Errorf("unsupported scalar expression node %T", e)
	}
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
		return true
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
	if from == memory.TypeString && (to == memory.TypeTimestamp || to == memory.TypeDuration) {
		return true
	}
	if to == memory.TypeString && (IsNumeric(from) || from == memory.TypeDate32 || from == memory.TypeTimestamp) {
		return true
	}
	return false
}
