package oracle

import (
	"fmt"
	"math"
	"math/bits"
	"strings"

	"gocolumn/expr"
)

// tri is a SQL three-valued truth value.
type tri int8

const (
	triFalse tri = iota
	triTrue
	triNull
)

// String renders the truth value for diagnostics.
func (t tri) String() string {
	switch t {
	case triTrue:
		return "true"
	case triNull:
		return "null"
	default:
		return "false"
	}
}

// triAnd, triOr, triNot implement strict SQL three-valued logic.
func triAnd(a, b tri) tri {
	if a == triFalse || b == triFalse {
		return triFalse
	}
	if a == triNull || b == triNull {
		return triNull
	}
	return triTrue
}

func triOr(a, b tri) tri {
	if a == triTrue || b == triTrue {
		return triTrue
	}
	if a == triNull || b == triNull {
		return triNull
	}
	return triFalse
}

func triNot(a tri) tri {
	if a == triNull {
		return triNull
	}
	if a == triTrue {
		return triFalse
	}
	return triTrue
}

// rowContext binds a row to its column list for expression evaluation.
type rowContext struct {
	columns []Column
	index   map[string]int
	row     []Value
}

func newRowContext(columns []Column) (rowContext, error) {
	ctx := rowContext{columns: columns, index: make(map[string]int, len(columns))}
	for i, c := range columns {
		if _, dup := ctx.index[c.Name]; dup {
			return rowContext{}, fmt.Errorf("oracle: duplicate column %q", c.Name)
		}
		ctx.index[c.Name] = i
	}
	return ctx, nil
}

func (c rowContext) lookup(name string) (Value, error) {
	i, ok := c.index[name]
	if !ok {
		return Value{}, fmt.Errorf("oracle: column %q not found", name)
	}
	return c.row[i], nil
}

// evalPredicate evaluates e under SQL three-valued logic. A row is selected
// only when the result is triTrue; NULL and false both reject it.
func (c rowContext) evalPredicate(e expr.Expr) (tri, error) {
	switch n := e.(type) {
	case expr.Binary:
		switch n.Op {
		case expr.OpAnd:
			a, err := c.evalPredicate(n.Left)
			if err != nil {
				return triFalse, err
			}
			b, err := c.evalPredicate(n.Right)
			if err != nil {
				return triFalse, err
			}
			return triAnd(a, b), nil
		case expr.OpOr:
			a, err := c.evalPredicate(n.Left)
			if err != nil {
				return triFalse, err
			}
			b, err := c.evalPredicate(n.Right)
			if err != nil {
				return triFalse, err
			}
			return triOr(a, b), nil
		case expr.OpGT, expr.OpGE, expr.OpLT, expr.OpLE, expr.OpEQ:
			left, err := c.evalScalar(n.Left)
			if err != nil {
				return triFalse, err
			}
			right, err := c.evalScalar(n.Right)
			if err != nil {
				return triFalse, err
			}
			return comparePredicate(left, right, n.Op)
		default:
			return triFalse, fmt.Errorf("oracle: unsupported predicate operator %s", n.Op)
		}
	case expr.Unary:
		switch n.Op {
		case expr.OpNot:
			v, err := c.evalPredicate(n.Value)
			if err != nil {
				return triFalse, err
			}
			return triNot(v), nil
		case expr.OpIsNull, expr.OpIsNotNull:
			v, err := c.evalScalar(n.Value)
			if err != nil {
				return triFalse, err
			}
			isNull := triFalse
			if v.Null {
				isNull = triTrue
			}
			if n.Op == expr.OpIsNotNull {
				return triNot(isNull), nil
			}
			return isNull, nil
		default:
			return triFalse, fmt.Errorf("oracle: unsupported unary predicate operator %s", n.Op)
		}
	default:
		return triFalse, fmt.Errorf("oracle: unsupported predicate node %T", e)
	}
}

// comparePredicate compares two scalar values. NULL on either side yields
// triNull. Numeric comparisons coerce like the engine's compiled predicates:
// a float64 participant compares in float64, and a uint64/int64 mix compares
// in uint64 with a non-negative int64 operand. Strings compare in byte order,
// matching bytes.Compare and Go's string ordering.
func comparePredicate(a, b Value, op expr.Op) (tri, error) {
	if a.Null || b.Null {
		return triNull, nil
	}
	// All comparisons involving NaN are false (never NULL), mirroring the
	// engine's compareF64 which relies on plain IEEE comparisons.
	if a.Kind == KindFloat64 && math.IsNaN(a.F64) || b.Kind == KindFloat64 && math.IsNaN(b.F64) {
		return triFalse, nil
	}
	cmp, err := compareValues(a, b)
	if err != nil {
		return triFalse, err
	}
	switch op {
	case expr.OpGT:
		return triOf(cmp > 0), nil
	case expr.OpGE:
		return triOf(cmp >= 0), nil
	case expr.OpLT:
		return triOf(cmp < 0), nil
	case expr.OpLE:
		return triOf(cmp <= 0), nil
	case expr.OpEQ:
		return triOf(cmp == 0), nil
	default:
		return triFalse, fmt.Errorf("oracle: unsupported comparison operator %s", op)
	}
}

func triOf(ok bool) tri {
	if ok {
		return triTrue
	}
	return triFalse
}

// compareValues returns -1, 0, or +1 for two non-NULL values, coercing across
// numeric kinds the way the engine's typed comparison compilation does.
// Float comparisons use plain Go operators, so any NaN participant makes every
// ordering comparison false; EQ against NaN is likewise false.
func compareValues(a, b Value) (int, error) {
	if a.Kind == KindFloat64 || b.Kind == KindFloat64 {
		fa, err := valueAsFloat64(a)
		if err != nil {
			return 0, err
		}
		fb, err := valueAsFloat64(b)
		if err != nil {
			return 0, err
		}
		switch {
		case fa < fb:
			return -1, nil
		case fa > fb:
			return 1, nil
		default:
			return 0, nil
		}
	}
	if a.Kind == KindUint64 || b.Kind == KindUint64 {
		ua, err := valueAsUint64(a)
		if err != nil {
			return 0, err
		}
		ub, err := valueAsUint64(b)
		if err != nil {
			return 0, err
		}
		return cmpOrdered(ua, ub), nil
	}
	switch a.Kind {
	case KindInt64:
		if b.Kind != KindInt64 {
			return 0, fmt.Errorf("oracle: cannot compare %s with %s", a.Kind, b.Kind)
		}
		return cmpOrdered(a.I64, b.I64), nil
	case KindString:
		if b.Kind != KindString {
			return 0, fmt.Errorf("oracle: cannot compare %s with %s", a.Kind, b.Kind)
		}
		return strings.Compare(a.Str, b.Str), nil
	case KindBool:
		// The engine stores bools physically as uint32 0/1, and its compiled
		// comparisons treat them as the unsigned family; the oracle mirrors
		// that by comparing bools through their 0/1 form against booleans or
		// non-negative integer literals.
		if b.Kind == KindBool {
			return cmpOrdered(boolAsInt(a.Bool), boolAsInt(b.Bool)), nil
		}
		if b.Kind == KindInt64 {
			return cmpOrdered(boolAsInt(a.Bool), b.I64), nil
		}
		return 0, fmt.Errorf("oracle: cannot compare %s with %s", a.Kind, b.Kind)
	default:
		return 0, fmt.Errorf("oracle: cannot compare %s with %s", a.Kind, b.Kind)
	}
}

func boolAsInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func valueAsFloat64(v Value) (float64, error) {
	switch v.Kind {
	case KindFloat64:
		return v.F64, nil
	case KindInt64:
		return float64(v.I64), nil
	case KindUint64:
		return float64(v.U64), nil
	default:
		return 0, fmt.Errorf("oracle: %s is not numeric", v.Kind)
	}
}

func valueAsUint64(v Value) (uint64, error) {
	switch v.Kind {
	case KindUint64:
		return v.U64, nil
	case KindInt64:
		if v.I64 < 0 {
			return 0, fmt.Errorf("oracle: negative int64 %d cannot compare against uint64", v.I64)
		}
		return uint64(v.I64), nil
	default:
		return 0, fmt.Errorf("oracle: %s is not an unsigned integer", v.Kind)
	}
}

// numericResultKind mirrors expr.CommonNumericType restricted to the oracle
// kinds: any float64 participant produces float64; uint64 only pairs with
// uint64 to stay unsigned, mixed uint64/int64 promotes to float64; otherwise
// the result is int64.
func numericResultKind(a, b Kind) (Kind, error) {
	num := func(k Kind) bool { return k == KindInt64 || k == KindUint64 || k == KindFloat64 }
	if !num(a) || !num(b) {
		return KindInvalid, fmt.Errorf("oracle: numeric operands required, got %s and %s", a, b)
	}
	if a == KindFloat64 || b == KindFloat64 {
		return KindFloat64, nil
	}
	if a == KindUint64 || b == KindUint64 {
		if a == KindUint64 && b == KindUint64 {
			return KindUint64, nil
		}
		return KindFloat64, nil
	}
	return KindInt64, nil
}

// evalScalar evaluates a projection expression. NULL propagates: any NULL
// input makes the result NULL of the result kind. Integer +, -, and * are
// checked and report an overflow error exactly like the engine; / always
// produces float64, so integer division by zero yields +Inf/-Inf/NaN instead
// of an error.
func (c rowContext) evalScalar(e expr.Expr) (Value, error) {
	switch n := e.(type) {
	case expr.Column:
		return c.lookup(n.Name)
	case expr.Int64Literal:
		return Int64(n.Value), nil
	case expr.Float64Literal:
		return Float64(n.Value), nil
	case expr.StringLiteral:
		return String(n.Value), nil
	case expr.Binary:
		switch n.Op {
		case expr.OpAdd, expr.OpSub, expr.OpMul, expr.OpDiv:
			left, err := c.evalScalar(n.Left)
			if err != nil {
				return Value{}, err
			}
			right, err := c.evalScalar(n.Right)
			if err != nil {
				return Value{}, err
			}
			return arithmetic(left, right, n.Op)
		case expr.OpConcat:
			left, err := c.evalScalar(n.Left)
			if err != nil {
				return Value{}, err
			}
			right, err := c.evalScalar(n.Right)
			if err != nil {
				return Value{}, err
			}
			if left.Kind != KindString || right.Kind != KindString {
				return Value{}, fmt.Errorf("oracle: || requires string operands, got %s and %s", left.Kind, right.Kind)
			}
			if left.Null || right.Null {
				return Null(KindString), nil
			}
			return String(left.Str + right.Str), nil
		default:
			return Value{}, fmt.Errorf("oracle: unsupported scalar operator %s", n.Op)
		}
	default:
		return Value{}, fmt.Errorf("oracle: unsupported scalar node %T", e)
	}
}

func arithmetic(a, b Value, op expr.Op) (Value, error) {
	kind, err := numericResultKind(a.Kind, b.Kind)
	if err != nil {
		return Value{}, err
	}
	if op == expr.OpDiv {
		kind = KindFloat64
	}
	if a.Null || b.Null {
		return Null(kind), nil
	}
	switch kind {
	case KindFloat64:
		fa, err := valueAsFloat64(a)
		if err != nil {
			return Value{}, err
		}
		fb, err := valueAsFloat64(b)
		if err != nil {
			return Value{}, err
		}
		switch op {
		case expr.OpAdd:
			return Float64(fa + fb), nil
		case expr.OpSub:
			return Float64(fa - fb), nil
		case expr.OpMul:
			return Float64(fa * fb), nil
		case expr.OpDiv:
			return Float64(fa / fb), nil
		}
	case KindInt64:
		switch op {
		case expr.OpAdd:
			// Go wraps on overflow; same-sign operands whose wrapped sum has
			// the opposite sign overflowed.
			sum := a.I64 + b.I64
			if (a.I64 >= 0) == (b.I64 >= 0) && (sum >= 0) != (a.I64 >= 0) {
				return Value{}, fmt.Errorf("oracle: int64 overflow: %d + %d", a.I64, b.I64)
			}
			return Int64(sum), nil
		case expr.OpSub:
			diff := a.I64 - b.I64
			if (a.I64 >= 0) != (b.I64 >= 0) && (diff >= 0) != (a.I64 >= 0) {
				return Value{}, fmt.Errorf("oracle: int64 overflow: %d - %d", a.I64, b.I64)
			}
			return Int64(diff), nil
		case expr.OpMul:
			product := a.I64 * b.I64
			if mulOverflows64(a.I64, b.I64, product) {
				return Value{}, fmt.Errorf("oracle: int64 overflow: %d * %d", a.I64, b.I64)
			}
			return Int64(product), nil
		}
	case KindUint64:
		switch op {
		case expr.OpAdd:
			sum, carry := bits.Add64(a.U64, b.U64, 0)
			if carry != 0 {
				return Value{}, fmt.Errorf("oracle: uint64 overflow: %d + %d", a.U64, b.U64)
			}
			return Uint64(sum), nil
		case expr.OpSub:
			diff, borrow := bits.Sub64(a.U64, b.U64, 0)
			if borrow != 0 {
				return Value{}, fmt.Errorf("oracle: uint64 underflow: %d - %d", a.U64, b.U64)
			}
			return Uint64(diff), nil
		case expr.OpMul:
			hi, lo := bits.Mul64(a.U64, b.U64)
			if hi != 0 {
				return Value{}, fmt.Errorf("oracle: uint64 overflow: %d * %d", a.U64, b.U64)
			}
			return Uint64(lo), nil
		}
	}
	return Value{}, fmt.Errorf("oracle: operator %s does not apply to %s and %s", op, a.Kind, b.Kind)
}

// mulOverflows64 reports whether a*b overflowed int64, given the wrapped
// product. The division round-trip misses exactly one case — MinInt64 * -1 —
// because Go defines MinInt64 / -1 as MinInt64, so it is checked explicitly.
func mulOverflows64(a, b, product int64) bool {
	if a == 0 || b == 0 {
		return false
	}
	if a == math.MinInt64 && b == -1 || b == math.MinInt64 && a == -1 {
		return true
	}
	return product/b != a
}
