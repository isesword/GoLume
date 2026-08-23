// Package expr defines logical scalar expressions and query descriptions used
// by the planner.
package expr

import (
	"fmt"
	"gocolumn/memory"
	"strconv"
)

// Op identifies a logical expression operator.
type Op uint8

// Expression operator constants.
const (
	OpInvalid Op = iota
	OpGT
	OpGE
	OpLT
	OpLE
	OpEQ
	OpAnd
	OpOr
	OpNot
	OpIsNull
	OpIsNotNull
	OpAdd
	OpSub
	OpMul
	OpDiv
	OpConcat
)

// Expr is a logical scalar expression node.
type Expr interface {
	exprNode()
	String() string
}

// Column references one input column by name.
type Column struct{ Name string }

func (Column) exprNode() {}

// String returns the column name.
func (c Column) String() string { return c.Name }

// Int64Literal is an int64 scalar literal.
type Int64Literal struct{ Value int64 }

func (Int64Literal) exprNode() {}

// String returns the decimal representation of the literal.
func (v Int64Literal) String() string { return fmt.Sprintf("%d", v.Value) }

// Float64Literal is a float64 scalar literal.
type Float64Literal struct{ Value float64 }

func (Float64Literal) exprNode() {}

// String returns a stable decimal representation of the literal.
func (v Float64Literal) String() string { return strconv.FormatFloat(v.Value, 'g', -1, 64) }

// StringLiteral is a UTF-8 scalar literal.
type StringLiteral struct{ Value string }

func (StringLiteral) exprNode() {}

// String returns a quoted representation of the literal.
func (v StringLiteral) String() string { return strconv.Quote(v.Value) }

// Date32Literal stores a logical date as days since the Unix epoch, matching
// Arrow date32 physical semantics.
type Date32Literal struct{ Days int32 }

func (Date32Literal) exprNode() {}

// String returns the date32 physical literal used by explain output.
func (v Date32Literal) String() string { return fmt.Sprintf("DATE32(%d)", v.Days) }

// TimestampLiteral stores a logical timestamp as an int64 value in the source
// field's Arrow timestamp unit. The logical type remains TypeTimestamp.
type TimestampLiteral struct{ Value int64 }

func (TimestampLiteral) exprNode() {}

// String returns the timestamp physical literal used by explain output.
func (v TimestampLiteral) String() string { return fmt.Sprintf("TIMESTAMP(%d)", v.Value) }

// Binary is a binary logical expression.
type Binary struct {
	Op          Op
	Left, Right Expr
}

func (Binary) exprNode() {}

// String returns a human-readable binary expression.
func (b Binary) String() string { return fmt.Sprintf("(%s %s %s)", b.Left, b.Op, b.Right) }

// Unary is a unary logical expression.
type Unary struct {
	Op    Op
	Value Expr
}

func (Unary) exprNode() {}

// String returns a human-readable unary expression.
func (u Unary) String() string { return fmt.Sprintf("%s(%s)", u.Op, u.Value) }

// Cast converts Value to Target using the expression engine's explicit cast
// rules. Unsafe or unsupported conversions fail during logical validation.
type Cast struct {
	Value  Expr
	Target memory.DataType
}

func (Cast) exprNode() {}

// String returns a SQL-like CAST expression.
func (c Cast) String() string { return fmt.Sprintf("CAST(%s AS %s)", c.Value, c.Target) }

// String returns the SQL-like spelling of the operator.
func (o Op) String() string {
	switch o {
	case OpGT:
		return ">"
	case OpGE:
		return ">="
	case OpLT:
		return "<"
	case OpLE:
		return "<="
	case OpEQ:
		return "="
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpNot:
		return "NOT"
	case OpIsNull:
		return "IS NULL"
	case OpIsNotNull:
		return "IS NOT NULL"
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpConcat:
		return "||"
	default:
		return "?"
	}
}

// Col constructs a column reference.
func Col(name string) Column { return Column{Name: name} }

// I64 constructs an int64 literal.
func I64(v int64) Int64Literal { return Int64Literal{Value: v} }

// F64 constructs a float64 literal.
func F64(v float64) Float64Literal { return Float64Literal{Value: v} }

// Str constructs a UTF-8 string literal.
func Str(v string) StringLiteral { return StringLiteral{Value: v} }

// Date32 constructs an Arrow-compatible date32 literal in days since Unix epoch.
func Date32(days int32) Date32Literal { return Date32Literal{Days: days} }

// Timestamp constructs an Arrow-compatible physical timestamp literal.
func Timestamp(v int64) TimestampLiteral { return TimestampLiteral{Value: v} }

// GT constructs a greater-than expression.
func GT(l, r Expr) Binary { return Binary{Op: OpGT, Left: l, Right: r} }

// GE constructs a greater-than-or-equal expression.
func GE(l, r Expr) Binary { return Binary{Op: OpGE, Left: l, Right: r} }

// LT constructs a less-than expression.
func LT(l, r Expr) Binary { return Binary{Op: OpLT, Left: l, Right: r} }

// LE constructs a less-than-or-equal expression.
func LE(l, r Expr) Binary { return Binary{Op: OpLE, Left: l, Right: r} }

// EQ constructs an equality expression.
func EQ(l, r Expr) Binary { return Binary{Op: OpEQ, Left: l, Right: r} }

// And constructs a SQL three-valued AND expression.
func And(l, r Expr) Binary { return Binary{Op: OpAnd, Left: l, Right: r} }

// Or constructs a SQL three-valued OR expression.
func Or(l, r Expr) Binary { return Binary{Op: OpOr, Left: l, Right: r} }

// Not constructs a SQL three-valued NOT expression.
func Not(v Expr) Unary { return Unary{Op: OpNot, Value: v} }

// IsNull constructs an IS NULL predicate.
func IsNull(v Expr) Unary { return Unary{Op: OpIsNull, Value: v} }

// IsNotNull constructs an IS NOT NULL predicate.
func IsNotNull(v Expr) Unary { return Unary{Op: OpIsNotNull, Value: v} }

// Lit constructs an int64 literal and is the preferred fluent-API spelling of I64.
func Lit(v int64) Int64Literal { return I64(v) }

// NamedExpr is one scalar expression with a stable output column name.
// Logical projections use NamedExpr so aliases are resolved before physical
// execution and downstream operators can address computed columns by name.
type NamedExpr struct {
	Expr  Expr
	Alias string
}

// Named constructs a named scalar projection expression.
func Named(e Expr, alias string) NamedExpr { return NamedExpr{Expr: e, Alias: alias} }

// Name returns the output column name. A bare Column can omit Alias and keeps
// its input name; all other expressions require an explicit alias.
func (n NamedExpr) Name() (string, error) {
	if n.Alias != "" {
		return n.Alias, nil
	}
	if c, ok := n.Expr.(Column); ok && c.Name != "" {
		return c.Name, nil
	}
	return "", fmt.Errorf("computed expression %T requires an alias", n.Expr)
}

// Add constructs a numeric addition expression. SQL NULL propagates through the result.
func Add(l, r Expr) Binary { return Binary{Op: OpAdd, Left: l, Right: r} }

// Sub constructs a numeric subtraction expression. SQL NULL propagates through the result.
func Sub(l, r Expr) Binary { return Binary{Op: OpSub, Left: l, Right: r} }

// Mul constructs a numeric multiplication expression. SQL NULL propagates through the result.
func Mul(l, r Expr) Binary { return Binary{Op: OpMul, Left: l, Right: r} }

// Div constructs a numeric division expression. Integer inputs are promoted to
// float64 so division has non-truncating analytical semantics.
func Div(l, r Expr) Binary { return Binary{Op: OpDiv, Left: l, Right: r} }

// Concat constructs a UTF-8 concatenation expression. NULL on either side
// produces NULL, matching the engine's scalar NULL-propagation rule.
func Concat(l, r Expr) Binary { return Binary{Op: OpConcat, Left: l, Right: r} }

// CastTo constructs an explicit scalar cast.
func CastTo(v Expr, target memory.DataType) Cast { return Cast{Value: v, Target: target} }

// Add constructs c + right.
func (c Column) Add(right Expr) Binary { return Add(c, right) }

// Sub constructs c - right.
func (c Column) Sub(right Expr) Binary { return Sub(c, right) }

// Mul constructs c * right.
func (c Column) Mul(right Expr) Binary { return Mul(c, right) }

// Div constructs c / right.
func (c Column) Div(right Expr) Binary { return Div(c, right) }

// Concat constructs c || right.
func (c Column) Concat(right Expr) Binary { return Concat(c, right) }

// Cast converts c to target.
func (c Column) Cast(target memory.DataType) Cast { return CastTo(c, target) }

// Alias names a column projection explicitly.
func (c Column) Alias(name string) NamedExpr { return Named(c, name) }

// Add constructs b + right.
func (b Binary) Add(right Expr) Binary { return Add(b, right) }

// Sub constructs b - right.
func (b Binary) Sub(right Expr) Binary { return Sub(b, right) }

// Mul constructs b * right.
func (b Binary) Mul(right Expr) Binary { return Mul(b, right) }

// Div constructs b / right.
func (b Binary) Div(right Expr) Binary { return Div(b, right) }

// Concat constructs b || right.
func (b Binary) Concat(right Expr) Binary { return Concat(b, right) }

// Cast converts b to target.
func (b Binary) Cast(target memory.DataType) Cast { return CastTo(b, target) }

// Alias assigns an output name to a computed binary expression.
func (b Binary) Alias(name string) NamedExpr { return Named(b, name) }

// Add constructs v + right.
func (v Int64Literal) Add(right Expr) Binary { return Add(v, right) }

// Sub constructs v - right.
func (v Int64Literal) Sub(right Expr) Binary { return Sub(v, right) }

// Mul constructs v * right.
func (v Int64Literal) Mul(right Expr) Binary { return Mul(v, right) }

// Div constructs v / right.
func (v Int64Literal) Div(right Expr) Binary { return Div(v, right) }

// Cast converts v to target.
func (v Int64Literal) Cast(target memory.DataType) Cast { return CastTo(v, target) }

// Alias assigns an output name to a literal projection.
func (v Int64Literal) Alias(name string) NamedExpr { return Named(v, name) }

// Add constructs v + right.
func (v Float64Literal) Add(right Expr) Binary { return Add(v, right) }

// Sub constructs v - right.
func (v Float64Literal) Sub(right Expr) Binary { return Sub(v, right) }

// Mul constructs v * right.
func (v Float64Literal) Mul(right Expr) Binary { return Mul(v, right) }

// Div constructs v / right.
func (v Float64Literal) Div(right Expr) Binary { return Div(v, right) }

// Cast converts v to target.
func (v Float64Literal) Cast(target memory.DataType) Cast { return CastTo(v, target) }

// Alias assigns an output name to a float literal projection.
func (v Float64Literal) Alias(name string) NamedExpr { return Named(v, name) }

// Concat constructs v || right.
func (v StringLiteral) Concat(right Expr) Binary { return Concat(v, right) }

// Cast converts v to target.
func (v StringLiteral) Cast(target memory.DataType) Cast { return CastTo(v, target) }

// Alias assigns an output name to a string literal projection.
func (v StringLiteral) Alias(name string) NamedExpr { return Named(v, name) }

// Cast converts c to target.
func (c Cast) Cast(target memory.DataType) Cast { return CastTo(c, target) }

// Alias assigns an output name to a cast expression.
func (c Cast) Alias(name string) NamedExpr { return Named(c, name) }

// Gt constructs c > right.
func (c Column) Gt(right Expr) Binary { return GT(c, right) }

// Ge constructs c >= right.
func (c Column) Ge(right Expr) Binary { return GE(c, right) }

// Lt constructs c < right.
func (c Column) Lt(right Expr) Binary { return LT(c, right) }

// Le constructs c <= right.
func (c Column) Le(right Expr) Binary { return LE(c, right) }

// Eq constructs c = right.
func (c Column) Eq(right Expr) Binary { return EQ(c, right) }

// IsNull constructs c IS NULL.
func (c Column) IsNull() Unary { return IsNull(c) }

// IsNotNull constructs c IS NOT NULL.
func (c Column) IsNotNull() Unary { return IsNotNull(c) }
