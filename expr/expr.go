// Package expr defines logical scalar expressions and query descriptions used
// by the planner.
package expr

import (
	"fmt"
	"gocolumn/memory"
	"strconv"
	"time"
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
	OpContains
	OpStartsWith
	OpEndsWith
	OpLength
	OpYear
	OpMonth
	OpDay
	OpHour
	OpMinute
	OpSecond
	OpByteLength
	OpTrim
	OpTrimStart
	OpTrimEnd
	OpLower
	OpUpper
	OpFind
	OpReplace
	OpReplaceAll
	OpSlice
	OpRegexMatch
	OpRegexExtract
	OpSplit
	OpRegexExtractAll
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

// TimestampLiteral stores a UTC Unix timestamp expressed in nanoseconds.
// The logical type remains TypeTimestamp.
type TimestampLiteral struct{ Value int64 }

func (TimestampLiteral) exprNode() {}

// String returns the timestamp physical literal used by explain output.
func (v TimestampLiteral) String() string { return fmt.Sprintf("TIMESTAMP(%d)", v.Value) }

// DurationLiteral stores an elapsed duration expressed in nanoseconds. It
// deliberately uses time.Duration at the API boundary so duration values
// cannot be confused with a timestamp or a raw integer delta.
type DurationLiteral struct{ Value time.Duration }

func (DurationLiteral) exprNode() {}

// String returns the duration physical literal used by explain output.
func (v DurationLiteral) String() string { return fmt.Sprintf("DURATION(%d)", v.Value) }

// TimestampUnit identifies the input unit accepted by an explicit timestamp
// literal constructor. Timestamp values inside the engine are always UTC Unix
// nanoseconds; this type exists only at the API boundary so a source unit is
// never inferred implicitly.
type TimestampUnit uint8

const (
	// TimestampUnitSecond interprets the literal value as Unix seconds.
	TimestampUnitSecond TimestampUnit = iota + 1
	// TimestampUnitMillisecond interprets the literal value as Unix milliseconds.
	TimestampUnitMillisecond
	// TimestampUnitMicrosecond interprets the literal value as Unix microseconds.
	TimestampUnitMicrosecond
)

// TimestampUnitLiteral stores an explicitly unit-tagged timestamp literal.
// It is normalized, with overflow checking, when an expression is compiled.
type TimestampUnitLiteral struct {
	Value int64
	Unit  TimestampUnit
}

func (TimestampUnitLiteral) exprNode() {}

// String returns the unit-preserving spelling used by explain output.
func (v TimestampUnitLiteral) String() string {
	switch v.Unit {
	case TimestampUnitSecond:
		return fmt.Sprintf("TIMESTAMP_SECONDS(%d)", v.Value)
	case TimestampUnitMillisecond:
		return fmt.Sprintf("TIMESTAMP_MILLIS(%d)", v.Value)
	case TimestampUnitMicrosecond:
		return fmt.Sprintf("TIMESTAMP_MICROS(%d)", v.Value)
	default:
		return fmt.Sprintf("TIMESTAMP_UNIT(%d)", v.Value)
	}
}

// Nanoseconds returns v normalized to the engine's UTC Unix-nanosecond
// timestamp contract. It reports an error instead of wrapping on overflow.
func (v TimestampUnitLiteral) Nanoseconds() (int64, error) {
	return normalizeTimestampUnitLiteral(v)
}

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

// Ternary is a logical expression with three scalar inputs. String
// replacement uses Value, First, and Second as value, old text, and new text;
// Slice uses them as value, offset, and length respectively.
type Ternary struct {
	Op                   Op
	Value, First, Second Expr
}

func (Ternary) exprNode() {}

// String returns a human-readable ternary expression.
func (t Ternary) String() string {
	return fmt.Sprintf("%s(%s, %s, %s)", t.Op, t.Value, t.First, t.Second)
}

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
	case OpContains:
		return "CONTAINS"
	case OpStartsWith:
		return "STARTS_WITH"
	case OpEndsWith:
		return "ENDS_WITH"
	case OpLength:
		return "LENGTH"
	case OpYear:
		return "YEAR"
	case OpMonth:
		return "MONTH"
	case OpDay:
		return "DAY"
	case OpHour:
		return "HOUR"
	case OpMinute:
		return "MINUTE"
	case OpSecond:
		return "SECOND"
	case OpByteLength:
		return "BYTE_LENGTH"
	case OpTrim:
		return "TRIM"
	case OpTrimStart:
		return "TRIM_START"
	case OpTrimEnd:
		return "TRIM_END"
	case OpLower:
		return "LOWER"
	case OpUpper:
		return "UPPER"
	case OpFind:
		return "FIND"
	case OpReplace:
		return "REPLACE"
	case OpReplaceAll:
		return "REPLACE_ALL"
	case OpSlice:
		return "SLICE"
	case OpRegexMatch:
		return "REGEX_MATCH"
	case OpRegexExtract:
		return "REGEX_EXTRACT"
	case OpSplit:
		return "SPLIT"
	case OpRegexExtractAll:
		return "REGEX_EXTRACT_ALL"
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

// Timestamp constructs a UTC Unix timestamp literal expressed in nanoseconds.
func Timestamp(v int64) TimestampLiteral { return TimestampLiteral{Value: v} }

// Duration constructs an elapsed-time literal normalized to nanoseconds.
func Duration(v time.Duration) DurationLiteral { return DurationLiteral{Value: v} }

// TimestampSeconds constructs a UTC timestamp literal from Unix seconds. The
// conversion to the engine's nanosecond contract is checked during compilation.
func TimestampSeconds(v int64) TimestampUnitLiteral {
	return TimestampUnitLiteral{Value: v, Unit: TimestampUnitSecond}
}

// TimestampMillis constructs a UTC timestamp literal from Unix milliseconds.
// The conversion to the engine's nanosecond contract is checked during compilation.
func TimestampMillis(v int64) TimestampUnitLiteral {
	return TimestampUnitLiteral{Value: v, Unit: TimestampUnitMillisecond}
}

// TimestampMicros constructs a UTC timestamp literal from Unix microseconds.
// The conversion to the engine's nanosecond contract is checked during compilation.
func TimestampMicros(v int64) TimestampUnitLiteral {
	return TimestampUnitLiteral{Value: v, Unit: TimestampUnitMicrosecond}
}

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

// Add constructs a numeric or temporal addition expression. Integer and
// temporal results are checked for overflow. Date values accept signed integer
// deltas in their physical unit; timestamps accept Duration values expressed in
// UTC nanoseconds. SQL NULL propagates.
func Add(l, r Expr) Binary { return Binary{Op: OpAdd, Left: l, Right: r} }

// Sub constructs a numeric or temporal subtraction expression. Integer and
// temporal results are checked for overflow. Timestamps can subtract a
// Duration or another timestamp; the latter produces a Duration. SQL NULL
// propagates.
func Sub(l, r Expr) Binary { return Binary{Op: OpSub, Left: l, Right: r} }

// Mul constructs a numeric multiplication expression. Integer results are
// checked for overflow; floating-point results retain IEEE-754 semantics. SQL
// NULL propagates.
func Mul(l, r Expr) Binary { return Binary{Op: OpMul, Left: l, Right: r} }

// Div constructs a numeric division expression. Integer inputs are promoted to
// float64 so division has non-truncating analytical semantics.
func Div(l, r Expr) Binary { return Binary{Op: OpDiv, Left: l, Right: r} }

// Concat constructs a UTF-8 concatenation expression. NULL on either side
// produces NULL, matching the engine's scalar NULL-propagation rule.
func Concat(l, r Expr) Binary { return Binary{Op: OpConcat, Left: l, Right: r} }

// Contains constructs a case-sensitive UTF-8 substring predicate. It is valid
// in filters and preserves SQL NULL semantics.
func Contains(value, needle Expr) Binary { return Binary{Op: OpContains, Left: value, Right: needle} }

// StartsWith constructs a case-sensitive UTF-8 prefix predicate.
func StartsWith(value, prefix Expr) Binary {
	return Binary{Op: OpStartsWith, Left: value, Right: prefix}
}

// EndsWith constructs a case-sensitive UTF-8 suffix predicate.
func EndsWith(value, suffix Expr) Binary { return Binary{Op: OpEndsWith, Left: value, Right: suffix} }

// Length constructs a UTF-8 character-count projection expression.
func Length(value Expr) Unary { return Unary{Op: OpLength, Value: value} }

// ByteLength constructs a UTF-8 byte-count projection expression. Unlike
// Length, it reports the number of encoded bytes rather than Unicode code
// points.
func ByteLength(value Expr) Unary { return Unary{Op: OpByteLength, Value: value} }

// Trim removes leading and trailing Unicode whitespace from a string.
func Trim(value Expr) Unary { return Unary{Op: OpTrim, Value: value} }

// TrimStart removes leading Unicode whitespace from a string.
func TrimStart(value Expr) Unary { return Unary{Op: OpTrimStart, Value: value} }

// TrimEnd removes trailing Unicode whitespace from a string.
func TrimEnd(value Expr) Unary { return Unary{Op: OpTrimEnd, Value: value} }

// Lower applies Unicode simple case mapping to lower case.
func Lower(value Expr) Unary { return Unary{Op: OpLower, Value: value} }

// Upper applies Unicode simple case mapping to upper case.
func Upper(value Expr) Unary { return Unary{Op: OpUpper, Value: value} }

// Find returns the zero-based Unicode code-point index of needle in value, or
// -1 when needle is absent. It performs a case-sensitive literal search.
func Find(value, needle Expr) Binary { return Binary{Op: OpFind, Left: value, Right: needle} }

// Replace replaces the first literal occurrence of old with new. It does not
// interpret either argument as a regular expression.
func Replace(value, old, new Expr) Ternary {
	return Ternary{Op: OpReplace, Value: value, First: old, Second: new}
}

// ReplaceAll replaces every literal occurrence of old with new. It does not
// interpret either argument as a regular expression.
func ReplaceAll(value, old, new Expr) Ternary {
	return Ternary{Op: OpReplaceAll, Value: value, First: old, Second: new}
}

// Slice returns up to length Unicode code points beginning at offset. A
// negative offset counts from the end of the value; a negative length is an
// error. Out-of-range offsets produce an empty string.
func Slice(value, offset, length Expr) Ternary {
	return Ternary{Op: OpSlice, Value: value, First: offset, Second: length}
}

// RegexMatch reports whether value contains a match for pattern. Pattern must
// be a UTF-8 string literal and is compiled once when the predicate is built.
func RegexMatch(value, pattern Expr) Binary {
	return Binary{Op: OpRegexMatch, Left: value, Right: pattern}
}

// RegexExtract returns capture group group from the first match of pattern.
// Pattern and group must be literals. Group zero selects the complete match;
// an absent match or unmatched optional group produces NULL.
func RegexExtract(value, pattern, group Expr) Ternary {
	return Ternary{Op: OpRegexExtract, Value: value, First: pattern, Second: group}
}

// Split separates value using a possibly row-dependent literal delimiter and
// returns List<String>. NULL propagates from either operand. An empty delimiter
// splits a non-empty value into Unicode code points.
func Split(value, delimiter Expr) Binary {
	return Binary{Op: OpSplit, Left: value, Right: delimiter}
}

// RegexExtractAll returns one element for every regular-expression match.
// Pattern and group must be literals. An unmatched optional capture produces
// a NULL child element while no matches produce a valid empty list.
func RegexExtractAll(value, pattern, group Expr) Ternary {
	return Ternary{Op: OpRegexExtractAll, Value: value, First: pattern, Second: group}
}

// Year extracts the UTC calendar year from a date or timestamp.
func Year(value Expr) Unary { return Unary{Op: OpYear, Value: value} }

// Month extracts the UTC calendar month from a date or timestamp.
func Month(value Expr) Unary { return Unary{Op: OpMonth, Value: value} }

// Day extracts the UTC calendar day of month from a date or timestamp.
func Day(value Expr) Unary { return Unary{Op: OpDay, Value: value} }

// Hour extracts the UTC hour from a timestamp.
func Hour(value Expr) Unary { return Unary{Op: OpHour, Value: value} }

// Minute extracts the UTC minute from a timestamp.
func Minute(value Expr) Unary { return Unary{Op: OpMinute, Value: value} }

// Second extracts the UTC second from a timestamp.
func Second(value Expr) Unary { return Unary{Op: OpSecond, Value: value} }

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

// Contains constructs a case-sensitive UTF-8 substring predicate.
func (c Column) Contains(needle Expr) Binary { return Contains(c, needle) }

// StartsWith constructs a case-sensitive UTF-8 prefix predicate.
func (c Column) StartsWith(prefix Expr) Binary { return StartsWith(c, prefix) }

// EndsWith constructs a case-sensitive UTF-8 suffix predicate.
func (c Column) EndsWith(suffix Expr) Binary { return EndsWith(c, suffix) }

// RegexMatch constructs a compiled regular-expression predicate.
func (c Column) RegexMatch(pattern Expr) Binary { return RegexMatch(c, pattern) }

// Length constructs a UTF-8 character-count projection expression.
func (c Column) Length() Unary { return Length(c) }

// ByteLength constructs a UTF-8 byte-count projection expression.
func (c Column) ByteLength() Unary { return ByteLength(c) }

// Trim removes leading and trailing Unicode whitespace from c.
func (c Column) Trim() Unary { return Trim(c) }

// TrimStart removes leading Unicode whitespace from c.
func (c Column) TrimStart() Unary { return TrimStart(c) }

// TrimEnd removes trailing Unicode whitespace from c.
func (c Column) TrimEnd() Unary { return TrimEnd(c) }

// Lower applies Unicode simple case mapping to lower case.
func (c Column) Lower() Unary { return Lower(c) }

// Upper applies Unicode simple case mapping to upper case.
func (c Column) Upper() Unary { return Upper(c) }

// Find returns the zero-based Unicode code-point index of needle, or -1.
func (c Column) Find(needle Expr) Binary { return Find(c, needle) }

// Replace replaces the first literal occurrence of old with new.
func (c Column) Replace(old, new Expr) Ternary { return Replace(c, old, new) }

// ReplaceAll replaces every literal occurrence of old with new.
func (c Column) ReplaceAll(old, new Expr) Ternary { return ReplaceAll(c, old, new) }

// Slice returns up to length Unicode code points beginning at offset.
func (c Column) Slice(offset, length Expr) Ternary { return Slice(c, offset, length) }

// RegexExtract returns a capture from the first regular-expression match.
func (c Column) RegexExtract(pattern, group Expr) Ternary {
	return RegexExtract(c, pattern, group)
}

// Split separates c using delimiter and returns List<String>.
func (c Column) Split(delimiter Expr) Binary { return Split(c, delimiter) }

// RegexExtractAll returns all captures of group from c.
func (c Column) RegexExtractAll(pattern, group Expr) Ternary {
	return RegexExtractAll(c, pattern, group)
}

// Year extracts the UTC calendar year from a date or timestamp.
func (c Column) Year() Unary { return Year(c) }

// Month extracts the UTC calendar month from a date or timestamp.
func (c Column) Month() Unary { return Month(c) }

// Day extracts the UTC calendar day of month from a date or timestamp.
func (c Column) Day() Unary { return Day(c) }

// Hour extracts the UTC hour from a timestamp.
func (c Column) Hour() Unary { return Hour(c) }

// Minute extracts the UTC minute from a timestamp.
func (c Column) Minute() Unary { return Minute(c) }

// Second extracts the UTC second from a timestamp.
func (c Column) Second() Unary { return Second(c) }

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

// Gt constructs b > right.
func (b Binary) Gt(right Expr) Binary { return GT(b, right) }

// Ge constructs b >= right.
func (b Binary) Ge(right Expr) Binary { return GE(b, right) }

// Lt constructs b < right.
func (b Binary) Lt(right Expr) Binary { return LT(b, right) }

// Le constructs b <= right.
func (b Binary) Le(right Expr) Binary { return LE(b, right) }

// Eq constructs b = right.
func (b Binary) Eq(right Expr) Binary { return EQ(b, right) }

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

// Add constructs v + right.
func (v DurationLiteral) Add(right Expr) Binary { return Add(v, right) }

// Sub constructs v - right.
func (v DurationLiteral) Sub(right Expr) Binary { return Sub(v, right) }

// Cast converts v to target.
func (v DurationLiteral) Cast(target memory.DataType) Cast { return CastTo(v, target) }

// Alias assigns an output name to a duration literal projection.
func (v DurationLiteral) Alias(name string) NamedExpr { return Named(v, name) }

// Cast converts c to target.
func (c Cast) Cast(target memory.DataType) Cast { return CastTo(c, target) }

// Alias assigns an output name to a cast expression.
func (c Cast) Alias(name string) NamedExpr { return Named(c, name) }

// Gt constructs a comparison with a unary scalar expression on the left.
func (u Unary) Gt(right Expr) Binary { return GT(u, right) }

// Ge constructs a comparison with a unary scalar expression on the left.
func (u Unary) Ge(right Expr) Binary { return GE(u, right) }

// Lt constructs a comparison with a unary scalar expression on the left.
func (u Unary) Lt(right Expr) Binary { return LT(u, right) }

// Le constructs a comparison with a unary scalar expression on the left.
func (u Unary) Le(right Expr) Binary { return LE(u, right) }

// Eq constructs a comparison with a unary scalar expression on the left.
func (u Unary) Eq(right Expr) Binary { return EQ(u, right) }

// Cast converts a unary scalar expression to target.
func (u Unary) Cast(target memory.DataType) Cast { return CastTo(u, target) }

// Alias assigns an output name to a unary scalar expression.
func (u Unary) Alias(name string) NamedExpr { return Named(u, name) }

// Concat constructs u || right.
func (u Unary) Concat(right Expr) Binary { return Concat(u, right) }

// Trim removes leading and trailing Unicode whitespace from u.
func (u Unary) Trim() Unary { return Trim(u) }

// TrimStart removes leading Unicode whitespace from u.
func (u Unary) TrimStart() Unary { return TrimStart(u) }

// TrimEnd removes trailing Unicode whitespace from u.
func (u Unary) TrimEnd() Unary { return TrimEnd(u) }

// Lower applies Unicode simple case mapping to lower case.
func (u Unary) Lower() Unary { return Lower(u) }

// Upper applies Unicode simple case mapping to upper case.
func (u Unary) Upper() Unary { return Upper(u) }

// ByteLength constructs a UTF-8 byte-count projection expression.
func (u Unary) ByteLength() Unary { return ByteLength(u) }

// Length constructs a UTF-8 character-count projection expression.
func (u Unary) Length() Unary { return Length(u) }

// Find returns the zero-based Unicode code-point index of needle, or -1.
func (u Unary) Find(needle Expr) Binary { return Find(u, needle) }

// Replace replaces the first literal occurrence of old with new.
func (u Unary) Replace(old, new Expr) Ternary { return Replace(u, old, new) }

// ReplaceAll replaces every literal occurrence of old with new.
func (u Unary) ReplaceAll(old, new Expr) Ternary { return ReplaceAll(u, old, new) }

// Slice returns up to length Unicode code points beginning at offset.
func (u Unary) Slice(offset, length Expr) Ternary { return Slice(u, offset, length) }

// RegexExtract returns a capture from the first regular-expression match.
func (u Unary) RegexExtract(pattern, group Expr) Ternary {
	return RegexExtract(u, pattern, group)
}

// Cast converts a ternary scalar expression to target.
func (t Ternary) Cast(target memory.DataType) Cast { return CastTo(t, target) }

// Alias assigns an output name to a ternary scalar expression.
func (t Ternary) Alias(name string) NamedExpr { return Named(t, name) }

// Concat constructs t || right.
func (t Ternary) Concat(right Expr) Binary { return Concat(t, right) }

// Trim removes leading and trailing Unicode whitespace from t.
func (t Ternary) Trim() Unary { return Trim(t) }

// TrimStart removes leading Unicode whitespace from t.
func (t Ternary) TrimStart() Unary { return TrimStart(t) }

// TrimEnd removes trailing Unicode whitespace from t.
func (t Ternary) TrimEnd() Unary { return TrimEnd(t) }

// Lower applies Unicode simple case mapping to lower case.
func (t Ternary) Lower() Unary { return Lower(t) }

// Upper applies Unicode simple case mapping to upper case.
func (t Ternary) Upper() Unary { return Upper(t) }

// Length constructs a UTF-8 character-count projection expression.
func (t Ternary) Length() Unary { return Length(t) }

// ByteLength constructs a UTF-8 byte-count projection expression.
func (t Ternary) ByteLength() Unary { return ByteLength(t) }

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
