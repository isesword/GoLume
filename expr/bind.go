package expr

import (
	"fmt"
	"reflect"
	"sort"
	"unicode/utf8"

	"gocolumn/memory"
)

// BoundExpr is the shared, typed expression representation. The DAG and all
// exposed slices are immutable after binding. Children preserve source operand
// order; CASE contains alternating condition/value children followed by ELSE.
// A missing CASE ELSE becomes a typed NULL child.
//
// MayError conservatively describes errors from evaluating row values, not
// cancellation, memory admission, or invalid source storage. It includes child
// errors even in statically dead branches: binding validates every branch but
// never executes value-dependent casts, arithmetic, string slicing, or JSON.
// Optimizers must preserve the evaluation domain of a demanded partial value.
// In particular, sequential Filters cannot blindly become eager SQL AND, and
// a false metadata conjunct cannot prune a potentially erroring sibling.
// Unused projection outputs may be pruned before execution.
type BoundExpr struct {
	Expr     Expr
	Field    memory.Field
	MayError bool
	Columns  []string
	Children []*BoundExpr
}

// OutputField returns an independently owned field description with the given
// output name. Bare columns retain their complete encoding and nested metadata.
func (b *BoundExpr) OutputField(name string) memory.Field {
	field := cloneBoundField(b.Field)
	field.Name = name
	return field
}

// Bind resolves columns, validates every expression node, and derives the full
// output schema, nullability, dependencies, and error effect without reading
// data. Repeated source nodes share immutable bound results within the call.
// All runtime value computations remain in the expression executor.
func Bind(schema memory.Schema, e Expr) (*BoundExpr, error) {
	return newExpressionBinder(schema).bind(e)
}

// expressionBinder belongs to one binding operation and one input schema. It
// retains source handles only until binding/compilation ends, never globally or
// in the executable Program. A reflect.Value key compares the handle itself,
// not the underlying expression: map[Expr] would recursively hash shared value
// nodes and panic for slice-backed CASE/COALESCE/IN/JSON nodes. Reboxed copies
// may have different handles; structural CSE still merges equivalent values.
//
// Only finished, independently owned bound nodes are shared. A nil map entry
// marks the active DFS path, so a cycle through a caller-owned operand slice
// fails validation rather than recursing indefinitely.
// Source expressions and schemas must not be mutated during a bind operation.
type expressionBinder struct {
	schema memory.Schema
	nodes  map[reflect.Value]*BoundExpr
}

func newExpressionBinder(schema memory.Schema) *expressionBinder {
	return &expressionBinder{schema: schema, nodes: make(map[reflect.Value]*BoundExpr)}
}

func (b *expressionBinder) bind(e Expr) (*BoundExpr, error) {
	if e == nil {
		return nil, fmt.Errorf("expression is nil")
	}
	key := reflect.ValueOf(e)
	if bound, ok := b.nodes[key]; ok {
		if bound == nil {
			return nil, fmt.Errorf("expression contains a cycle at %T", e)
		}
		return bound, nil
	}
	b.nodes[key] = nil
	bound, err := b.bindNode(e)
	if err != nil {
		delete(b.nodes, key)
		return nil, err
	}
	b.nodes[key] = bound
	return bound, nil
}

func (b *expressionBinder) bindNode(e Expr) (*BoundExpr, error) {
	bound := &BoundExpr{Expr: e}
	bindChildren := func(values ...Expr) error {
		for _, value := range values {
			child, err := b.bind(value)
			if err != nil {
				return err
			}
			bound.Children = append(bound.Children, child)
		}
		return nil
	}
	switch n := e.(type) {
	case Column:
		field, ok := b.schema.Field(n.Name)
		if !ok {
			return nil, fmt.Errorf("column %q not found", n.Name)
		}
		if field.Type == memory.TypeDecimal && (field.DecimalPrecision != 0 || field.DecimalScale != 0) {
			if err := field.ValidateDecimal128(); err != nil {
				return nil, err
			}
		}
		bound.Field = cloneBoundField(field)
		bound.Columns = []string{n.Name}
	case Int64Literal:
		bound.Field = computedField(memory.TypeInt64, false)
	case Float64Literal:
		bound.Field = computedField(memory.TypeFloat64, false)
	case Decimal128Literal:
		field, err := memory.Decimal128Field("", false, n.Precision, n.Scale)
		if err != nil {
			return nil, err
		}
		if !memory.Decimal128FitsPrecision(n.Value, n.Precision) {
			return nil, fmt.Errorf("decimal literal coefficient exceeds precision %d", n.Precision)
		}
		bound.Field = field
	case Decimal128NullLiteral:
		field, err := memory.Decimal128Field("", true, n.Precision, n.Scale)
		if err != nil {
			return nil, err
		}
		bound.Field = field
	case BoolLiteral:
		bound.Field = computedField(memory.TypeBool, false)
	case NullLiteral:
		if !conditionalScalarType(n.Type) {
			return nil, fmt.Errorf("NULL requires a supported scalar type, got %s", n.Type)
		}
		bound.Field = computedField(n.Type, true)
	case StringLiteral:
		if !utf8.ValidString(n.Value) {
			return nil, fmt.Errorf("string literal is not valid UTF-8")
		}
		bound.Field = computedField(memory.TypeString, false)
	case Date32Literal:
		bound.Field = computedField(memory.TypeDate32, false)
	case TimestampLiteral:
		bound.Field = computedField(memory.TypeTimestamp, false)
	case DurationLiteral:
		bound.Field = computedField(memory.TypeDuration, false)
	case TimestampUnitLiteral:
		if _, err := n.Nanoseconds(); err != nil {
			return nil, err
		}
		bound.Field = computedField(memory.TypeTimestamp, false)
	case Decimal128CastExpr:
		if err := bindChildren(n.Value); err != nil {
			return nil, err
		}
		input := bound.Children[0].Field
		if input.Type == memory.TypeDecimal {
			if err := input.ValidateDecimal128(); err != nil {
				return nil, err
			}
		} else if !isSignedInteger(input.Type) && !unsignedInteger(input.Type) && input.Type != memory.TypeString {
			return nil, fmt.Errorf("CAST_DECIMAL128 requires integer, string, or native decimal128 input, got %s", input.Type)
		}
		field, err := memory.Decimal128Field("", input.Nullable || n.Tolerant, n.Precision, n.Scale)
		if err != nil {
			return nil, err
		}
		bound.Field, bound.MayError = field, !n.Tolerant
		n.Value = bound.Children[0].Expr
		bound.Expr = n
	case Cast:
		if err := bindChildren(n.Value); err != nil {
			return nil, err
		}
		input := bound.Children[0].Field
		if input.Type == memory.TypeDecimal {
			if err := input.ValidateDecimal128(); err != nil {
				return nil, err
			}
		}
		if !CanCast(input.Type, n.Target) {
			return nil, fmt.Errorf("cannot cast %s to %s", input.Type, n.Target)
		}
		bound.Field = computedField(n.Target, input.Nullable)
		bound.MayError = castMayError(input.Type, n.Target)
		bound.Expr = Cast{Value: bound.Children[0].Expr, Target: n.Target}
	case Decimal128DivideExpr:
		if err := bindChildren(n.Left, n.Right); err != nil {
			return nil, err
		}
		left, right := bound.Children[0].Field, bound.Children[1].Field
		for _, field := range []memory.Field{left, right} {
			if err := field.ValidateDecimal128(); err != nil {
				return nil, fmt.Errorf("DIVIDE_DECIMAL128 requires two native decimal128 operands: %w", err)
			}
		}
		field, err := memory.Decimal128Field("", left.Nullable || right.Nullable, n.Precision, n.Scale)
		if err != nil {
			return nil, err
		}
		bound.Field, bound.MayError = field, true
		bound.Expr = Decimal128DivideExpr{Left: bound.Children[0].Expr, Right: bound.Children[1].Expr, Precision: n.Precision, Scale: n.Scale}
	case Decimal128RescaleExpr:
		if err := bindChildren(n.Value); err != nil {
			return nil, err
		}
		input := bound.Children[0].Field
		if err := input.ValidateDecimal128(); err != nil {
			return nil, fmt.Errorf("RESCALE_DECIMAL128 requires native decimal128 input: %w", err)
		}
		field, err := memory.Decimal128Field("", input.Nullable, n.Precision, n.Scale)
		if err != nil {
			return nil, err
		}
		bound.Field, bound.MayError = field, true
		bound.Expr = Decimal128RescaleExpr{Value: bound.Children[0].Expr, Precision: n.Precision, Scale: n.Scale}
	case TryCastExpr:
		if err := bindChildren(n.Value); err != nil {
			return nil, err
		}
		input := bound.Children[0].Field
		if input.Type == memory.TypeDecimal {
			if err := input.ValidateDecimal128(); err != nil {
				return nil, err
			}
		}
		if !(input.Type == memory.TypeDecimal && (isSignedInteger(n.Target) || unsignedInteger(n.Target))) && (!IsNumeric(n.Target) || !(IsNumeric(input.Type) || input.Type == memory.TypeString)) {
			return nil, fmt.Errorf("TRY_CAST requires numeric/string input and numeric target, or native decimal128 input and integer target, got %s to %s", input.Type, n.Target)
		}
		bound.Field = computedField(n.Target, true)
		bound.Expr = TryCastExpr{Value: bound.Children[0].Expr, Target: n.Target}
	case JSONDecodeExpr:
		if err := bindChildren(n.Value); err != nil {
			return nil, err
		}
		if input := bound.Children[0].Field.Type; input != memory.TypeString {
			return nil, fmt.Errorf("JSON_DECODE requires string input, got %s", input)
		}
		bound.Field = cloneBoundField(memory.StructField("", true, n.Schema.Fields...))
		if err := bound.Field.ValidateStruct(); err != nil {
			return nil, err
		}
		bound.MayError = true
		bound.Expr = JSONDecodeExpr{Value: bound.Children[0].Expr, Schema: memory.NewSchema(bound.Field.Children...)}
	case Binary:
		if err := bindChildren(n.Left, n.Right); err != nil {
			return nil, err
		}
		if err := bindBinary(bound, n); err != nil {
			return nil, err
		}
		bound.Expr = Binary{Op: n.Op, Left: bound.Children[0].Expr, Right: bound.Children[1].Expr}
	case Unary:
		if err := bindChildren(n.Value); err != nil {
			return nil, err
		}
		if err := bindUnary(bound, n); err != nil {
			return nil, err
		}
		bound.Expr = Unary{Op: n.Op, Value: bound.Children[0].Expr}
	case Ternary:
		if err := bindChildren(n.Value, n.First, n.Second); err != nil {
			return nil, err
		}
		if err := bindTernary(bound, n); err != nil {
			return nil, err
		}
		bound.Expr = Ternary{Op: n.Op, Value: bound.Children[0].Expr, First: bound.Children[1].Expr, Second: bound.Children[2].Expr}
	case IfExpr:
		if err := bindChildren(n.Condition, n.Then, n.Else); err != nil {
			return nil, err
		}
		if bound.Children[0].Field.Type != memory.TypeBool {
			return nil, fmt.Errorf("IF condition must be bool, got %s", bound.Children[0].Field.Type)
		}
		field, err := branchField(bound.Children[1:])
		if err != nil {
			return nil, fmt.Errorf("IF: %w", err)
		}
		bound.Field = field
		bound.Expr = IfExpr{Condition: bound.Children[0].Expr, Then: bound.Children[1].Expr, Else: bound.Children[2].Expr}
	case CaseExpr:
		if len(n.Whens) == 0 {
			return nil, fmt.Errorf("CASE requires at least one WHEN branch")
		}
		values := make([]*BoundExpr, 0, len(n.Whens)+1)
		branches := make([]WhenThen, 0, len(n.Whens))
		for i, branch := range n.Whens {
			if err := bindChildren(branch.When, branch.Then); err != nil {
				return nil, fmt.Errorf("CASE branch %d: %w", i, err)
			}
			condition, value := bound.Children[2*i], bound.Children[2*i+1]
			if condition.Field.Type != memory.TypeBool {
				return nil, fmt.Errorf("CASE branch %d condition must be bool, got %s", i, condition.Field.Type)
			}
			values = append(values, value)
			branches = append(branches, WhenThen{When: condition.Expr, Then: value.Expr})
		}
		if n.Else != nil {
			if err := bindChildren(n.Else); err != nil {
				return nil, fmt.Errorf("CASE ELSE: %w", err)
			}
			values = append(values, bound.Children[len(bound.Children)-1])
		}
		field, err := branchField(values)
		if err != nil {
			return nil, fmt.Errorf("CASE: %w", err)
		}
		if n.Else == nil {
			if field.Type == memory.TypeDecimal {
				nullField := cloneBoundField(field)
				nullField.Nullable = true
				bound.Children = append(bound.Children, &BoundExpr{Expr: NullLiteral{Type: field.Type}, Field: nullField})
			} else if err := bindChildren(NullLiteral{Type: field.Type}); err != nil {
				return nil, err
			}
			field.Nullable = true
		}
		bound.Field = field
		otherwise := bound.Children[len(bound.Children)-1].Expr
		if n.Else == nil && field.Type == memory.TypeDecimal {
			// Keep the source expression re-bindable: the implicit NULL child
			// carries precision/scale in its bound field, not in NullLiteral.
			otherwise = nil
		}
		bound.Expr = CaseExpr{Whens: branches, Else: otherwise}
	case CoalesceExpr:
		if len(n.Values) == 0 {
			return nil, fmt.Errorf("COALESCE requires at least one value")
		}
		if err := bindChildren(n.Values...); err != nil {
			return nil, err
		}
		field, err := branchField(bound.Children)
		if err != nil {
			return nil, fmt.Errorf("COALESCE: %w", err)
		}
		field.Nullable = true
		values := make([]Expr, len(bound.Children))
		for i, child := range bound.Children {
			field.Nullable = field.Nullable && child.Field.Nullable
			values[i] = child.Expr
		}
		bound.Field = field
		bound.Expr = CoalesceExpr{Values: values}
	case InExpr:
		if len(n.Values) == 0 {
			return nil, fmt.Errorf("IN requires at least one candidate")
		}
		if err := bindChildren(n.Value); err != nil {
			return nil, err
		}
		if err := bindChildren(n.Values...); err != nil {
			return nil, err
		}
		nullable := bound.Children[0].Field.Nullable
		values := make([]Expr, len(n.Values))
		for i, child := range bound.Children[1:] {
			if err := bindComparison(bound.Children[0], child); err != nil {
				return nil, fmt.Errorf("IN candidate %d: %w", i, err)
			}
			nullable = nullable || child.Field.Nullable
			values[i] = child.Expr
		}
		bound.Field = computedField(memory.TypeBool, nullable)
		bound.Expr = InExpr{Value: bound.Children[0].Expr, Values: values}
	default:
		return nil, fmt.Errorf("unsupported expression node %T", e)
	}
	if len(bound.Children) != 0 {
		dependencies := make(map[string]struct{})
		for _, child := range bound.Children {
			bound.MayError = bound.MayError || child.MayError
			for _, column := range child.Columns {
				dependencies[column] = struct{}{}
			}
		}
		for column := range dependencies {
			bound.Columns = append(bound.Columns, column)
		}
		sort.Strings(bound.Columns)
	}
	return bound, nil
}

func bindBinary(bound *BoundExpr, n Binary) error {
	if _, ok := mathBinaryOp(n.Op); ok {
		return bindMathBinary(bound, n)
	}
	left, right := bound.Children[0].Field, bound.Children[1].Field
	nullable := left.Nullable || right.Nullable
	if (left.Type == memory.TypeDecimal || right.Type == memory.TypeDecimal) && (n.Op == OpAdd || n.Op == OpSub || n.Op == OpMul) {
		return bindDecimalArithmetic(bound, n.Op)
	}
	switch n.Op {
	case OpGT, OpGE, OpLT, OpLE, OpEQ:
		if err := bindComparison(bound.Children[0], bound.Children[1]); err != nil {
			return fmt.Errorf("operator %s: %w", n.Op, err)
		}
		bound.Field = computedField(memory.TypeBool, nullable)
	case OpAnd, OpOr:
		if left.Type != memory.TypeBool || right.Type != memory.TypeBool {
			return fmt.Errorf("operator %s requires bool operands, got %s and %s", n.Op, left.Type, right.Type)
		}
		bound.Field = computedField(memory.TypeBool, nullable)
	case OpAdd, OpSub:
		if result, ok := temporalArithmeticType(n.Op, left.Type, right.Type); ok {
			bound.Field = computedField(result, nullable)
			bound.MayError = true
			return nil
		}
		if temporalComparisonType(left.Type) || temporalComparisonType(right.Type) {
			return fmt.Errorf("operator %s does not support temporal operands %s and %s", n.Op, left.Type, right.Type)
		}
		fallthrough
	case OpMul:
		typ, err := CommonNumericType(left.Type, right.Type)
		if err != nil {
			return fmt.Errorf("operator %s: %w", n.Op, err)
		}
		bound.Field = computedField(typ, nullable)
		bound.MayError = typ != memory.TypeFloat64
	case OpDiv:
		if !IsNumeric(left.Type) || !IsNumeric(right.Type) {
			return fmt.Errorf("operator / requires numeric operands, got %s and %s", left.Type, right.Type)
		}
		bound.Field = computedField(memory.TypeFloat64, nullable)
	case OpConcat, OpFind, OpSplit, OpContains, OpStartsWith, OpEndsWith, OpRegexMatch:
		if left.Type != memory.TypeString || right.Type != memory.TypeString {
			return fmt.Errorf("%s requires string operands, got %s and %s", n.Op, left.Type, right.Type)
		}
		switch n.Op {
		case OpConcat:
			bound.Field = computedField(memory.TypeString, nullable)
		case OpFind:
			bound.Field = computedField(memory.TypeInt64, nullable)
		case OpSplit:
			bound.Field = memory.ListStringField("", nullable, false)
		default:
			bound.Field = computedField(memory.TypeBool, nullable)
		}
		if n.Op == OpRegexMatch {
			pattern, ok := bound.Children[1].Expr.(StringLiteral)
			if !ok {
				return fmt.Errorf("REGEX_MATCH pattern must be a string literal")
			}
			if _, err := compileRegex(pattern.Value); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported binary operator %s", n.Op)
	}
	return nil
}

func bindUnary(bound *BoundExpr, n Unary) error {
	if _, ok := mathUnaryOp(n.Op); ok {
		return bindMathUnary(bound, n)
	}
	input := bound.Children[0].Field
	switch n.Op {
	case OpNot:
		if input.Type != memory.TypeBool {
			return fmt.Errorf("NOT requires bool input, got %s", input.Type)
		}
		bound.Field = computedField(memory.TypeBool, input.Nullable)
	case OpIsNull, OpIsNotNull:
		bound.Field = computedField(memory.TypeBool, false)
	case OpLength, OpByteLength, OpTrim, OpTrimStart, OpTrimEnd, OpLower, OpUpper:
		if input.Type != memory.TypeString {
			return fmt.Errorf("%s requires string input, got %s", n.Op, input.Type)
		}
		typ := memory.TypeString
		if n.Op == OpLength || n.Op == OpByteLength {
			typ = memory.TypeInt64
		}
		bound.Field = computedField(typ, input.Nullable)
	case OpYear, OpMonth, OpDay:
		if input.Type != memory.TypeDate32 && input.Type != memory.TypeDate64 && input.Type != memory.TypeTimestamp {
			return fmt.Errorf("%s requires date32, date64, or timestamp input, got %s", n.Op, input.Type)
		}
		bound.Field = computedField(memory.TypeInt64, input.Nullable)
	case OpHour, OpMinute, OpSecond:
		if input.Type != memory.TypeTimestamp {
			return fmt.Errorf("%s requires timestamp input, got %s", n.Op, input.Type)
		}
		bound.Field = computedField(memory.TypeInt64, input.Nullable)
	default:
		return fmt.Errorf("unsupported unary operator %s", n.Op)
	}
	return nil
}

func bindTernary(bound *BoundExpr, n Ternary) error {
	value, first, second := bound.Children[0].Field, bound.Children[1].Field, bound.Children[2].Field
	nullable := value.Nullable || first.Nullable || second.Nullable
	switch n.Op {
	case OpReplace, OpReplaceAll:
		if value.Type != memory.TypeString || first.Type != memory.TypeString || second.Type != memory.TypeString {
			return fmt.Errorf("%s requires string value, old text, and new text; got %s, %s, and %s", n.Op, value.Type, first.Type, second.Type)
		}
		bound.Field = computedField(memory.TypeString, nullable)
	case OpSlice:
		if value.Type != memory.TypeString || !isSignedInteger(first.Type) || !isSignedInteger(second.Type) {
			return fmt.Errorf("SLICE requires string value and signed integer offset/length, got %s, %s, and %s", value.Type, first.Type, second.Type)
		}
		bound.Field = computedField(memory.TypeString, nullable)
		bound.MayError = true
	case OpRegexExtract, OpRegexExtractAll:
		if value.Type != memory.TypeString || first.Type != memory.TypeString || !isSignedInteger(second.Type) {
			return fmt.Errorf("%s requires string value, string-literal pattern, and int64-literal group; got %s, %s, and %s", n.Op, value.Type, first.Type, second.Type)
		}
		pattern, patternOK := bound.Children[1].Expr.(StringLiteral)
		group, groupOK := bound.Children[2].Expr.(Int64Literal)
		if !patternOK || !groupOK {
			return fmt.Errorf("%s pattern must be a string literal and group must be an int64 literal", n.Op)
		}
		if _, _, err := compileRegexExtract(pattern.Value, group.Value); err != nil {
			return err
		}
		if n.Op == OpRegexExtractAll {
			bound.Field = memory.ListStringField("", value.Nullable, true)
		} else {
			bound.Field = computedField(memory.TypeString, true)
		}
	default:
		return fmt.Errorf("unsupported ternary operator %s", n.Op)
	}
	return nil
}

// bindComparison keeps comparison domains separate from arithmetic promotion.
// Native integer comparisons, including mixed signed/unsigned values, must not
// round through float64. Floating comparisons use IEEE equality and ordering.
func bindComparison(left, right *BoundExpr) error {
	if left.Field.Type == memory.TypeDecimal || right.Field.Type == memory.TypeDecimal {
		if err := left.Field.ValidateDecimal128(); err != nil {
			return fmt.Errorf("decimal comparison requires two native decimal128 operands: %w", err)
		}
		if err := right.Field.ValidateDecimal128(); err != nil {
			return fmt.Errorf("decimal comparison requires two native decimal128 operands: %w", err)
		}
		return nil
	}
	a, b := left.Field.Type, right.Field.Type
	if IsNumeric(a) && IsNumeric(b) {
		return nil
	}
	if a == b {
		switch a {
		case memory.TypeBool, memory.TypeString, memory.TypeBinary,
			memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp,
			memory.TypeTime32, memory.TypeTime64, memory.TypeDuration:
			return nil
		}
	}
	// A physical-unit int64 literal can be compared with a semantic temporal
	// value, matching the established predicate spelling. Integer columns do
	// not implicitly acquire date/time meaning.
	if temporalComparisonType(a) {
		if _, ok := right.Expr.(Int64Literal); ok {
			return nil
		}
	}
	if temporalComparisonType(b) {
		if _, ok := left.Expr.(Int64Literal); ok {
			return nil
		}
	}
	// Existing bool predicates expose 0/1 physical values to nonnegative
	// int64 literals, but bool remains distinct from numeric arithmetic.
	if a == memory.TypeBool {
		if literal, ok := right.Expr.(Int64Literal); ok && literal.Value >= 0 {
			return nil
		}
	}
	if b == memory.TypeBool {
		if literal, ok := left.Expr.(Int64Literal); ok && literal.Value >= 0 {
			return nil
		}
	}
	if a == memory.TypeBinary && b == memory.TypeString {
		if _, ok := right.Expr.(StringLiteral); ok {
			return nil
		}
	}
	if b == memory.TypeBinary && a == memory.TypeString {
		if _, ok := left.Expr.(StringLiteral); ok {
			return nil
		}
	}
	return fmt.Errorf("incompatible comparison operands %s and %s", a, b)
}

func branchField(values []*BoundExpr) (memory.Field, error) {
	if values[0].Field.Type == memory.TypeDecimal {
		field := values[0].OutputField("")
		if err := field.ValidateDecimal128(); err != nil {
			return memory.Field{}, err
		}
		for _, value := range values[1:] {
			if err := value.Field.ValidateDecimal128(); err != nil {
				return memory.Field{}, err
			}
			if field.DecimalPrecision != value.Field.DecimalPrecision || field.DecimalScale != value.Field.DecimalScale {
				return memory.Field{}, fmt.Errorf("decimal conditional branches require identical precision and scale")
			}
			field.Nullable = field.Nullable || value.Field.Nullable
		}
		return field, nil
	}
	field := computedField(values[0].Field.Type, values[0].Field.Nullable)
	if !conditionalScalarType(field.Type) {
		return memory.Field{}, fmt.Errorf("conditional result type %s is not supported", field.Type)
	}
	for _, value := range values[1:] {
		typ := value.Field.Type
		if !conditionalScalarType(typ) {
			return memory.Field{}, fmt.Errorf("conditional result type %s is not supported", typ)
		}
		if typ != field.Type {
			common, err := CommonNumericType(field.Type, typ)
			if err != nil {
				return memory.Field{}, fmt.Errorf("incompatible conditional result types %s and %s", field.Type, typ)
			}
			field = computedField(common, field.Nullable)
		}
		field.Nullable = field.Nullable || value.Field.Nullable
	}
	return field, nil
}

func conditionalScalarType(typ memory.DataType) bool {
	return IsNumeric(typ) || temporalComparisonType(typ) || typ == memory.TypeString || typ == memory.TypeBool
}

func temporalComparisonType(typ memory.DataType) bool {
	switch typ {
	case memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration:
		return true
	default:
		return false
	}
}

func computedField(typ memory.DataType, nullable bool) memory.Field {
	field := memory.Field{Type: typ, Nullable: nullable, Encoding: memory.EncodingPlain}
	if typ == memory.TypeString || typ == memory.TypeBinary {
		field.Encoding = memory.EncodingView
	}
	return field
}

func cloneBoundField(field memory.Field) memory.Field {
	if len(field.Children) != 0 {
		children := make([]memory.Field, len(field.Children))
		for i, child := range field.Children {
			children[i] = cloneBoundField(child)
		}
		field.Children = children
	}
	return field
}

func castMayError(from, to memory.DataType) bool {
	if from == memory.TypeString && from != to {
		return true
	}
	if from == to || to == memory.TypeString || to == memory.TypeFloat64 {
		return false
	}
	if to == memory.TypeFloat32 {
		return from == memory.TypeFloat64
	}
	if from == memory.TypeString {
		return true
	}
	if from == memory.TypeDate32 && (to == memory.TypeInt32 || to == memory.TypeInt64) ||
		from == memory.TypeInt32 && to == memory.TypeDate32 ||
		from == memory.TypeInt64 && (to == memory.TypeTimestamp || to == memory.TypeDuration) ||
		(from == memory.TypeTimestamp || from == memory.TypeDuration) && to == memory.TypeInt64 {
		return false
	}
	if isSignedInteger(from) && isSignedInteger(to) {
		return integerWidth(from) > integerWidth(to)
	}
	if unsignedInteger(from) && unsignedInteger(to) {
		return integerWidth(from) > integerWidth(to)
	}
	if unsignedInteger(from) && isSignedInteger(to) {
		return integerWidth(from) >= integerWidth(to)
	}
	return true
}

func unsignedInteger(typ memory.DataType) bool {
	return typ == memory.TypeUint8 || typ == memory.TypeUint16 || typ == memory.TypeUint32 || typ == memory.TypeUint64
}

func integerWidth(typ memory.DataType) int {
	switch typ {
	case memory.TypeInt8, memory.TypeUint8:
		return 8
	case memory.TypeInt16, memory.TypeUint16:
		return 16
	case memory.TypeInt32, memory.TypeUint32:
		return 32
	case memory.TypeInt64, memory.TypeUint64:
		return 64
	default:
		return 0
	}
}
