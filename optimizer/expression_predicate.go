package optimizer

import (
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
)

// predicateThroughExpressions translates only column identities, never computed
// values. Projection expressions all read the same input schema: replacements
// and swaps therefore require simultaneous, not recursive, substitution.
func predicateThroughExpressions(n *logical.ExprProjection, predicate expr.Expr) expr.Expr {
	if predicate == nil {
		return nil
	}
	schema, err := logical.InferSchema(n.Input)
	if err != nil {
		return nil
	}
	names := make(map[string]string)
	if n.IncludeInput {
		for _, field := range schema.Fields {
			names[field.Name] = field.Name
		}
	}
	for _, value := range n.Exprs {
		bound, err := expr.Bind(schema, value.Expr)
		// Even an unrelated output must not lose rows on which it would error.
		// MayError includes guarded children. Dead-output pruning runs later, so
		// checking every output here deliberately errs on the side of safety.
		if err != nil || bound.MayError {
			return nil
		}
		name, err := value.Name()
		if err != nil {
			return nil
		}
		delete(names, name)
		if column, ok := value.Expr.(expr.Column); ok {
			names[name] = column.Name
		}
	}
	rewritten, ok := renamePredicateColumns(predicate, names)
	if !ok {
		return nil
	}
	bound, err := expr.Bind(schema, rewritten)
	if err != nil || bound.MayError || bound.Field.Type != memory.TypeBool {
		return nil
	}
	return rewritten
}

// renamePredicateColumns preserves operand order and guarded structure. Unknown
// expression nodes fail closed so adding a new expression cannot silently leak
// an output name into the input namespace. Caller-owned slices are never edited.
func renamePredicateColumns(value expr.Expr, names map[string]string) (expr.Expr, bool) {
	ok := true
	rename := func(child expr.Expr) expr.Expr {
		rewritten, valid := renamePredicateColumns(child, names)
		ok = ok && valid
		return rewritten
	}
	switch n := value.(type) {
	case expr.Column:
		name, found := names[n.Name]
		return expr.Col(name), found
	case expr.Int64Literal, expr.Float64Literal, expr.StringLiteral,
		expr.BoolLiteral, expr.NullLiteral, expr.Date32Literal,
		expr.TimestampLiteral, expr.TimestampUnitLiteral, expr.DurationLiteral:
		return value, true
	case expr.Binary:
		n.Left, n.Right = rename(n.Left), rename(n.Right)
		return n, ok
	case expr.Unary:
		n.Value = rename(n.Value)
		return n, ok
	case expr.Ternary:
		n.Value, n.First, n.Second = rename(n.Value), rename(n.First), rename(n.Second)
		return n, ok
	case expr.Cast:
		n.Value = rename(n.Value)
		return n, ok
	case expr.Decimal128RescaleExpr:
		n.Value = rename(n.Value)
		return n, ok
	case expr.TryCastExpr:
		n.Value = rename(n.Value)
		return n, ok
	case expr.JSONDecodeExpr:
		n.Value = rename(n.Value)
		return n, ok
	case expr.IfExpr:
		n.Condition, n.Then, n.Else = rename(n.Condition), rename(n.Then), rename(n.Else)
		return n, ok
	case expr.CaseExpr:
		n.Whens = append([]expr.WhenThen(nil), n.Whens...)
		for i := range n.Whens {
			n.Whens[i].When, n.Whens[i].Then = rename(n.Whens[i].When), rename(n.Whens[i].Then)
		}
		if n.Else != nil {
			n.Else = rename(n.Else)
		}
		return n, ok
	case expr.CoalesceExpr:
		n.Values = append([]expr.Expr(nil), n.Values...)
		for i := range n.Values {
			n.Values[i] = rename(n.Values[i])
		}
		return n, ok
	case expr.InExpr:
		n.Value = rename(n.Value)
		n.Values = append([]expr.Expr(nil), n.Values...)
		for i := range n.Values {
			n.Values[i] = rename(n.Values[i])
		}
		return n, ok
	default:
		return nil, false
	}
}
