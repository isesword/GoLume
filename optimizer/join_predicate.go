package optimizer

import (
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/logical"
	"gocolumn/memory"
)

// joinColumn records the exact output namespace, including collisions with
// earlier right outputs (not just left input names). collision is the earlier
// binding whose presence caused the suffix; pruning must retain that binding.
type joinColumn struct {
	output, input string
	right         bool
	collision     int
}

func joinColumns(n *logical.Join, left, right memory.Schema) []joinColumn {
	columns := make([]joinColumn, 0, len(left.Fields)+len(right.Fields))
	seen := make(map[string]int)
	for _, field := range left.Fields {
		seen[field.Name] = len(columns)
		columns = append(columns, joinColumn{output: field.Name, input: field.Name, collision: -1})
	}
	if n.Type == join.Semi || n.Type == join.Anti {
		return columns
	}
	suffix := n.Suffix
	if suffix == "" {
		suffix = "_right"
	}
	for _, field := range right.Fields {
		column := joinColumn{output: field.Name, input: field.Name, right: true, collision: -1}
		if index, exists := seen[field.Name]; exists {
			column.output += suffix
			column.collision = index
		}
		seen[column.output] = len(columns)
		columns = append(columns, column)
	}
	return columns
}

// predicatesThroughJoin copies only infallible, side-local conjuncts below an
// inner join. The original predicate remains above the join. An AND may split,
// but an OR/conditional must move intact and reference only one input. Checking
// the entire predicate before splitting preserves guarded error domains.
func predicatesThroughJoin(n *logical.Join, predicate expr.Expr) (expr.Expr, expr.Expr) {
	if n.Type != join.Inner || predicate == nil || mayError(n, predicate) ||
		!joinInputInfallible(n.Left) || !joinInputInfallible(n.Right) {
		return nil, nil
	}
	left, err := logical.InferSchema(n.Left)
	if err != nil {
		return nil, nil
	}
	right, err := logical.InferSchema(n.Right)
	if err != nil {
		return nil, nil
	}
	leftNames, rightNames := make(map[string]string), make(map[string]string)
	for _, column := range joinColumns(n, left, right) {
		if column.right {
			rightNames[column.output] = column.input
		} else {
			leftNames[column.output] = column.input
		}
	}
	var leftPredicate, rightPredicate expr.Expr
	var visit func(expr.Expr)
	visit = func(value expr.Expr) {
		if binary, ok := value.(expr.Binary); ok && binary.Op == expr.OpAnd {
			visit(binary.Left)
			visit(binary.Right)
			return
		}
		// Constants alone offer no ownership evidence. Keep them as residuals.
		if len(expr.Columns(value)) == 0 {
			return
		}
		if renamed, ok := renamePredicateColumns(value, leftNames); ok && !mayError(n.Left, renamed) {
			leftPredicate = expr.MergeAnd(leftPredicate, renamed)
		} else if renamed, ok := renamePredicateColumns(value, rightNames); ok && !mayError(n.Right, renamed) {
			rightPredicate = expr.MergeAnd(rightPredicate, renamed)
		}
	}
	visit(predicate)
	return leftPredicate, rightPredicate
}

// Removing rows on either side can change which rows the other side demands
// (notably for streaming merge joins and downstream limits). Do not introduce
// that change when either subtree contains fallible expressions. Unknown and
// aggregate/window operators fail closed rather than assuming infallibility.
func joinInputInfallible(plan logical.Plan) bool {
	schema, err := logical.InferSchema(plan)
	if err != nil {
		return false
	}
	for _, field := range schema.Fields {
		// Native decimals are transport-only for now. A pushed filter can
		// reject the right payload before the join rejects the left payload,
		// changing the first error even when the predicate uses integers.
		if field.Type == memory.TypeDecimal && field.DecimalPrecision != 0 {
			return false
		}
	}
	switch n := plan.(type) {
	case *logical.Scan:
		return true
	case *logical.Filter:
		return !mayError(n.Input, n.Predicate) && joinInputInfallible(n.Input)
	case *logical.Projection:
		return joinInputInfallible(n.Input)
	case *logical.ExprProjection:
		for _, named := range n.Exprs {
			if mayError(n.Input, named.Expr) {
				return false
			}
		}
		return joinInputInfallible(n.Input)
	case *logical.Sort:
		return joinOrderInfallible(n.Input, n.By)
	case *logical.TopK:
		return joinOrderInfallible(n.Input, n.By)
	case *logical.Limit:
		return joinInputInfallible(n.Input)
	case *logical.Join:
		return joinInputInfallible(n.Left) && joinInputInfallible(n.Right)
	default:
		return false
	}
}

func joinOrderInfallible(input logical.Plan, keys []logical.SortKey) bool {
	return !sortKeysMayError(input, keys) && joinInputInfallible(input)
}
