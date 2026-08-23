package expr

import "sort"

// Columns returns the sorted, de-duplicated column names referenced by e.
func Columns(e Expr) []string {
	if e == nil {
		return nil
	}
	set := make(map[string]struct{})
	var visit func(Expr)
	visit = func(n Expr) {
		switch x := n.(type) {
		case Column:
			set[x.Name] = struct{}{}
		case Binary:
			visit(x.Left)
			visit(x.Right)
		case Unary:
			visit(x.Value)
		case Cast:
			visit(x.Value)
		}
	}
	visit(e)
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// MergeAnd combines predicates with SQL AND while preserving nil as the
// identity element. It is used by optimizer rules that merge adjacent filters
// or attach multiple residual predicates to one scan.
func MergeAnd(left, right Expr) Expr {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return And(left, right)
}
