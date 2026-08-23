package optimizer

import (
	"fmt"
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
)

// Result contains an optimized logical plan and the rules that changed it.
type Result struct {
	Plan         logical.Plan
	AppliedRules []string
}

// Optimize validates plan and applies the current logical optimizer rules:
// filter merge, projection merge/removal, projection pushdown, and predicate
// pushdown annotations for source-level pruning.
func Optimize(plan logical.Plan) (Result, error) {
	if plan == nil {
		return Result{}, fmt.Errorf("logical plan is nil")
	}
	if _, err := logical.InferSchema(plan); err != nil {
		return Result{}, err
	}
	var rules []string
	p := mergeFilters(plan, &rules)
	p = mergeProjections(p, &rules)
	p = mergeLimits(p, &rules)
	p = pushPredicates(p, nil, &rules)
	p, err := pushProjection(p, nil, &rules)
	if err != nil {
		return Result{}, err
	}
	p = removeNoopProjections(p, &rules)
	if _, err := logical.InferSchema(p); err != nil {
		return Result{}, fmt.Errorf("optimized plan invalid: %w", err)
	}
	return Result{Plan: p, AppliedRules: uniqueStrings(rules)}, nil
}

func mergeFilters(plan logical.Plan, rules *[]string) logical.Plan {
	switch n := plan.(type) {
	case *logical.Filter:
		child := mergeFilters(n.Input, rules)
		if inner, ok := child.(*logical.Filter); ok {
			*rules = append(*rules, "filter-merge")
			return &logical.Filter{Input: inner.Input, Predicate: expr.MergeAnd(inner.Predicate, n.Predicate)}
		}
		return &logical.Filter{Input: child, Predicate: n.Predicate}
	case *logical.Projection:
		return &logical.Projection{Input: mergeFilters(n.Input, rules), Columns: append([]string(nil), n.Columns...)}
	case *logical.ExprProjection:
		return &logical.ExprProjection{Input: mergeFilters(n.Input, rules), Exprs: cloneNamedExprs(n.Exprs), IncludeInput: n.IncludeInput}
	case *logical.Join:
		return &logical.Join{Left: mergeFilters(n.Left, rules), Right: mergeFilters(n.Right, rules), Type: n.Type, Keys: append([]logical.JoinKey(nil), n.Keys...), Suffix: n.Suffix, RadixBits: n.RadixBits, Workers: n.Workers}
	case *logical.Sort:
		return &logical.Sort{Input: mergeFilters(n.Input, rules), By: cloneSortKeys(n.By)}
	case *logical.TopK:
		return &logical.TopK{Input: mergeFilters(n.Input, rules), K: n.K, By: cloneSortKeys(n.By)}
	case *logical.Limit:
		return &logical.Limit{Input: mergeFilters(n.Input, rules), N: n.N}
	case *logical.Aggregate:
		return &logical.Aggregate{Input: mergeFilters(n.Input, rules), GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}
	case *logical.Scan:
		return cloneScan(n)
	default:
		return plan
	}
}

func mergeProjections(plan logical.Plan, rules *[]string) logical.Plan {
	switch n := plan.(type) {
	case *logical.Projection:
		child := mergeProjections(n.Input, rules)
		if inner, ok := child.(*logical.Projection); ok {
			*rules = append(*rules, "projection-merge")
			return &logical.Projection{Input: inner.Input, Columns: append([]string(nil), n.Columns...)}
		}
		return &logical.Projection{Input: child, Columns: append([]string(nil), n.Columns...)}
	case *logical.Filter:
		return &logical.Filter{Input: mergeProjections(n.Input, rules), Predicate: n.Predicate}
	case *logical.ExprProjection:
		return &logical.ExprProjection{Input: mergeProjections(n.Input, rules), Exprs: cloneNamedExprs(n.Exprs), IncludeInput: n.IncludeInput}
	case *logical.Join:
		return &logical.Join{Left: mergeProjections(n.Left, rules), Right: mergeProjections(n.Right, rules), Type: n.Type, Keys: append([]logical.JoinKey(nil), n.Keys...), Suffix: n.Suffix, RadixBits: n.RadixBits, Workers: n.Workers}
	case *logical.Sort:
		return &logical.Sort{Input: mergeProjections(n.Input, rules), By: cloneSortKeys(n.By)}
	case *logical.TopK:
		return &logical.TopK{Input: mergeProjections(n.Input, rules), K: n.K, By: cloneSortKeys(n.By)}
	case *logical.Limit:
		return &logical.Limit{Input: mergeProjections(n.Input, rules), N: n.N}
	case *logical.Aggregate:
		return &logical.Aggregate{Input: mergeProjections(n.Input, rules), GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}
	case *logical.Scan:
		return cloneScan(n)
	default:
		return plan
	}
}

func mergeLimits(plan logical.Plan, rules *[]string) logical.Plan {
	switch n := plan.(type) {
	case *logical.Limit:
		child := mergeLimits(n.Input, rules)
		if inner, ok := child.(*logical.Limit); ok {
			limit := n.N
			if inner.N < limit {
				limit = inner.N
			}
			*rules = append(*rules, "limit-merge")
			return &logical.Limit{Input: inner.Input, N: limit}
		}
		if top, ok := child.(*logical.TopK); ok && int64(top.K) > n.N {
			*rules = append(*rules, "limit-into-topk")
			return &logical.TopK{Input: top.Input, K: int(n.N), By: cloneSortKeys(top.By)}
		}
		if sorted, ok := child.(*logical.Sort); ok && n.N <= int64(^uint(0)>>1) {
			*rules = append(*rules, "sort-limit-to-topk")
			return &logical.TopK{Input: sorted.Input, K: int(n.N), By: cloneSortKeys(sorted.By)}
		}
		return &logical.Limit{Input: child, N: n.N}
	case *logical.Filter:
		return &logical.Filter{Input: mergeLimits(n.Input, rules), Predicate: n.Predicate}
	case *logical.Projection:
		return &logical.Projection{Input: mergeLimits(n.Input, rules), Columns: append([]string(nil), n.Columns...)}
	case *logical.ExprProjection:
		return &logical.ExprProjection{Input: mergeLimits(n.Input, rules), Exprs: cloneNamedExprs(n.Exprs), IncludeInput: n.IncludeInput}
	case *logical.Sort:
		return &logical.Sort{Input: mergeLimits(n.Input, rules), By: cloneSortKeys(n.By)}
	case *logical.TopK:
		return &logical.TopK{Input: mergeLimits(n.Input, rules), K: n.K, By: cloneSortKeys(n.By)}
	case *logical.Join:
		return &logical.Join{Left: mergeLimits(n.Left, rules), Right: mergeLimits(n.Right, rules), Type: n.Type, Keys: append([]logical.JoinKey(nil), n.Keys...), Suffix: n.Suffix, RadixBits: n.RadixBits, Workers: n.Workers}
	case *logical.Aggregate:
		return &logical.Aggregate{Input: mergeLimits(n.Input, rules), GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}
	case *logical.Scan:
		return cloneScan(n)
	default:
		return plan
	}
}

// pushPredicates annotates scans with every filter predicate that can cross
// column-only projections. Filters are intentionally retained as residual
// row-level predicates because row-group statistics only prove that a group can
// be skipped; they do not identify matching rows inside a retained group.
func pushPredicates(plan logical.Plan, inherited expr.Expr, rules *[]string) logical.Plan {
	switch n := plan.(type) {
	case *logical.Filter:
		combined := expr.MergeAnd(inherited, n.Predicate)
		child := pushPredicates(n.Input, combined, rules)
		return &logical.Filter{Input: child, Predicate: n.Predicate}
	case *logical.Projection:
		return &logical.Projection{Input: pushPredicates(n.Input, inherited, rules), Columns: append([]string(nil), n.Columns...)}
	case *logical.ExprProjection:
		// A predicate above a computed projection may reference aliases or replaced
		// columns, so it cannot be pushed below the expression boundary without
		// substitution analysis. Filters already inside the child still push down.
		return &logical.ExprProjection{Input: pushPredicates(n.Input, nil, rules), Exprs: cloneNamedExprs(n.Exprs), IncludeInput: n.IncludeInput}
	case *logical.Sort:
		return &logical.Sort{Input: pushPredicates(n.Input, inherited, rules), By: cloneSortKeys(n.By)}
	case *logical.TopK:
		return &logical.TopK{Input: pushPredicates(n.Input, nil, rules), K: n.K, By: cloneSortKeys(n.By)}
	case *logical.Limit:
		return &logical.Limit{Input: pushPredicates(n.Input, nil, rules), N: n.N}
	case *logical.Join:
		// A predicate above a join cannot be pushed blindly to either input: for
		// outer joins it may change NULL-extension semantics, and even for inner
		// joins expression ownership must first be resolved. Optimize predicates
		// already inside each side independently and retain inherited predicates
		// above the join as residual filters.
		return &logical.Join{Left: pushPredicates(n.Left, nil, rules), Right: pushPredicates(n.Right, nil, rules), Type: n.Type, Keys: append([]logical.JoinKey(nil), n.Keys...), Suffix: n.Suffix, RadixBits: n.RadixBits, Workers: n.Workers}
	case *logical.Aggregate:
		// Predicates above an aggregate are HAVING predicates and may not be
		// pushed through the aggregation boundary. Predicates already inside the
		// aggregate subtree are optimized independently.
		return &logical.Aggregate{Input: pushPredicates(n.Input, nil, rules), GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}

	case *logical.Scan:
		out := cloneScan(n)
		if inherited != nil {
			out.PushedPredicate = expr.MergeAnd(out.PushedPredicate, inherited)
			*rules = append(*rules, "predicate-pushdown")
		}
		return out
	default:
		return plan
	}
}

// pushProjection propagates the columns required by each operator to the scan.
// nil means the caller requires every output column from the current node.
func pushProjection(plan logical.Plan, required map[string]struct{}, rules *[]string) (logical.Plan, error) {
	switch n := plan.(type) {
	case *logical.Aggregate:
		childRequired := make(map[string]struct{})
		for _, name := range n.GroupBy {
			childRequired[name] = struct{}{}
		}
		for _, a := range n.Aggregates {
			if a.Column != "" {
				childRequired[a.Column] = struct{}{}
			}
		}
		child, err := pushProjection(n.Input, childRequired, rules)
		if err != nil {
			return nil, err
		}
		return &logical.Aggregate{Input: child, GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}, nil

	case *logical.Filter:
		childRequired := cloneSet(required)
		if childRequired == nil {
			childRequired = allColumns(n.Input)
		}
		for _, name := range expr.Columns(n.Predicate) {
			childRequired[name] = struct{}{}
		}
		child, err := pushProjection(n.Input, childRequired, rules)
		if err != nil {
			return nil, err
		}
		return &logical.Filter{Input: child, Predicate: n.Predicate}, nil

	case *logical.Projection:
		projectionSet := make(map[string]struct{}, len(n.Columns))
		for _, name := range n.Columns {
			projectionSet[name] = struct{}{}
		}
		childRequired := make(map[string]struct{})
		if required == nil {
			for name := range projectionSet {
				childRequired[name] = struct{}{}
			}
		} else {
			for name := range required {
				if _, ok := projectionSet[name]; ok {
					childRequired[name] = struct{}{}
				}
			}
		}
		child, err := pushProjection(n.Input, childRequired, rules)
		if err != nil {
			return nil, err
		}
		return &logical.Projection{Input: child, Columns: append([]string(nil), n.Columns...)}, nil

	case *logical.ExprProjection:
		inSchema, err := logical.InferSchema(n.Input)
		if err != nil {
			return nil, err
		}
		wanted := required
		if wanted == nil {
			outSchema, err := logical.InferSchema(n)
			if err != nil {
				return nil, err
			}
			wanted = make(map[string]struct{}, len(outSchema.Fields))
			for _, f := range outSchema.Fields {
				wanted[f.Name] = struct{}{}
			}
		}
		produced := make(map[string]expr.NamedExpr, len(n.Exprs))
		kept := make([]expr.NamedExpr, 0, len(n.Exprs))
		for _, e := range n.Exprs {
			name, err := e.Name()
			if err != nil {
				return nil, err
			}
			produced[name] = e
			if _, ok := wanted[name]; ok {
				kept = append(kept, e)
			}
		}
		childReq := make(map[string]struct{})
		for name := range wanted {
			if e, computed := produced[name]; computed {
				for _, dep := range expr.Columns(e.Expr) {
					childReq[dep] = struct{}{}
				}
				continue
			}
			if n.IncludeInput {
				if _, ok := inSchema.Field(name); ok {
					childReq[name] = struct{}{}
				}
			}
		}
		child, err := pushProjection(n.Input, childReq, rules)
		if err != nil {
			return nil, err
		}
		if len(kept) != len(n.Exprs) {
			*rules = append(*rules, "expression-pruning")
		}
		return &logical.ExprProjection{Input: child, Exprs: kept, IncludeInput: n.IncludeInput}, nil

	case *logical.Sort:
		childRequired := cloneSet(required)
		if childRequired == nil {
			childRequired = allColumns(n.Input)
		}
		for _, key := range n.By {
			childRequired[key.Column] = struct{}{}
		}
		child, err := pushProjection(n.Input, childRequired, rules)
		if err != nil {
			return nil, err
		}
		return &logical.Sort{Input: child, By: cloneSortKeys(n.By)}, nil

	case *logical.TopK:
		childRequired := cloneSet(required)
		if childRequired == nil {
			childRequired = allColumns(n.Input)
		}
		for _, key := range n.By {
			childRequired[key.Column] = struct{}{}
		}
		child, err := pushProjection(n.Input, childRequired, rules)
		if err != nil {
			return nil, err
		}
		return &logical.TopK{Input: child, K: n.K, By: cloneSortKeys(n.By)}, nil

	case *logical.Limit:
		child, err := pushProjection(n.Input, required, rules)
		if err != nil {
			return nil, err
		}
		return &logical.Limit{Input: child, N: n.N}, nil

	case *logical.Join:
		leftSchema, err := logical.InferSchema(n.Left)
		if err != nil {
			return nil, err
		}
		rightSchema, err := logical.InferSchema(n.Right)
		if err != nil {
			return nil, err
		}
		leftReq := make(map[string]struct{})
		rightReq := make(map[string]struct{})
		for _, key := range n.Keys {
			leftReq[key.Left] = struct{}{}
			rightReq[key.Right] = struct{}{}
		}
		if required == nil {
			for _, f := range leftSchema.Fields {
				leftReq[f.Name] = struct{}{}
			}
			for _, f := range rightSchema.Fields {
				rightReq[f.Name] = struct{}{}
			}
		} else {
			suffix := n.Suffix
			if suffix == "" {
				suffix = "_right"
			}
			leftNames := make(map[string]struct{}, len(leftSchema.Fields))
			for _, f := range leftSchema.Fields {
				leftNames[f.Name] = struct{}{}
				if _, ok := required[f.Name]; ok {
					leftReq[f.Name] = struct{}{}
				}
			}
			for _, f := range rightSchema.Fields {
				outName := f.Name
				if _, dup := leftNames[outName]; dup {
					outName += suffix
				}
				if _, ok := required[outName]; ok {
					rightReq[f.Name] = struct{}{}
				}
			}
		}
		left, err := pushProjection(n.Left, leftReq, rules)
		if err != nil {
			return nil, err
		}
		right, err := pushProjection(n.Right, rightReq, rules)
		if err != nil {
			return nil, err
		}
		*rules = append(*rules, "join-projection-pushdown")
		return &logical.Join{Left: left, Right: right, Type: n.Type, Keys: append([]logical.JoinKey(nil), n.Keys...), Suffix: n.Suffix, RadixBits: n.RadixBits, Workers: n.Workers}, nil

	case *logical.Scan:
		out := cloneScan(n)
		base := n.Source.Schema()
		if required == nil {
			out.Projection = schemaNames(base)
		} else {
			out.Projection = orderedRequired(base, required)
			if len(out.Projection) != len(required) {
				return nil, fmt.Errorf("projection pushdown references columns not present in scan")
			}
		}
		if !equalStrings(out.Projection, schemaNames(base)) {
			*rules = append(*rules, "projection-pushdown")
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported logical node %T", plan)
	}
}

func removeNoopProjections(plan logical.Plan, rules *[]string) logical.Plan {
	switch n := plan.(type) {
	case *logical.Projection:
		child := removeNoopProjections(n.Input, rules)
		childSchema, err := logical.InferSchema(child)
		if err == nil && equalStrings(n.Columns, schemaNames(childSchema)) {
			*rules = append(*rules, "remove-noop-projection")
			return child
		}
		return &logical.Projection{Input: child, Columns: append([]string(nil), n.Columns...)}
	case *logical.Filter:
		return &logical.Filter{Input: removeNoopProjections(n.Input, rules), Predicate: n.Predicate}
	case *logical.ExprProjection:
		return &logical.ExprProjection{Input: removeNoopProjections(n.Input, rules), Exprs: cloneNamedExprs(n.Exprs), IncludeInput: n.IncludeInput}
	case *logical.Sort:
		return &logical.Sort{Input: removeNoopProjections(n.Input, rules), By: cloneSortKeys(n.By)}
	case *logical.TopK:
		return &logical.TopK{Input: removeNoopProjections(n.Input, rules), K: n.K, By: cloneSortKeys(n.By)}
	case *logical.Limit:
		return &logical.Limit{Input: removeNoopProjections(n.Input, rules), N: n.N}
	case *logical.Join:
		return &logical.Join{Left: removeNoopProjections(n.Left, rules), Right: removeNoopProjections(n.Right, rules), Type: n.Type, Keys: append([]logical.JoinKey(nil), n.Keys...), Suffix: n.Suffix, RadixBits: n.RadixBits, Workers: n.Workers}
	case *logical.Aggregate:
		return &logical.Aggregate{Input: removeNoopProjections(n.Input, rules), GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}
	case *logical.Scan:
		return cloneScan(n)
	default:
		return plan
	}
}

func allColumns(plan logical.Plan) map[string]struct{} {
	s, err := logical.InferSchema(plan)
	if err != nil {
		return make(map[string]struct{})
	}
	out := make(map[string]struct{}, len(s.Fields))
	for _, f := range s.Fields {
		out[f.Name] = struct{}{}
	}
	return out
}

func schemaNames(s memory.Schema) []string {
	out := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		out[i] = f.Name
	}
	return out
}

func orderedRequired(s memory.Schema, required map[string]struct{}) []string {
	out := make([]string, 0, len(required))
	for _, f := range s.Fields {
		if _, ok := required[f.Name]; ok {
			out = append(out, f.Name)
		}
	}
	return out
}

func cloneSet(in map[string]struct{}) map[string]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func cloneScan(n *logical.Scan) *logical.Scan {
	if n == nil {
		return nil
	}
	out := *n
	out.Projection = append([]string(nil), n.Projection...)
	out.Pushdown.Projection = append([]string(nil), n.Pushdown.Projection...)
	return &out
}

func cloneNamedExprs(in []expr.NamedExpr) []expr.NamedExpr {
	return append([]expr.NamedExpr(nil), in...)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func cloneSortKeys(in []logical.SortKey) []logical.SortKey {
	return append([]logical.SortKey(nil), in...)
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
