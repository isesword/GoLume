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
		if inner, ok := child.(*logical.Filter); ok && !mayError(inner, n.Predicate) {
			// Sequential filters demand the outer expression only for TRUE rows.
			// SQL AND also demands its right side for NULL left values, so a
			// fallible outer expression must retain its own row-selection boundary.
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
	case *logical.ConfiguredAggregate:
		return cloneConfiguredAggregate(n, mergeFilters(n.Input, rules))
	case *logical.Aggregate:
		return &logical.Aggregate{Input: mergeFilters(n.Input, rules), GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}
	case *logical.Window:
		return cloneWindow(n, mergeFilters(n.Input, rules))
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
	case *logical.ConfiguredAggregate:
		return cloneConfiguredAggregate(n, mergeProjections(n.Input, rules))
	case *logical.Aggregate:
		return &logical.Aggregate{Input: mergeProjections(n.Input, rules), GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}
	case *logical.Window:
		return cloneWindow(n, mergeProjections(n.Input, rules))
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
	case *logical.ConfiguredAggregate:
		return cloneConfiguredAggregate(n, mergeLimits(n.Input, rules))
	case *logical.Aggregate:
		return &logical.Aggregate{Input: mergeLimits(n.Input, rules), GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}
	case *logical.Window:
		return cloneWindow(n, mergeLimits(n.Input, rules))
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
		var combined expr.Expr
		if !mayError(n.Input, n.Predicate) {
			combined = expr.MergeAnd(n.Predicate, inherited)
		}
		// An outer predicate cannot remove rows before a fallible inner filter:
		// doing so could suppress an error that the original plan demanded.
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
		for _, key := range n.By {
			value, err := key.Expression()
			if err != nil || mayError(n.Input, value) {
				inherited = nil
				break
			}
		}
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
	case *logical.ConfiguredAggregate:
		return cloneConfiguredAggregate(n, pushPredicates(n.Input, nil, rules))
	case *logical.Aggregate:
		// Predicates above an aggregate are HAVING predicates and may not be
		// pushed through the aggregation boundary. Predicates already inside the
		// aggregate subtree are optimized independently.
		return &logical.Aggregate{Input: pushPredicates(n.Input, nil, rules), GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}
	case *logical.Window:
		// A filter above a window changes the rows contributing to rank and
		// rolling frames, even when it mentions only inherited input columns.
		// Keep inherited predicates above this strict semantic boundary while
		// still optimizing predicates already present in the child subtree.
		return cloneWindow(n, pushPredicates(n.Input, nil, rules))

	case *logical.Scan:
		out := cloneScan(n)
		if out.PushedPredicate != nil && mayError(n, out.PushedPredicate) {
			out.PushedPredicate = nil
		}
		if inherited != nil {
			out.PushedPredicate = expr.MergeAnd(out.PushedPredicate, inherited)
			*rules = append(*rules, "predicate-pushdown")
		}
		return out
	default:
		return plan
	}
}

// mayError treats an expression that cannot be rebound as unsafe. Optimize has
// already validated the original plan; this conservative result prevents a
// rewrite from widening or suppressing its demanded row-value error domain.
func mayError(input logical.Plan, value expr.Expr) bool {
	schema, err := logical.InferSchema(input)
	if err != nil {
		return true
	}
	bound, err := expr.Bind(schema, value)
	return err != nil || bound.MayError
}

// pushProjection propagates the columns required by each operator to the scan.
// nil means the caller requires every output column from the current node.
// Projection nodes are narrowed to the columns their parent actually consumes
// so the node column list always matches what its pruned child provides, and
// WithColumns replacements whose original input column was pruned are ordered
// back into their original position.
func pushProjection(plan logical.Plan, required map[string]struct{}, rules *[]string) (logical.Plan, error) {
	switch n := plan.(type) {
	case *logical.Window:
		inSchema, err := logical.InferSchema(n.Input)
		if err != nil {
			return nil, err
		}
		wanted := required
		if wanted == nil {
			wanted = allColumns(n)
		}
		produced := make(map[string]logical.WindowExpr, len(n.Exprs))
		kept := make([]logical.WindowExpr, 0, len(n.Exprs))
		for _, window := range n.Exprs {
			produced[window.Alias] = window
			if _, ok := wanted[window.Alias]; ok {
				kept = append(kept, window)
			}
		}
		childRequired := make(map[string]struct{})
		for name := range wanted {
			if _, computed := produced[name]; computed {
				continue
			}
			if _, ok := inSchema.Field(name); ok {
				childRequired[name] = struct{}{}
			}
		}
		if len(kept) == 0 {
			child, err := pushProjection(n.Input, childRequired, rules)
			if err != nil {
				return nil, err
			}
			*rules = append(*rules, "window-elimination")
			return child, nil
		}
		for _, name := range n.Spec.PartitionBy {
			childRequired[name] = struct{}{}
		}
		for i, key := range n.Spec.By {
			value, err := key.Expression()
			if err != nil {
				return nil, fmt.Errorf("window order key %d: %w", i, err)
			}
			for _, name := range expr.Columns(value) {
				childRequired[name] = struct{}{}
			}
		}
		for _, window := range kept {
			if window.Column != "" {
				childRequired[window.Column] = struct{}{}
			}
		}
		child, err := pushProjection(n.Input, childRequired, rules)
		if err != nil {
			return nil, err
		}
		if len(kept) != len(n.Exprs) {
			*rules = append(*rules, "window-expression-pruning")
		}
		// A kept expression whose alias overwrites an input column replaces it
		// in place; restore the original position when pruning removed the
		// overwritten column from the child.
		return restoreReplacementOrder(cloneWindowExprs(n, child, kept), inSchema, "window-reorder", rules)
	case *logical.ConfiguredAggregate:
		aggs, _, err := expr.NormalizeAggregates(n.Aggregates)
		if err != nil {
			return nil, err
		}
		childRequired := make(map[string]struct{})
		for _, name := range n.GroupBy {
			childRequired[name] = struct{}{}
		}
		for _, a := range aggs {
			if a.Column != "" {
				childRequired[a.Column] = struct{}{}
			}
		}
		child, err := pushProjection(n.Input, childRequired, rules)
		if err != nil {
			return nil, err
		}
		return cloneConfiguredAggregate(n, child), nil
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
		columns := append([]string(nil), n.Columns...)
		if required != nil {
			// The child was pruned to the intersection, so this node must
			// project exactly that intersection or its column list would
			// reference columns the child no longer outputs.
			kept := make([]string, 0, len(childRequired))
			for _, name := range n.Columns {
				if _, ok := childRequired[name]; ok {
					kept = append(kept, name)
				}
			}
			columns = kept
		}
		return &logical.Projection{Input: child, Columns: columns}, nil

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
		dependencies := make(map[string][]string, len(n.Exprs))
		kept := make([]expr.NamedExpr, 0, len(n.Exprs))
		for _, e := range n.Exprs {
			name, err := e.Name()
			if err != nil {
				return nil, err
			}
			produced[name] = e
			bound, err := expr.Bind(inSchema, e.Expr)
			if err != nil {
				return nil, err
			}
			dependencies[name] = bound.Columns
			if _, ok := wanted[name]; ok {
				// Keep the established dead-output pruning contract: only
				// retained outputs are demanded by the optimized query.
				kept = append(kept, e)
			}
		}
		childReq := make(map[string]struct{})
		for _, e := range kept {
			name, _ := e.Name()
			for _, dep := range dependencies[name] {
				childReq[dep] = struct{}{}
			}
		}
		for name := range wanted {
			if _, computed := produced[name]; computed {
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
		if len(kept) == 0 && n.IncludeInput {
			*rules = append(*rules, "expression-projection-elimination")
			return child, nil
		}
		if len(kept) != len(n.Exprs) {
			*rules = append(*rules, "expression-pruning")
		}
		projected := &logical.ExprProjection{Input: child, Exprs: kept, IncludeInput: n.IncludeInput}
		if n.IncludeInput {
			return restoreReplacementOrder(projected, inSchema, "expression-projection-reorder", rules)
		}
		return projected, nil

	case *logical.Sort:
		childRequired := cloneSet(required)
		if childRequired == nil {
			childRequired = allColumns(n.Input)
		}
		for i, key := range n.By {
			value, err := key.Expression()
			if err != nil {
				return nil, fmt.Errorf("sort key %d: %w", i, err)
			}
			for _, name := range expr.Columns(value) {
				childRequired[name] = struct{}{}
			}
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
		for i, key := range n.By {
			value, err := key.Expression()
			if err != nil {
				return nil, fmt.Errorf("top-k key %d: %w", i, err)
			}
			for _, name := range expr.Columns(value) {
				childRequired[name] = struct{}{}
			}
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

// restoreReplacementOrder wraps a pruned node with a column-only reorder when
// pruning displaced an in-place replacement. It covers both WithColumns
// (ExprProjection IncludeInput) replacements and window expressions whose
// alias overwrites an input column: a kept replacement loses that column's
// original position whenever the column itself was pruned from the child
// (because the replacement does not depend on it, or a later projection
// replaced it again): the pruned child no longer carries the column, so
// replace-in-place semantics would append the replacement at the end. Both
// node kinds replace in place, so the output is reordered to keep every
// surviving input column at its original position while new columns stay
// appended at the end. The reorder is a pure permutation of the node output;
// it never resurrects pruned expressions or dead input columns. rule names
// the AppliedRules entry recorded when a reorder is inserted.
func restoreReplacementOrder(node logical.Plan, inSchema memory.Schema, rule string, rules *[]string) (logical.Plan, error) {
	schema, err := logical.InferSchema(node)
	if err != nil {
		return nil, err
	}
	actual := schemaNames(schema)
	inPositions := make(map[string]int, len(inSchema.Fields))
	for i, f := range inSchema.Fields {
		inPositions[f.Name] = i
	}
	stable := true
	last := -1
	for _, name := range actual {
		pos, ok := inPositions[name]
		if !ok {
			continue
		}
		if pos < last {
			stable = false
			break
		}
		last = pos
	}
	if stable {
		return node, nil
	}
	present := make(map[string]struct{}, len(actual))
	for _, name := range actual {
		present[name] = struct{}{}
	}
	ordered := make([]string, 0, len(actual))
	for _, f := range inSchema.Fields {
		if _, ok := present[f.Name]; ok {
			ordered = append(ordered, f.Name)
		}
	}
	for _, name := range actual {
		if _, ok := inPositions[name]; !ok {
			ordered = append(ordered, name)
		}
	}
	*rules = append(*rules, rule)
	return &logical.Projection{Input: node, Columns: ordered}, nil
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
	case *logical.ConfiguredAggregate:
		return cloneConfiguredAggregate(n, removeNoopProjections(n.Input, rules))
	case *logical.Aggregate:
		return &logical.Aggregate{Input: removeNoopProjections(n.Input, rules), GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}
	case *logical.Window:
		return cloneWindow(n, removeNoopProjections(n.Input, rules))
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

func cloneWindow(n *logical.Window, input logical.Plan) *logical.Window {
	return cloneWindowExprs(n, input, n.Exprs)
}

func cloneWindowExprs(n *logical.Window, input logical.Plan, expressions []logical.WindowExpr) *logical.Window {
	if n == nil {
		return nil
	}
	return &logical.Window{
		Input: input,
		Spec: logical.WindowSpec{
			PartitionBy: append([]string(nil), n.Spec.PartitionBy...),
			By:          cloneSortKeys(n.Spec.By),
		},
		Exprs: append([]logical.WindowExpr(nil), expressions...),
	}
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

func cloneConfiguredAggregate(n *logical.ConfiguredAggregate, input logical.Plan) *logical.ConfiguredAggregate {
	return &logical.ConfiguredAggregate{Input: input, GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.AggregateSpec(nil), n.Aggregates...), Hints: n.Hints}
}
