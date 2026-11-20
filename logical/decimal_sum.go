package logical

import (
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
)

// inferDecimalSum preserves the legacy numeric dtype/result protocol. It only
// claims a plan when an aggregate references a root native Decimal field.
func inferDecimalSum(n *Aggregate, in memory.Schema) (memory.Schema, bool, error) {
	native := false
	for _, a := range n.Aggregates {
		if f, ok := in.Field(a.Column); ok && f.Type == memory.TypeDecimal && f.DecimalPrecision != 0 {
			native = true
		}
	}
	if !native {
		return memory.Schema{}, false, nil
	}
	if len(n.GroupBy) > 2 {
		return memory.Schema{}, true, fmt.Errorf("native Decimal grouping requires one or two Int64 keys")
	}
	var keys [2]memory.Field
	for i, name := range n.GroupBy {
		f, ok := in.Field(name)
		if !ok || f.Type != memory.TypeInt64 {
			return memory.Schema{}, true, fmt.Errorf("native Decimal grouping requires one or two Int64 keys")
		}
		for j := 0; j < i; j++ {
			if keys[j].Name == name {
				return memory.Schema{}, true, fmt.Errorf("aggregate contains duplicate output %q", name)
			}
		}
		keys[i] = f
	}
	hasSum := false
	for _, a := range n.Aggregates {
		if a.Kind == expr.AggSum {
			hasSum = true
		}
	}
	if !hasSum {
		return memory.Schema{}, true, fmt.Errorf("native Decimal aggregation requires at least one native Decimal SUM")
	}
	fields := make([]memory.Field, len(n.Aggregates))
	var names map[string]struct{}
	if len(n.GroupBy) == 2 {
		names = map[string]struct{}{keys[0].Name: {}, keys[1].Name: {}}
	}
	for i, a := range n.Aggregates {
		name := a.Alias
		if name == "" {
			name = fmt.Sprintf("agg_%d", i)
		}
		if names != nil {
			if _, duplicate := names[name]; duplicate {
				return memory.Schema{}, true, fmt.Errorf("aggregate contains duplicate output %q", name)
			}
			names[name] = struct{}{}
		}
		// COUNT(*) ignores Column, matching the existing aggregate contract.
		if a.Kind == expr.AggCountRows {
			fields[i] = memory.Field{Name: name, Type: memory.TypeUint64}
			continue
		}
		f, ok := in.Field(a.Column)
		if !ok || a.Column == "" || (a.Kind != expr.AggSum && a.Kind != expr.AggCountValues) || f.Type != memory.TypeDecimal || f.DecimalPrecision == 0 {
			if len(n.GroupBy) == 0 {
				return memory.Schema{}, true, fmt.Errorf("native Decimal aggregation requires global native Decimal SUM with optional COUNT(*) or COUNT(native Decimal)")
			}
			return memory.Schema{}, true, fmt.Errorf("native Decimal grouped aggregation requires native Decimal SUM with optional COUNT(*) or COUNT(native Decimal)")
		}
		if e := f.ValidateDecimal128(); e != nil {
			return memory.Schema{}, true, e
		}
		if a.Kind == expr.AggCountValues {
			fields[i] = memory.Field{Name: name, Type: memory.TypeUint64}
			continue
		}
		out, e := memory.Decimal128Field(name, true, 38, f.DecimalScale)
		if e != nil {
			return memory.Schema{}, true, e
		}
		fields[i] = out
	}
	if len(n.GroupBy) != 0 {
		fields = append(keys[:len(n.GroupBy):len(n.GroupBy)], fields...)
	}
	return memory.NewSchema(fields...), true, nil
}
