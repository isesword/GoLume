package lazy

import (
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/logical"
)

// Quantile constructs an exact numeric quantile for AggConfigured.
func Quantile(column string, probability float64, mode aggregate.QuantileMode, alias string) expr.QuantileSpec {
	return expr.Quantile(column, probability, mode, alias)
}

// Median constructs an exact numeric midpoint median for AggConfigured.
func Median(column, alias string) expr.QuantileSpec { return expr.Median(column, alias) }

// AggConfigured computes global legacy and configured aggregates together.
func (f LazyFrame) AggConfigured(aggregates ...expr.AggregateSpec) LazyFrame {
	return f.GroupBy().AggConfigured(aggregates...)
}

// AggConfigured completes a grouped parameterized aggregation without executing
// it. It copies specifications; the caller may subsequently reuse its slice.
func (g GroupedFrame) AggConfigured(aggregates ...expr.AggregateSpec) LazyFrame {
	f := g.frame
	copied := append([]expr.AggregateSpec(nil), aggregates...)
	if _, _, err := expr.NormalizeAggregates(copied); err != nil {
		f.err = err
		return f
	}
	f.plan = &logical.ConfiguredAggregate{Input: f.plan, GroupBy: append([]string(nil), g.keys...), Aggregates: copied, Hints: f.hints}
	f.hints = expr.Hints{}
	return f
}

func terminalAggregate(plan logical.Plan) bool {
	switch plan.(type) {
	case *logical.Aggregate, *logical.ConfiguredAggregate:
		return true
	}
	return false
}
