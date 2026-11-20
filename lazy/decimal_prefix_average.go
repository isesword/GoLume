package lazy

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/engine"
	"gocolumn/logical"
	"gocolumn/source"
)

// WithDecimalPrefixAverage adds an explicitly rounded Decimal128 prefix AVG.
// It requires exactly the key/value columns from an already ordered bounded
// scan, a required strictly increasing Int64 key, and finite MemoryOnly execution.
// It never inserts a sort or accepts an upstream FILTER.
func (f LazyFrame) WithDecimalPrefixAverage(column, orderBy, alias string, precision, scale int32, rounding aggregate.DecimalAverageRoundingMode) LazyFrame {
	if f.err != nil {
		return f
	}
	f.plan = &logical.DecimalPrefixAverage{Input: f.plan, Column: column, OrderBy: orderBy, Alias: alias, Precision: precision, Scale: scale, Rounding: rounding}
	return f
}

func (f LazyFrame) validateDecimalPrefixExecution() error {
	if err := f.validateTimestampRangeExecution(); err != nil {
		return err
	}
	if !logical.ContainsDecimalPrefixAverage(f.plan) {
		return nil
	}
	if _, err := logical.InferSchema(f.plan); err != nil {
		return err
	}
	policy, err := f.effectiveExecutionPolicy()
	if err != nil {
		return err
	}
	if policy != engine.ExecutionMemoryOnly || f.session == nil || f.session.engine == nil || f.session.engine.Memory().Limit() <= 0 {
		return fmt.Errorf("decimal prefix AVG requires finite MemoryOnly execution")
	}
	return nil
}

// This witness is attached only after logical validation has followed a direct
// Scan -> Projection lineage to the original OrderingProvider. It does not
// accept computed/renamed keys or manufacture ordering from a column name.
type decimalPrefixProjectedSource struct {
	source.Source
	key string
}

func (s *decimalPrefixProjectedSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: s.key}}
}
func (s *decimalPrefixProjectedSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanOf(s.Source)
}
