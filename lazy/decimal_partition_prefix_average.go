package lazy

import (
	"gocolumn/aggregate"
	"gocolumn/logical"
	"gocolumn/source"
)

// WithPartitionedDecimalPrefixAverage adds an explicitly rounded Decimal128 prefix AVG.
// It requires exactly partition/order/value columns from an already ordered bounded
// scan, NULL-first ascending Int64 partitions, a required strictly increasing
// Int64 order within each partition, and finite MemoryOnly execution.
// It never inserts a sort or accepts an upstream FILTER.
func (f LazyFrame) WithPartitionedDecimalPrefixAverage(column, partitionBy, orderBy, alias string, precision, scale int32, rounding aggregate.DecimalAverageRoundingMode) LazyFrame {
	if f.err != nil {
		return f
	}
	f.plan = &logical.PartitionedDecimalPrefixAverage{Input: f.plan, Column: column, PartitionBy: partitionBy, OrderBy: orderBy, Alias: alias, Precision: precision, Scale: scale, Rounding: rounding}
	return f
}

type decimalPartitionPrefixProjectedSource struct {
	source.Source
	key       string
	partition string
}

func (s *decimalPartitionPrefixProjectedSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: s.partition}, {Name: s.key}}
}
func (s *decimalPartitionPrefixProjectedSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanOf(s.Source)
}
