package logical

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"gocolumn/source"
)

// PartitionedDecimalPrefixAverage is an explicit, partitioned ROWS prefix, not a legacy Window.
type PartitionedDecimalPrefixAverage struct {
	Input                               Plan
	Column, PartitionBy, OrderBy, Alias string
	Precision, Scale                    int32
	Rounding                            aggregate.DecimalAverageRoundingMode
}

func (*PartitionedDecimalPrefixAverage) planNode() {}

func inferPartitionedDecimalPrefixAverage(n *PartitionedDecimalPrefixAverage) (memory.Schema, error) {
	if n == nil || n.Column == "" || n.OrderBy == "" || n.Alias == "" || n.Column == n.OrderBy || n.PartitionBy == "" || n.PartitionBy == n.Column || n.PartitionBy == n.OrderBy {
		return memory.Schema{}, fmt.Errorf("partitioned decimal prefix AVG requires distinct key, value and output alias")
	}
	scan, err := decimalPrefixScan(n.Input)
	if err != nil {
		return memory.Schema{}, err
	}
	schema, err := InferSchema(n.Input)
	if err != nil {
		return memory.Schema{}, err
	}
	if len(schema.Fields) != 3 {
		return memory.Schema{}, fmt.Errorf("partitioned decimal prefix AVG requires exactly partition, order and value columns")
	}
	partition, found := schema.Field(n.PartitionBy)
	if !found || partition.Type != memory.TypeInt64 || partition.Encoding != memory.EncodingPlain || partition.DictionaryDomain != "" || len(partition.Children) != 0 || partition.ArrowType != "" || partition.DecimalPrecision != 0 || partition.DecimalScale != 0 {
		return memory.Schema{}, fmt.Errorf("partition key must be plain Int64")
	}
	k, ok := schema.Field(n.OrderBy)
	if !ok || k.Type != memory.TypeInt64 || k.Nullable || k.Encoding != memory.EncodingPlain || k.DictionaryDomain != "" || len(k.Children) != 0 || k.ArrowType != "" || k.DecimalPrecision != 0 || k.DecimalScale != 0 {
		return memory.Schema{}, fmt.Errorf("partitioned decimal prefix AVG requires a plain required Int64 key")
	}
	v, ok := schema.Field(n.Column)
	if !ok {
		return memory.Schema{}, fmt.Errorf("partitioned decimal prefix AVG value missing")
	}
	if err := v.ValidateDecimal128(); err != nil {
		return memory.Schema{}, err
	}
	if _, exists := schema.Field(n.Alias); exists {
		return memory.Schema{}, fmt.Errorf("partitioned decimal prefix AVG alias exists")
	}
	op, ok := scan.Source.(source.OrderingProvider)
	if !ok {
		return memory.Schema{}, fmt.Errorf("partitioned decimal prefix AVG requires proven ascending ordering")
	}
	order := op.Ordering()
	if len(order) < 2 || order[0].Name != n.PartitionBy || order[0].Descending || order[0].NullsLast || order[1].Name != n.OrderBy || order[1].Descending {
		return memory.Schema{}, fmt.Errorf("partitioned decimal prefix AVG requires proven ascending ordering")
	}
	if !decimalPrefixBounded(source.StreamPlanOf(scan.Source)) {
		return memory.Schema{}, fmt.Errorf("partitioned decimal prefix AVG requires bounded non-spilling scan")
	}
	_, _, err = (aggregate.DecimalSumState{}).FinalizeAverage(aggregate.DecimalAverageOptions{InputScale: v.DecimalScale, Precision: n.Precision, Scale: n.Scale, Mode: n.Rounding})
	if err != nil {
		return memory.Schema{}, err
	}
	out, err := memory.Decimal128Field(n.Alias, true, n.Precision, n.Scale)
	if err != nil {
		return memory.Schema{}, err
	}
	return memory.NewSchema(append(append([]memory.Field(nil), schema.Fields...), out)...), nil
}
