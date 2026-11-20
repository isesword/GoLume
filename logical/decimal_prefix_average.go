package logical

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"gocolumn/source"
)

// DecimalPrefixAverage is an explicit, unpartitioned ROWS prefix, not a legacy Window.
type DecimalPrefixAverage struct {
	Input                  Plan
	Column, OrderBy, Alias string
	Precision, Scale       int32
	Rounding               aggregate.DecimalAverageRoundingMode
}

func (*DecimalPrefixAverage) planNode() {}

func decimalPrefixScan(p Plan) (*Scan, error) {
	switch n := p.(type) {
	case *Scan:
		if n == nil || n.Source == nil || n.PushedPredicate != nil {
			return nil, fmt.Errorf("decimal prefix AVG requires an unfiltered scan")
		}
		return n, nil
	case *Projection:
		return decimalPrefixScan(n.Input)
	default:
		return nil, fmt.Errorf("decimal prefix AVG supports only Scan and column Projection upstream")
	}
}
func inferDecimalPrefixAverage(n *DecimalPrefixAverage) (memory.Schema, error) {
	if n == nil || n.Column == "" || n.OrderBy == "" || n.Alias == "" || n.Column == n.OrderBy {
		return memory.Schema{}, fmt.Errorf("decimal prefix AVG requires distinct key, value and output alias")
	}
	scan, err := decimalPrefixScan(n.Input)
	if err != nil {
		return memory.Schema{}, err
	}
	schema, err := InferSchema(n.Input)
	if err != nil {
		return memory.Schema{}, err
	}
	if len(schema.Fields) != 2 {
		return memory.Schema{}, fmt.Errorf("decimal prefix AVG requires exactly key and value columns")
	}
	k, ok := schema.Field(n.OrderBy)
	if !ok || k.Type != memory.TypeInt64 || k.Nullable || k.Encoding != memory.EncodingPlain || k.DictionaryDomain != "" || len(k.Children) != 0 || k.ArrowType != "" || k.DecimalPrecision != 0 || k.DecimalScale != 0 {
		return memory.Schema{}, fmt.Errorf("decimal prefix AVG requires a plain required Int64 key")
	}
	v, ok := schema.Field(n.Column)
	if !ok {
		return memory.Schema{}, fmt.Errorf("decimal prefix AVG value missing")
	}
	if err := v.ValidateDecimal128(); err != nil {
		return memory.Schema{}, err
	}
	if _, exists := schema.Field(n.Alias); exists {
		return memory.Schema{}, fmt.Errorf("decimal prefix AVG alias exists")
	}
	op, ok := scan.Source.(source.OrderingProvider)
	if !ok {
		return memory.Schema{}, fmt.Errorf("decimal prefix AVG requires proven ascending ordering")
	}
	order := op.Ordering()
	if len(order) == 0 || order[0].Name != n.OrderBy || order[0].Descending {
		return memory.Schema{}, fmt.Errorf("decimal prefix AVG requires proven ascending ordering")
	}
	if !decimalPrefixBounded(source.StreamPlanOf(scan.Source)) {
		return memory.Schema{}, fmt.Errorf("decimal prefix AVG requires bounded non-spilling scan")
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
func decimalPrefixBounded(n source.StreamPlanNode) bool {
	if n.Class != source.StreamPipeline && n.Class != source.StreamBounded {
		return false
	}
	for _, c := range n.Children {
		if !decimalPrefixBounded(c) {
			return false
		}
	}
	return true
}

// ContainsDecimalPrefixAverage identifies the explicit execution contract even
// when an outer projection or aggregate no longer exposes its result column.
func ContainsDecimalPrefixAverage(p Plan) bool {
	switch n := p.(type) {
	case *PartitionedDecimalPrefixAverage, *DecimalPrefixAverage:
		return true
	case *Projection:
		return ContainsDecimalPrefixAverage(n.Input)
	case *ExprProjection:
		return ContainsDecimalPrefixAverage(n.Input)
	case *Filter:
		return ContainsDecimalPrefixAverage(n.Input)
	case *Sort:
		return ContainsDecimalPrefixAverage(n.Input)
	case *TopK:
		return ContainsDecimalPrefixAverage(n.Input)
	case *Limit:
		return ContainsDecimalPrefixAverage(n.Input)
	case *Aggregate:
		return ContainsDecimalPrefixAverage(n.Input)
	case *ConfiguredAggregate:
		return ContainsDecimalPrefixAverage(n.Input)
	case *Window:
		return ContainsDecimalPrefixAverage(n.Input)
	case *ConfiguredWindow:
		return ContainsDecimalPrefixAverage(n.Input)
	case *Join:
		return ContainsDecimalPrefixAverage(n.Left) || ContainsDecimalPrefixAverage(n.Right)
	default:
		return false
	}
}
