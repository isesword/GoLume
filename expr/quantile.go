package expr

import (
	"fmt"
	"gocolumn/aggregate"
)

// AggQuantile identifies a configured exact numeric order-statistic aggregate.
// Use Quantile or Median with configured query APIs; a bare Aggregate does not
// carry its probability/interpolation options and is rejected.
const AggQuantile AggregateKind = AggNUnique + 1

// AggregateSpec is a legacy Aggregate or an explicitly configured aggregate.
// Existing positional Aggregate literals remain source-compatible.
type AggregateSpec interface{ aggregateSpec() }

func (Aggregate) aggregateSpec() {}

// QuantileSpec configures an exact numeric quantile. Values are copied at bind
// time; mutation of the caller's specification cannot change a prepared query.
type QuantileSpec struct {
	Column      string
	Alias       string
	Probability float64
	Mode        aggregate.QuantileMode
}

func (QuantileSpec) aggregateSpec() {}

// Quantile requests an exact quantile with explicit interpolation mode.
func Quantile(column string, probability float64, mode aggregate.QuantileMode, alias string) QuantileSpec {
	return QuantileSpec{Column: column, Alias: alias, Probability: probability, Mode: mode}
}

// Median requests the midpoint of the two central non-NULL order statistics.
func Median(column, alias string) QuantileSpec {
	return Quantile(column, .5, aggregate.QuantileMidpoint, alias)
}

// ConfiguredQuery is the additive query form accepting legacy and parameterized
// aggregates together, without changing the original Query struct layout.
type ConfiguredQuery struct {
	Filter     Expr
	GroupBy    []string
	Aggregates []AggregateSpec
	Hints      Hints
}

// QuantileOption associates immutable interpolation options with an output slot.
type QuantileOption struct {
	Output      int
	Probability float64
	Mode        aggregate.QuantileMode
}

// NormalizeAggregates validates and copies configured specifications into common
// aggregate descriptors and separately typed options. It does not inspect data.
func NormalizeAggregates(specs []AggregateSpec) ([]Aggregate, []QuantileOption, error) {
	aggs := make([]Aggregate, len(specs))
	var options []QuantileOption
	for i, spec := range specs {
		switch a := spec.(type) {
		case Aggregate:
			if a.Kind == AggQuantile {
				return nil, nil, fmt.Errorf("quantile requires an explicit configuration")
			}
			aggs[i] = a
		case QuantileSpec:
			if _, err := aggregate.PlanQuantile(0, a.Probability, a.Mode); err != nil {
				return nil, nil, err
			}
			aggs[i] = Aggregate{Kind: AggQuantile, Column: a.Column, Alias: a.Alias}
			options = append(options, QuantileOption{Output: i, Probability: a.Probability, Mode: a.Mode})
		default:
			return nil, nil, fmt.Errorf("unsupported aggregate specification %T", spec)
		}
	}
	return aggs, options, nil
}
