package expr

import "fmt"

// AggDecimalAverage requires an explicit DecimalAverageSpec. Bare Avg remains
// unchanged, and a bare Aggregate cannot carry precision, scale or rounding.
const AggDecimalAverage AggregateKind = AggQuantile + 1

// DecimalAverageSpec configures a native Decimal AVG for global or supported grouped queries. Target metadata and
// rounding are explicit; input scale is bound from the native Decimal schema.
// The value is copied by configured query builders and preparation.
type DecimalAverageSpec struct {
	Column, Alias    string
	Precision, Scale int32
	Mode             DecimalRoundingMode
}

func (DecimalAverageSpec) aggregateSpec() {}

// AvgDecimal128 requests one final rounding of an exact coefficient sum divided
// by its non-NULL count. Precision must be1..38, scale0..precision and mode one of
// the six explicit modes (zero is invalid). Only configured query APIs accept it.
func AvgDecimal128(column string, precision, scale int32, mode DecimalRoundingMode, alias string) DecimalAverageSpec {
	return DecimalAverageSpec{Column: column, Alias: alias, Precision: precision, Scale: scale, Mode: mode}
}

// DecimalAverageOption retains configuration by output slot without enlarging
// the positional Aggregate or Query structs.
type DecimalAverageOption struct {
	Output           int
	Precision, Scale int32
	Mode             DecimalRoundingMode
}

// NormalizeConfiguredAggregates extends normalization with native Decimal AVG
// configuration while preserving NormalizeAggregates' original signature.
// It validates and copies all options, including those on empty/dead queries.
func NormalizeConfiguredAggregates(specs []AggregateSpec) ([]Aggregate, []QuantileOption, []DecimalAverageOption, error) {
	transformed := append([]AggregateSpec(nil), specs...)
	var averages []DecimalAverageOption
	for i, spec := range specs {
		a, ok := spec.(DecimalAverageSpec)
		if !ok {
			continue
		}
		if a.Precision < 1 || a.Precision > 38 || a.Scale < 0 || a.Scale > a.Precision {
			return nil, nil, nil, fmt.Errorf("Decimal AVG requires explicit valid precision/scale")
		}
		switch a.Mode {
		case DecimalRoundTowardZero, DecimalRoundAwayFromZero, DecimalRoundFloor, DecimalRoundCeil, DecimalRoundHalfEven, DecimalRoundHalfAwayFromZero:
		default:
			return nil, nil, nil, fmt.Errorf("Decimal AVG requires explicit valid rounding mode")
		}
		if a.Column == "" {
			return nil, nil, nil, fmt.Errorf("Decimal AVG requires an input column")
		}
		transformed[i] = Aggregate{Kind: AggSum, Column: a.Column, Alias: a.Alias}
		averages = append(averages, DecimalAverageOption{Output: i, Precision: a.Precision, Scale: a.Scale, Mode: a.Mode})
	}
	aggs, quantiles, err := NormalizeAggregates(transformed)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, o := range averages {
		aggs[o.Output].Kind = AggDecimalAverage
	}
	return aggs, quantiles, averages, nil
}
