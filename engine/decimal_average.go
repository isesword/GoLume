package engine

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
)

// Configured averages reuse exact sum states but never narrow those states
// through SUM.Finalize before division. Preparation owns immutable options;
// per-query state/scratch and output leases remain the existing Decimal fees.
func compileDecimalAverageQuery(schema memory.Schema, q expr.Query, quantiles []expr.QuantileOption, options []expr.DecimalAverageOption) (CompiledQuery, error) {
	if len(quantiles) != 0 {
		return CompiledQuery{}, fmt.Errorf("configured Decimal AVG cannot mix quantiles")
	}
	transformed := q
	transformed.Aggregates = append([]expr.Aggregate(nil), q.Aggregates...)
	configured := make([]aggregate.DecimalAverageOptions, len(q.Aggregates))
	for _, o := range options {
		if o.Output < 0 || o.Output >= len(q.Aggregates) || q.Aggregates[o.Output].Kind != expr.AggDecimalAverage || configured[o.Output].Mode != 0 {
			return CompiledQuery{}, fmt.Errorf("invalid Decimal AVG output configuration")
		}
		f, ok := schema.Field(q.Aggregates[o.Output].Column)
		if !ok || f.Type != memory.TypeDecimal || f.DecimalPrecision == 0 {
			return CompiledQuery{}, fmt.Errorf("configured Decimal AVG requires root native Decimal input")
		}
		if e := f.ValidateDecimal128(); e != nil {
			return CompiledQuery{}, e
		}
		mode, e := decimalAverageMode(o.Mode)
		if e != nil {
			return CompiledQuery{}, e
		}
		opt := aggregate.DecimalAverageOptions{InputScale: f.DecimalScale, Precision: o.Precision, Scale: o.Scale, Mode: mode}
		if _, _, e = (aggregate.DecimalSumState{}).FinalizeAverage(opt); e != nil {
			return CompiledQuery{}, e
		}
		configured[o.Output] = opt
		transformed.Aggregates[o.Output].Kind = expr.AggSum
	}
	for i, a := range q.Aggregates {
		if a.Kind == expr.AggDecimalAverage && configured[i].Mode == 0 {
			return CompiledQuery{}, fmt.Errorf("Decimal AVG output requires explicit configuration")
		}
	}
	cq, e := compileQuery(schema, transformed)
	if e != nil {
		return CompiledQuery{}, e
	}
	if cq.MultiAggregate == nil || (cq.MultiAggregate.DecimalFields == nil && cq.MultiAggregate.DecimalGroupedFields == nil) {
		return CompiledQuery{}, fmt.Errorf("configured Decimal AVG requires native Decimal aggregation plan")
	}
	cq.Aggregates = append([]expr.Aggregate(nil), q.Aggregates...)
	cq.MultiAggregate.DecimalAverages = configured
	return cq, nil
}
func decimalAverageMode(mode expr.DecimalRoundingMode) (aggregate.DecimalAverageRoundingMode, error) {
	switch mode {
	case expr.DecimalRoundTowardZero:
		return aggregate.DecimalAverageTowardZero, nil
	case expr.DecimalRoundAwayFromZero:
		return aggregate.DecimalAverageAwayFromZero, nil
	case expr.DecimalRoundFloor:
		return aggregate.DecimalAverageFloor, nil
	case expr.DecimalRoundCeil:
		return aggregate.DecimalAverageCeil, nil
	case expr.DecimalRoundHalfEven:
		return aggregate.DecimalAverageHalfEven, nil
	case expr.DecimalRoundHalfAwayFromZero:
		return aggregate.DecimalAverageHalfAwayFromZero, nil
	default:
		return 0, aggregate.ErrDecimalAverageOptions
	}
}
func decimalValueKind(kind expr.AggregateKind) bool {
	return kind == expr.AggSum || kind == expr.AggDecimalAverage
}
func (p *PreparedQuery) finalizeDecimal(lane int, state aggregate.DecimalSumState) (memory.Decimal128, bool, error) {
	if p.cq.Aggregates[lane].Kind == expr.AggDecimalAverage {
		return state.FinalizeAverage(p.cq.MultiAggregate.DecimalAverages[lane])
	}
	return state.Finalize()
}
