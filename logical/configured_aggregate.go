package logical

import (
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
)

// ConfiguredAggregate carries parameterized aggregates without changing the
// positional layout of Aggregate. Specifications are copied by lazy builders.
type ConfiguredAggregate struct {
	Input      Plan
	GroupBy    []string
	Aggregates []expr.AggregateSpec
	Hints      expr.Hints
}

func (*ConfiguredAggregate) planNode() {}

func inferConfiguredAggregate(n *ConfiguredAggregate) (memory.Schema, error) {
	if n == nil || n.Input == nil {
		return memory.Schema{}, fmt.Errorf("configured aggregate requires input")
	}
	aggs, options, averages, err := expr.NormalizeConfiguredAggregates(n.Aggregates)
	if err != nil {
		return memory.Schema{}, err
	}
	if len(averages) > 0 {
		if len(options) > 0 {
			return memory.Schema{}, fmt.Errorf("configured Decimal AVG cannot mix quantiles")
		}
		names := make(map[string]struct{}, len(aggs)+len(n.GroupBy))
		for _, key := range n.GroupBy {
			names[key] = struct{}{}
		}
		for i, a := range aggs {
			name := a.Alias
			if name == "" {
				name = fmt.Sprintf("agg_%d", i)
			}
			if _, ok := names[name]; ok {
				return memory.Schema{}, fmt.Errorf("aggregate contains duplicate output %q", name)
			}
			names[name] = struct{}{}
		}
		input, e := InferSchema(n.Input)
		if e != nil {
			return memory.Schema{}, e
		}
		for _, o := range averages {
			f, ok := input.Field(aggs[o.Output].Column)
			if !ok || f.Type != memory.TypeDecimal || f.DecimalPrecision == 0 {
				return memory.Schema{}, fmt.Errorf("configured Decimal AVG requires root native Decimal input")
			}
			if e := f.ValidateDecimal128(); e != nil {
				return memory.Schema{}, e
			}
			aggs[o.Output].Kind = expr.AggSum
		}
		schema, e := InferSchema(&Aggregate{Input: n.Input, GroupBy: n.GroupBy, Aggregates: aggs, Hints: n.Hints})
		if e != nil {
			return memory.Schema{}, e
		}
		for _, o := range averages {
			idx := len(n.GroupBy) + o.Output
			name := schema.Fields[idx].Name
			schema.Fields[idx], e = memory.Decimal128Field(name, true, o.Precision, o.Scale)
			if e != nil {
				return memory.Schema{}, e
			}
		}
		return memory.NewSchema(schema.Fields...), nil
	}
	// AVG has the same numeric acceptance, nullable result and output dtype as
	// quantile. Reuse all existing column/alias/group validation before restoring
	// the configured operation at physical lowering.
	for _, o := range options {
		aggs[o.Output].Kind = expr.AggAvg
	}
	schema, err := InferSchema(&Aggregate{Input: n.Input, GroupBy: n.GroupBy, Aggregates: aggs, Hints: n.Hints})
	if err != nil {
		return memory.Schema{}, err
	}
	if len(options) > 0 {
		for i := range n.GroupBy {
			if schema.Fields[i].Type == memory.TypeString || schema.Fields[i].Type == memory.TypeBinary {
				schema.Fields[i].Encoding = memory.EncodingView
				schema.Fields[i].DictionaryDomain = ""
			}
		}
	}
	return schema, nil
}
