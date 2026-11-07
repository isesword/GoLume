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
	aggs, options, err := expr.NormalizeAggregates(n.Aggregates)
	if err != nil {
		return memory.Schema{}, err
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
