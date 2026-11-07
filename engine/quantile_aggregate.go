package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
)

// CompiledQuantiles stores independent input lanes and configured output ranks.
// All slices are immutable after preparation.
type CompiledQuantiles struct {
	Fields     []memory.Field
	CountLane  []int
	Outputs    []compiledQuantileOutput
	OutputSlot []int
}
type compiledQuantileOutput struct {
	Lane        int
	Probability float64
	Mode        aggregate.QuantileMode
}

// PrepareConfigured binds legacy and parameterized aggregates in one query.
// Query/Aggregate positional layouts and existing Prepare behavior are unchanged.
func (e *Engine) PrepareConfigured(ctx context.Context, src source.Source, q expr.ConfiguredQuery, opts PrepareOptions) (*PreparedQuery, error) {
	aggs, options, err := expr.NormalizeAggregates(q.Aggregates)
	if err != nil {
		return nil, err
	}
	return e.prepare(ctx, src, expr.Query{Filter: q.Filter, GroupBy: append([]string(nil), q.GroupBy...), Aggregates: aggs, Hints: q.Hints}, opts, options)
}

func compileConfiguredQuery(schema memory.Schema, q expr.Query, options []expr.QuantileOption) (CompiledQuery, error) {
	if len(options) == 0 {
		return compileQuery(schema, q)
	}
	transformed := q
	transformed.Aggregates = append([]expr.Aggregate(nil), q.Aggregates...)
	quantiles := &CompiledQuantiles{OutputSlot: make([]int, len(q.Aggregates))}
	for i := range quantiles.OutputSlot {
		quantiles.OutputSlot[i] = -1
	}
	for _, option := range options {
		if option.Output < 0 || option.Output >= len(q.Aggregates) || q.Aggregates[option.Output].Kind != expr.AggQuantile || quantiles.OutputSlot[option.Output] >= 0 {
			return CompiledQuery{}, fmt.Errorf("invalid quantile output configuration")
		}
		if _, err := aggregate.PlanQuantile(0, option.Probability, option.Mode); err != nil {
			return CompiledQuery{}, err
		}
		a := q.Aggregates[option.Output]
		f, ok := schema.Field(a.Column)
		if !ok {
			return CompiledQuery{}, fmt.Errorf("quantile column %q not found", a.Column)
		}
		if _, err := aggregate.QuantileOutputType(f.Type); err != nil {
			return CompiledQuery{}, err
		}
		lane := -1
		for i, field := range quantiles.Fields {
			if field.Name == f.Name {
				lane = i
				break
			}
		}
		if lane < 0 {
			if uint64(len(quantiles.Fields)) >= math.MaxUint32 {
				return CompiledQuery{}, fmt.Errorf("too many quantile input lanes")
			}
			lane = len(quantiles.Fields)
			quantiles.Fields = append(quantiles.Fields, f)
		}
		quantiles.OutputSlot[option.Output] = len(quantiles.Outputs)
		quantiles.Outputs = append(quantiles.Outputs, compiledQuantileOutput{Lane: lane, Probability: option.Probability, Mode: option.Mode})
		transformed.Aggregates[option.Output].Kind = expr.AggCountValues
	}
	for i, a := range q.Aggregates {
		if a.Kind == expr.AggQuantile && quantiles.OutputSlot[i] < 0 {
			return CompiledQuery{}, fmt.Errorf("quantile output lacks configuration")
		}
	}
	cq, err := compileQuery(schema, transformed)
	if err != nil {
		return CompiledQuery{}, err
	}
	if cq.MultiAggregate == nil {
		cq.MultiAggregate, err = compileMultiAggregate(schema, transformed.Aggregates, true)
		if err != nil {
			return CompiledQuery{}, err
		}
	}
	m := cq.MultiAggregate
	m.Native = true
	m.Quantiles = quantiles
	// An empty distinct descriptor selects the shared tuple ordering operator;
	// quantiles use their own tag and never deduplicate equal observations.
	if m.Distinct == nil {
		m.Distinct = &CompiledDistinct{OutputLane: make([]int, len(q.Aggregates))}
		for i := range m.Distinct.OutputLane {
			m.Distinct.OutputLane[i] = -1
		}
	}
	for _, f := range quantiles.Fields {
		lane := -1
		for i, name := range m.Columns {
			if name == f.Name {
				lane = i
				break
			}
		}
		if lane < 0 {
			return CompiledQuery{}, fmt.Errorf("quantile count lane missing")
		}
		quantiles.CountLane = append(quantiles.CountLane, lane)
	}
	cq.Aggregates = append([]expr.Aggregate(nil), q.Aggregates...)
	cq.FusedCapable = false
	if len(q.GroupBy) > 0 {
		if cq.GroupShape != GroupShapeGeneric {
			cq.KeyWidthBytes += canonicalGroupHeaderBytes * len(q.GroupBy)
		}
		cq.GroupShape = GroupShapeGeneric
	}
	return cq, nil
}

func (p *PreparedQuery) quantiles() *CompiledQuantiles {
	if p.cq.MultiAggregate == nil {
		return nil
	}
	return p.cq.MultiAggregate.Quantiles
}
func (p *PreparedQuery) quantileRecordValue(r aggregateRecord, index int) (memory.DataType, uint64, bool) {
	q := p.quantiles()
	slot := q.OutputSlot[index]
	o := q.Outputs[slot]
	typ, _ := aggregate.QuantileOutputType(q.Fields[o.Lane].Type)
	offset := len(p.distinct().Fields)*8 + slot*9
	return typ, binary.LittleEndian.Uint64(r.rawStates[offset+1 : offset+9]), r.rawStates[offset] == 1
}
