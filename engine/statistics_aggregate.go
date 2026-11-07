package engine

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"math"
)

// CompiledStatistics holds independent fixed-size statistics lanes.
type CompiledStatistics struct {
	Columns    []string
	Types      []memory.DataType
	Layouts    []aggregate.StatisticsLayout
	OutputLane []int
	Endpoints  bool
}

func compileStatistics(schema memory.Schema, aggs []expr.Aggregate) (*CompiledStatistics, error) {
	s := &CompiledStatistics{OutputLane: make([]int, len(aggs))}
	for i := range s.OutputLane {
		s.OutputLane[i] = -1
	}
	for i, a := range aggs {
		if !expr.IsStatistics(a.Kind) {
			continue
		}
		f, ok := schema.Field(a.Column)
		if !ok {
			return nil, fmt.Errorf("aggregate column %q not found", a.Column)
		}
		if _, err := expr.StatisticsType(f.Type, a.Kind); err != nil {
			return nil, err
		}
		lane := -1
		for j, name := range s.Columns {
			if name == a.Column {
				lane = j
				break
			}
		}
		if lane < 0 {
			lane = len(s.Columns)
			s.Columns = append(s.Columns, a.Column)
			s.Types = append(s.Types, f.Type)
			s.Layouts = append(s.Layouts, aggregate.StatisticsLayout{})
		}
		s.OutputLane[i] = lane
		switch a.Kind {
		case expr.AggFirst:
			s.Layouts[lane].First = true
			s.Endpoints = true
		case expr.AggLast:
			s.Layouts[lane].Last = true
			s.Endpoints = true
		default:
			s.Layouts[lane].Moments = true
		}
	}
	if len(s.Columns) == 0 {
		return nil, nil
	}
	return s, nil
}
func (p *PreparedQuery) statistics() *CompiledStatistics {
	if p.cq.MultiAggregate == nil {
		return nil
	}
	return p.cq.MultiAggregate.Statistics
}
func (p *PreparedQuery) orderedStatistics() bool {
	return p.statistics() != nil && p.statistics().Endpoints
}
func (p *PreparedQuery) statisticsRecordValue(r aggregateRecord, index int) (memory.DataType, uint64, bool) {
	s := p.statistics()
	lane := s.OutputLane[index]
	state := statisticsAggregateRecordState(r, lane, s.Types[lane], s.Layouts[lane])
	kind := p.cq.Aggregates[index].Kind
	if kind == expr.AggFirst || kind == expr.AggLast {
		e, _ := aggregate.RestoreEndpoints(state.Endpoints)
		if kind == expr.AggFirst {
			bits, valid := e.First()
			return s.Types[lane], bits, valid
		}
		bits, valid := e.Last()
		return s.Types[lane], bits, valid
	}
	m, _ := aggregate.RestoreMoments(state.Moments)
	ddof := uint64(1)
	if kind == expr.AggVarPopulation || kind == expr.AggStdPopulation {
		ddof = 0
	}
	var value float64
	var valid bool
	if kind == expr.AggStd || kind == expr.AggStdPopulation {
		value, valid = m.StdDev(ddof)
	} else {
		value, valid = m.Variance(ddof)
	}
	if s.Types[lane] == memory.TypeFloat32 {
		return memory.TypeFloat32, uint64(math.Float32bits(float32(value))), valid
	}
	return memory.TypeFloat64, math.Float64bits(value), valid
}

func statisticsInputBits(b *memory.Batch, name string, typ memory.DataType, row int) (uint64, bool) {
	switch typ {
	case memory.TypeBool:
		v := b.Uint32s[name]
		return uint64(v.Values[row]), v.IsValid(row)
	case memory.TypeDate32, memory.TypeTime32:
		v := b.Int32s[name]
		return uint64(int64(v.Values[row])), v.IsValid(row)
	case memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		v := b.Int64s[name]
		return uint64(v.Values[row]), v.IsValid(row)
	}
	return nativeAggregateBits(b, name, typ, aggregate.Layout{Sum: true}, row)
}
