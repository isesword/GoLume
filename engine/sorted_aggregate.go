package engine

import (
	"bytes"
	"gocolumn/memory"
)

func aggregateKeysEqual(shape GroupShape, left, right aggregateRecord) bool {
	if left.validMask != right.validMask {
		return false
	}
	switch shape {
	case GroupShapePairFixed:
		return left.a == right.a && left.b == right.b
	case GroupShapeStringView:
		return bytes.Equal(left.stringKey, right.stringKey)
	default:
		return left.a == right.a
	}
}

func (p *PreparedQuery) bindSortedBatch(batch *memory.Batch) (aggregateBatchBinding, error) {
	binding := aggregateBatchBinding{}
	var err error
	if p.cq.ValueColumn != "" {
		binding.values, err = batch.Int64(p.cq.ValueColumn)
		if err != nil {
			return binding, err
		}
	}
	if p.cq.GroupShape == GroupShapeStringView {
		binding.strings, err = batch.String(p.cq.GroupBy[0].Name)
		return binding, err
	}
	binding.first, err = fixedFromBatch(batch, p.cq.GroupBy[0])
	if err != nil {
		return binding, err
	}
	if p.cq.GroupShape == GroupShapePairFixed {
		binding.second, err = fixedFromBatch(batch, p.cq.GroupBy[1])
	}
	return binding, err
}

func (p *PreparedQuery) materializeRecord(record aggregateRecord) AggregateRow {
	state := record.state
	row := AggregateRow{Sum: state.Sum, Count: state.CountRows, Aggregates: p.materializeAggregates(state)}
	switch p.cq.GroupShape {
	case GroupShapePairFixed:
		row.Keys = []KeyPart{
			p.decodeFixedValid(p.cq.GroupBy[0], record.a, record.validMask&1 != 0),
			p.decodeFixedValid(p.cq.GroupBy[1], record.b, record.validMask&2 != 0),
		}
	case GroupShapeStringView:
		row.Keys = []KeyPart{{Valid: record.validMask&1 != 0, Type: memory.TypeString}}
		if row.Keys[0].Valid {
			row.Keys[0].String = string(record.stringKey)
		}
	default:
		row.Keys = []KeyPart{p.decodeFixedValid(p.cq.GroupBy[0], record.a, record.validMask&1 != 0)}
	}
	return row
}
