package engine

import (
	"bytes"
	"context"
	"gocolumn/memory"
)

func aggregateKeysEqual(shape GroupShape, left, right aggregateRecord) bool {
	if left.validMask != right.validMask {
		return false
	}
	switch shape {
	case GroupShapePairFixed:
		return left.a == right.a && left.b == right.b
	case GroupShapeStringView, GroupShapeGeneric:
		return bytes.Equal(left.stringKey, right.stringKey)
	default:
		return left.a == right.a
	}
}

func (p *PreparedQuery) bindSortedBatch(ctx context.Context, batch *memory.Batch, workspace *aggregatePredicateWorkspace) (aggregateBatchBinding, error) {
	binding := aggregateBatchBinding{batch: batch}
	if err := p.bindMultiValues(batch); err != nil {
		return binding, err
	}
	var err error
	if p.cq.ValueColumn != "" && (!p.nativeAggregate() || p.cq.MultiAggregate.Types[0] == memory.TypeInt64) {
		binding.values, err = batch.Int64(p.cq.ValueColumn)
		if err != nil {
			return binding, err
		}
	}
	if p.cq.GroupShape == GroupShapeGlobal {
		return binding, nil
	}
	if p.cq.GroupShape == GroupShapeGeneric {
		canonical, bindErr := workspace.bindKeys(ctx, p.cq.GenericFields, batch)
		binding.canonical = canonical
		return binding, bindErr
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
	row := AggregateRow{Sum: state.Sum, Count: state.CountRows, Aggregates: p.materializeRecordAggregates(record)}
	switch p.cq.GroupShape {
	case GroupShapeGlobal:
		// A global aggregate has no materialized key.
	case GroupShapePairFixed:
		row.Keys = []KeyPart{
			p.decodeFixedValid(p.cq.GroupBy[0], record.a, record.validMask&1 != 0),
			p.decodeFixedValid(p.cq.GroupBy[1], record.b, record.validMask&2 != 0),
		}
	case GroupShapeStringView, GroupShapeGeneric:
		row.Keys = []KeyPart{{Valid: record.validMask&1 != 0, Type: memory.TypeString}}
		if row.Keys[0].Valid {
			row.Keys[0].String = string(record.stringKey)
		}
	default:
		row.Keys = []KeyPart{p.decodeFixedValid(p.cq.GroupBy[0], record.a, record.validMask&1 != 0)}
	}
	return row
}

// aggregateKeyMatches avoids copying aggregate state while comparing adjacent
// keys. A row's input key is independent of its aggregate-state representation.
func aggregateKeyMatches(shape GroupShape, left *aggregateRecord, right aggregateKey) bool {
	if left.validMask != right.validMask {
		return false
	}
	switch shape {
	case GroupShapePairFixed:
		return left.a == right.a && left.b == right.b
	case GroupShapeStringView, GroupShapeGeneric:
		return bytes.Equal(left.stringKey, right.stringKey)
	default:
		return left.a == right.a
	}
}
