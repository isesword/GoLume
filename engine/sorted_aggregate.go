package engine

import (
	"bytes"
	"context"
	"fmt"
	"gocolumn/aggregate"
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

func cloneAggregateKey(shape GroupShape, record aggregateRecord) aggregateRecord {
	copyRecord := aggregateRecord{a: record.a, b: record.b, validMask: record.validMask}
	if shape == GroupShapeStringView && record.validMask&1 != 0 {
		copyRecord.stringKey = append([]byte(nil), record.stringKey...)
	}
	return copyRecord
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

func (p *PreparedQuery) materializeSortedRecord(record aggregateRecord) AggregateRow {
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

// executeSorted performs adjacent-key aggregation with one SoA state slot. It
// is selected only when the source explicitly guarantees the group columns as
// a global ordering prefix.
func (p *PreparedQuery) executeSorted(ctx context.Context, workspace *queryWorkspace, opts ExecuteOptions) (Result, error) {
	if len(workspace.workers) != 1 {
		return Result{}, fmt.Errorf("sorted aggregation requires one sequential reader")
	}
	worker := &workspace.workers[0]
	state := aggregate.NewInt64Vector(1, p.cq.AggregateLayout)
	var current aggregateRecord
	haveCurrent := false
	result := Result{}
	flush := func() {
		if !haveCurrent {
			return
		}
		current.state = state.Result(0)
		result.GroupCount++
		if opts.Materialize {
			result.Rows = append(result.Rows, p.materializeSortedRecord(current))
		}
	}
	for _, partition := range p.parts {
		if err := worker.reader.ReadPartition(ctx, partition, worker.batch); err != nil {
			return Result{}, err
		}
		selection, _, err := p.selection(worker)
		if err != nil {
			return Result{}, err
		}
		binding, err := p.bindSortedBatch(worker.batch)
		if err != nil {
			return Result{}, err
		}
		err = forSelected(selection, worker.batch.Rows, func(row int) error {
			key := binding.record(p.cq.GroupShape, row)
			value, valid := binding.value(row)
			if !haveCurrent || !aggregateKeysEqual(p.cq.GroupShape, current, key) {
				flush()
				current = cloneAggregateKey(p.cq.GroupShape, key)
				state.InitValue(0, value, valid)
				haveCurrent = true
			} else {
				state.AddValue(0, value, valid)
			}
			result.SelectedRows++
			if valid {
				result.Checksum += value
			}
			return nil
		})
		if err != nil {
			return Result{}, err
		}
	}
	flush()
	return result, nil
}
