package engine

import (
	"context"
	"fmt"
	"gocolumn/hashagg"
	"gocolumn/memory"
	"gocolumn/planner"
)

// aggregateResidentBatch specializes the native hash operator, independent of
// its terminal consumer. Input has already been bounded by the scan contract.
func (p *PreparedQuery) aggregateResidentBatch(ctx context.Context, batch *memory.Batch, table *aggregateStateTable, workspace *aggregatePredicateWorkspace) (selected, groups int, checksum int64, err error) {
	if err = ctx.Err(); err != nil {
		return
	}
	var vals *memory.Int64Vector
	if p.cq.ValueColumn != "" {
		vals, err = batch.Int64(p.cq.ValueColumn)
		if err != nil {
			return
		}
	}
	if p.strategy.Kind == planner.StrategyDirectFused && p.cq.SimpleComparison != nil && vals != nil {
		switch p.cq.GroupShape {
		case GroupShapeI32:
			keys, err := batch.Int32(p.cq.GroupBy[0].Name)
			if err != nil {
				return 0, 0, 0, err
			}
			return aggregateFusedBlocks(ctx, p, batch, keys.Values, keys.Validity, vals, workspace.fusedMask, table.i32.AggregateSelectedNullableValues)
		case GroupShapeI64:
			keys, err := batch.Int64(p.cq.GroupBy[0].Name)
			if err != nil {
				return 0, 0, 0, err
			}
			return aggregateFusedBlocks(ctx, p, batch, keys.Values, keys.Validity, vals, workspace.fusedMask, table.u64.AggregateSelectedNullableValues)
		case GroupShapeU32:
			keys, err := batch.Uint32(p.cq.GroupBy[0].Name)
			if err != nil {
				return 0, 0, 0, err
			}
			return aggregateFusedBlocks(ctx, p, batch, keys.Values, keys.Validity, vals, workspace.fusedMask, table.u32.AggregateSelectedNullableValues)
		case GroupShapeDictionaryString:
			keys, err := batch.DictionaryString(p.cq.GroupBy[0].Name)
			if err != nil {
				return 0, 0, 0, err
			}
			if keys.DictionaryDomain != p.cq.GroupBy[0].DictionaryDomain {
				return 0, 0, 0, fmt.Errorf("dictionary domain changed")
			}
			return aggregateFusedBlocks(ctx, p, batch, keys.Indices, keys.Validity, vals, workspace.fusedMask, table.u32.AggregateSelectedNullableValues)
		}
	}
	var aggregate func(int) error
	switch p.cq.GroupShape {
	case GroupShapeI32:
		keys, err := batch.Int32(p.cq.GroupBy[0].Name)
		if err != nil {
			return 0, 0, 0, err
		}
		aggregate = func(i int) error {
			v, vv := aggregateValueAt(vals, i)
			created, err := table.i32.TryAddNullableValue(keys.Values[i], keys.IsValid(i), v, vv)
			if err != nil {
				return err
			}
			if created {
				groups++
			}
			if vv {
				checksum += v
			}
			return nil
		}
	case GroupShapeI64:
		keys, err := batch.Int64(p.cq.GroupBy[0].Name)
		if err != nil {
			return 0, 0, 0, err
		}
		aggregate = func(i int) error {
			v, vv := aggregateValueAt(vals, i)
			created, err := table.u64.TryAddNullableValue(uint64(keys.Values[i]), keys.IsValid(i), v, vv)
			if err != nil {
				return err
			}
			if created {
				groups++
			}
			if vv {
				checksum += v
			}
			return nil
		}
	case GroupShapeU32:
		keys, err := batch.Uint32(p.cq.GroupBy[0].Name)
		if err != nil {
			return 0, 0, 0, err
		}
		aggregate = func(i int) error {
			v, vv := aggregateValueAt(vals, i)
			created, err := table.u32.TryAddNullableValue(keys.Values[i], keys.IsValid(i), v, vv)
			if err != nil {
				return err
			}
			if created {
				groups++
			}
			if vv {
				checksum += v
			}
			return nil
		}
	case GroupShapeDictionaryString:
		keys, err := batch.DictionaryString(p.cq.GroupBy[0].Name)
		if err != nil {
			return 0, 0, 0, err
		}
		if keys.DictionaryDomain != p.cq.GroupBy[0].DictionaryDomain {
			return 0, 0, 0, fmt.Errorf("dictionary domain changed")
		}
		aggregate = func(i int) error {
			v, vv := aggregateValueAt(vals, i)
			created, err := table.u32.TryAddNullableValue(keys.Indices[i], keys.IsValid(i), v, vv)
			if err != nil {
				return err
			}
			if created {
				groups++
			}
			if vv {
				checksum += v
			}
			return nil
		}
	case GroupShapeStringView:
		keys, err := batch.String(p.cq.GroupBy[0].Name)
		if err != nil {
			return 0, 0, 0, err
		}
		aggregate = func(i int) error {
			v, vv := aggregateValueAt(vals, i)
			ng, err := table.str.AddNullableValue(keys.ValueBytes(i), keys.IsValid(i), v, vv)
			if err != nil {
				return err
			}
			if ng {
				groups++
			}
			if vv {
				checksum += v
			}
			return nil
		}
	case GroupShapePairFixed:
		a, err := fixedFromBatch(batch, p.cq.GroupBy[0])
		if err != nil {
			return 0, 0, 0, err
		}
		b, err := fixedFromBatch(batch, p.cq.GroupBy[1])
		if err != nil {
			return 0, 0, 0, err
		}
		aggregate = func(i int) error {
			validMask := uint8(0)
			if a.valid(i) {
				validMask |= 1
			}
			if b.valid(i) {
				validMask |= 2
			}
			v, vv := aggregateValueAt(vals, i)
			created, err := table.pair.TryAddNullableValue(hashagg.Key128{A: a.at(i), B: b.at(i)}, validMask, v, vv)
			if err != nil {
				return err
			}
			if created {
				groups++
			}
			if vv {
				checksum += v
			}
			return nil
		}
	}

	if p.strategy.Kind == planner.StrategyDirectFused && p.cq.SimpleComparison != nil {
		selected, err = forSimpleSelected(batch, p.cq.SimpleComparison, p.e.kernels, workspace.fusedMask, aggregate)
		return
	}
	selection, evaluateErr := workspace.evaluate(ctx, p.cq.Predicate, batch, p.e.kernels)
	if evaluateErr != nil {
		return 0, 0, 0, evaluateErr
	}
	selected = memory.CountBitmap(selection.Truth)
	// Fixed-width single-key vector kernels avoid per-row callback dispatch.
	if vals != nil {
		var summary hashagg.Summary
		switch p.cq.GroupShape {
		case GroupShapeI32:
			keys := batch.Int32s[p.cq.GroupBy[0].Name]
			summary, err = table.i32.AggregateSelectedNullableValues(keys.Values, keys.Validity, vals.Values, vals.Validity, selection.Truth)
		case GroupShapeI64:
			keys := batch.Int64s[p.cq.GroupBy[0].Name]
			summary, err = table.u64.AggregateSelectedNullableValues(keys.Values, keys.Validity, vals.Values, vals.Validity, selection.Truth)
		case GroupShapeU32:
			keys := batch.Uint32s[p.cq.GroupBy[0].Name]
			summary, err = table.u32.AggregateSelectedNullableValues(keys.Values, keys.Validity, vals.Values, vals.Validity, selection.Truth)
		case GroupShapeDictionaryString:
			keys := batch.DictStrings[p.cq.GroupBy[0].Name]
			summary, err = table.u32.AggregateSelectedNullableValues(keys.Indices, keys.Validity, vals.Values, vals.Validity, selection.Truth)
		default:
			err = forSelectedFrom(selection.Truth, batch.Rows, 0, aggregate)
			return
		}
		return selected, summary.Groups, summary.Checksum, err
	}
	err = forSelectedFrom(selection.Truth, batch.Rows, 0, aggregate)
	return
}

func aggregateValueAt(values *memory.Int64Vector, row int) (int64, bool) {
	if values == nil {
		return 0, false
	}
	return values.Values[row], values.IsValid(row)
}

// aggregateFusedBlocks uses a single temporary word and calls the existing
// selected-row state kernel once per block, eliminating per-row callback and
// nullable-key dispatch. Layout, growth, NULL state, and accounting stay shared.
func aggregateFusedBlocks[K int32 | uint32 | int64](ctx context.Context, query *PreparedQuery, batch *memory.Batch, keys []K, keyValidity memory.Bitmap, values *memory.Int64Vector, mask memory.Bitmap, aggregate func([]K, memory.Bitmap, []int64, memory.Bitmap, memory.Bitmap) (hashagg.Summary, error)) (selected, groups int, checksum int64, err error) {
	selected, err = forSimpleSelectedBlocks(batch, query.cq.SimpleComparison, query.e.kernels, mask, func(base int, word memory.Bitmap) error {
		if base&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if word[0] == 0 {
			return nil
		}
		end := min(base+64, batch.Rows)
		summary, err := aggregate(keys[base:end], blockValidity(keyValidity, base), values.Values[base:end], blockValidity(values.Validity, base), word)
		if err != nil {
			return err
		}
		groups += summary.Groups
		checksum += summary.Checksum
		return nil
	})
	return
}
