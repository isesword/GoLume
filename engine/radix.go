package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/hashagg"
	"gocolumn/memory"
	"gocolumn/radix"
	"gocolumn/source"
	"math/bits"
)

func compactU64Nullable(keys func(int) uint64, valid func(int) bool, vals []int64, sel memory.Bitmap, rows int, outK []uint64, outV []int64) (n int, nullCount int, nullSum int64) {
	for wi, word := range sel {
		base := wi << 6
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			i := base + bit
			if i >= rows {
				break
			}
			if !valid(i) {
				nullCount++
				nullSum += vals[i]
				word &= word - 1
				continue
			}
			outK[n] = keys(i)
			outV[n] = vals[i]
			n++
			word &= word - 1
		}
	}
	return n, nullCount, nullSum
}

func compactPair(a, b fixedColumn, vals []int64, sel memory.Bitmap, rows int, outK []hashagg.Key128, outValid []uint8, outV []int64) int {
	n := 0
	for wi, word := range sel {
		base := wi << 6
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			i := base + bit
			if i >= rows {
				break
			}
			validMask := uint8(0)
			key := hashagg.Key128{}
			if a.valid(i) {
				validMask |= 1
				key.A = a.at(i)
			}
			if b.valid(i) {
				validMask |= 2
				key.B = b.at(i)
			}
			outK[n] = key
			outValid[n] = validMask
			outV[n] = vals[i]
			n++
			word &= word - 1
		}
	}
	return n
}

func (p *PreparedQuery) compactRadixPartition(ctx context.Context, worker *workerWorkspace, partition source.Partition) error {
	worker.selectedRows, worker.nullRows, worker.nullSum = 0, 0, 0
	if err := worker.reader.ReadPartition(ctx, partition, worker.batch); err != nil {
		return err
	}
	selection, _, err := p.selection(worker)
	if err != nil {
		return err
	}
	values, err := worker.batch.Int64(p.cq.ValueColumn)
	if err != nil {
		return err
	}
	rows := worker.batch.Rows
	switch p.cq.GroupShape {
	case GroupShapeI32:
		keys, err := worker.batch.Int32(p.cq.GroupBy[0].Name)
		if err != nil {
			return err
		}
		worker.selectedRows, worker.nullRows, worker.nullSum = compactU64Nullable(
			func(i int) uint64 { return uint64(uint32(keys.Values[i])) }, keys.IsValid, values.Values, selection, rows, worker.selectedKeys, worker.selectedVals)
	case GroupShapeI64:
		keys, err := worker.batch.Int64(p.cq.GroupBy[0].Name)
		if err != nil {
			return err
		}
		worker.selectedRows, worker.nullRows, worker.nullSum = compactU64Nullable(
			func(i int) uint64 { return uint64(keys.Values[i]) }, keys.IsValid, values.Values, selection, rows, worker.selectedKeys, worker.selectedVals)
	case GroupShapeU32:
		keys, err := worker.batch.Uint32(p.cq.GroupBy[0].Name)
		if err != nil {
			return err
		}
		worker.selectedRows, worker.nullRows, worker.nullSum = compactU64Nullable(
			func(i int) uint64 { return uint64(keys.Values[i]) }, keys.IsValid, values.Values, selection, rows, worker.selectedKeys, worker.selectedVals)
	case GroupShapeDictionaryString:
		keys, err := worker.batch.DictionaryString(p.cq.GroupBy[0].Name)
		if err != nil {
			return err
		}
		if keys.DictionaryDomain != p.cq.GroupBy[0].DictionaryDomain {
			return fmt.Errorf("dictionary domain changed for %q", p.cq.GroupBy[0].Name)
		}
		worker.selectedRows, worker.nullRows, worker.nullSum = compactU64Nullable(
			func(i int) uint64 { return uint64(keys.Indices[i]) }, keys.IsValid, values.Values, selection, rows, worker.selectedKeys, worker.selectedVals)
	case GroupShapePairFixed:
		a, err := fixedFromBatch(worker.batch, p.cq.GroupBy[0])
		if err != nil {
			return err
		}
		b, err := fixedFromBatch(worker.batch, p.cq.GroupBy[1])
		if err != nil {
			return err
		}
		worker.selectedRows = compactPair(a, b, values.Values, selection, rows, worker.selectedPairs, worker.selectedPairValid, worker.selectedVals)
	default:
		return fmt.Errorf("radix unsupported for group shape %d", p.cq.GroupShape)
	}
	return nil
}

// executeRadix keeps only workers*morsel rows of classification scratch. The
// partition-owned hash state persists across chunks, replacing the former
// input-sized selected-key/value arrays.
func (p *PreparedQuery) executeRadix(ctx context.Context, workspace *queryWorkspace, opts ExecuteOptions) (Result, error) {
	if !workspace.radixBounded {
		return p.executeResidentRadix(ctx, workspace, opts)
	}
	workers := p.strategy.Workers
	if workers < 1 {
		workers = 1
	}
	if workers > len(workspace.workers) {
		workers = len(workspace.workers)
	}
	if p.cq.GroupShape == GroupShapePairFixed {
		workspace.radixPair.Begin()
	} else {
		workspace.radix64.Begin()
	}
	var total radix.Summary
	nullRows := 0
	nullSum := int64(0)
	for wave := 0; wave < len(p.parts); wave += workers {
		tasks := workers
		if remain := len(p.parts) - wave; tasks > remain {
			tasks = remain
		}
		if err := p.e.scheduler.ParallelFor(ctx, tasks, tasks, func(task int) error {
			return p.compactRadixPartition(ctx, &workspace.workers[task], p.parts[wave+task])
		}); err != nil {
			return Result{}, err
		}
		waveRows := 0
		for task := 0; task < tasks; task++ {
			worker := &workspace.workers[task]
			rows := worker.selectedRows
			if p.cq.GroupShape == GroupShapePairFixed {
				copy(workspace.selectedPairs[waveRows:waveRows+rows], worker.selectedPairs[:rows])
				copy(workspace.selectedPairValid[waveRows:waveRows+rows], worker.selectedPairValid[:rows])
			} else {
				copy(workspace.selectedKeys[waveRows:waveRows+rows], worker.selectedKeys[:rows])
			}
			copy(workspace.selectedVals[waveRows:waveRows+rows], worker.selectedVals[:rows])
			waveRows += rows
			nullRows += worker.nullRows
			nullSum += worker.nullSum
		}
		var summary radix.Summary
		var err error
		if p.cq.GroupShape == GroupShapePairFixed {
			summary, err = workspace.radixPair.AggregateNullableChunk(ctx, p.e.scheduler,
				workspace.selectedPairs[:waveRows], workspace.selectedPairValid[:waveRows], workspace.selectedVals[:waveRows], workers)
		} else {
			summary, err = workspace.radix64.AggregateChunk(ctx, p.e.scheduler,
				workspace.selectedKeys[:waveRows], workspace.selectedVals[:waveRows], workers)
		}
		if errors.Is(err, radix.ErrLocalTableFull) {
			return p.executeRadixFallbackReplay(ctx, workspace, opts)
		}
		if err != nil {
			return Result{}, err
		}
		total.Groups += summary.Groups
		total.Rows += summary.Rows
		total.Checksum += summary.Checksum
	}
	result := Result{GroupCount: total.Groups, SelectedRows: total.Rows + nullRows, Checksum: total.Checksum + nullSum}
	if p.cq.GroupShape != GroupShapePairFixed && nullRows > 0 {
		result.GroupCount++
	}
	if !opts.Materialize {
		return result, nil
	}
	if p.cq.GroupShape == GroupShapePairFixed {
		groups := workspace.radixPair.Materialize(nil)
		result.Rows = make([]AggregateRow, len(groups))
		for index, group := range groups {
			result.Rows[index] = AggregateRow{
				Keys: []KeyPart{
					p.decodeFixedValid(p.cq.GroupBy[0], group.Key.A, group.ValidMask&1 != 0),
					p.decodeFixedValid(p.cq.GroupBy[1], group.Key.B, group.ValidMask&2 != 0),
				},
				Sum: group.Sum, Count: group.Count, Aggregates: p.materializeAggregates(sumCountAggregateState(group.Sum, group.Count)),
			}
		}
		return result, nil
	}
	groups := workspace.radix64.Materialize(nil)
	result.Rows = make([]AggregateRow, len(groups), len(groups)+1)
	for index, group := range groups {
		result.Rows[index] = AggregateRow{Keys: []KeyPart{p.decodeFixed(p.cq.GroupBy[0], group.Key)}, Sum: group.Sum, Count: group.Count, Aggregates: p.materializeAggregates(sumCountAggregateState(group.Sum, group.Count))}
	}
	if nullRows > 0 {
		result.Rows = append(result.Rows, AggregateRow{Keys: []KeyPart{p.decodeFixedValid(p.cq.GroupBy[0], 0, false)}, Sum: nullSum, Count: uint64(nullRows), Aggregates: p.materializeAggregates(sumCountAggregateState(nullSum, uint64(nullRows)))})
	}
	return result, nil
}

// executeResidentRadix is the explicitly unbounded performance path. It keeps
// input-proportional compacted key/value buffers so all row groups can be
// partitioned in one pass. Finite-memory and ExecutionBounded plans never call
// this method; they use executeRadix's morsel-bounded wave workspace instead.
func (p *PreparedQuery) executeResidentRadix(ctx context.Context, w *queryWorkspace, opts ExecuteOptions) (Result, error) {
	workers := p.strategy.Workers
	if workers < 1 {
		workers = 1
	}
	if workers > len(w.workers) {
		workers = len(w.workers)
	}
	clear(w.chunkLens)
	clear(w.nullCounts)
	clear(w.nullSums)
	if err := p.e.scheduler.ParallelFor(ctx, workers, workers, func(worker int) error {
		ww := &w.workers[worker]
		for pi := worker; pi < len(p.parts); pi += workers {
			if err := ww.reader.ReadPartition(ctx, p.parts[pi], ww.batch); err != nil {
				return err
			}
			sel, _, err := p.selection(ww)
			if err != nil {
				return err
			}
			vals, err := ww.batch.Int64(p.cq.ValueColumn)
			if err != nil {
				return err
			}
			base := p.partBase[pi]
			rows := ww.batch.Rows
			switch p.cq.GroupShape {
			case GroupShapeI32:
				keys, err := ww.batch.Int32(p.cq.GroupBy[0].Name)
				if err != nil {
					return err
				}
				w.chunkLens[pi], w.nullCounts[pi], w.nullSums[pi] = compactU64Nullable(
					func(i int) uint64 { return uint64(uint32(keys.Values[i])) }, keys.IsValid, vals.Values, sel, rows, w.selectedKeys[base:base+rows], w.selectedVals[base:base+rows])
			case GroupShapeI64:
				keys, err := ww.batch.Int64(p.cq.GroupBy[0].Name)
				if err != nil {
					return err
				}
				w.chunkLens[pi], w.nullCounts[pi], w.nullSums[pi] = compactU64Nullable(
					func(i int) uint64 { return uint64(keys.Values[i]) }, keys.IsValid, vals.Values, sel, rows, w.selectedKeys[base:base+rows], w.selectedVals[base:base+rows])
			case GroupShapeU32:
				keys, err := ww.batch.Uint32(p.cq.GroupBy[0].Name)
				if err != nil {
					return err
				}
				w.chunkLens[pi], w.nullCounts[pi], w.nullSums[pi] = compactU64Nullable(
					func(i int) uint64 { return uint64(keys.Values[i]) }, keys.IsValid, vals.Values, sel, rows, w.selectedKeys[base:base+rows], w.selectedVals[base:base+rows])
			case GroupShapeDictionaryString:
				keys, err := ww.batch.DictionaryString(p.cq.GroupBy[0].Name)
				if err != nil {
					return err
				}
				if keys.DictionaryDomain != p.cq.GroupBy[0].DictionaryDomain {
					return fmt.Errorf("dictionary domain changed for %q", p.cq.GroupBy[0].Name)
				}
				w.chunkLens[pi], w.nullCounts[pi], w.nullSums[pi] = compactU64Nullable(
					func(i int) uint64 { return uint64(keys.Indices[i]) }, keys.IsValid, vals.Values, sel, rows, w.selectedKeys[base:base+rows], w.selectedVals[base:base+rows])
			case GroupShapePairFixed:
				a, err := fixedFromBatch(ww.batch, p.cq.GroupBy[0])
				if err != nil {
					return err
				}
				b, err := fixedFromBatch(ww.batch, p.cq.GroupBy[1])
				if err != nil {
					return err
				}
				w.chunkLens[pi] = compactPair(a, b, vals.Values, sel, rows, w.selectedPairs[base:base+rows], w.selectedPairValid[base:base+rows], w.selectedVals[base:base+rows])
			default:
				return fmt.Errorf("radix unsupported for group shape %d", p.cq.GroupShape)
			}
		}
		return nil
	}); err != nil {
		return Result{}, err
	}
	total := 0
	for pi, n := range w.chunkLens {
		src := p.partBase[pi]
		if src != total && n > 0 {
			if p.cq.GroupShape == GroupShapePairFixed {
				copy(w.selectedPairs[total:total+n], w.selectedPairs[src:src+n])
				copy(w.selectedPairValid[total:total+n], w.selectedPairValid[src:src+n])
			} else {
				copy(w.selectedKeys[total:total+n], w.selectedKeys[src:src+n])
			}
			copy(w.selectedVals[total:total+n], w.selectedVals[src:src+n])
		}
		total += n
	}
	nullRows := 0
	nullSum := int64(0)
	for i := range w.nullCounts {
		nullRows += w.nullCounts[i]
		nullSum += w.nullSums[i]
	}
	if p.cq.GroupShape == GroupShapePairFixed {
		summary, err := w.radixPair.AggregateNullable(ctx, p.e.scheduler, w.selectedPairs[:total], w.selectedPairValid[:total], w.selectedVals[:total], workers)
		if errors.Is(err, radix.ErrLocalTableFull) {
			return p.executeRadixFallbackReplay(ctx, w, opts)
		}
		if err != nil {
			return Result{}, err
		}
		result := Result{GroupCount: summary.Groups, SelectedRows: summary.Rows, Checksum: summary.Checksum}
		if opts.Materialize {
			groups := w.radixPair.Materialize(nil)
			result.Rows = make([]AggregateRow, len(groups))
			for i, group := range groups {
				result.Rows[i] = AggregateRow{Keys: []KeyPart{
					p.decodeFixedValid(p.cq.GroupBy[0], group.Key.A, group.ValidMask&1 != 0),
					p.decodeFixedValid(p.cq.GroupBy[1], group.Key.B, group.ValidMask&2 != 0),
				}, Sum: group.Sum, Count: group.Count, Aggregates: p.materializeAggregates(sumCountAggregateState(group.Sum, group.Count))}
			}
		}
		return result, nil
	}
	summary, err := w.radix64.Aggregate(ctx, p.e.scheduler, w.selectedKeys[:total], w.selectedVals[:total], workers)
	if errors.Is(err, radix.ErrLocalTableFull) {
		return p.executeRadixFallbackReplay(ctx, w, opts)
	}
	if err != nil {
		return Result{}, err
	}
	result := Result{GroupCount: summary.Groups, SelectedRows: summary.Rows + nullRows, Checksum: summary.Checksum + nullSum}
	if nullRows > 0 {
		result.GroupCount++
	}
	if opts.Materialize {
		groups := w.radix64.Materialize(nil)
		result.Rows = make([]AggregateRow, len(groups), len(groups)+1)
		for i, group := range groups {
			result.Rows[i] = AggregateRow{Keys: []KeyPart{p.decodeFixed(p.cq.GroupBy[0], group.Key)}, Sum: group.Sum, Count: group.Count, Aggregates: p.materializeAggregates(sumCountAggregateState(group.Sum, group.Count))}
		}
		if nullRows > 0 {
			result.Rows = append(result.Rows, AggregateRow{Keys: []KeyPart{p.decodeFixedValid(p.cq.GroupBy[0], 0, false)}, Sum: nullSum, Count: uint64(nullRows), Aggregates: p.materializeAggregates(sumCountAggregateState(nullSum, uint64(nullRows)))})
		}
	}
	return result, nil
}

func (p *PreparedQuery) executeRadixFallbackReplay(ctx context.Context, workspace *queryWorkspace, opts ExecuteOptions) (Result, error) {
	reader, err := p.src.NewReader(ctx, source.ReadOptions{Kernels: p.e.kernels})
	if err != nil {
		return Result{}, err
	}
	defer reader.Close()
	batch := memory.NewBatch()
	defer batch.ReleaseOwned()
	var evaluation expr.EvalWorkspace
	var summary hashagg.Summary
	if p.cq.GroupShape == GroupShapePairFixed {
		table, err := workspace.ensureFallbackPair(ctx)
		if err != nil {
			return Result{}, err
		}
		table.Reset()
		for _, partition := range p.parts {
			if err := reader.ReadPartition(ctx, partition, batch); err != nil {
				return Result{}, err
			}
			selection, err := p.cq.Predicate.Evaluate(batch, &evaluation, p.e.kernels)
			if err != nil {
				return Result{}, err
			}
			values, err := batch.Int64(p.cq.ValueColumn)
			if err != nil {
				return Result{}, err
			}
			a, err := fixedFromBatch(batch, p.cq.GroupBy[0])
			if err != nil {
				return Result{}, err
			}
			b, err := fixedFromBatch(batch, p.cq.GroupBy[1])
			if err != nil {
				return Result{}, err
			}
			err = forSelected(selection.Truth, batch.Rows, func(row int) error {
				mask := uint8(0)
				key := hashagg.Key128{}
				if a.valid(row) {
					mask |= 1
					key.A = a.at(row)
				}
				if b.valid(row) {
					mask |= 2
					key.B = b.at(row)
				}
				created, err := table.TryAddNullable(key, mask, values.Values[row])
				if err != nil {
					return err
				}
				if created {
					summary.Groups++
				}
				summary.Rows++
				summary.Checksum += values.Values[row]
				return nil
			})
			if err != nil {
				return Result{}, err
			}
		}
		result := Result{GroupCount: summary.Groups, SelectedRows: summary.Rows, Checksum: summary.Checksum}
		if opts.Materialize {
			p.materializePairResult(&result, table)
		}
		return result, nil
	}
	table, err := workspace.ensureFallbackU64(ctx)
	if err != nil {
		return Result{}, err
	}
	table.Reset()
	for _, partition := range p.parts {
		if err := reader.ReadPartition(ctx, partition, batch); err != nil {
			return Result{}, err
		}
		selection, err := p.cq.Predicate.Evaluate(batch, &evaluation, p.e.kernels)
		if err != nil {
			return Result{}, err
		}
		values, err := batch.Int64(p.cq.ValueColumn)
		if err != nil {
			return Result{}, err
		}
		keys, err := fixedFromBatch(batch, p.cq.GroupBy[0])
		if err != nil {
			return Result{}, err
		}
		err = forSelected(selection.Truth, batch.Rows, func(row int) error {
			created, err := table.TryAddNullable(keys.at(row), keys.valid(row), values.Values[row])
			if err != nil {
				return err
			}
			if created {
				summary.Groups++
			}
			summary.Rows++
			summary.Checksum += values.Values[row]
			return nil
		})
		if err != nil {
			return Result{}, err
		}
	}
	result := Result{GroupCount: summary.Groups, SelectedRows: summary.Rows, Checksum: summary.Checksum}
	if opts.Materialize {
		groups := table.Materialize(nil)
		result.Rows = make([]AggregateRow, len(groups))
		for index, group := range groups {
			result.Rows[index] = AggregateRow{Keys: []KeyPart{p.decodeFixedValid(p.cq.GroupBy[0], group.Key, group.Valid)}, Sum: group.Sum, Count: group.Count, Aggregates: p.materializeAggregates(group.State)}
		}
	}
	return result, nil
}
