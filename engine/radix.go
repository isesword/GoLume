package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/hashagg"
	"gocolumn/memory"
	"gocolumn/radix"
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

func (p *PreparedQuery) executeRadix(ctx context.Context, w *queryWorkspace, opts ExecuteOptions) (Result, error) {
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
	// Pack per-partition compact regions to the prefix. copy is overlap-safe.
	total := 0
	for pi, n := range w.chunkLens {
		src := p.partBase[pi]
		if src != total && n > 0 {
			switch p.cq.GroupShape {
			case GroupShapePairFixed:
				copy(w.selectedPairs[total:total+n], w.selectedPairs[src:src+n])
				copy(w.selectedPairValid[total:total+n], w.selectedPairValid[src:src+n])
			default:
				copy(w.selectedKeys[total:total+n], w.selectedKeys[src:src+n])
			}
			copy(w.selectedVals[total:total+n], w.selectedVals[src:src+n])
		}
		total += n
	}
	var nullCount int
	var nullSum int64
	for i := range w.nullCounts {
		nullCount += w.nullCounts[i]
		nullSum += w.nullSums[i]
	}
	var out Result
	switch p.cq.GroupShape {
	case GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapeDictionaryString:
		s, err := w.radix64.Aggregate(ctx, p.e.scheduler, w.selectedKeys[:total], w.selectedVals[:total], workers)
		usedFallback := false
		if errors.Is(err, radix.ErrLocalTableFull) {
			tab, ferr := w.ensureFallbackU64(ctx)
			if ferr != nil {
				return Result{}, ferr
			}
			tab.Reset()
			var fs hashagg.Summary
			for i, key := range w.selectedKeys[:total] {
				created, aerr := tab.TryAdd(key, w.selectedVals[i])
				if aerr != nil {
					return Result{}, aerr
				}
				if created {
					fs.Groups++
				}
				fs.Rows++
				fs.Checksum += w.selectedVals[i]
			}
			s = radix.Summary{Groups: fs.Groups, Rows: fs.Rows, Checksum: fs.Checksum}
			usedFallback = true
		} else if err != nil {
			return Result{}, err
		}
		out = Result{GroupCount: s.Groups, SelectedRows: s.Rows + nullCount, Checksum: s.Checksum + nullSum}
		if nullCount > 0 {
			out.GroupCount++
		}
		if opts.Materialize {
			var gs []radix.Group
			if usedFallback {
				fg := w.fallbackU64.Materialize(nil)
				gs = make([]radix.Group, len(fg))
				for i, g := range fg {
					gs[i] = radix.Group{Key: g.Key, Sum: g.Sum, Count: g.Count}
				}
			} else {
				gs = w.radix64.Materialize(nil)
			}
			out.Rows = make([]AggregateRow, len(gs))
			for i, g := range gs {
				out.Rows[i] = AggregateRow{Keys: []KeyPart{p.decodeFixed(p.cq.GroupBy[0], g.Key)}, Sum: g.Sum, Count: g.Count, Aggregates: p.materializeAggregates(sumCountAggregateState(g.Sum, g.Count))}
			}
			if nullCount > 0 {
				out.Rows = append(out.Rows, AggregateRow{Keys: []KeyPart{p.decodeFixedValid(p.cq.GroupBy[0], 0, false)}, Sum: nullSum, Count: uint64(nullCount), Aggregates: p.materializeAggregates(sumCountAggregateState(nullSum, uint64(nullCount)))})
			}
		}
	case GroupShapePairFixed:
		s, err := w.radixPair.AggregateNullable(ctx, p.e.scheduler, w.selectedPairs[:total], w.selectedPairValid[:total], w.selectedVals[:total], workers)
		usedFallback := false
		if errors.Is(err, radix.ErrLocalTableFull) {
			tab, ferr := w.ensureFallbackPair(ctx)
			if ferr != nil {
				return Result{}, ferr
			}
			tab.Reset()
			var fs hashagg.Summary
			for i, key := range w.selectedPairs[:total] {
				created, aerr := tab.TryAddNullable(key, w.selectedPairValid[i], w.selectedVals[i])
				if aerr != nil {
					return Result{}, aerr
				}
				if created {
					fs.Groups++
				}
				fs.Rows++
				fs.Checksum += w.selectedVals[i]
			}
			s = radix.Summary{Groups: fs.Groups, Rows: fs.Rows, Checksum: fs.Checksum}
			usedFallback = true
		} else if err != nil {
			return Result{}, err
		}
		out = Result{GroupCount: s.Groups, SelectedRows: s.Rows, Checksum: s.Checksum}
		if opts.Materialize {
			var gs []radix.PairGroup
			if usedFallback {
				fg := w.fallbackPair.Materialize(nil)
				gs = make([]radix.PairGroup, len(fg))
				for i, g := range fg {
					gs[i] = radix.PairGroup{Key: g.Key, ValidMask: g.ValidMask, Sum: g.Sum, Count: g.Count}
				}
			} else {
				gs = w.radixPair.Materialize(nil)
			}
			out.Rows = make([]AggregateRow, len(gs))
			for i, g := range gs {
				out.Rows[i] = AggregateRow{Keys: []KeyPart{p.decodeFixedValid(p.cq.GroupBy[0], g.Key.A, g.ValidMask&1 != 0), p.decodeFixedValid(p.cq.GroupBy[1], g.Key.B, g.ValidMask&2 != 0)}, Sum: g.Sum, Count: g.Count, Aggregates: p.materializeAggregates(sumCountAggregateState(g.Sum, g.Count))}
			}
		}
	}
	return out, nil
}

var _ = radix.Summary{}
