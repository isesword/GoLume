package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/hashagg"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math/bits"
)

func (p *PreparedQuery) executeDirectFused(ctx context.Context, w *queryWorkspace, opts ExecuteOptions) (Result, error) {
	if p.cq.SimpleComparison == nil {
		return Result{}, fmt.Errorf("direct-fused requires a simple numeric comparison")
	}
	ww := &w.workers[0]
	var out Result
	switch p.cq.GroupShape {
	case GroupShapeI32:
		w.i32.Reset()
	case GroupShapeI64:
		w.u64.Reset()
	case GroupShapeU32, GroupShapeDictionaryString:
		w.u32.Reset()
	case GroupShapePairFixed:
		w.pair.Reset()
	case GroupShapeStringView:
		w.str.Reset()
	}
	for _, part := range p.parts {
		if err := ww.reader.ReadPartition(ctx, part, ww.batch); err != nil {
			return Result{}, err
		}
		vals, err := p.aggregateInput(ww.batch)
		if err != nil {
			return Result{}, err
		}
		var aggregate func(int) error
		switch p.cq.GroupShape {
		case GroupShapeI32:
			keys, err := ww.batch.Int32(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			aggregate = func(i int) error {
				v, vv := aggregateValueAt(vals, i)
				created, err := w.i32.TryAddNullableValue(keys.Values[i], keys.IsValid(i), v, vv)
				if err != nil {
					return err
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
				return nil
			}
		case GroupShapeI64:
			keys, err := ww.batch.Int64(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			aggregate = func(i int) error {
				v, vv := aggregateValueAt(vals, i)
				created, err := w.u64.TryAddNullableValue(uint64(keys.Values[i]), keys.IsValid(i), v, vv)
				if err != nil {
					return err
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
				return nil
			}
		case GroupShapeU32:
			keys, err := ww.batch.Uint32(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			aggregate = func(i int) error {
				v, vv := aggregateValueAt(vals, i)
				created, err := w.u32.TryAddNullableValue(keys.Values[i], keys.IsValid(i), v, vv)
				if err != nil {
					return err
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
				return nil
			}
		case GroupShapeDictionaryString:
			keys, err := ww.batch.DictionaryString(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			if keys.DictionaryDomain != p.cq.GroupBy[0].DictionaryDomain {
				return Result{}, fmt.Errorf("dictionary domain changed")
			}
			aggregate = func(i int) error {
				v, vv := aggregateValueAt(vals, i)
				created, err := w.u32.TryAddNullableValue(keys.Indices[i], keys.IsValid(i), v, vv)
				if err != nil {
					return err
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
				return nil
			}
		case GroupShapeStringView:
			keys, err := ww.batch.String(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			aggregate = func(i int) error {
				v, vv := aggregateValueAt(vals, i)
				ng, err := w.str.AddNullableValue(keys.ValueBytes(i), keys.IsValid(i), v, vv)
				if err != nil {
					return err
				}
				if ng {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
				return nil
			}
		case GroupShapePairFixed:
			a, err := fixedFromBatch(ww.batch, p.cq.GroupBy[0])
			if err != nil {
				return Result{}, err
			}
			b, err := fixedFromBatch(ww.batch, p.cq.GroupBy[1])
			if err != nil {
				return Result{}, err
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
				created, err := w.pair.TryAddNullableValue(hashagg.Key128{A: a.at(i), B: b.at(i)}, validMask, v, vv)
				if err != nil {
					return err
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
				return nil
			}
		}
		selected, err := forSimpleSelected(ww.batch, p.cq.SimpleComparison, p.e.kernels, aggregate)
		if err != nil {
			return Result{}, err
		}
		out.SelectedRows += selected
	}
	if opts.Materialize {
		switch p.cq.GroupShape {
		case GroupShapeI32:
			p.materializeI32Result(&out, w.i32)
		case GroupShapeI64:
			p.materializeU64Result(&out, w.u64)
		case GroupShapeU32, GroupShapeDictionaryString:
			p.materializeU32Result(&out, w.u32)
		case GroupShapePairFixed:
			p.materializePairResult(&out, w.pair)
		case GroupShapeStringView:
			gs := w.str.Materialize(nil)
			out.Rows = make([]AggregateRow, len(gs))
			for i, g := range gs {
				out.Rows[i] = AggregateRow{Keys: []KeyPart{{Valid: g.Valid, Type: p.cq.GroupBy[0].Type, String: g.Key}}, Sum: g.Sum, Count: g.Count, Aggregates: p.materializeAggregates(g.State)}
			}
		}
	}
	return out, nil
}

// forSimpleSelected produces one temporary 64-row mask at a time and consumes
// its set lanes immediately. It gives DirectFused the selected kernel backend
// without materializing a partition-sized selection bitmap.
func forSimpleSelected(batch *memory.Batch, comparison *expr.SimpleComparison, kernels kernel.Set, fn func(int) error) (int, error) {
	op, err := directCompareOp(comparison.Op)
	if err != nil {
		return 0, err
	}
	word := memory.Bitmap{0}
	selected := 0
	switch comparison.Type {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		v, err := batch.Int32(comparison.Column)
		if err != nil {
			return 0, err
		}
		literal := int32(comparison.LiteralI64)
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareInt32(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := consumeSelectedWord(base, word[0], fn); err != nil {
				return 0, err
			}
		}
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		v, err := batch.Int64(comparison.Column)
		if err != nil {
			return 0, err
		}
		literal := comparison.LiteralI64
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareInt64(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := consumeSelectedWord(base, word[0], fn); err != nil {
				return 0, err
			}
		}
	case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32:
		v, err := batch.Uint32(comparison.Column)
		if err != nil {
			return 0, err
		}
		literal := uint32(comparison.LiteralU64)
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareUint32(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := consumeSelectedWord(base, word[0], fn); err != nil {
				return 0, err
			}
		}
	case memory.TypeUint64:
		v, err := batch.Uint64(comparison.Column)
		if err != nil {
			return 0, err
		}
		literal := comparison.LiteralU64
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareUint64(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := consumeSelectedWord(base, word[0], fn); err != nil {
				return 0, err
			}
		}
	case memory.TypeFloat32, memory.TypeFloat64:
		v, err := batch.Float64(comparison.Column)
		if err != nil {
			return 0, err
		}
		literal := comparison.LiteralF64
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareFloat64(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := consumeSelectedWord(base, word[0], fn); err != nil {
				return 0, err
			}
		}
	default:
		return 0, fmt.Errorf("direct-fused comparison type %s unsupported", comparison.Type)
	}
	return selected, nil
}

func consumeSelectedWord(base int, word uint64, fn func(int) error) error {
	for mask := word; mask != 0; mask &= mask - 1 {
		if err := fn(base + bits.TrailingZeros64(mask)); err != nil {
			return err
		}
	}
	return nil
}

func blockValidity(validity memory.Bitmap, base int) memory.Bitmap {
	if validity == nil {
		return nil
	}
	return validity[base/64:]
}

func directCompareOp(op expr.Op) (kernel.CompareOp, error) {
	switch op {
	case expr.OpGT:
		return kernel.CompareGT, nil
	case expr.OpGE:
		return kernel.CompareGE, nil
	case expr.OpLT:
		return kernel.CompareLT, nil
	case expr.OpLE:
		return kernel.CompareLE, nil
	case expr.OpEQ:
		return kernel.CompareEQ, nil
	default:
		return 0, fmt.Errorf("direct-fused comparison operator %s unsupported", op)
	}
}

func (p *PreparedQuery) executeBitmapDirect(ctx context.Context, w *queryWorkspace, opts ExecuteOptions) (Result, error) {
	ww := &w.workers[0]
	var out Result
	switch p.cq.GroupShape {
	case GroupShapeI32:
		w.i32.Reset()
	case GroupShapeI64:
		w.u64.Reset()
	case GroupShapeU32, GroupShapeDictionaryString:
		w.u32.Reset()
	case GroupShapePairFixed:
		w.pair.Reset()
	case GroupShapeStringView:
		w.str.Reset()
	}
	for _, part := range p.parts {
		if err := ww.reader.ReadPartition(ctx, part, ww.batch); err != nil {
			return Result{}, err
		}
		sel, n, err := p.selection(ww)
		if err != nil {
			return Result{}, err
		}
		out.SelectedRows += n
		vals, err := p.aggregateInput(ww.batch)
		if err != nil {
			return Result{}, err
		}
		switch p.cq.GroupShape {
		case GroupShapeI32:
			keys, err := ww.batch.Int32(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			if vals == nil {
				err = forSelected(sel, ww.batch.Rows, func(i int) error {
					created, e := w.i32.TryAddNullableValue(keys.Values[i], keys.IsValid(i), 0, false)
					if created {
						out.GroupCount++
					}
					return e
				})
				if err != nil {
					return Result{}, err
				}
			} else {
				s, e := w.i32.AggregateSelectedNullableValues(keys.Values, keys.Validity, vals.Values, vals.Validity, sel)
				if e != nil {
					return Result{}, e
				}
				out.GroupCount += s.Groups
				out.Checksum += s.Checksum
			}
		case GroupShapeI64:
			keys, err := ww.batch.Int64(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			if vals == nil {
				err = forSelected(sel, ww.batch.Rows, func(i int) error {
					created, e := w.u64.TryAddNullableValue(uint64(keys.Values[i]), keys.IsValid(i), 0, false)
					if created {
						out.GroupCount++
					}
					return e
				})
				if err != nil {
					return Result{}, err
				}
			} else {
				ss, e := w.u64.AggregateSelectedNullableValues(keys.Values, keys.Validity, vals.Values, vals.Validity, sel)
				if e != nil {
					return Result{}, e
				}
				out.GroupCount += ss.Groups
				out.Checksum += ss.Checksum
			}
		case GroupShapeU32:
			keys, err := ww.batch.Uint32(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			if vals == nil {
				err = forSelected(sel, ww.batch.Rows, func(i int) error {
					created, e := w.u32.TryAddNullableValue(keys.Values[i], keys.IsValid(i), 0, false)
					if created {
						out.GroupCount++
					}
					return e
				})
				if err != nil {
					return Result{}, err
				}
			} else {
				s, e := w.u32.AggregateSelectedNullableValues(keys.Values, keys.Validity, vals.Values, vals.Validity, sel)
				if e != nil {
					return Result{}, e
				}
				out.GroupCount += s.Groups
				out.Checksum += s.Checksum
			}
		case GroupShapeDictionaryString:
			keys, err := ww.batch.DictionaryString(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			if keys.DictionaryDomain != p.cq.GroupBy[0].DictionaryDomain {
				return Result{}, fmt.Errorf("dictionary domain changed")
			}
			if vals == nil {
				err = forSelected(sel, ww.batch.Rows, func(i int) error {
					created, e := w.u32.TryAddNullableValue(keys.Indices[i], keys.IsValid(i), 0, false)
					if created {
						out.GroupCount++
					}
					return e
				})
				if err != nil {
					return Result{}, err
				}
			} else {
				s, e := w.u32.AggregateSelectedNullableValues(keys.Indices, keys.Validity, vals.Values, vals.Validity, sel)
				if e != nil {
					return Result{}, e
				}
				out.GroupCount += s.Groups
				out.Checksum += s.Checksum
			}
		case GroupShapeStringView:
			keys, err := ww.batch.String(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			err = forSelected(sel, ww.batch.Rows, func(i int) error {
				v, vv := aggregateValueAt(vals, i)
				ng, e := w.str.AddNullableValue(keys.ValueBytes(i), keys.IsValid(i), v, vv)
				if ng {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
				return e
			})
			if err != nil {
				return Result{}, err
			}
		case GroupShapePairFixed:
			a, err := fixedFromBatch(ww.batch, p.cq.GroupBy[0])
			if err != nil {
				return Result{}, err
			}
			b, err := fixedFromBatch(ww.batch, p.cq.GroupBy[1])
			if err != nil {
				return Result{}, err
			}
			if ww.batch.Rows > len(ww.selectedPairs) {
				return Result{}, fmt.Errorf("pair aggregation batch rows %d exceed workspace capacity %d", ww.batch.Rows, len(ww.selectedPairs))
			}
			keys := ww.selectedPairs[:ww.batch.Rows]
			keyValidity := ww.selectedPairValid[:ww.batch.Rows]
			clear(keyValidity)
			for i := 0; i < ww.batch.Rows; i++ {
				keys[i] = hashagg.Key128{A: a.at(i), B: b.at(i)}
				if a.valid(i) {
					keyValidity[i] |= 1
				}
				if b.valid(i) {
					keyValidity[i] |= 2
				}
			}
			valueValues := ww.selectedVals[:ww.batch.Rows]
			var valueValidity memory.Bitmap
			if vals.Validity != nil {
				valueValidity = vals.Validity
			}
			for i := range valueValues {
				valueValues[i], _ = aggregateValueAt(vals, i)
			}
			s, e := w.pair.AggregateSelectedNullableValues(keys, keyValidity, valueValues, valueValidity, sel)
			if e != nil {
				return Result{}, e
			}
			out.GroupCount += s.Groups
			out.Checksum += s.Checksum
			if e != nil {
				return Result{}, err
			}
		}
	}
	if opts.Materialize {
		switch p.cq.GroupShape {
		case GroupShapeI32:
			p.materializeI32Result(&out, w.i32)
		case GroupShapeI64:
			p.materializeU64Result(&out, w.u64)
		case GroupShapeU32, GroupShapeDictionaryString:
			p.materializeU32Result(&out, w.u32)
		case GroupShapePairFixed:
			p.materializePairResult(&out, w.pair)
		case GroupShapeStringView:
			gs := w.str.Materialize(nil)
			out.Rows = make([]AggregateRow, len(gs))
			for i, g := range gs {
				out.Rows[i] = AggregateRow{Keys: []KeyPart{{Valid: g.Valid, Type: p.cq.GroupBy[0].Type, String: g.Key}}, Sum: g.Sum, Count: g.Count, Aggregates: p.materializeAggregates(g.State)}
			}
		}
	}
	return out, nil
}
