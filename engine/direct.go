package engine

import (
	"context"
	"fmt"
	"gocolumn/hashagg"
)

func (p *PreparedQuery) executeDirectFused(ctx context.Context, w *queryWorkspace, opts ExecuteOptions) (Result, error) {
	if p.cq.SimpleGT == nil {
		return Result{}, fmt.Errorf("direct-fused requires simple GT predicate")
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
		pred, err := ww.batch.Int64(p.cq.SimpleGT.Column)
		if err != nil {
			return Result{}, err
		}
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
			for i, x := range pred.Values {
				if pred.Validity != nil && !memorySet(pred.Validity, i) {
					continue
				}
				if x <= p.cq.SimpleGT.Threshold {
					continue
				}
				out.SelectedRows++
				v, vv := aggregateValueAt(vals, i)
				created, err := w.i32.TryAddNullableValue(keys.Values[i], keys.IsValid(i), v, vv)
				if err != nil {
					return Result{}, err
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
			}
		case GroupShapeI64:
			keys, err := ww.batch.Int64(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			for i, x := range pred.Values {
				if pred.Validity != nil && !memorySet(pred.Validity, i) {
					continue
				}
				if x <= p.cq.SimpleGT.Threshold {
					continue
				}
				out.SelectedRows++
				v, vv := aggregateValueAt(vals, i)
				created, err := w.u64.TryAddNullableValue(uint64(keys.Values[i]), keys.IsValid(i), v, vv)
				if err != nil {
					return Result{}, err
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
			}
		case GroupShapeU32:
			keys, err := ww.batch.Uint32(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			for i, x := range pred.Values {
				if pred.Validity != nil && !memorySet(pred.Validity, i) {
					continue
				}
				if x <= p.cq.SimpleGT.Threshold {
					continue
				}
				out.SelectedRows++
				v, vv := aggregateValueAt(vals, i)
				created, err := w.u32.TryAddNullableValue(keys.Values[i], keys.IsValid(i), v, vv)
				if err != nil {
					return Result{}, err
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
			}
		case GroupShapeDictionaryString:
			keys, err := ww.batch.DictionaryString(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			if keys.DictionaryDomain != p.cq.GroupBy[0].DictionaryDomain {
				return Result{}, fmt.Errorf("dictionary domain changed")
			}
			for i, x := range pred.Values {
				if pred.Validity != nil && !memorySet(pred.Validity, i) {
					continue
				}
				if x <= p.cq.SimpleGT.Threshold {
					continue
				}
				out.SelectedRows++
				v, vv := aggregateValueAt(vals, i)
				created, err := w.u32.TryAddNullableValue(keys.Indices[i], keys.IsValid(i), v, vv)
				if err != nil {
					return Result{}, err
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
			}
		case GroupShapeStringView:
			keys, err := ww.batch.String(p.cq.GroupBy[0].Name)
			if err != nil {
				return Result{}, err
			}
			for i, x := range pred.Values {
				if pred.Validity != nil && !memorySet(pred.Validity, i) {
					continue
				}
				if x <= p.cq.SimpleGT.Threshold {
					continue
				}
				out.SelectedRows++
				v, vv := aggregateValueAt(vals, i)
				ng, err := w.str.AddNullableValue(keys.ValueBytes(i), keys.IsValid(i), v, vv)
				if err != nil {
					return Result{}, err
				}
				if ng {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
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
			for i, x := range pred.Values {
				if pred.Validity != nil && !memorySet(pred.Validity, i) {
					continue
				}
				if x <= p.cq.SimpleGT.Threshold {
					continue
				}
				out.SelectedRows++
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
					return Result{}, err
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
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
			err = forSelected(sel, ww.batch.Rows, func(i int) error {
				validMask := uint8(0)
				if a.valid(i) {
					validMask |= 1
				}
				if b.valid(i) {
					validMask |= 2
				}
				v, vv := aggregateValueAt(vals, i)
				created, e := w.pair.TryAddNullableValue(hashagg.Key128{A: a.at(i), B: b.at(i)}, validMask, v, vv)
				if e != nil {
					return e
				}
				if created {
					out.GroupCount++
				}
				if vv {
					out.Checksum += v
				}
				return nil
			})
			if err != nil {
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
