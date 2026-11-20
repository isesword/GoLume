package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
)

func (p *PreparedQuery) decimalGrouped() bool {
	return p != nil && p.cq.MultiAggregate != nil && p.cq.MultiAggregate.DecimalGroupedFields != nil
}
func (p *PreparedQuery) decimalLaneFields() []memory.Field {
	if p.decimalGrouped() {
		return p.cq.MultiAggregate.DecimalGroupedFields
	}
	return p.cq.MultiAggregate.DecimalFields
}

type decimalGroupedStream struct {
	*decimalGlobalStream
	ordering  [2]source.OrderingColumn
	numKeys   int
	row       int
	truth     memory.Bitmap
	eof, have bool
	key       decimalGroupKey
	last      decimalGroupKey
	lastSet   bool
	groups    int
	pageRows  int
}

type decimalGroupKey struct {
	values [2]int64
	valid  [2]bool
}

func decimalGroupKeyAt(v [2]*memory.Int64Vector, n, row int) (key decimalGroupKey) {
	for i := 0; i < n; i++ {
		if v[i].IsValid(row) {
			key.values[i], key.valid[i] = v[i].Values[row], true
		}
	}
	return
}

func (s *decimalGroupedStream) outOfOrder(a, b decimalGroupKey) bool {
	for i := 0; i < s.numKeys; i++ {
		if a.valid[i] != b.valid[i] {
			return (!a.valid[i] && s.ordering[i].NullsLast) || (a.valid[i] && !s.ordering[i].NullsLast)
		}
		if a.valid[i] && a.values[i] != b.values[i] {
			if s.ordering[i].Descending {
				return b.values[i] > a.values[i]
			}
			return b.values[i] < a.values[i]
		}
	}
	return false
}

func (p *PreparedQuery) newDecimalGroupedStream(ctx context.Context, schema memory.Schema, opts source.ReadOptions, outputRows int, acquire bool) (source.BatchStream, error) {
	return p.newDecimalGroupedStreamWithManagers(ctx, schema, opts, outputRows, acquire, nil)
}
func (p *PreparedQuery) newDecimalGroupedStreamWithManagers(ctx context.Context, schema memory.Schema, opts source.ReadOptions, outputRows int, acquire bool, wrap func(memory.Manager) memory.Manager) (source.BatchStream, error) {
	if !p.decimalGrouped() {
		return nil, fmt.Errorf("Decimal grouped SUM requires global ordering")
	}
	var ordering [2]source.OrderingColumn
	nKeys := len(p.cq.GroupBy)
	provider, ok := p.src.(source.OrderingProvider)
	if !ok || nKeys < 1 || nKeys > len(ordering) {
		return nil, fmt.Errorf("Decimal grouped SUM requires global ordering")
	}
	proof := provider.Ordering()
	if len(proof) < nKeys {
		return nil, fmt.Errorf("Decimal grouped SUM requires global ordering on all keys")
	}
	for i := 0; i < nKeys; i++ {
		if proof[i].Name != p.cq.GroupBy[i].Name {
			return nil, fmt.Errorf("Decimal grouped SUM requires global ordering on all keys")
		}
		ordering[i] = proof[i]
	}
	b, e := p.newDecimalGlobalStreamWithManagers(ctx, schema, opts, acquire, wrap)
	if e != nil {
		return nil, e
	}
	pageRows := outputRows
	if pageRows < 1 {
		pageRows = opts.BatchRows
	}
	if pageRows < 1 || pageRows > 64 {
		pageRows = 64
	}
	if opts.BatchRows > 0 && pageRows > opts.BatchRows {
		pageRows = opts.BatchRows
	}
	rowBytes := int64(16*nKeys + 24*len(p.decimalLaneFields()))
	if opts.BatchBytes > 0 && int64(pageRows) > opts.BatchBytes/rowBytes {
		pageRows = int(opts.BatchBytes / rowBytes)
		if pageRows < 1 {
			pageRows = 1
		}
	}
	return &decimalGroupedStream{pageRows: pageRows, decimalGlobalStream: b.(*decimalGlobalStream), ordering: ordering, numKeys: nKeys}, nil
}

// validateBatch validates every binding and every input key before consuming any
// row from this batch. Ordering includes filtered rows and spans batch boundaries.
func (s *decimalGroupedStream) validateBatch(ctx context.Context) error {
	ordered := !s.p.decimalResident()
	b := &s.inputBatch
	if b.Rows < 0 {
		return fmt.Errorf("Decimal grouped negative rows")
	}
	var vectors [2]*memory.Int64Vector
	for i, f := range s.p.cq.GenericFields {
		k := b.Int64s[f.Name]
		if k == nil || len(k.Values) != b.Rows || k.Validity != nil && len(k.Validity) < memory.WordsFor(b.Rows) {
			return fmt.Errorf("Decimal grouped key binding mismatch")
		}
		vectors[i] = k
	}
	last, set := s.last, s.lastSet
	for row := 0; row < b.Rows; row++ {
		if row%64 == 0 {
			if e := ctx.Err(); e != nil {
				return e
			}
		}
		key := decimalGroupKeyAt(vectors, s.numKeys, row)
		for i, f := range s.p.cq.GenericFields {
			if !key.valid[i] && !f.Nullable {
				return fmt.Errorf("Decimal grouped nonnullable key NULL")
			}
		}
		if set && ordered && s.outOfOrder(last, key) {
			return fmt.Errorf("Decimal grouped global ordering violated")
		}
		last, set = key, true
	}
	for i, f := range s.p.decimalLaneFields() {
		if e := ctx.Err(); e != nil {
			return e
		}
		if s.p.cq.Aggregates[i].Kind == expr.AggCountRows {
			continue
		}
		v := b.Decimals[f.Name]
		if v == nil || v.Precision != f.DecimalPrecision || v.Scale != f.DecimalScale || len(v.Values) != b.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(b.Rows) {
			return fmt.Errorf("Decimal grouped binding mismatch %q", f.Name)
		}
		for row := 0; row < b.Rows; row++ {
			if row%64 == 0 {
				if e := ctx.Err(); e != nil {
					return e
				}
			}
			if !v.IsValid(row) {
				if !f.Nullable {
					return fmt.Errorf("Decimal grouped nonnullable input NULL")
				}
				continue
			}
			if !v.Values[row].FitsInPrecision(f.DecimalPrecision) {
				return fmt.Errorf("Decimal grouped coefficient overflow")
			}
		}
	}
	selection, e := s.predicate.evaluate(ctx, s.p.cq.Predicate, b, s.p.e.kernels)
	if e != nil {
		return e
	}
	s.truth = selection.Truth
	s.last, s.lastSet = last, set
	return nil
}
func (s *decimalGroupedStream) addRow(ctx context.Context, row int) error {
	if s.selected == math.MaxInt {
		return fmt.Errorf("Decimal SUM selected-row statistics exceed MaxInt")
	}
	copy(s.scratch, s.states)
	copy(s.countScratch, s.counts)
	for i, f := range s.p.decimalLaneFields() {
		if i%64 == 0 {
			if e := ctx.Err(); e != nil {
				return e
			}
		}
		kind := s.p.cq.Aggregates[i].Kind
		if kind == expr.AggCountRows {
			if e := decimalCountIncrement(&s.countScratch[i]); e != nil {
				return e
			}
			continue
		}
		v := s.inputBatch.Decimals[f.Name]
		if !v.IsValid(row) {
			continue
		}
		if kind == expr.AggCountValues {
			if e := decimalCountIncrement(&s.countScratch[i]); e != nil {
				return e
			}
		} else {
			if e := s.scratch[i].Add(v.Values[row]); e != nil {
				return e
			}
		}
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	copy(s.states, s.scratch)
	copy(s.counts, s.countScratch)
	s.selected++
	return nil
}

// Pages of at most 64 rows bound output independently of total groups. The current
// input batch remains borrowed across Next when a boundary row is pending.
func (s *decimalGroupedStream) emit(ctx context.Context, dst *memory.Batch) (bool, error) {
	for i, state := range s.states {
		if i%64 == 0 {
			if e := ctx.Err(); e != nil {
				return s.fail(e)
			}
		}
		// Retain legacy SUM preflight/error precedence. AVG is finalized only
		// once below; fail() rolls back the undelivered page on error.
		if s.p.cq.Aggregates[i].Kind == expr.AggSum {
			if _, _, e := state.Finalize(); e != nil {
				return s.fail(e)
			}
		}
	}
	if s.groups == math.MaxInt {
		return s.fail(fmt.Errorf("Decimal grouped group statistics exceed MaxInt"))
	}
	n := len(s.states)
	if s.output == nil {
		per := int64(1024 + 24*s.pageRows)
		// The second nullable key has its own vector/payload admission.
		// Keep the established single-key envelope exactly unchanged.
		fixed := int64(9216 + 16*s.pageRows*s.numKeys + 1024*(s.numKeys-1))
		if int64(n) > (math.MaxInt64-fixed)/per {
			return s.fail(memory.ErrBudgetExceeded)
		}
		lease, e := s.outputManager.Reserve(ctx, fixed+int64(n)*per)
		if e != nil {
			return s.fail(e)
		}
		s.outputLease = lease
		s.saved = *dst
		s.output = dst
		*dst = memory.Batch{Int64s: make(map[string]*memory.Int64Vector, s.numKeys), Decimals: make(map[string]*memory.Decimal128Vector), Uint64s: make(map[string]*memory.Uint64Vector)}
		for _, f := range s.p.cq.GenericFields {
			dst.Int64s[f.Name] = &memory.Int64Vector{Values: make([]int64, s.pageRows), Validity: memory.EnsureBitmap(nil, s.pageRows)}
		}
		for i, a := range s.p.cq.Aggregates {
			if i%64 == 0 {
				if e := ctx.Err(); e != nil {
					return s.fail(e)
				}
			}
			f := s.p.executionSchema.Fields[i+s.numKeys]
			if decimalValueKind(a.Kind) {
				dst.Decimals[f.Name] = &memory.Decimal128Vector{Precision: f.DecimalPrecision, Scale: f.DecimalScale, Values: make([]memory.Decimal128, s.pageRows), Validity: memory.EnsureBitmap(nil, s.pageRows)}
			} else {
				dst.Uint64s[f.Name] = &memory.Uint64Vector{Values: make([]uint64, s.pageRows)}
			}
		}
	}
	row := dst.Rows
	for i, f := range s.p.cq.GenericFields {
		if s.key.valid[i] {
			dst.Int64s[f.Name].Values[row] = s.key.values[i]
			memory.Set(dst.Int64s[f.Name].Validity, row)
		}
	}
	for i, state := range s.states {
		if i%64 == 0 {
			if e := ctx.Err(); e != nil {
				return s.fail(e)
			}
		}
		f := s.p.executionSchema.Fields[i+s.numKeys]
		if !decimalValueKind(s.p.cq.Aggregates[i].Kind) {
			dst.Uint64s[f.Name].Values[row] = s.counts[i]
			continue
		}
		v, valid, e := s.p.finalizeDecimal(i, state)
		if e != nil {
			return s.fail(e)
		}
		dst.Decimals[f.Name].Values[row] = v
		if valid {
			memory.Set(dst.Decimals[f.Name].Validity, row)
		}
	}
	if e := ctx.Err(); e != nil {
		return s.fail(e)
	}
	dst.Rows++
	clear(s.states)
	clear(s.counts)
	s.have = false
	s.groups++
	return dst.Rows == s.pageRows, nil
}
func (s *decimalGroupedStream) finishPage(ctx context.Context, dst *memory.Batch) (bool, error) {
	i := 0
	check := func() error {
		i++
		if i%64 == 1 {
			return ctx.Err()
		}
		return nil
	}
	for _, v := range dst.Int64s {
		if e := check(); e != nil {
			return s.fail(e)
		}
		v.Values = v.Values[:dst.Rows]
		v.Validity = v.Validity[:memory.WordsFor(dst.Rows)]
	}
	for _, v := range dst.Decimals {
		if e := check(); e != nil {
			return s.fail(e)
		}
		v.Values = v.Values[:dst.Rows]
		v.Validity = v.Validity[:memory.WordsFor(dst.Rows)]
	}
	for _, v := range dst.Uint64s {
		if e := check(); e != nil {
			return s.fail(e)
		}
		v.Values = v.Values[:dst.Rows]
	}
	return true, nil
}
func (s *decimalGroupedStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.releaseOutput()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminal != nil {
		return false, s.terminal
	}
	if ctx == nil || dst == nil {
		return s.fail(fmt.Errorf("Decimal grouped requires context and destination"))
	}
	var e error
	ctx, e = aggregateStreamContext(ctx, s.budget)
	if e != nil {
		return s.fail(e)
	}
	for {
		if e = ctx.Err(); e != nil {
			return s.fail(e)
		}
		if s.eof {
			if s.have {
				if _, e := s.emit(ctx, dst); e != nil {
					return false, e
				}
			}
			if s.output != nil && dst.Rows > 0 {
				return s.finishPage(ctx, dst)
			}
			return false, s.cleanup()
		}
		if s.row >= s.inputBatch.Rows {
			s.truth = nil
			s.inputBatch.ClearColumns()
			s.row = 0
			ok, e := s.input.Next(ctx, &s.inputBatch)
			if e != nil {
				return s.fail(e)
			}
			if !ok {
				s.inputBatch.ClearColumns()
				e = s.input.Close()
				s.input = nil
				if e != nil {
					return s.fail(e)
				}
				s.eof = true
				continue
			}
			if e = s.validateBatch(ctx); e != nil {
				return s.fail(e)
			}
			if s.inputBatch.Rows == 0 {
				continue
			}
		}
		row := s.row
		if !memory.IsSet(s.truth, row) {
			s.row++
			continue
		}
		var vectors [2]*memory.Int64Vector
		for i, f := range s.p.cq.GenericFields {
			vectors[i] = s.inputBatch.Int64s[f.Name]
		}
		key := decimalGroupKeyAt(vectors, s.numKeys, row)
		if s.have && key != s.key {
			full, e := s.emit(ctx, dst)
			if e != nil {
				return false, e
			}
			if full {
				return s.finishPage(ctx, dst)
			}
			continue
		}
		if e = s.addRow(ctx, row); e != nil {
			return s.fail(e)
		}
		if !s.have {
			s.key, s.have = key, true
		}
		s.row++
	}
}
func (s *decimalGroupedStream) consumeRecords(ctx context.Context, sink func(aggregateRecord) error) (AggregateStreamStats, error) {
	if sink != nil {
		return AggregateStreamStats{}, fmt.Errorf("Decimal grouped record materialization unsupported; use BatchStream")
	}
	b := memory.NewBatch()
	for {
		ok, e := s.Next(ctx, b)
		if e != nil {
			return AggregateStreamStats{}, e
		}
		if !ok {
			break
		}
	}
	return AggregateStreamStats{SelectedRows: s.selected, Groups: s.groups}, nil
}

func (s *decimalGroupedStream) fail(e error) (bool, error) {
	s.truth = nil
	return s.decimalGlobalStream.fail(e)
}
func (s *decimalGroupedStream) Close() error { s.truth = nil; return s.decimalGlobalStream.Close() }
