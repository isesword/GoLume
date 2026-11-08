package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
)

// aggregateRecordConsumer is the terminal sink of a native aggregate operator.
// It shares aggregation, state ownership and cancellation with BatchStream but
// avoids allocating column vectors when the caller requests records or counts.
type aggregateRecordConsumer interface {
	consumeRecords(context.Context, func(aggregateRecord) error) (AggregateStreamStats, error)
}

func clearAggregateOutput(dst *memory.Batch) {
	if dst != nil {
		dst.ClearColumns()
	}
}

func (s *residentAggregateStream) consumeRecords(ctx context.Context, sink func(aggregateRecord) error) (AggregateStreamStats, error) {
	if err := s.consume(ctx); err != nil {
		return AggregateStreamStats{}, s.abort(err)
	}
	if sink != nil {
		if err := s.visit(func(record aggregateRecord) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return sink(record)
		}); err != nil {
			return AggregateStreamStats{}, s.abort(err)
		}
	}
	s.exhausted = true
	s.recordStats()
	s.finishActive()
	return s.Stats(), nil
}

func (s *sortedAggregateStream) consumeRecords(ctx context.Context, sink func(aggregateRecord) error) (AggregateStreamStats, error) {
	s.recordSink = sink
	defer func() { s.recordSink = nil }()
	for {
		ok, err := s.next(ctx, nil)
		if err != nil {
			return AggregateStreamStats{}, err
		}
		if !ok {
			return s.Stats(), nil
		}
	}
}

func (s *spillableAggregateStream) consumeRecords(ctx context.Context, sink func(aggregateRecord) error) (AggregateStreamStats, error) {
	if err := s.prepareInput(ctx); err != nil {
		return AggregateStreamStats{}, s.abort(err)
	}
	// Keep the table pinned while a sink borrows state or key storage. A sink
	// may reserve memory (and trigger the spill arbiter), so do not hold mu or
	// advertise this borrowed table as spillable during the visit.
	s.mu.Lock()
	owner := s.table
	s.table = nil
	s.mu.Unlock()
	if owner != nil {
		cursor, emitted := s.outputCursor, s.outputEmitted
		var err error
		if sink != nil {
			for emitted < s.groupCount {
				next := cursor
				_, err = owner.table.visitChunk(&next, 1, func(record aggregateRecord) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					return sink(record)
				})
				if err != nil {
					break
				}
				cursor, emitted = next, emitted+1
			}
		}
		if errors.Is(err, memory.ErrBudgetExceeded) {
			// Internal collectors admit a record before appending it. A failed
			// record therefore remains in the suffix and can be retried after
			// releasing the table, without repeating accepted records.
			s.mu.Lock()
			s.table = owner
			s.outputCursor, s.outputEmitted = cursor, emitted
			s.mu.Unlock()
			if _, spillErr := s.state.RequestHardSpill(ctx, owner.retainedBytes()); spillErr != nil {
				return AggregateStreamStats{}, s.abort(errors.Join(err, spillErr))
			}
			s.mu.Lock()
			pending := s.table != nil
			s.mu.Unlock()
			if pending {
				return AggregateStreamStats{}, s.abort(err)
			}
		} else {
			owner.close()
			if err != nil {
				return AggregateStreamStats{}, s.abort(err)
			}
		}
	}

	for len(s.outputRuns) > 0 {
		if err := ctx.Err(); err != nil {
			return AggregateStreamStats{}, s.abort(err)
		}
		ref := s.outputRuns[0]
		if sink != nil {
			if err := s.restoreRun(ctx, ref, func(record aggregateRecord) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return sink(record)
			}); err != nil {
				return AggregateStreamStats{}, s.abort(err)
			}
		}
		if err := s.store.Delete(ctx, ref.handle); err != nil {
			return AggregateStreamStats{}, s.abort(err)
		}
		s.outputRuns = s.outputRuns[1:]
	}
	s.exhausted = true
	s.recordStats()
	s.finishActive()
	return s.Stats(), nil
}

func (s *spillableAggregateStream) prepareInput(ctx context.Context) error {
	if s.prepared {
		return nil
	}
	if err := s.consumeInput(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	hasRuns := false
	for _, refs := range s.partitions {
		hasRuns = hasRuns || len(refs.Values) != 0
	}
	if !hasRuns {
		// No partial state reached disk. Keep this admitted table authoritative
		// and emit bounded output directly; merely enabling spill needs no I/O.
		s.residentOutput = true
		if s.table != nil {
			s.groupCount = s.table.table.len()
		}
	}
	retained := int64(0)
	if hasRuns && s.table != nil {
		retained = s.table.retainedBytes()
	}
	s.mu.Unlock()
	if retained > 0 {
		if _, err := s.state.RequestHardSpill(ctx, retained); err != nil {
			return err
		}
		s.mu.Lock()
		pending := s.table != nil && s.table.table.len() != 0
		s.mu.Unlock()
		if pending {
			return fmt.Errorf("aggregate final state could not be spilled")
		}
	}
	if hasRuns {
		if err := s.finishPartitions(ctx); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.prepared = true
	for i := range s.partitions {
		s.partitions[i].Release()
	}
	s.partitions = nil
	s.mu.Unlock()
	return nil
}
