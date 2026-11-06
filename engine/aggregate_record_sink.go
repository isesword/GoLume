package engine

import (
	"context"
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
	if err := s.finishPartitions(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.prepared = true
	s.partitions = nil
	s.mu.Unlock()
	s.recordStats()
	return nil
}
