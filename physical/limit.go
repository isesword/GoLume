package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
)

// LimitSource stops producing rows after a global row limit while preserving
// input order. The limit is reader-local, so independent PreparedFrame Execute
// calls do not share counters.
type LimitSource struct {
	input source.Source
	n     int64
}

// NewLimitSource wraps input with a non-negative global row limit.
func NewLimitSource(input source.Source, n int64) (source.Source, error) {
	if input == nil {
		return nil, fmt.Errorf("limit input is nil")
	}
	if n < 0 {
		return nil, fmt.Errorf("limit must be non-negative")
	}
	return &LimitSource{input: input, n: n}, nil
}

// Schema preserves the input schema.
func (s *LimitSource) Schema() memory.Schema { return s.input.Schema() }

// Replayability forwards the child snapshot contract.
func (s *LimitSource) Replayability() source.Replayability { return source.ReplayabilityOf(s.input) }

// StreamPlan reports an order-preserving pipeline node with bounded state.
func (s *LimitSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{
		Operator:          "Limit",
		Class:             source.StreamPipeline,
		Detail:            fmt.Sprintf("n=%d", s.n),
		PartitionParallel: false,
		Children:          []source.StreamPlanNode{source.StreamPlanOf(s.input)},
	}
}

// Stats returns an upper bound capped by the configured limit.
func (s *LimitSource) Stats(ctx context.Context) (source.Stats, error) {
	st, err := s.input.Stats(ctx)
	if err != nil {
		return source.Stats{}, err
	}
	if st.Rows < 0 || st.Rows > s.n {
		st.Rows = s.n
	}
	st.RowsUpperBound = true
	return st, nil
}

// Partitions preserves child partitions because filters below the limit can
// make partition row counts upper bounds rather than exact counts.
func (s *LimitSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

// NewReader creates an independent reader with its own remaining-row counter.
func (s *LimitSource) NewReader(ctx context.Context, opts source.ReadOptions) (source.Reader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	r, err := s.input.NewReader(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &limitReader{child: r, remaining: s.n, input: memory.NewBatch(), rows: make([]int, 0, 1024)}, nil
}

// NewBatchStream creates a native early-stopping limit stream.
func (s *LimitSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts, nil, false)
}

// NewBatchStreamWithPartitions propagates prepared partition metadata.
func (s *LimitSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts, parts, true)
}

func (s *LimitSource) newBatchStream(ctx context.Context, opts source.ReadOptions, parts []source.Partition, prepared bool) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	child, err := openUnaryInputStream(ctx, s.input, opts, parts, prepared)
	if err != nil {
		return nil, err
	}
	stream := &limitBatchStream{remaining: s.n, input: memory.NewBatch(), rows: make([]int, 0, 1024)}
	stream.state.child = child
	stream.state.closeOwn = func() error {
		stream.input.ReleaseOwned()
		return nil
	}
	return stream, nil
}

type limitReader struct {
	child     source.Reader
	remaining int64
	input     *memory.Batch
	rows      []int
}

type limitBatchStream struct {
	state     unaryStreamState
	remaining int64
	input     *memory.Batch
	rows      []int
}

// Next returns at most the remaining prefix and stops upstream once satisfied.
func (s *limitBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if stop, err := s.state.before(ctx, dst); stop {
		return false, err
	}
	if s.remaining <= 0 {
		dst.ClearColumns()
		return false, s.state.finish()
	}
	for {
		ok, err := s.state.child.Next(ctx, s.input)
		if err != nil {
			dst.ClearColumns()
			return false, s.state.fail(err)
		}
		if !ok {
			dst.ClearColumns()
			return false, s.state.finish()
		}
		if s.input.Rows == 0 {
			continue
		}
		if int64(s.input.Rows) <= s.remaining {
			dst.ClearColumns()
			bindAll(dst, s.input)
			dst.Reset(s.input.Rows)
			s.remaining -= int64(s.input.Rows)
			return true, nil
		}
		n := int(s.remaining)
		s.rows = s.rows[:0]
		for i := 0; i < n; i++ {
			s.rows = append(s.rows, i)
		}
		if err := takeRows(ctx, dst, s.input, s.rows); err != nil {
			dst.ClearColumns()
			return false, s.state.fail(err)
		}
		s.remaining = 0
		return true, nil
	}
}

// Close releases limit workspace and the child stream.
func (s *limitBatchStream) Close() error { return s.state.Close() }

// Close releases child-reader resources.
func (r *limitReader) Close() error { return r.child.Close() }

// ReadPartition forwards at most the remaining prefix rows from one partition.
func (r *limitReader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	if r.remaining <= 0 {
		dst.ClearColumns()
		dst.Reset(0)
		return nil
	}
	if err := r.child.ReadPartition(ctx, p, r.input); err != nil {
		return err
	}
	if int64(r.input.Rows) <= r.remaining {
		dst.ClearColumns()
		bindAll(dst, r.input)
		dst.Reset(r.input.Rows)
		r.remaining -= int64(r.input.Rows)
		return nil
	}
	n := int(r.remaining)
	r.rows = r.rows[:0]
	for i := 0; i < n; i++ {
		r.rows = append(r.rows, i)
	}
	if err := takeRows(ctx, dst, r.input, r.rows); err != nil {
		return err
	}
	r.remaining = 0
	return nil
}

// Dictionary forwards stable dictionary metadata from the input.
func (s *LimitSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
	dp, ok := s.input.(source.DictionaryProvider)
	if !ok {
		return source.Dictionary{}, fmt.Errorf("limit input has no dictionary provider")
	}
	return dp.Dictionary(ctx, column)
}

var _ source.StreamPlanProvider = (*LimitSource)(nil)
var _ source.ReplayabilityProvider = (*LimitSource)(nil)
var _ source.BatchStreamProvider = (*LimitSource)(nil)
var _ source.PreparedBatchStreamProvider = (*LimitSource)(nil)
var _ source.BatchStream = (*limitBatchStream)(nil)
