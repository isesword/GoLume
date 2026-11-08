package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
)

// LimitSource stops producing rows after a global row limit while preserving
// input order. The limit is reader-local, so independent PreparedDataFrame Execute
// calls do not share counters. Output payloads are borrowed until Next or Close.
// Partial prefixes reserve any new range metadata before allocation.
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

// NewBatchStream creates a native early-stopping limit stream.
func (s *LimitSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts)
}

func (s *LimitSource) newBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("limit context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	manager := opts.MemoryManager
	query := memory.QueryBudgetFromContext(ctx)
	if query != nil {
		opts.MemoryManager = query.Operator("SourceScan", memory.MemoryWorkspace)
		manager = query.Operator("Limit", memory.MemoryWorkspace)
	}
	child, err := source.OpenBatchStream(ctx, s.input, opts)
	if err != nil {
		return nil, err
	}
	stream := &limitBatchStream{remaining: s.n, input: memory.NewBatch(), manager: manager, query: query}
	stream.state.child = child
	stream.state.closeOwn = func() error {
		stream.clearOutput()
		// Most children own borrowed bindings and clear their shared Owned flags
		// on Close. Legacy materializers instead transfer owned arrays into our
		// input batch. Let the child release its bindings first, then release
		// any ownership left in input; neither case may leak or double release.
		var closeErr error
		if stream.state.child != nil {
			closeErr = stream.state.child.Close()
			stream.state.child = nil
		}
		stream.input.ReleaseOwned()
		stream.input = nil
		stream.manager = nil
		stream.query = nil
		return closeErr
	}
	return stream, nil
}

type limitBatchStream struct {
	state                 unaryStreamState
	remaining             int64
	input, view, borrowed *memory.Batch
	manager               memory.Manager
	query                 *memory.QueryBudget
	viewLease             memory.Reservation
}

// Next returns at most the remaining prefix and stops upstream once satisfied.
func (s *limitBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.clearOutput()
	if stop, err := s.state.before(ctx, dst); stop {
		if err != nil && !s.state.closed && s.state.terminalErr == nil {
			err = s.state.fail(err)
		}
		return false, err
	}
	dst.ClearColumns()
	if query := memory.QueryBudgetFromContext(ctx); query != nil && query != s.query {
		return false, s.state.fail(fmt.Errorf("limit stream cannot change query ownership"))
	} else if query == nil && s.query != nil {
		ctx = memory.WithQueryBudget(ctx, s.query)
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
		if err := ctx.Err(); err != nil {
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
			borrowFilterOwners(dst)
			dst.Reset(s.input.Rows)
			s.borrowed = dst
			s.remaining -= int64(s.input.Rows)
			return true, nil
		}
		n := int(s.remaining)
		if err := s.borrowPrefix(ctx, n); err != nil {
			dst.ClearColumns()
			return false, s.state.fail(err)
		}
		bindAll(dst, s.view)
		borrowFilterOwners(dst)
		dst.Reset(n)
		s.borrowed = dst
		s.remaining = 0
		return true, nil
	}
}

// borrowPrefix keeps payloads charged to the live child. Only rebased offsets
// and misaligned nested validity buffers are newly allocated and charged here.
func (s *limitBatchStream) borrowPrefix(ctx context.Context, rows int) error {
	bytes, err := memory.BorrowedRangeMetadataBytes(s.input, 0, rows)
	if err != nil {
		return err
	}
	if bytes > 0 && s.manager != nil {
		s.viewLease, err = s.manager.Reserve(ctx, bytes)
		if err != nil {
			return fmt.Errorf("reserve limit range metadata: %w", err)
		}
	}
	s.view, err = memory.BorrowBatchRange(s.input, 0, rows)
	return err
}

func (s *limitBatchStream) clearOutput() {
	if s.borrowed != nil {
		s.borrowed.ClearColumns()
		s.borrowed = nil
	}
	if s.view != nil {
		s.view.ClearColumns()
		s.view = nil
	}
	if s.viewLease != nil {
		s.viewLease.Release()
		s.viewLease = nil
	}
}

// Close releases limit workspace and the child stream.
func (s *limitBatchStream) Close() error { return s.state.Close() }

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
var _ source.BatchStream = (*limitBatchStream)(nil)
