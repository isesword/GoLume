package source

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
)

// ErrBatchStreamClosed is returned when Next is called after an explicit
// Close. Reaching the natural end of a stream continues to return ok=false and
// a nil error.
var ErrBatchStreamClosed = errors.New("batch stream closed")

// BatchStream incrementally fills a caller-owned reusable Batch. A successful
// Next transfers borrowed vector bindings into dst until the caller invokes
// Next again, clears dst, or closes the owning query. BatchStream values are
// single-consumer and are not safe for concurrent Next calls.
type BatchStream interface {
	Next(ctx context.Context, dst *memory.Batch) (ok bool, err error)
	Close() error
}

// BatchStreamDiagnostics is an optional, cold-path interface used by
// ExplainAnalyze wrappers after execution. Implementations must return a copy
// and must not make normal Next calls depend on tracing being enabled.
type BatchStreamDiagnostics interface {
	StreamDiagnostics() map[string]string
}

// BatchStreamProvider lets a source provide a native stream. Sources that only
// implement the partitioned Source contract are adapted by OpenBatchStream.
type BatchStreamProvider interface {
	NewBatchStream(ctx context.Context, opts ReadOptions) (BatchStream, error)
}

// PreparedBatchStreamProvider lets a physical wrapper propagate the immutable
// partition list already resolved during Prepare through a native stream chain.
// It prevents re-listing or copying one entry per morsel on every execution.
type PreparedBatchStreamProvider interface {
	NewBatchStreamWithPartitions(ctx context.Context, opts ReadOptions, parts []Partition) (BatchStream, error)
}

// OpenBatchStream opens a native stream when available, otherwise it adapts the
// existing Source/Reader partition contract. The adapter deliberately lives at
// the source boundary so physical consumers do not need a second eager path.
func OpenBatchStream(ctx context.Context, src Source, opts ReadOptions) (BatchStream, error) {
	return openBatchStream(ctx, src, opts, nil, false)
}

// OpenBatchStreamWithPartitions is the prepared-plan variant of
// OpenBatchStream. Legacy Source adapters borrow the immutable partition slice
// already resolved during Prepare instead of listing and copying it again.
// Native BatchStreamProvider implementations ignore parts.
func OpenBatchStreamWithPartitions(ctx context.Context, src Source, opts ReadOptions, parts []Partition) (BatchStream, error) {
	return openBatchStream(ctx, src, opts, parts, true)
}

func openBatchStream(ctx context.Context, src Source, opts ReadOptions, preparedParts []Partition, havePreparedParts bool) (BatchStream, error) {
	if src == nil {
		return nil, fmt.Errorf("batch stream source is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("batch stream context is nil")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if havePreparedParts {
		if provider, ok := src.(PreparedBatchStreamProvider); ok {
			stream, err := provider.NewBatchStreamWithPartitions(ctx, opts, preparedParts)
			if err != nil {
				return nil, err
			}
			if stream == nil {
				return nil, fmt.Errorf("source %T returned a nil prepared batch stream", src)
			}
			return stream, nil
		}
	}
	if provider, ok := src.(BatchStreamProvider); ok {
		stream, err := provider.NewBatchStream(ctx, opts)
		if err != nil {
			return nil, err
		}
		if stream == nil {
			return nil, fmt.Errorf("source %T returned a nil batch stream", src)
		}
		return stream, nil
	}
	parts := preparedParts
	if !havePreparedParts {
		var err error
		parts, err = src.Partitions(ctx)
		if err != nil {
			return nil, err
		}
	}
	reader, err := src.NewReader(ctx, opts)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, fmt.Errorf("source %T returned a nil reader", src)
	}
	return &partitionBatchStream{reader: reader, parts: parts}, nil
}

type partitionBatchStream struct {
	reader      Reader
	parts       []Partition
	next        int
	exhausted   bool
	closed      bool
	terminalErr error
	closeErr    error
}

// Next adapts the next legacy partition into one borrowed output Batch.
func (s *partitionBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("batch stream destination is nil")
	}
	if ctx == nil {
		dst.ClearColumns()
		return false, fmt.Errorf("batch stream context is nil")
	}
	if s.closed {
		dst.ClearColumns()
		return false, ErrBatchStreamClosed
	}
	if s.terminalErr != nil {
		dst.ClearColumns()
		return false, s.terminalErr
	}
	if s.exhausted {
		dst.ClearColumns()
		return false, nil
	}
	for s.next < len(s.parts) {
		if err := ctx.Err(); err != nil {
			dst.ClearColumns()
			return false, s.fail(err)
		}
		part := s.parts[s.next]
		s.next++
		if err := s.reader.ReadPartition(ctx, part, dst); err != nil {
			dst.ClearColumns()
			return false, s.fail(err)
		}
		// The legacy partition adapter preserves one output per partition,
		// including a schema-bearing zero-row Batch. Native stream operators may
		// instead consume an input without returning from Next, which is how the
		// protocol represents zero outputs without losing empty-result schemas.
		return true, nil
	}
	s.exhausted = true
	dst.ClearColumns()
	if err := s.closeReader(); err != nil {
		s.terminalErr = err
		return false, err
	}
	return false, nil
}

func (s *partitionBatchStream) fail(err error) error {
	if closeErr := s.closeReader(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	s.terminalErr = err
	return err
}

func (s *partitionBatchStream) closeReader() error {
	if s.reader == nil {
		return s.closeErr
	}
	s.closeErr = s.reader.Close()
	s.reader = nil
	return s.closeErr
}

// Close releases the adapted Reader exactly once.
func (s *partitionBatchStream) Close() error {
	if s == nil || s.closed {
		return s.closeErr
	}
	s.closed = true
	return s.closeReader()
}

// DrainStats summarizes an internal stream sink without retaining output.
type DrainStats struct {
	Batches int64
	Rows    int64
	Bytes   int64
}

// DrainBatchStream consumes and releases every output Batch. It is the minimal
// bounded sink used by execution tests and benchmarks before public sinks are
// introduced. DrainBatchStream always closes stream before returning.
func DrainBatchStream(ctx context.Context, stream BatchStream) (stats DrainStats, err error) {
	if stream == nil {
		return DrainStats{}, fmt.Errorf("cannot drain a nil batch stream")
	}
	batch := memory.NewBatch()
	defer batch.ReleaseOwned()
	defer func() {
		if closeErr := stream.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	for {
		ok, nextErr := stream.Next(ctx, batch)
		if nextErr != nil {
			return DrainStats{}, nextErr
		}
		if !ok {
			return stats, nil
		}
		stats.Batches++
		stats.Rows += int64(batch.Rows)
		stats.Bytes += memory.BatchBytes(batch)
		batch.ClearColumns()
	}
}
