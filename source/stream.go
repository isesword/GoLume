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

// BatchStreamProvider is the native execution contract required by Source.
type BatchStreamProvider interface {
	NewBatchStream(ctx context.Context, opts ReadOptions) (BatchStream, error)
}

// PreparedBatchStreamProvider lets a physical wrapper propagate the immutable
// partition list already resolved during Prepare through a native stream chain.
// It prevents re-listing or copying one entry per morsel on every execution.
// Implementations must use exactly this sequence, including duplicates and
// an empty/nil sequence, and must not modify the borrowed immutable slice.
type PreparedBatchStreamProvider interface {
	NewBatchStreamWithPartitions(ctx context.Context, opts ReadOptions, parts []Partition) (BatchStream, error)
}

// OpenBatchStream validates execution options and opens the source's native stream.
func OpenBatchStream(ctx context.Context, src Source, opts ReadOptions) (BatchStream, error) {
	return openBatchStream(ctx, src, opts, nil, false)
}

// OpenBatchStreamWithPartitions opens exactly the supplied partition sequence.
// Sources without prepared partition support return an error rather than
// silently ignoring the caller's selected sequence.
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
	if opts.MemoryManager == nil {
		if query := memory.QueryBudgetFromContext(ctx); query != nil {
			opts.MemoryManager = query.Operator("SourceScan", memory.MemoryWorkspace)
		}
	}
	var stream BatchStream
	var err error
	if havePreparedParts {
		provider, ok := src.(PreparedBatchStreamProvider)
		if !ok {
			return nil, fmt.Errorf("source %T does not support prepared partition streams", src)
		}
		stream, err = provider.NewBatchStreamWithPartitions(ctx, opts, preparedParts)
	} else {
		stream, err = src.NewBatchStream(ctx, opts)
	}
	if err != nil {
		return nil, err
	}
	if stream == nil {
		return nil, fmt.Errorf("source %T returned a nil batch stream", src)
	}
	return stream, nil
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
