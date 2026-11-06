package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"runtime/debug"
	"sync"
)

// ErrBatchViewExpired reports use of a borrowed BatchView after the iterator
// advanced or closed.
var ErrBatchViewExpired = errors.New("batch view expired")

// BatchOptions overrides result-stream morsel limits. Zero values retain the
// Session pipeline defaults.
type BatchOptions struct {
	TargetBatchBytes int64
	MaxBatchBytes    int64
	MaxInFlightBytes int64
}

func (o BatchOptions) apply(base pipeline.Options) (pipeline.Options, error) {
	if o.TargetBatchBytes < 0 || o.MaxBatchBytes < 0 || o.MaxInFlightBytes < 0 {
		return pipeline.Options{}, fmt.Errorf("batch byte limits must be non-negative")
	}
	if o.TargetBatchBytes > 0 {
		base.TargetMorselBytes = o.TargetBatchBytes
	}
	if o.MaxBatchBytes > 0 {
		base.MaxMorselBytes = o.MaxBatchBytes
	} else if base.MaxMorselBytes < base.TargetMorselBytes {
		base.MaxMorselBytes = base.TargetMorselBytes
	}
	if o.MaxInFlightBytes > 0 {
		base.MaxInFlightBytes = o.MaxInFlightBytes
	} else if base.MaxInFlightBytes < base.MaxMorselBytes {
		base.MaxInFlightBytes = base.MaxMorselBytes
	}
	if err := base.Validate(); err != nil {
		return pipeline.Options{}, err
	}
	return base, nil
}

// BatchView is a borrowed read-only view of the iterator's current columnar
// batch. It remains valid only until the next Next call or Close. Column returns
// the same typed read-only Series accessors used by DataFrame.
type BatchView struct{ state *dataFrameState }

// Schema returns a copy of the current batch schema.
func (v BatchView) Schema() memory.Schema {
	if !v.Valid() {
		return memory.Schema{}
	}
	return DataFrame{state: v.state}.Schema()
}

// Len returns the current batch row count, or zero after expiry.
func (v BatchView) Len() int64 {
	if !v.Valid() {
		return 0
	}
	return DataFrame{state: v.state}.Height()
}

// Columns returns current batch column names in schema order.
func (v BatchView) Columns() []string {
	if !v.Valid() {
		return nil
	}
	return DataFrame{state: v.state}.Columns()
}

// Column returns a borrowed typed column spanning this one batch.
func (v BatchView) Column(name string) (Series, error) {
	if !v.Valid() {
		return Series{}, ErrBatchViewExpired
	}
	return DataFrame{state: v.state}.Column(name)
}

// Valid reports whether the iterator still owns this generation.
func (v BatchView) Valid() bool {
	if v.state == nil {
		return false
	}
	v.state.mu.RLock()
	defer v.state.mu.RUnlock()
	return !v.state.closed
}

func expireBatchView(state *dataFrameState) {
	if state == nil {
		return
	}
	state.mu.Lock()
	state.closed = true
	state.batches = nil
	state.rows = 0
	state.mu.Unlock()
}

// BatchReader is a single-consumer pull iterator. Next invalidates the previous
// BatchView. Close is mandatory when iteration stops early and is idempotent.
type BatchReader struct {
	prepared     *PreparedDataFrame
	ownedPrepare bool
	stream       source.BatchStream
	budget       *memory.QueryBudget
	ownedBudget  bool
	baseContext  context.Context
	schema       memory.Schema
	batch        *memory.Batch
	view         *dataFrameState
	err          error
	closeErr     error
	closed       bool
	// Analyze snapshots only final diagnostics, never a per-batch event log.
	captureDiagnostics bool
	diagnostics        map[string]string
	// holdBudgetUntilClose is used by native sinks whose encoders may allocate
	// while finalizing after the upstream reaches EOF.
	holdBudgetUntilClose bool
	mu                   sync.Mutex
}

// CollectBatches prepares one query and returns its borrowed pull iterator.
// Closing the iterator also closes the one-shot prepared plan.
func (f LazyFrame) CollectBatches(ctx context.Context, opts BatchOptions) (*BatchReader, error) {
	if ctx == nil {
		return nil, fmt.Errorf("collect batches context is nil")
	}
	prepared, err := f.Prepare(ctx)
	if err != nil {
		return nil, err
	}
	reader, err := prepared.ExecuteBatches(ctx, opts)
	if err != nil {
		_ = prepared.Close()
		return nil, err
	}
	reader.ownedPrepare = true
	return reader, nil
}

// ExecuteBatches opens one independent borrowed result stream from a prepared
// DataFrame plan. Multiple streams may run concurrently when the source allows
// concurrent readers. A QueryBudget in ctx is borrowed and remains open after
// stream cleanup; otherwise the reader owns a new execution budget.
func (p *PreparedDataFrame) ExecuteBatches(ctx context.Context, opts BatchOptions) (*BatchReader, error) {
	reader, err, cleanupErr := p.openBatches(ctx, opts)
	return reader, errors.Join(err, cleanupErr)
}

// openBatches is the single execution opener. Analyze retains the two failure
// categories while public iterators return their joined error.
func (p *PreparedDataFrame) openBatches(ctx context.Context, opts BatchOptions) (*BatchReader, error, error) {
	if p == nil {
		return nil, fmt.Errorf("nil PreparedDataFrame"), nil
	}
	if ctx == nil {
		return nil, fmt.Errorf("prepared batch context is nil"), nil
	}
	pipelineOptions, err := opts.apply(p.pipeline)
	if err != nil {
		return nil, err, nil
	}
	p.lifeMu.Lock()
	if p.closed {
		p.lifeMu.Unlock()
		return nil, ErrPreparedFrameClosed, nil
	}
	p.active.Add(1)
	p.lifeMu.Unlock()
	active := true
	defer func() {
		if active {
			p.active.Done()
		}
	}()
	queryBudget, ownedBudget, err := p.acquireQueryMemory(ctx)
	if err != nil {
		return nil, err, nil
	}
	openContext := memory.WithQueryBudget(ctx, queryBudget)
	exchangeManager := memory.ClassifiedManager(p.manager, "Exchange", memory.MemoryInFlightBatch)
	stream, err := pipeline.OpenBatchStream(openContext, p.src, source.ReadOptions{Kernels: p.kernels}, p.parts, exchangeManager, pipelineOptions, p.observer)
	if err != nil {
		var cleanupErr error
		if ownedBudget {
			cleanupErr = queryBudget.Close()
		}
		return nil, err, cleanupErr
	}
	active = false
	return &BatchReader{prepared: p, stream: stream, budget: queryBudget, ownedBudget: ownedBudget, baseContext: ctx, schema: p.schema, batch: memory.NewBatch()}, nil, nil
}

// Schema returns a copy of the result schema, including after exhaustion.
func (r *BatchReader) Schema() memory.Schema {
	if r == nil {
		return memory.Schema{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return memory.NewSchema(r.schema.Fields...)
}

// Next advances to the next borrowed batch. It returns false on exhaustion or
// error; call Err to distinguish them. A nil context is an error.
func (r *BatchReader) Next(ctx context.Context) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.err != nil {
		return false
	}
	if ctx == nil {
		r.err = fmt.Errorf("batch reader context is nil")
		r.finishLocked()
		return false
	}
	if err := ctx.Err(); err != nil {
		r.err = err
		r.finishLocked()
		return false
	}
	if err := r.baseContext.Err(); err != nil {
		r.err = err
		r.finishLocked()
		return false
	}
	expireBatchView(r.view)
	r.view = nil
	nextContext, cancel := context.WithCancelCause(ctx)
	stopBase := context.AfterFunc(r.baseContext, func() { cancel(context.Cause(r.baseContext)) })
	nextContext = memory.WithQueryBudget(nextContext, r.budget)
	ok, err := r.stream.Next(nextContext, r.batch)
	stopBase()
	cancel(nil)
	if err != nil {
		r.err = err
		r.finishLocked()
		return false
	}
	if !ok {
		r.finishLockedWithBudget(!r.holdBudgetUntilClose)
		return false
	}
	r.view = &dataFrameState{schema: r.schema, batches: []*memory.Batch{r.batch}, rows: int64(r.batch.Rows)}
	return true
}

// Batch returns the current borrowed view. Before the first successful Next,
// after exhaustion, or after Close it returns an invalid view.
func (r *BatchReader) Batch() BatchView {
	r.mu.Lock()
	defer r.mu.Unlock()
	return BatchView{state: r.view}
}

// Err returns the terminal execution or cleanup error, if any.
func (r *BatchReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return errors.Join(r.err, r.closeErr)
}

// Close releases the physical stream, any execution-owned query budget, and
// the one-shot prepared plan. Caller-supplied query budgets remain open.
func (r *BatchReader) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.finishLocked()
	err := errors.Join(r.err, r.closeErr)
	r.mu.Unlock()
	return err
}

func (r *BatchReader) finishLocked() {
	r.finishLockedWithBudget(true)
}

func (r *BatchReader) finishLockedWithBudget(closeBudget bool) {
	r.closed = true
	expireBatchView(r.view)
	r.view = nil
	if r.batch != nil {
		r.batch.ReleaseOwned()
	}
	if r.stream != nil {
		r.snapshotDiagnosticsLocked()
		r.closeErr = errors.Join(r.closeErr, r.stream.Close())
		r.snapshotDiagnosticsLocked()
		r.stream = nil
	}
	if closeBudget && r.budget != nil {
		if r.ownedBudget {
			r.closeErr = errors.Join(r.closeErr, r.budget.Close())
		}
		r.budget = nil
	}
	if r.prepared != nil {
		r.prepared.active.Done()
		if r.ownedPrepare {
			r.closeErr = errors.Join(r.closeErr, r.prepared.Close())
		}
		r.prepared = nil
	}
}

// snapshotDiagnosticsLocked merges final snapshots so diagnostics finalized by
// Close replace earlier values without losing values cleared during cleanup.
func (r *BatchReader) snapshotDiagnosticsLocked() {
	if !r.captureDiagnostics {
		return
	}
	provider, ok := r.stream.(source.BatchStreamDiagnostics)
	if !ok {
		return
	}
	for key, value := range provider.StreamDiagnostics() {
		if r.diagnostics == nil {
			r.diagnostics = make(map[string]string)
		}
		r.diagnostics[key] = value
	}
}

// BatchConsumer consumes one borrowed BatchView. The callback must return
// before retaining any data; copy explicitly when values must escape.
type BatchConsumer func(context.Context, BatchView) error

// SinkBatches executes one or more consumers concurrently for each batch.
// The upstream advances only after every consumer acknowledges that batch, so
// each downstream has one in-flight view and a slow consumer cannot create an
// unbounded shared queue.
func (f LazyFrame) SinkBatches(ctx context.Context, opts BatchOptions, consumers ...BatchConsumer) error {
	if ctx == nil {
		return fmt.Errorf("consume batches context is nil")
	}
	if len(consumers) == 0 {
		return fmt.Errorf("SinkBatches requires at least one consumer")
	}
	for index, consumer := range consumers {
		if consumer == nil {
			return fmt.Errorf("batch consumer %d is nil", index)
		}
	}
	reader, err := f.CollectBatches(ctx, opts)
	if err != nil {
		return err
	}
	for reader.Next(ctx) {
		view := reader.Batch()
		batchContext, cancel := context.WithCancel(ctx)
		results := make(chan error, len(consumers))
		for index, consumer := range consumers {
			go func(index int, consume BatchConsumer) {
				defer func() {
					if recovered := recover(); recovered != nil {
						results <- fmt.Errorf("batch consumer %d panicked: %v\n%s", index, recovered, debug.Stack())
					}
				}()
				results <- consume(batchContext, view)
			}(index, consumer)
		}
		var batchErr error
		for range consumers {
			consumerErr := <-results
			if consumerErr != nil {
				batchErr = errors.Join(batchErr, consumerErr)
				cancel()
			}
		}
		cancel()
		if batchErr != nil {
			return errors.Join(batchErr, reader.Close())
		}
	}
	return errors.Join(reader.Err(), reader.Close())
}
