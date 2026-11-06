package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/engine"
	"gocolumn/kernel"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/optimizer"
	"gocolumn/physical"
	"gocolumn/pipeline"
	"gocolumn/source"
	"io"
	"sync"
)

// ErrPreparedFrameClosed is returned when a prepared row pipeline is executed
// after Close.
var ErrPreparedFrameClosed = errors.New("prepared frame closed")

// ErrDataFrameClosed is returned when a borrowed view is used after its owning
// DataFrame has been closed.
var ErrDataFrameClosed = errors.New("data frame closed")

type dataFrameState struct {
	mu           sync.RWMutex
	schema       memory.Schema
	batches      []*memory.Batch
	rows         int64
	reservations []memory.Reservation
	closed       bool
}

// DataFrame is an owned materialized columnar result. Its private shared state
// makes value copies safe and allows borrowed FrameView and Series values to
// observe Close reliably.
type DataFrame struct{ state *dataFrameState }

func newDataFrame(schema memory.Schema) DataFrame {
	return DataFrame{state: &dataFrameState{schema: schema}}
}

// Close releases owned source-native arrays retained by materialized batches.
// It is safe to call Close more than once. Specialized Go vectors require no
// explicit release.
func (f *DataFrame) Close() error {
	if f == nil || f.state == nil {
		return nil
	}
	s := f.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, batch := range s.batches {
		if batch != nil {
			batch.ReleaseOwned()
		}
	}
	for i := len(s.reservations) - 1; i >= 0; i-- {
		s.reservations[i].Release()
	}
	s.reservations = nil
	s.batches = nil
	s.rows = 0
	return nil
}

func (f DataFrame) batches() []*memory.Batch {
	if f.state == nil {
		return nil
	}
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	if f.state.closed {
		return nil
	}
	return f.state.batches
}

// PreparedDataFrame is a reusable row-producing physical pipeline. Join build
// state, scalar programs, pushdown decisions, and source wrappers are prepared
// once; each Execute opens independent streams and owns its result batches.
type PreparedDataFrame struct {
	engine          *engine.Engine
	src             source.Source
	parts           []source.Partition
	schema          memory.Schema
	kernels         kernel.Set
	manager         memory.Manager
	pipeline        pipeline.Options
	observer        observer.Observer
	planInfo        engine.PlanInfo
	executionPolicy engine.ExecutionPolicy
	closers         []io.Closer
	lifeMu          sync.Mutex
	active          sync.WaitGroup
	closed          bool
	closeDone       chan struct{}
	closeErr        error
}

// Prepare optimizes and lowers any row-producing plan, including a terminal
// Aggregate, into a reusable DataFrame pipeline.
func (f LazyFrame) Prepare(ctx context.Context) (*PreparedDataFrame, error) {
	prepared, preparation, err := f.prepareFrame(ctx)
	return prepared, errors.Join(err, preparation.cleanupErr)
}

// Collect executes the lazy plan once and returns an owned DataFrame.
func (f LazyFrame) Collect(ctx context.Context) (DataFrame, error) {
	p, err := f.Prepare(ctx)
	if err != nil {
		return DataFrame{}, err
	}
	defer p.Close()
	return p.Execute(ctx)
}

// framePreparation preserves completed planning stages even when a later stage
// fails. The caller receives the primary failure separately from cleanup errors.
type framePreparation struct {
	optimized  optimizer.Result
	physical   logical.Plan
	pushdown   source.PushdownReport
	cleanupErr error
}

func (f LazyFrame) prepareFrame(ctx context.Context) (prepared *PreparedDataFrame, preparation framePreparation, resultErr error) {
	if f.err != nil {
		return nil, preparation, f.err
	}
	if ctx == nil {
		return nil, preparation, fmt.Errorf("prepare context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, preparation, err
	}
	if f.session == nil || f.session.engine == nil {
		return nil, preparation, fmt.Errorf("lazy frame has no execution session")
	}
	policy, err := f.effectiveExecutionPolicy()
	if err != nil {
		return nil, preparation, err
	}
	ctx = engine.WithExecutionPolicy(ctx, policy)
	preparation.optimized, err = optimizer.Optimize(f.plan)
	if err != nil {
		return nil, preparation, err
	}
	ctx = withJoinFeedbackKey(ctx, planFeedbackKey(preparation.optimized.Plan))
	preparation.physical, preparation.pushdown, err = applyPushdown(ctx, preparation.optimized.Plan)
	if err != nil {
		return nil, preparation, err
	}
	if err := f.validateExecutionLogical(preparation.physical); err != nil {
		return nil, preparation, err
	}
	src, pending, closers, err := lowerSourcePipeline(ctx, preparation.physical, f.session.engine.Memory(), f.session.engine.KernelSet(), f.session.engine)
	defer func() {
		if prepared == nil {
			for i := len(closers) - 1; i >= 0; i-- {
				preparation.cleanupErr = errors.Join(preparation.cleanupErr, closers[i].Close())
			}
		}
	}()
	if err != nil {
		return nil, preparation, err
	}
	if pending != nil {
		childID := profileSourceID(src)
		src, err = physical.NewFilterSource(src, pending)
		if err != nil {
			return nil, preparation, err
		}
		src = profileSource(ctx, src, "filter", "terminal residual filter", childID)
	}
	if err := f.validateExecutionSource(src); err != nil {
		return nil, preparation, err
	}
	parts, err := src.Partitions(ctx)
	if err != nil {
		return nil, preparation, err
	}
	pipelineOpts := f.session.engine.PipelineOptions()
	if err := pipelineOpts.Validate(); err != nil {
		return nil, preparation, err
	}
	return &PreparedDataFrame{engine: f.session.engine, src: src, parts: parts, schema: src.Schema(), kernels: f.session.engine.KernelSet(), manager: f.session.engine.Memory(), pipeline: pipelineOpts, observer: f.session.engine.Observer(), planInfo: preparedPlanInfo(src), executionPolicy: policy, closers: closers}, preparation, nil
}

// acquireQueryMemory shares ownership and policy checks across owned results,
// borrowed iterators, Analyze, and file sinks.
func (p *PreparedDataFrame) acquireQueryMemory(ctx context.Context) (*memory.QueryBudget, bool, error) {
	budget, owned, err := p.engine.AcquireQueryMemory(ctx)
	if err != nil {
		return nil, false, err
	}
	if p.executionPolicy == engine.ExecutionBounded && budget.Limit() <= 0 {
		if owned {
			return nil, false, errors.Join(ErrBoundedMemoryLimitRequired, budget.Close())
		}
		return nil, false, ErrBoundedMemoryLimitRequired
	}
	return budget, owned, nil
}

// Schema returns the prepared row-pipeline output schema.
func (p *PreparedDataFrame) Schema() memory.Schema {
	if p == nil {
		return memory.Schema{}
	}
	return p.schema
}

// Execute consumes the prepared stream graph and returns owned columnar batches.
// Multiple Execute calls may run concurrently when the underlying sources support
// independent concurrent streams. A QueryBudget supplied in ctx is borrowed;
// only an internally created budget is closed by execution.
func (p *PreparedDataFrame) Execute(ctx context.Context) (frame DataFrame, resultErr error) {
	if p == nil {
		return DataFrame{}, fmt.Errorf("nil PreparedDataFrame")
	}
	p.lifeMu.Lock()
	if p.closed {
		p.lifeMu.Unlock()
		return DataFrame{}, ErrPreparedFrameClosed
	}
	p.active.Add(1)
	p.lifeMu.Unlock()
	defer p.active.Done()
	if ctx == nil {
		return DataFrame{}, fmt.Errorf("prepared frame context is nil")
	}
	queryBudget, ownedBudget, err := p.acquireQueryMemory(ctx)
	if err != nil {
		return DataFrame{}, err
	}
	ctx = memory.WithQueryBudget(ctx, queryBudget)
	defer func() {
		if !ownedBudget {
			return
		}
		if closeErr := queryBudget.Close(); closeErr != nil {
			if resultErr == nil {
				_ = frame.Close()
				frame = DataFrame{}
			}
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()

	exchangeManager := memory.ClassifiedManager(p.manager, "Exchange", memory.MemoryInFlightBatch)
	stream, err := pipeline.OpenBatchStream(ctx, p.src, source.ReadOptions{Kernels: p.kernels}, p.parts, exchangeManager, p.pipeline, p.observer)
	if err != nil {
		return DataFrame{}, err
	}
	out := newDataFrame(p.schema)
	scratch := memory.NewBatch()
	outputManager := memory.ClassifiedManager(p.manager, "Collect", memory.MemoryOutput)
	defer scratch.ReleaseOwned()
	for {
		ok, nextErr := stream.Next(ctx, scratch)
		if nextErr != nil {
			closeErr := stream.Close()
			_ = out.Close()
			return DataFrame{}, errors.Join(nextErr, closeErr)
		}
		if !ok {
			break
		}
		reservation, err := outputManager.Reserve(ctx, memory.BatchBytes(scratch))
		if err != nil {
			closeErr := stream.Close()
			_ = out.Close()
			return DataFrame{}, errors.Join(err, closeErr)
		}
		owned, err := memory.CloneBatch(scratch)
		if err != nil {
			reservation.Release()
			closeErr := stream.Close()
			_ = out.Close()
			return DataFrame{}, errors.Join(err, closeErr)
		}
		out.state.batches = append(out.state.batches, owned)
		out.state.reservations = append(out.state.reservations, reservation)
		out.state.rows += int64(owned.Rows)
	}
	if err := stream.Close(); err != nil {
		_ = out.Close()
		return DataFrame{}, err
	}
	return out, nil
}

// Close waits for active executions and releases retained physical resources,
// including prepared join build-side memory reservations. Concurrent callers all
// wait for the same cleanup and receive its error; resources are closed once.
func (p *PreparedDataFrame) Close() error {
	if p == nil {
		return nil
	}
	p.lifeMu.Lock()
	if p.closed {
		done := p.closeDone
		p.lifeMu.Unlock()
		<-done
		return p.closeErr
	}
	p.closed = true
	p.closeDone = make(chan struct{})
	p.lifeMu.Unlock()
	p.active.Wait()
	var closeErr error
	for i := len(p.closers) - 1; i >= 0; i-- {
		closeErr = errors.Join(closeErr, p.closers[i].Close())
	}
	p.closers = nil
	p.closeErr = closeErr
	close(p.closeDone)
	return closeErr
}
