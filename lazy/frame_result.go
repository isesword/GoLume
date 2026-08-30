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

// Frame is retained as a compatibility alias for DataFrame.
type Frame = DataFrame

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

// PreparedFrame is a reusable row-producing physical pipeline. Join build
// state, scalar programs, pushdown decisions, and source wrappers are prepared
// once; each Execute creates only independent readers and owned result batches.
type PreparedFrame struct {
	engine   *engine.Engine
	src      source.Source
	parts    []source.Partition
	schema   memory.Schema
	kernels  kernel.Set
	manager  memory.Manager
	pipeline pipeline.Options
	observer observer.Observer
	planInfo engine.PlanInfo
	closers  []io.Closer
	lifeMu   sync.Mutex
	lastMu   sync.Mutex
	lastDiag map[string]string
	active   sync.WaitGroup
	closed   bool
}

// PreparedDataFrame is the primary name for a reusable DataFrame-producing
// physical pipeline.
type PreparedDataFrame = PreparedFrame

// Prepare optimizes and lowers any row-producing plan, including a terminal
// Aggregate, into a reusable DataFrame pipeline.
func (f LazyFrame) Prepare(ctx context.Context) (*PreparedDataFrame, error) {
	return f.PrepareFrame(ctx)
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

// PrepareFrame is a compatibility spelling of Prepare.
//
// Deprecated: use Prepare.
func (f LazyFrame) PrepareFrame(ctx context.Context) (*PreparedFrame, error) {
	prepared, _, _, err := f.prepareFrame(ctx)
	return prepared, err
}

func (f LazyFrame) prepareFrame(ctx context.Context) (*PreparedFrame, logical.Plan, source.PushdownReport, error) {
	if f.err != nil {
		return nil, nil, source.PushdownReport{}, f.err
	}
	if f.session == nil || f.session.engine == nil {
		return nil, nil, source.PushdownReport{}, fmt.Errorf("lazy frame has no execution session")
	}
	policy, err := f.effectiveExecutionPolicy()
	if err != nil {
		return nil, nil, source.PushdownReport{}, err
	}
	ctx = engine.WithExecutionPolicy(ctx, policy)
	optimized, err := optimizer.Optimize(f.plan)
	if err != nil {
		return nil, nil, source.PushdownReport{}, err
	}
	ctx = withJoinFeedbackKey(ctx, planFeedbackKey(optimized.Plan))
	physicalPlan, report, err := applyPushdown(ctx, optimized.Plan)
	if err != nil {
		return nil, nil, source.PushdownReport{}, err
	}
	if err := f.validateExecutionLogical(physicalPlan); err != nil {
		return nil, nil, source.PushdownReport{}, err
	}
	src, pending, closers, err := lowerSourcePipeline(ctx, physicalPlan, f.session.engine.Memory(), f.session.engine.KernelSet(), f.session.engine)
	if err != nil {
		return nil, nil, source.PushdownReport{}, err
	}
	cleanup := func() {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i].Close()
		}
	}
	if pending != nil {
		childID := profileSourceID(src)
		src, err = physical.NewFilterSource(src, pending)
		if err != nil {
			cleanup()
			return nil, nil, source.PushdownReport{}, err
		}
		src = profileSource(ctx, src, "filter", "terminal residual filter", childID)
	}
	if err := f.validateExecutionSource(src); err != nil {
		cleanup()
		return nil, nil, source.PushdownReport{}, err
	}
	parts, err := src.Partitions(ctx)
	if err != nil {
		cleanup()
		return nil, nil, source.PushdownReport{}, err
	}
	pipelineOpts := f.session.engine.PipelineOptions()
	if err := pipelineOpts.Validate(); err != nil {
		cleanup()
		return nil, nil, source.PushdownReport{}, err
	}
	return &PreparedFrame{engine: f.session.engine, src: src, parts: parts, schema: src.Schema(), kernels: f.session.engine.KernelSet(), manager: f.session.engine.Memory(), pipeline: pipelineOpts, observer: f.session.engine.Observer(), planInfo: preparedPlanInfo(src), closers: closers}, physicalPlan, report, nil
}

// CollectFrame is a compatibility spelling of Collect.
//
// Deprecated: use Collect.
func (f LazyFrame) CollectFrame(ctx context.Context) (Frame, error) {
	p, err := f.PrepareFrame(ctx)
	if err != nil {
		return Frame{}, err
	}
	defer p.Close()
	return p.Execute(ctx)
}

// Schema returns the prepared row-pipeline output schema.
func (p *PreparedFrame) Schema() memory.Schema {
	if p == nil {
		return memory.Schema{}
	}
	return p.schema
}

// Execute reads every prepared partition and returns owned materialized columnar
// batches. Multiple Execute calls may run concurrently using independent source
// readers when the underlying source supports concurrent readers.
func (p *PreparedFrame) Execute(ctx context.Context) (frame Frame, resultErr error) {
	if p == nil {
		return Frame{}, fmt.Errorf("nil PreparedFrame")
	}
	p.lifeMu.Lock()
	if p.closed {
		p.lifeMu.Unlock()
		return Frame{}, ErrPreparedFrameClosed
	}
	p.active.Add(1)
	p.lifeMu.Unlock()
	defer p.active.Done()
	if ctx == nil {
		return Frame{}, fmt.Errorf("prepared frame context is nil")
	}
	queryBudget, err := p.engine.BeginQueryMemory()
	if err != nil {
		return Frame{}, err
	}
	ctx = memory.WithQueryBudget(ctx, queryBudget)
	defer func() {
		if closeErr := queryBudget.Close(); closeErr != nil {
			if resultErr == nil {
				_ = frame.Close()
				frame = Frame{}
			}
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()

	exchangeManager := memory.ClassifiedManager(p.manager, "Exchange", memory.MemoryInFlightBatch)
	stream, err := pipeline.OpenBatchStream(ctx, p.src, source.ReadOptions{Kernels: p.kernels}, p.parts, exchangeManager, p.pipeline, p.observer)
	if err != nil {
		return Frame{}, err
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
			return Frame{}, errors.Join(nextErr, closeErr)
		}
		if !ok {
			p.rememberDiagnostics(stream)
			break
		}
		reservation, err := outputManager.Reserve(ctx, memory.BatchBytes(scratch))
		if err != nil {
			closeErr := stream.Close()
			_ = out.Close()
			return Frame{}, errors.Join(err, closeErr)
		}
		owned, err := memory.CloneBatch(scratch)
		if err != nil {
			reservation.Release()
			closeErr := stream.Close()
			_ = out.Close()
			return Frame{}, errors.Join(err, closeErr)
		}
		out.state.batches = append(out.state.batches, owned)
		out.state.reservations = append(out.state.reservations, reservation)
		out.state.rows += int64(owned.Rows)
	}
	if err := stream.Close(); err != nil {
		_ = out.Close()
		return Frame{}, err
	}
	return out, nil
}

func (p *PreparedFrame) rememberDiagnostics(stream source.BatchStream) {
	diagnostics, ok := stream.(source.BatchStreamDiagnostics)
	if !ok {
		return
	}
	p.lastMu.Lock()
	p.lastDiag = diagnostics.StreamDiagnostics()
	p.lastMu.Unlock()
}

func (p *PreparedFrame) lastDiagnostics() map[string]string {
	p.lastMu.Lock()
	defer p.lastMu.Unlock()
	out := make(map[string]string, len(p.lastDiag))
	for key, value := range p.lastDiag {
		out[key] = value
	}
	return out
}

// Close waits for active executions and releases retained physical resources,
// including prepared join build-side memory reservations.
func (p *PreparedFrame) Close() error {
	if p == nil {
		return nil
	}
	p.lifeMu.Lock()
	if p.closed {
		p.lifeMu.Unlock()
		return nil
	}
	p.closed = true
	p.lifeMu.Unlock()
	p.active.Wait()
	var first error
	for i := len(p.closers) - 1; i >= 0; i-- {
		if err := p.closers[i].Close(); err != nil && first == nil {
			first = err
		}
	}
	p.closers = nil
	return first
}
