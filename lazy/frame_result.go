package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/optimizer"
	"gocolumn/physical"
	"gocolumn/source"
	"io"
	"sync"
)

// ErrPreparedFrameClosed is returned when a prepared row pipeline is executed
// after Close.
var ErrPreparedFrameClosed = errors.New("prepared frame closed")

// Frame is an owned materialized columnar result produced by CollectFrame.
// Batches contain deep-copied specialized vectors and remain valid after the
// source reader and lazy session are closed.
type Frame struct {
	Schema  memory.Schema
	Batches []*memory.Batch
	Rows    int64
}

// Close releases owned source-native arrays retained by materialized batches.
// It is safe to call Close more than once. Specialized Go vectors require no
// explicit release.
func (f *Frame) Close() error {
	if f == nil {
		return nil
	}
	for _, batch := range f.Batches {
		if batch != nil {
			batch.ReleaseOwned()
		}
	}
	f.Batches = nil
	f.Rows = 0
	return nil
}

// PreparedFrame is a reusable row-producing physical pipeline. Join build
// state, scalar programs, pushdown decisions, and source wrappers are prepared
// once; each Execute creates only independent readers and owned result batches.
type PreparedFrame struct {
	src     source.Source
	parts   []source.Partition
	schema  memory.Schema
	closers []io.Closer
	lifeMu  sync.Mutex
	active  sync.WaitGroup
	closed  bool
}

// PrepareFrame optimizes and lowers a row-producing LazyFrame. Aggregate plans
// use Prepare instead because they terminate in the specialized hash aggregate
// engine.
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
	optimized, err := optimizer.Optimize(f.plan)
	if err != nil {
		return nil, nil, source.PushdownReport{}, err
	}
	physicalPlan, report, err := applyPushdown(ctx, optimized.Plan)
	if err != nil {
		return nil, nil, source.PushdownReport{}, err
	}
	if _, ok := physicalPlan.(*logical.Aggregate); ok {
		return nil, nil, source.PushdownReport{}, fmt.Errorf("aggregate plans use Prepare, not PrepareFrame")
	}
	src, pending, closers, err := lowerSourcePipeline(ctx, physicalPlan, f.session.engine.Memory(), f.session.engine.KernelCapabilities().Mode, f.session.engine)
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
		src, err = physical.NewFilterSource(src, pending, f.session.engine.KernelCapabilities().Mode)
		if err != nil {
			cleanup()
			return nil, nil, source.PushdownReport{}, err
		}
		src = profileSource(ctx, src, "filter", "terminal residual filter", childID)
	}
	parts, err := src.Partitions(ctx)
	if err != nil {
		cleanup()
		return nil, nil, source.PushdownReport{}, err
	}
	return &PreparedFrame{src: src, parts: parts, schema: src.Schema(), closers: closers}, physicalPlan, report, nil
}

// CollectFrame prepares and executes a row-producing lazy plan once.
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
func (p *PreparedFrame) Execute(ctx context.Context) (Frame, error) {
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

	r, err := p.src.NewReader(ctx)
	if err != nil {
		return Frame{}, err
	}
	defer r.Close()
	out := Frame{Schema: p.schema}
	scratch := memory.NewBatch()
	defer scratch.ReleaseOwned()
	for _, part := range p.parts {
		if err := r.ReadPartition(ctx, part, scratch); err != nil {
			return Frame{}, err
		}
		owned, err := memory.CloneBatch(scratch)
		if err != nil {
			_ = out.Close()
			return Frame{}, err
		}
		out.Batches = append(out.Batches, owned)
		out.Rows += int64(owned.Rows)
	}
	return out, nil
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
