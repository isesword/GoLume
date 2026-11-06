// Package engine compiles logical queries into reusable physical execution
// plans over columnar sources.
package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/scheduler"
	"gocolumn/source"
)

// Engine owns shared execution resources such as the worker scheduler, kernel
// dispatch table, and query memory manager.
type Engine struct {
	opts      Options
	kernels   kernel.Set
	scheduler scheduler.Scheduler
	memory    *memory.EngineBudget
	feedback  *feedbackStore
}

// New constructs an Engine using normalized execution options.
func New(opts Options) *Engine {
	opts.normalize()
	return &Engine{opts: opts, kernels: kernel.New(opts.KernelMode), scheduler: scheduler.NewPool(opts.Workers), memory: newMemoryManager(opts.MemoryLimitBytes), feedback: newFeedbackStore()}
}

// Close releases shared scheduler resources. Prepared queries should be closed
// before closing their owning engine.
func (e *Engine) Close() error { return e.scheduler.Close() }

// KernelCapabilities reports the active scalar/SIMD kernel backend.
func (e *Engine) KernelCapabilities() kernel.Capabilities { return e.kernels.Capabilities() }

// KernelSet returns the immutable dispatch handle shared by all execution paths.
func (e *Engine) KernelSet() kernel.Set { return e.kernels }

// Memory returns the engine-level query memory manager.
func (e *Engine) Memory() memory.Manager { return e.memory }

// BeginQueryMemory creates one execution-scoped child of the shared engine
// budget. Physical managers route reservations through it when bound to ctx.
func (e *Engine) BeginQueryMemory() (*memory.QueryBudget, error) {
	if e == nil || e.memory == nil {
		return nil, fmt.Errorf("engine memory budget is nil")
	}
	return e.memory.BeginQuery(memory.BudgetOptions{SoftLimitBytes: e.opts.QueryMemorySoftLimitBytes, HardLimitBytes: e.opts.MemoryLimitBytes, Spill: e.opts.Spill})
}

// SpillConfigured reports whether queries may lazily create a spill store.
func (e *Engine) SpillConfigured() bool {
	return e != nil && e.opts.Spill.Enabled()
}

// SpillLimit returns the per-query disk quota; zero means spill is disabled.
func (e *Engine) SpillLimit() int64 {
	if e == nil || !e.opts.Spill.Enabled() {
		return 0
	}
	return e.opts.Spill.MaxBytes
}

// ExecutionPolicy returns the session-wide physical execution default.
func (e *Engine) ExecutionPolicy() ExecutionPolicy {
	if e == nil {
		return ExecutionAuto
	}
	return e.opts.ExecutionPolicy
}

// PipelineOptions returns the normalized query pipeline defaults.
func (e *Engine) PipelineOptions() pipeline.Options {
	if e == nil {
		return (pipeline.Options{}).Normalize(1)
	}
	return e.opts.Pipeline
}

// Observer returns the session-wide stage observer.
func (e *Engine) Observer() observer.Observer {
	if e == nil || e.opts.Observer == nil {
		return observer.Nop{}
	}
	return e.opts.Observer
}

// Execute is the convenience one-shot API. Repeated workloads should call
// Prepare once and reuse the compiled plan. Each Execute opens native streams
// with query-scoped memory and releases all execution state before returning.
func (e *Engine) Execute(ctx context.Context, src source.Source, q expr.Query, execOpts ExecuteOptions) (Result, error) {
	p, err := e.Prepare(ctx, src, q, PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		return Result{}, err
	}
	defer p.Close()
	return p.Execute(ctx, execOpts)
}

// MaxWorkers returns the maximum scheduler parallelism owned by the engine.
func (e *Engine) MaxWorkers() int {
	if e == nil || e.scheduler == nil {
		return 1
	}
	return e.scheduler.MaxWorkers()
}

// ParallelFor executes independent tasks on the engine's persistent worker
// pool. It allows physical operators to reuse scheduler resources without
// creating per-query goroutine pools.
func (e *Engine) ParallelFor(ctx context.Context, tasks, parallelism int, fn func(task int) error) error {
	if e == nil || e.scheduler == nil {
		return fmt.Errorf("engine scheduler is nil")
	}
	return e.scheduler.ParallelFor(ctx, tasks, parallelism, fn)
}
