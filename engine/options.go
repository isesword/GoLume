package engine

import (
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/planner"
	"runtime"
	"strings"
)

const maxDefaultPipelineWorkers = 4

// ErrBoundedMemoryLimitRequired reports a bounded query configured without a
// finite engine memory limit.
var ErrBoundedMemoryLimitRequired = errors.New("bounded execution requires a positive memory limit")

// ErrBoundedSpillRequired reports a stateful bounded query without a finite
// spill quota. GoLume never silently writes unbounded temporary data.
var ErrBoundedSpillRequired = errors.New("bounded stateful execution requires an explicit positive spill quota")

// UnsupportedBoundedPlanError reports an input-proportional physical node
// that does not yet have a bounded or spillable implementation.
type UnsupportedBoundedPlanError struct {
	Operator string
	Path     []string
	Detail   string
	Cause    error
}

// Error describes the unsupported node and its root-to-node plan path.
func (e *UnsupportedBoundedPlanError) Error() string {
	if e == nil {
		return "bounded execution contains an unsupported node"
	}
	path := strings.Join(e.Path, " -> ")
	if path == "" {
		path = e.Operator
	}
	if e.Detail != "" {
		return fmt.Sprintf("bounded execution rejects %s at %s: %s", e.Operator, path, e.Detail)
	}
	return fmt.Sprintf("bounded execution rejects %s at %s: operator has input-proportional state without spill", e.Operator, path)
}

// Unwrap preserves typed resource/configuration causes for errors.Is.
func (e *UnsupportedBoundedPlanError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// ExecutionPolicy controls whether physical operators may retain in-memory
// state, spill, or must prove a bounded resource contract.
type ExecutionPolicy uint8

const (
	// ExecutionAuto selects batch-local, resident, or spillable strategies per
	// physical node. It is the zero-value production default.
	ExecutionAuto ExecutionPolicy = iota
	// ExecutionMemoryOnly disables spill and is intended for small workloads,
	// diagnostics, and performance comparison.
	ExecutionMemoryOnly
	// ExecutionBounded requires every input-proportional intermediate state to
	// have a hard bound or a configured spill implementation.
	ExecutionBounded
)

// String returns the stable Explain spelling for p.
func (p ExecutionPolicy) String() string {
	switch p {
	case ExecutionAuto:
		return "auto"
	case ExecutionMemoryOnly:
		return "memory-only"
	case ExecutionBounded:
		return "bounded"
	default:
		return fmt.Sprintf("execution-policy-%d", p)
	}
}

// Validate rejects unknown execution policies.
func (p ExecutionPolicy) Validate() error {
	if p > ExecutionBounded {
		return fmt.Errorf("invalid execution policy %d", p)
	}
	return nil
}

// Options configures shared engine resources and physical planning defaults.
type Options struct {
	Workers          int
	KernelMode       kernel.Mode
	MemoryLimitBytes int64
	// PreparedCacheBytes bounds continuously charged idle resident state across
	// prepared executions. Zero disables reuse. DefaultOptions uses 64 MiB.
	PreparedCacheBytes        int64
	QueryMemorySoftLimitBytes int64
	Spill                     SpillOptions
	ExecutionPolicy           ExecutionPolicy
	Pipeline                  pipeline.Options
	CostModel                 planner.CostModel
	Observer                  observer.Observer
}

// SpillOptions configures an explicit query-scoped disk directory and finite
// quota. The zero value disables spill without filesystem side effects.
type SpillOptions = memory.SpillOptions

// DefaultOptions returns production-oriented defaults based on GOMAXPROCS.
func DefaultOptions() Options {
	workers := runtime.GOMAXPROCS(0)
	pipelineWorkers := workers
	if pipelineWorkers > maxDefaultPipelineWorkers {
		pipelineWorkers = maxDefaultPipelineWorkers
	}
	return Options{Workers: workers, KernelMode: kernel.ModeAuto, MemoryLimitBytes: 0, PreparedCacheBytes: 64 << 20, ExecutionPolicy: ExecutionAuto, Pipeline: (pipeline.Options{Workers: pipelineWorkers}).Normalize(workers), CostModel: planner.Heuristic{}, Observer: observer.Nop{}}
}

func (o *Options) normalize() {
	if o.Workers < 1 {
		o.Workers = 1
	}
	if o.CostModel == nil {
		o.CostModel = planner.Heuristic{}
	}
	if o.Observer == nil {
		o.Observer = observer.Nop{}
	}
	o.Pipeline = o.Pipeline.Normalize(o.Workers)
}

func newMemoryManager(limit int64) *memory.EngineBudget { return memory.NewEngineBudget(limit) }
