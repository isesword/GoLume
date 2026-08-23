package engine

import (
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/planner"
	"runtime"
)

// Options configures shared engine resources and physical planning defaults.
type Options struct {
	Workers          int
	KernelMode       kernel.Mode
	MemoryLimitBytes int64
	CostModel        planner.CostModel
	Observer         observer.Observer
}

// DefaultOptions returns production-oriented defaults based on GOMAXPROCS.
func DefaultOptions() Options {
	return Options{Workers: runtime.GOMAXPROCS(0), KernelMode: kernel.ModeAuto, MemoryLimitBytes: 0, CostModel: planner.Heuristic{}, Observer: observer.Nop{}}
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
}

func newMemoryManager(limit int64) memory.Manager { return memory.NewBudgetManager(limit) }
