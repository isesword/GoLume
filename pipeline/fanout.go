package pipeline

import (
	"errors"
	"fmt"
	"gocolumn/source"
)

// ErrUnboundedFanOut reports a non-replayable branch that has no byte-bounded
// multiplexer budget.
var ErrUnboundedFanOut = errors.New("fan-out requires stable replay or a positive bounded multiplexer budget")

// FanOutStrategy is the physical input-sharing choice for multiple consumers.
type FanOutStrategy uint8

const (
	// FanOutRescan creates an independent stream over the same stable snapshot.
	FanOutRescan FanOutStrategy = iota
	// FanOutBoundedMultiplexer shares one scan through byte-bounded branch queues.
	FanOutBoundedMultiplexer
)

// String returns the stable Explain spelling for a fan-out strategy.
func (s FanOutStrategy) String() string {
	switch s {
	case FanOutRescan:
		return "rescan"
	case FanOutBoundedMultiplexer:
		return "bounded-multiplexer"
	default:
		return fmt.Sprintf("fanout-%d", s)
	}
}

// FanOutPlan records a bounded planner decision; it never authorizes an
// unbounded shared cache.
type FanOutPlan struct {
	Strategy         FanOutStrategy
	Branches         int
	MaxBufferedBytes int64
	Replayability    source.Replayability
}

// PlanFanOut prefers rescanning a stable snapshot. Unknown/non-replayable
// inputs require the configured in-flight byte limit for a future multiplexer.
func PlanFanOut(src source.Source, branches int, maxBufferedBytes int64) (FanOutPlan, error) {
	if src == nil {
		return FanOutPlan{}, fmt.Errorf("fan-out source is nil")
	}
	if branches < 2 {
		return FanOutPlan{}, fmt.Errorf("fan-out requires at least two branches")
	}
	replay := source.ReplayabilityOf(src)
	if replay == source.ReplayStable {
		return FanOutPlan{Strategy: FanOutRescan, Branches: branches, Replayability: replay}, nil
	}
	if maxBufferedBytes <= 0 {
		return FanOutPlan{}, ErrUnboundedFanOut
	}
	return FanOutPlan{Strategy: FanOutBoundedMultiplexer, Branches: branches, MaxBufferedBytes: maxBufferedBytes, Replayability: replay}, nil
}
