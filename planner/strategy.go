package planner

import (
	"fmt"
	"gocolumn/kernel"
)

// Kind identifies a physical aggregation execution strategy.
type Kind uint8

const (
	// StrategyDirectFused evaluates a simple predicate and aggregates selected rows in one scalar pass.
	StrategyDirectFused Kind = iota
	// StrategyBitmapDirect materializes a reusable selection bitmap before direct aggregation.
	StrategyBitmapDirect
	// StrategyRadix partitions selected keys and executes partition-owned aggregation in parallel.
	StrategyRadix
)

// String returns the stable textual name of the strategy.
func (k Kind) String() string {
	switch k {
	case StrategyDirectFused:
		return "direct-fused"
	case StrategyBitmapDirect:
		return "bitmap-direct"
	case StrategyRadix:
		return "radix"
	default:
		return "unknown"
	}
}

// Strategy is the physical aggregation decision produced by a CostModel.
type Strategy struct {
	Kind      Kind
	Workers   int
	RadixBits uint
	Reason    string
}

// Input contains runtime statistics and capabilities used to choose a physical strategy.
type Input struct {
	Rows                int64
	Partitions          int
	Workers             int
	Selectivity         float64
	Cardinality         int
	ReuseSelection      bool
	Kernel              kernel.Capabilities
	ForceStrategy       string
	ForceRadixBits      uint
	FusedCapable        bool
	RadixCapable        bool
	KeyWidthBytes       int
	PredicateComplexity int
}

// CostModel selects a physical aggregation strategy from runtime statistics.
type CostModel interface{ Choose(Input) (Strategy, error) }

// Heuristic is the default rule-and-cost hybrid used until a calibrated cost model replaces it.
type Heuristic struct{}

// Choose selects a physical strategy while honoring explicit strategy hints.
func (Heuristic) Choose(in Input) (Strategy, error) {
	fusedCapable := in.FusedCapable || in.PredicateComplexity == 0
	radixCapable := in.RadixCapable || in.KeyWidthBytes == 0
	workers := in.Workers
	if workers < 1 {
		workers = 1
	}
	if in.Partitions > 0 && workers > in.Partitions {
		workers = in.Partitions
	}
	if workers < 1 {
		workers = 1
	}
	bits := in.ForceRadixBits
	if bits == 0 {
		bits = 8
	}
	if in.ForceStrategy != "" {
		switch in.ForceStrategy {
		case "direct-fused":
			if !fusedCapable {
				return Strategy{}, fmt.Errorf("direct-fused is not valid for this predicate")
			}
			return Strategy{Kind: StrategyDirectFused, Workers: 1, Reason: "forced"}, nil
		case "bitmap-direct":
			return Strategy{Kind: StrategyBitmapDirect, Workers: 1, Reason: "forced"}, nil
		case "radix":
			if !radixCapable {
				return Strategy{}, fmt.Errorf("radix is not available for this key representation")
			}
			return Strategy{Kind: StrategyRadix, Workers: workers, RadixBits: bits, Reason: "forced"}, nil
		default:
			return Strategy{}, fmt.Errorf("unknown forced strategy %q", in.ForceStrategy)
		}
	}
	selected := float64(in.Rows) * in.Selectivity
	if in.Selectivity <= 0 {
		selected = float64(in.Rows) * .5
	}
	// Wider keys become cache-unfriendly earlier, so lower the radix threshold.
	rowThreshold := 128000.0
	cardThreshold := 32000
	if in.KeyWidthBytes >= 16 {
		rowThreshold = 96000
		cardThreshold = 24000
	}
	if radixCapable && workers > 1 && selected >= rowThreshold && in.Cardinality >= cardThreshold {
		return Strategy{Kind: StrategyRadix, Workers: workers, RadixBits: bits, Reason: "parallel high-cardinality aggregation"}, nil
	}
	if !fusedCapable {
		return Strategy{Kind: StrategyBitmapDirect, Workers: 1, Reason: "predicate requires expression program"}, nil
	}
	if in.ReuseSelection || in.PredicateComplexity > 1 {
		return Strategy{Kind: StrategyBitmapDirect, Workers: 1, Reason: "selection bitmap is reusable or predicate is composite"}, nil
	}
	// M4/NEON measurements showed a simple fused predicate can beat bitmap
	// materialization. x86 with >=256-bit vectors can justify bitmap execution.
	if in.Kernel.HardwareSIMD && in.Kernel.VectorBits >= 256 {
		return Strategy{Kind: StrategyBitmapDirect, Workers: 1, Reason: "wide hardware SIMD available"}, nil
	}
	return Strategy{Kind: StrategyDirectFused, Workers: 1, Reason: "avoid bitmap materialization for simple predicate"}, nil
}
