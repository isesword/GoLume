package planner

import (
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// Kind identifies a physical aggregation execution strategy.
type Kind uint8

const (
	// StrategyDirectFused evaluates a simple predicate and aggregates selected rows in one scalar pass.
	StrategyDirectFused Kind = iota
	// StrategyBitmapDirect materializes a reusable selection bitmap before direct aggregation.
	StrategyBitmapDirect
	// StrategyPartitionedHash assigns native hash-state shards to independent workers.
	StrategyPartitionedHash
	// StrategySpillHash uses the query-budgeted partitioned hash aggregate stream.
	StrategySpillHash
	// StrategySorted aggregates adjacent equal keys with O(1) group state when
	// the source proves a compatible global ordering prefix.
	StrategySorted
)

// String returns the stable textual name of the strategy.
func (k Kind) String() string {
	switch k {
	case StrategyDirectFused:
		return "direct-fused"
	case StrategyBitmapDirect:
		return "bitmap-direct"
	case StrategyPartitionedHash:
		return "partitioned-hash"
	case StrategySpillHash:
		return "spill-hash"
	case StrategySorted:
		return "sorted"
	default:
		return "unknown"
	}
}

// Strategy is the physical aggregation decision produced by a CostModel.
type Strategy struct {
	Kind    Kind
	Workers int
	Reason  string
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
	FusedCapable        bool
	KeyWidthBytes       int
	PredicateComplexity int
	ComparisonID        kernel.KernelID
	HasComparisonID     bool
	SortedCapable       bool
}

// CostModel selects a physical aggregation strategy from runtime statistics.
type CostModel interface{ Choose(Input) (Strategy, error) }

// Heuristic is the default rule-and-cost hybrid used until a calibrated cost model replaces it.
type Heuristic struct{}

// Choose selects a physical strategy while honoring explicit strategy hints.
func (Heuristic) Choose(in Input) (Strategy, error) {
	fusedCapable := in.FusedCapable
	workers := in.Workers
	if workers < 1 {
		workers = 1
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
		case "partitioned-hash":
			return Strategy{Kind: StrategyPartitionedHash, Workers: workers, Reason: "forced native partition ownership"}, nil
		case "radix":
			return Strategy{}, fmt.Errorf("radix aggregation execution was removed; use native direct/bitmap/partitioned-hash or bounded spill aggregation")
		case "sorted":
			if !in.SortedCapable {
				return Strategy{}, fmt.Errorf("sorted aggregation requires a proven group-key ordering prefix")
			}
			return Strategy{Kind: StrategySorted, Workers: 1, Reason: "forced ordered input"}, nil
		default:
			return Strategy{}, fmt.Errorf("unknown forced strategy %q", in.ForceStrategy)
		}
	}
	if in.SortedCapable {
		return Strategy{Kind: StrategySorted, Workers: 1, Reason: "input ordering matches group keys"}, nil
	}
	// Hash ownership is divided within each bounded batch, independently of
	// source partitions. One row group can therefore use multiple workers.
	selected := float64(in.Rows) * in.Selectivity
	if in.Selectivity <= 0 {
		selected = float64(in.Rows) * .5
	}
	rowThreshold, cardThreshold := 128000.0, 32000
	if in.KeyWidthBytes >= 16 {
		rowThreshold, cardThreshold = 96000, 24000
	}
	if workers > 1 && selected >= rowThreshold && in.Cardinality >= cardThreshold {
		return Strategy{Kind: StrategyPartitionedHash, Workers: workers, Reason: "native parallel high-cardinality hash ownership"}, nil
	}
	if !fusedCapable {
		return Strategy{Kind: StrategyBitmapDirect, Workers: 1, Reason: "predicate requires expression program"}, nil
	}
	if in.ReuseSelection || in.PredicateComplexity > 1 {
		return Strategy{Kind: StrategyBitmapDirect, Workers: 1, Reason: "selection bitmap is reusable or predicate is composite"}, nil
	}
	if in.HasComparisonID && approvedBitmapStrategy(in.Kernel, in.ComparisonID, in.Selectivity) {
		return Strategy{Kind: StrategyBitmapDirect, Workers: 1, Reason: "benchmark-approved SIMD selection bucket"}, nil
	}
	return Strategy{Kind: StrategyDirectFused, Workers: 1, Reason: "avoid bitmap materialization for simple predicate"}, nil
}

type bitmapApproval struct {
	arch               string
	vectorBits         int
	id                 kernel.KernelID
	selectivityPercent int
}

var int32GT, _ = kernel.ComparisonID(memory.TypeInt32, kernel.CompareGT)

// Entries are operation-specific and backed by the committed 1s x 10 arm64
// DirectFused/Bitmap benchmark. The 1% bucket stays DirectFused because its
// 4.40% gain did not clear the 10% policy threshold.
var bitmapApprovals = map[bitmapApproval]struct{}{
	{arch: "arm64", vectorBits: 128, id: int32GT, selectivityPercent: 10}:  {},
	{arch: "arm64", vectorBits: 128, id: int32GT, selectivityPercent: 50}:  {},
	{arch: "arm64", vectorBits: 128, id: int32GT, selectivityPercent: 90}:  {},
	{arch: "arm64", vectorBits: 128, id: int32GT, selectivityPercent: 100}: {},
}

func approvedBitmapStrategy(caps kernel.Capabilities, id kernel.KernelID, selectivity float64) bool {
	capability, ok := caps.For(id)
	if !ok || !capability.HardwareSIMD {
		return false
	}
	bucket := 100
	switch {
	case selectivity <= .01:
		bucket = 1
	case selectivity <= .10:
		bucket = 10
	case selectivity <= .50:
		bucket = 50
	case selectivity <= .90:
		bucket = 90
	}
	_, ok = bitmapApprovals[bitmapApproval{arch: caps.Arch(), vectorBits: capability.VectorBits, id: id, selectivityPercent: bucket}]
	return ok
}
