package planner

import (
	"gocolumn/kernel"
	"testing"
)

func TestHeuristicARMStyleUsesFused(t *testing.T) {
	s, err := (Heuristic{}).Choose(Input{Rows: 1 << 20, Partitions: 8, Workers: 1, FusedCapable: true, Selectivity: .45, Cardinality: 1 << 18, Kernel: kernel.New(kernel.ModeAuto).Capabilities()})
	if err != nil {
		t.Fatal(err)
	}
	if s.Kind != StrategyDirectFused {
		t.Fatalf("strategy=%+v", s)
	}
}
func TestHeuristicUsesNativePartitionOwnership(t *testing.T) {
	s, err := (Heuristic{}).Choose(Input{Rows: 1 << 20, Partitions: 8, Workers: 4, Selectivity: .45, Cardinality: 1 << 18})
	if err != nil {
		t.Fatal(err)
	}
	if s.Kind != StrategyPartitionedHash || s.Workers != 4 {
		t.Fatalf("strategy=%+v", s)
	}
}

func TestHeuristicPrefersProvenSortedAggregation(t *testing.T) {
	strategy, err := (Heuristic{}).Choose(Input{Rows: 1 << 20, Partitions: 4, Workers: 4, Selectivity: 1, Cardinality: 1 << 18, SortedCapable: true})
	if err != nil {
		t.Fatal(err)
	}
	if strategy.Kind != StrategySorted || strategy.Workers != 1 {
		t.Fatalf("strategy=%+v", strategy)
	}
	if _, err := (Heuristic{}).Choose(Input{ForceStrategy: "sorted"}); err == nil {
		t.Fatal("forced sorted strategy must require ordering proof")
	}
}

func TestRemovedRadixStrategyRejected(t *testing.T) {
	if _, err := (Heuristic{}).Choose(Input{ForceStrategy: "radix"}); err == nil {
		t.Fatal("removed strategy accepted")
	}
	if StrategySpillHash.String() != "spill-hash" {
		t.Fatal("incorrect spill strategy label")
	}
}

func TestNativePartitionOwnershipPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   Input
		kind    Kind
		workers int
	}{
		{"single source partition", Input{Rows: 1 << 20, Partitions: 1, Workers: 4, Cardinality: 1 << 18}, StrategyPartitionedHash, 4},
		{"small row count", Input{Rows: 1000, Workers: 4, FusedCapable: true, Cardinality: 1 << 18}, StrategyDirectFused, 1},
		{"small cardinality", Input{Rows: 1 << 20, Workers: 4, FusedCapable: true, Cardinality: 100}, StrategyDirectFused, 1},
		{"wide key threshold", Input{Rows: 200000, Selectivity: .5, Workers: 4, Cardinality: 24000, KeyWidthBytes: 16}, StrategyPartitionedHash, 4},
		{"narrow key threshold", Input{Rows: 200000, Selectivity: .5, Workers: 4, Cardinality: 24000, KeyWidthBytes: 4, FusedCapable: true}, StrategyDirectFused, 1},
		{"forced one worker", Input{Workers: 1, ForceStrategy: "partitioned-hash"}, StrategyPartitionedHash, 1},
		{"forced default worker", Input{ForceStrategy: "partitioned-hash"}, StrategyPartitionedHash, 1},
		{"forced bitmap stays serial", Input{Rows: 1 << 20, Workers: 4, Cardinality: 1 << 18, ForceStrategy: "bitmap-direct"}, StrategyBitmapDirect, 1},
		{"forced fused stays serial", Input{Rows: 1 << 20, Workers: 4, Cardinality: 1 << 18, ForceStrategy: "direct-fused", FusedCapable: true}, StrategyDirectFused, 1},
		{"sorted preferred", Input{Rows: 1 << 20, Workers: 4, Cardinality: 1 << 18, SortedCapable: true}, StrategySorted, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := (Heuristic{}).Choose(tc.input)
			if err != nil || got.Kind != tc.kind || got.Workers != tc.workers {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		})
	}
	if StrategyPartitionedHash.String() != "partitioned-hash" {
		t.Fatal("invalid native partition strategy label")
	}
}

func TestZeroComplexityDoesNotInventFusedCapability(t *testing.T) {
	got, err := (Heuristic{}).Choose(Input{Rows: 10, PredicateComplexity: 0})
	if err != nil || got.Kind != StrategyBitmapDirect {
		t.Fatalf("strategy=%+v err=%v", got, err)
	}
	if _, err := (Heuristic{}).Choose(Input{ForceStrategy: "direct-fused", PredicateComplexity: 0}); err == nil {
		t.Fatal("unsupported forced fused strategy accepted")
	}
}
