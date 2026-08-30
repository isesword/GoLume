package planner

import (
	"gocolumn/kernel"
	"testing"
)

func TestHeuristicARMStyleUsesFused(t *testing.T) {
	s, err := (Heuristic{}).Choose(Input{Rows: 1 << 20, Partitions: 8, Workers: 1, Selectivity: .45, Cardinality: 1 << 18, Kernel: kernel.New(kernel.ModeAuto).Capabilities()})
	if err != nil {
		t.Fatal(err)
	}
	if s.Kind != StrategyDirectFused {
		t.Fatalf("strategy=%+v", s)
	}
}
func TestHeuristicParallelUsesRadix(t *testing.T) {
	s, err := (Heuristic{}).Choose(Input{Rows: 1 << 20, Partitions: 8, Workers: 4, Selectivity: .45, Cardinality: 1 << 18})
	if err != nil {
		t.Fatal(err)
	}
	if s.Kind != StrategyRadix || s.Workers != 4 {
		t.Fatalf("strategy=%+v", s)
	}
}

func TestHeuristicPrefersProvenSortedAggregation(t *testing.T) {
	strategy, err := (Heuristic{}).Choose(Input{Rows: 1 << 20, Partitions: 4, Workers: 4, Selectivity: 1, Cardinality: 1 << 18, RadixCapable: true, SortedCapable: true})
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
