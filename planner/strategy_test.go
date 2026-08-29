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
