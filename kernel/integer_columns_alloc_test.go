//go:build !race

package kernel

import (
	"gocolumn/memory"
	"testing"
)

// Exact allocation assertions are kept out of race-instrumented runs, matching
// the numeric query allocation tests; semantic tests still run under race.
func integerColumnAllocations[T integerColumnNumber](t *testing.T, bind func(Set) integerColumnCall[T]) {
	a, b := make([]T, 1025), make([]T, 1025)
	av, bv := integerColumnValidity(1025, "mixed"), integerColumnValidity(1025, "even")
	out := make(memory.Bitmap, 17)
	for _, runner := range integerColumnRunners(bind) {
		for op := CompareGT; op <= CompareEQ; op++ {
			if got := testing.AllocsPerRun(30, func() { runner.call(a, b, av, bv, out, op) }); got != 0 {
				t.Fatalf("backend=%s op=%s allocations=%g", runner.name, op, got)
			}
		}
	}
}
func TestIntegerColumnZeroAllocations(t *testing.T) {
	t.Run("int8", func(t *testing.T) {
		integerColumnAllocations(t, func(s Set) integerColumnCall[int8] { return s.CompareInt8Columns })
	})
	t.Run("int16", func(t *testing.T) {
		integerColumnAllocations(t, func(s Set) integerColumnCall[int16] { return s.CompareInt16Columns })
	})
	t.Run("int64", func(t *testing.T) {
		integerColumnAllocations(t, func(s Set) integerColumnCall[int64] { return s.CompareInt64Columns })
	})
	t.Run("uint8", func(t *testing.T) {
		integerColumnAllocations(t, func(s Set) integerColumnCall[uint8] { return s.CompareUint8Columns })
	})
	t.Run("uint16", func(t *testing.T) {
		integerColumnAllocations(t, func(s Set) integerColumnCall[uint16] { return s.CompareUint16Columns })
	})
	t.Run("uint64", func(t *testing.T) {
		integerColumnAllocations(t, func(s Set) integerColumnCall[uint64] { return s.CompareUint64Columns })
	})
}
