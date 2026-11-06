package parquet

import (
	"context"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"math/bits"
	"path/filepath"
	"testing"
)

func TestFloatMetadataPruningPreservesExactComparisonTruth(t *testing.T) {
	for _, tc := range []struct {
		name      string
		values    []float64
		predicate expr.Expr
		matched   int
	}{
		{"not_nan_literal", []float64{1}, expr.Not(expr.LT(expr.Col("f"), expr.F64(math.NaN()))), 1},
		{"not_nan_value", []float64{1, math.NaN()}, expr.Not(expr.LT(expr.Col("f"), expr.F64(2))), 1},
		{"not_equal_nan_value", []float64{1, math.NaN()}, expr.Not(expr.EQ(expr.Col("f"), expr.F64(1))), 1},
		{"nan_literal_false", []float64{1}, expr.LT(expr.Col("f"), expr.F64(math.NaN())), 0},
		{"unrepresentable_positive_integer", []float64{1 << 53}, expr.LT(expr.Col("f"), expr.I64(1<<53+1)), 1},
		{"unrepresentable_negative_integer", []float64{-1 << 53}, expr.GT(expr.Col("f"), expr.I64(-1<<53-1)), 1},
		{"max_int64_boundary", []float64{0x1p63}, expr.LT(expr.Col("f"), expr.I64(math.MaxInt64)), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "exact.parquet")
			writeDoubleBoolFixture(t, path, tc.values, nil, make([]bool, len(tc.values)), nil)
			base, err := OpenFile(path)
			if err != nil {
				t.Fatal(err)
			}
			pushed, report, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: tc.predicate})
			if err != nil {
				t.Fatal(err)
			}
			if tc.matched > 0 && report.PrunedPartitions != 0 {
				t.Fatalf("metadata pruned a matching row: %+v", report)
			}
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				for _, input := range []source.Source{base, pushed} {
					if got := countExactFloatMatches(t, input, tc.predicate, mode); got != tc.matched {
						t.Fatalf("%s %T matched=%d want=%d", mode, input, got, tc.matched)
					}
				}
			}
		})
	}
}

func countExactFloatMatches(t *testing.T, input source.Source, predicate expr.Expr, mode kernel.Mode) int {
	t.Helper()
	ctx := context.Background()
	program, err := expr.Compile(input.Schema(), predicate)
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := expr.NewWorkspace(ctx, program, memory.NewBudgetManager(0))
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	kernels := kernel.New(mode)
	stream, err := input.NewBatchStream(ctx, source.ReadOptions{Kernels: kernels})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	batch := memory.NewBatch()
	matched := 0
	for {
		ok, err := stream.Next(ctx, batch)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return matched
		}
		value, err := workspace.EvaluateBool(ctx, batch, kernels)
		if err != nil {
			t.Fatal(err)
		}
		for i, truth := range value.Truth {
			if value.Validity != nil {
				truth &= value.Validity[i]
			}
			if i == memory.WordsFor(batch.Rows)-1 && batch.Rows&63 != 0 {
				truth &= uint64(1)<<uint(batch.Rows&63) - 1
			}
			matched += bits.OnesCount64(truth)
		}
	}
}
