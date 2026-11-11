package parquet

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/source"
)

func metadataCacheGroup(s *Source, index int) *pruningRowGroup {
	return &pruningRowGroup{RowGroupMetaData: s.meta.RowGroup(index), file: s.meta, index: index, statistics: &pruningStatisticsCache{}}
}

func TestPruningStatisticsCacheReusesAndBoundsResults(t *testing.T) {
	for _, hasStats := range []bool{false, true} {
		s := metadataCacheFixture(t, 2, 8, hasStats)
		for group := range 2 {
			rg := metadataCacheGroup(s, group)
			first := make([]metadata.TypedStatistics, len(s.allBindings))
			for i, binding := range s.allBindings {
				var err error
				first[i], err = columnStatistics(rg, binding)
				if err != nil {
					t.Fatal(err)
				}
				if (first[i] != nil) != hasStats {
					t.Fatalf("stats=%v column=%d result=%v", hasStats, i, first[i])
				}
				got, err := columnStatistics(rg, binding)
				if err != nil || got != first[i] {
					t.Fatalf("column=%d not reused: %v", i, err)
				}
				if cached, ok := rg.statistics.lookup(i); !ok || cached != first[i] {
					t.Fatalf("column=%d not cached (including nil)", i)
				}
			}
			if rg.statistics.count != len(rg.statistics.inline) {
				t.Fatalf("cache grew past inline capacity: %d", rg.statistics.count)
			}
			for i := 4; i < 8; i++ {
				got, err := columnStatistics(rg, s.allBindings[i])
				if err != nil || got != first[i] {
					t.Fatalf("retained column %d changed: %v", i, err)
				}
			}
			if _, ok := rg.statistics.lookup(0); ok {
				t.Fatal("oldest column was not evicted")
			}
			reloaded, err := columnStatistics(rg, s.allBindings[0])
			if err != nil {
				t.Fatal(err)
			}
			if hasStats && reloaded == first[0] {
				t.Fatal("evicted statistics were not decoded again")
			}
			if !reflect.DeepEqual(reloaded, first[0]) {
				t.Fatal("eviction changed statistics")
			}
			if cached, ok := rg.statistics.lookup(0); !ok || cached != reloaded {
				t.Fatal("reloaded statistics not cached")
			}
		}
	}
}

// Give each leaf fresh statistics state to reproduce the pre-cache evaluator.
func metadataTruthWithoutReuse(rg *pruningRowGroup, p expr.Expr, bindings map[string]columnBinding) (truthSet, error) {
	if b, ok := p.(expr.Binary); ok && (b.Op == expr.OpAnd || b.Op == expr.OpOr) {
		left, err := metadataTruthWithoutReuse(rg, b.Left, bindings)
		if err != nil {
			return truthAny, err
		}
		right, err := metadataTruthWithoutReuse(rg, b.Right, bindings)
		if err != nil {
			return truthAny, err
		}
		return combineTruth(b.Op, left, right), nil
	}
	if u, ok := p.(expr.Unary); ok && u.Op == expr.OpNot {
		value, err := metadataTruthWithoutReuse(rg, u.Value, bindings)
		return notTruth(value), err
	}
	fresh := &pruningRowGroup{RowGroupMetaData: rg.RowGroupMetaData, file: rg.file, index: rg.index}
	return predicateTruth(fresh, p, bindings)
}

func checkMetadataCacheTruth(t *testing.T, s *Source, predicates []expr.Expr) {
	t.Helper()
	bindings := make(map[string]columnBinding)
	for _, binding := range s.allBindings {
		bindings[binding.Name] = binding
	}
	for _, predicate := range predicates {
		for group := range s.allParts {
			got, gotErr := predicateTruth(metadataCacheGroup(s, group), predicate, bindings)
			want, wantErr := metadataTruthWithoutReuse(metadataCacheGroup(s, group), predicate, bindings)
			if got != want || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("%s group=%d got=%b,%v want=%b,%v", predicate, group, got, gotErr, want, wantErr)
			}
		}
		pushed, _, err := s.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			got := countExactFloatMatches(t, pushed, predicate, mode)
			want := countExactFloatMatches(t, s, predicate, mode)
			if got != want {
				t.Fatalf("%s mode=%s pushed=%d unpruned=%d", predicate, mode, got, want)
			}
		}
	}
}

func TestPruningStatisticsCachePreservesBoundaryTruth(t *testing.T) {
	x := expr.Col("x")
	p := expr.In(x, expr.I64(math.MaxInt64), expr.I64(math.MinInt64), expr.I64(1<<53+1))
	integerPredicates := []expr.Expr{
		expr.Or(expr.And(expr.GE(x, expr.I64(-5)), expr.LE(x, expr.I64(20))), expr.IsNull(x)),
		expr.And(p, expr.Not(expr.LT(x, expr.I64(0)))),
		expr.Or(expr.And(expr.EQ(x, expr.I64(50)), expr.IsNotNull(x)), expr.Not(p)),
		expr.And(expr.GE(expr.Col("id"), expr.I64(0)), expr.And(p, expr.LT(expr.Col("id"), expr.I64(15)))),
	}
	for _, corruption := range []string{"none", "missing", "null_count", "short_bound", "one_bound", "reversed"} {
		t.Run(corruption, func(t *testing.T) {
			s := openLiteralInFixture(t, true, true)
			raw := s.meta.RowGroups[0].Columns[1].MetaData
			switch corruption {
			case "missing":
				raw.Statistics = nil
			case "null_count":
				raw.Statistics.NullCount = nil
			case "short_bound":
				raw.Statistics.MinValue = []byte{0}
			case "one_bound":
				raw.Statistics.MaxValue = nil
			case "reversed":
				raw.Statistics.MinValue = binary.LittleEndian.AppendUint64(nil, 10)
				raw.Statistics.MaxValue = binary.LittleEndian.AppendUint64(nil, 0)
			}
			checkMetadataCacheTruth(t, s, integerPredicates)
		})
	}
	t.Run("float_NaN_infinity_exact_integer", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "cache-floats.parquet")
		values := []float64{math.Inf(-1), -0x1p63, -1 << 53, math.Copysign(0, -1), 0, 1 << 53, 0x1p63, math.Inf(1), math.NaN()}
		writeDoubleBoolFixture(t, path, values, nil, make([]bool, len(values)), nil)
		s, err := OpenFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f := expr.Col("f")
		checkMetadataCacheTruth(t, s, []expr.Expr{
			expr.And(expr.Not(expr.LT(f, expr.F64(2))), expr.IsNotNull(f)),
			expr.Or(expr.LT(f, expr.I64(1<<53+1)), expr.Not(expr.GE(f, expr.I64(math.MaxInt64)))),
			expr.Or(expr.Not(expr.EQ(f, expr.F64(math.NaN()))), expr.IsNull(f)),
			expr.And(expr.GE(f, expr.F64(math.Inf(-1))), expr.LE(f, expr.F64(math.Inf(1)))),
		})
	})
}

func TestPruningStatisticsCacheDoesNotCrossEvaluations(t *testing.T) {
	s := metadataCacheFixture(t, 3, 8, true)
	req := source.PushdownRequest{Predicate: expr.And(expr.GE(expr.Col("c0"), expr.I64(0)), expr.LE(expr.Col("c0"), expr.I64(2)))}
	check := func(want []int) {
		t.Helper()
		out, _, err := s.ApplyPushdown(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		parts, err := out.Partitions(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		got := make([]int, 0, len(parts))
		for _, p := range parts {
			got = append(got, p.ID)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("parts=%v want=%v", got, want)
		}
	}
	check([]int{0})
	raw := s.meta.RowGroups[0].Columns[0].MetaData.Statistics
	before := raw.NullCount
	nulls := int64(4)
	raw.NullCount = &nulls
	check([]int{})
	raw.NullCount = before
	check([]int{0})
}

func TestPruningStatisticsCacheConcurrentImmutableClones(t *testing.T) {
	s := metadataCacheFixture(t, 16, 8, true)
	ctx := context.Background()
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range 12 {
				group := (worker + round) % 16
				col := fmt.Sprintf("c%d", worker)
				value := int64(group*10 + worker)
				var p expr.Expr = expr.And(expr.GE(expr.Col(col), expr.I64(value)), expr.LE(expr.Col(col), expr.I64(value+2)))
				// Visit every physical column and revisit it, exercising private
				// eviction as well as cache hits concurrently.
				for column := range 8 {
					name := fmt.Sprintf("c%d", column)
					lo := int64(group*10 + column)
					p = expr.And(p, expr.And(expr.GE(expr.Col(name), expr.I64(lo)), expr.LE(expr.Col(name), expr.I64(lo+2))))
				}
				out, _, err := s.ApplyPushdown(ctx, source.PushdownRequest{Predicate: p})
				if err != nil {
					t.Error(err)
					return
				}
				// Reapply from an immutable clone, not just the initial Source.
				out, _, err = out.(source.PushdownSource).ApplyPushdown(ctx, source.PushdownRequest{Predicate: p})
				if err != nil {
					t.Error(err)
					return
				}
				parts, err := out.Partitions(ctx)
				if err != nil || len(parts) != 1 || parts[0].ID != group {
					t.Errorf("parts=%v group=%d err=%v", parts, group, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
