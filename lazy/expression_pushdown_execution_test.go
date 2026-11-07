package lazy

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/optimizer"
	"gocolumn/physical"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	parquetsource "gocolumn/source/parquet"
)

// Execute the original logical tree directly as an independent no-optimizer
// oracle. Both paths retain physical residual evaluation and row order.
func expressionPushdownIDs(plan logical.Plan) ([]int64, error) {
	ctx := context.Background()
	manager := memory.NewBudgetManager(64 << 20)
	kernels := kernel.New(kernel.ModeScalar)
	src, pending, closers, err := lowerSourcePipeline(ctx, plan, manager, kernels, nil)
	defer func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i].Close()
		}
	}()
	if err != nil {
		return nil, err
	}
	if pending != nil {
		src, err = physical.NewFilterSource(src, pending)
		if err != nil {
			return nil, err
		}
	}
	stream, err := source.OpenBatchStream(ctx, src, source.ReadOptions{Kernels: kernels, MemoryManager: manager})
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	var ids []int64
	batch := memory.NewBatch()
	for {
		ok, err := stream.Next(ctx, batch)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		ids = append(ids, batch.Int64s["id"].Values[:batch.Rows]...)
	}
	return ids, nil
}

func TestExpressionPushdownExecutionDifferential(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3, 4}}
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3, 4}, Validity: memory.Bitmap{27}}
	batch.Int64s["y"] = &memory.Int64Vector{Values: []int64{4, 3, 2, 1, 0}}
	batch.Float64s["f"] = &memory.Float64Vector{Values: []float64{math.NaN(), -1, 0, 1, math.Inf(1)}}
	batch.Reset(5)
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "y", Type: memory.TypeInt64}, memory.Field{Name: "f", Type: memory.TypeFloat64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	scan := &logical.Scan{Source: src}
	project := func(input logical.Plan, names ...expr.NamedExpr) logical.Plan {
		return &logical.ExprProjection{Input: input, Exprs: names, IncludeInput: true}
	}
	for _, tc := range []struct {
		name  string
		input logical.Plan
		pred  expr.Expr
		want  []int64
	}{
		{"passthrough_nullable", project(scan, expr.Named(expr.F64(1), "extra")), expr.GT(expr.Col("x"), expr.I64(1)), []int64{3, 4}},
		{"alias_nullable", project(scan, expr.Named(expr.Col("x"), "alias")), expr.GT(expr.Col("alias"), expr.I64(1)), []int64{3, 4}},
		{"simultaneous_swap", project(scan, expr.Named(expr.Col("y"), "x"), expr.Named(expr.Col("x"), "y")), expr.GT(expr.Col("x"), expr.I64(2)), []int64{0, 1}},
		{"alias_chain", project(project(scan, expr.Named(expr.Col("x"), "a")), expr.Named(expr.Col("a"), "b")), expr.GT(expr.Col("b"), expr.I64(1)), []int64{3, 4}},
		{"computed_overwrite", project(scan, expr.Named(expr.I64(9), "x")), expr.GT(expr.Col("x"), expr.I64(5)), []int64{0, 1, 2, 3, 4}},
		{"nan_alias", project(scan, expr.Named(expr.Col("f"), "alias")), expr.GT(expr.Col("alias"), expr.F64(0)), []int64{3, 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := &logical.Filter{Input: tc.input, Predicate: tc.pred}
			original, err := expressionPushdownIDs(plan)
			if err != nil {
				t.Fatal(err)
			}
			optimized, err := optimizer.Optimize(plan)
			if err != nil {
				t.Fatal(err)
			}
			optimized.Plan, _, err = applyPushdown(context.Background(), optimized.Plan)
			if err != nil {
				t.Fatal(err)
			}
			got, err := expressionPushdownIDs(optimized.Plan)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, tc.want) || !reflect.DeepEqual(got, original) {
				t.Fatalf("original=%v optimized=%v want=%v", original, got, tc.want)
			}
		})
	}
}

func expressionPushdownParquet(tb testing.TB, groups, rows int) source.Source {
	tb.Helper()
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{schema.NewInt64Node("id", arrowparquet.Repetitions.Required, -1), schema.NewInt64Node("x", arrowparquet.Repetitions.Required, -1)}, -1)
	if err != nil {
		tb.Fatal(err)
	}
	path := filepath.Join(tb.TempDir(), "expression-pushdown.parquet")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	writer := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithDictionaryDefault(false))))
	for group := 0; group < groups; group++ {
		values := make([]int64, rows)
		for row := range values {
			values[row] = int64(group*rows + row)
		}
		rg := writer.AppendRowGroup()
		for col := 0; col < 2; col++ {
			c, err := rg.NextColumn()
			if err != nil {
				tb.Fatal(err)
			}
			if _, err = c.(*file.Int64ColumnChunkWriter).WriteBatch(values, nil, nil); err != nil {
				tb.Fatal(err)
			}
			if err = c.Close(); err != nil {
				tb.Fatal(err)
			}
		}
		if err := rg.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		tb.Fatal(err)
	}
	src, err := parquetsource.OpenFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	return src
}

func expressionPushdownScan(plan logical.Plan) *logical.Scan {
	switch n := plan.(type) {
	case *logical.Scan:
		return n
	case *logical.Filter:
		return expressionPushdownScan(n.Input)
	case *logical.ExprProjection:
		return expressionPushdownScan(n.Input)
	case *logical.Projection:
		return expressionPushdownScan(n.Input)
	}
	return nil
}

func TestExpressionPushdownParquetPruningAndErrorBarrier(t *testing.T) {
	src := expressionPushdownParquet(t, 3, 100)
	for _, fallible := range []bool{false, true} {
		t.Run(map[bool]string{false: "alias_prunes_keeps_residual_order", true: "fallible_projection_does_not_hide_overflow"}[fallible], func(t *testing.T) {
			named := []expr.NamedExpr{expr.Named(expr.Col("x"), "alias")}
			if fallible {
				named = append(named, expr.Named(expr.CastTo(expr.Col("x"), memory.TypeInt8), "checked"))
			}
			// Only the first group can match; filtering before projection would hide
			// the overflow in later groups. Safe aliases may prune those groups.
			plan := &logical.Filter{Input: &logical.ExprProjection{Input: &logical.Scan{Source: src}, Exprs: named, IncludeInput: true}, Predicate: expr.LT(expr.Col("alias"), expr.I64(50))}
			optimized, err := optimizer.Optimize(plan)
			if err != nil {
				t.Fatal(err)
			}
			optimized.Plan, _, err = applyPushdown(context.Background(), optimized.Plan)
			if err != nil {
				t.Fatal(err)
			}
			scan := expressionPushdownScan(optimized.Plan)
			if scan == nil {
				t.Fatal("missing scan")
			}
			original, originalErr := expressionPushdownIDs(plan)
			got, gotErr := expressionPushdownIDs(optimized.Plan)
			if fallible {
				if originalErr == nil || gotErr == nil {
					t.Fatalf("overflow must survive: original=%v optimized=%v", originalErr, gotErr)
				}
				if originalErr.Error() != gotErr.Error() {
					t.Fatalf("different errors: original=%v optimized=%v", originalErr, gotErr)
				}
				if scan.PushedPredicate != nil {
					t.Fatalf("fallible projection crossed: %s", scan.PushedPredicate)
				}
				return
			}
			if originalErr != nil || gotErr != nil {
				t.Fatalf("original=%v optimized=%v", originalErr, gotErr)
			}
			if !reflect.DeepEqual(got, original) || len(got) != 50 {
				t.Fatalf("original=%v optimized=%v", original, got)
			}
			if scan.Pushdown.PrunedPartitions != 2 || scan.Pushdown.SelectedRows != 100 {
				t.Fatalf("expected two groups pruned, one group residual-filtered: %+v", scan.Pushdown)
			}
		})
	}
}

func BenchmarkExpressionProjectionPredicatePushdown(b *testing.B) {
	src := expressionPushdownParquet(b, 16, 4096)
	plan := &logical.Filter{Input: &logical.ExprProjection{Input: &logical.Scan{Source: src}, Exprs: []expr.NamedExpr{expr.Named(expr.Col("x"), "alias"), expr.Named(expr.GT(expr.Col("x"), expr.I64(1024)), "computed")}, IncludeInput: true}, Predicate: expr.LT(expr.Col("alias"), expr.I64(2048))}
	optimized, err := optimizer.Optimize(plan)
	if err != nil {
		b.Fatal(err)
	}
	optimized.Plan, _, err = applyPushdown(context.Background(), optimized.Plan)
	if err != nil {
		b.Fatal(err)
	}
	stats, err := src.Stats(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		plan logical.Plan
		rows int
	}{{"unoptimized", plan, int(stats.Rows)}, {"optimized", optimized.Plan, int(expressionPushdownScan(optimized.Plan).Pushdown.SelectedRows)}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(tc.rows), "source_rows/op")
			for i := 0; i < b.N; i++ {
				ids, err := expressionPushdownIDs(tc.plan)
				if err != nil {
					b.Fatal(err)
				}
				if len(ids) != 2048 || ids[2047] != 2047 {
					b.Fatal("incorrect result")
				}
			}
		})
	}
}

func TestExpressionPushdownGuardedCastPreservesInputFilter(t *testing.T) {
	src := expressionPushdownParquet(t, 3, 100)
	// The lower filter is what makes the narrowing cast safe. A later filter
	// cannot be moved ahead of this fallible expression projection.
	plan := &logical.Filter{
		Input: &logical.ExprProjection{
			Input:        &logical.Filter{Input: &logical.Scan{Source: src}, Predicate: expr.LT(expr.Col("x"), expr.I64(100))},
			IncludeInput: true,
			Exprs:        []expr.NamedExpr{expr.Named(expr.CastTo(expr.Col("x"), memory.TypeInt8), "checked"), expr.Named(expr.Col("x"), "alias")},
		},
		Predicate: expr.GT(expr.Col("alias"), expr.I64(49)),
	}
	optimized, err := optimizer.Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	scan := expressionPushdownScan(optimized.Plan)
	if scan.PushedPredicate == nil || scan.PushedPredicate.String() != expr.LT(expr.Col("x"), expr.I64(100)).String() {
		t.Fatalf("only the lower guard may reach scan: %v", scan.PushedPredicate)
	}
	optimized.Plan, _, err = applyPushdown(context.Background(), optimized.Plan)
	if err != nil {
		t.Fatal(err)
	}
	original, err := expressionPushdownIDs(plan)
	if err != nil {
		t.Fatal(err)
	}
	got, err := expressionPushdownIDs(optimized.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, original) || len(got) != 50 || got[0] != 50 || got[49] != 99 {
		t.Fatalf("original=%v optimized=%v", original, got)
	}
}

func TestExpressionPushdownConditionalErrorBarriers(t *testing.T) {
	src := expressionPushdownParquet(t, 3, 100)
	checked := expr.CastTo(expr.CastTo(expr.Col("x"), memory.TypeInt8), memory.TypeInt64)
	for _, tc := range []struct {
		name  string
		value expr.Expr
		fails bool
	}{
		{"if_retained", expr.If(expr.Bool(true), checked, expr.I64(0)), true},
		{"case_retained", expr.Case([]expr.WhenThen{{When: expr.Bool(true), Then: checked}}, expr.I64(0)), true},
		{"coalesce_retained", expr.Coalesce(checked, expr.I64(0)), true},
		{"if_dead_child", expr.If(expr.Bool(false), checked, expr.I64(0)), false},
		{"case_dead_child", expr.Case([]expr.WhenThen{{When: expr.Bool(false), Then: checked}}, expr.I64(0)), false},
		{"coalesce_dead_child", expr.Coalesce(expr.I64(0), checked), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := &logical.Filter{Input: &logical.ExprProjection{Input: &logical.Scan{Source: src}, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(tc.value, "checked")}}, Predicate: expr.LT(expr.Col("x"), expr.I64(50))}
			optimized, err := optimizer.Optimize(plan)
			if err != nil {
				t.Fatal(err)
			}
			scan := expressionPushdownScan(optimized.Plan)
			if scan.PushedPredicate != nil {
				t.Fatalf("conditional with fallible child crossed: %s", scan.PushedPredicate)
			}
			optimized.Plan, _, err = applyPushdown(context.Background(), optimized.Plan)
			if err != nil {
				t.Fatal(err)
			}
			original, originalErr := expressionPushdownIDs(plan)
			got, gotErr := expressionPushdownIDs(optimized.Plan)
			if tc.fails {
				if originalErr == nil || gotErr == nil {
					t.Fatalf("expected retained error: original=%v optimized=%v", originalErr, gotErr)
				}
				if originalErr.Error() != gotErr.Error() {
					t.Fatalf("different errors: original=%v optimized=%v", originalErr, gotErr)
				}
				return
			}
			if originalErr != nil || gotErr != nil || !reflect.DeepEqual(original, got) || len(got) != 50 {
				t.Fatalf("original=%v (%v) optimized=%v (%v)", original, originalErr, got, gotErr)
			}
		})
	}
}

func TestExpressionPushdownFallibleOuterPredicateBarrier(t *testing.T) {
	src := expressionPushdownParquet(t, 3, 100)
	// The outer fallible comparison is evaluated even for rows that the later
	// safe conjunct would reject. Its error must not disappear via pruning.
	plan := &logical.Filter{Input: &logical.ExprProjection{Input: &logical.Scan{Source: src}, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.Col("x"), "alias")}}, Predicate: expr.And(expr.GT(expr.CastTo(expr.Col("alias"), memory.TypeInt8), expr.I64(0)), expr.LT(expr.Col("alias"), expr.I64(50)))}
	optimized, err := optimizer.Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	scan := expressionPushdownScan(optimized.Plan)
	if scan.PushedPredicate != nil {
		t.Fatalf("fallible predicate crossed: %v", scan.PushedPredicate)
	}
	optimized.Plan, _, err = applyPushdown(context.Background(), optimized.Plan)
	if err != nil {
		t.Fatal(err)
	}
	_, originalErr := expressionPushdownIDs(plan)
	_, gotErr := expressionPushdownIDs(optimized.Plan)
	if originalErr == nil || gotErr == nil || originalErr.Error() != gotErr.Error() {
		t.Fatalf("original=%v optimized=%v", originalErr, gotErr)
	}
}
