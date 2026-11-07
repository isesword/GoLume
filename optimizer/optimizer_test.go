package optimizer

import (
	"context"
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
)

type testSource struct{ schema memory.Schema }

func (s *testSource) Schema() memory.Schema                                { return s.schema }
func (*testSource) Stats(context.Context) (source.Stats, error)            { return source.Stats{}, nil }
func (*testSource) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (*testSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return nil, nil
}

func TestProjectionAndPredicatePushdown(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
		memory.Field{Name: "unused", Type: memory.TypeInt64},
	)}
	plan := &logical.Aggregate{
		Input: &logical.Filter{
			Input:     &logical.Scan{Source: src},
			Predicate: expr.GT(expr.Col("pred"), expr.Lit(10)),
		},
		GroupBy: []string{"key"},
		Aggregates: []expr.Aggregate{
			expr.Sum("value", "sum"),
			expr.Count("rows"),
		},
	}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	scan := findScan(t, got.Plan)
	wantCols := []string{"pred", "key", "value"}
	if !equalStrings(scan.Projection, wantCols) {
		t.Fatalf("projection=%v want=%v", scan.Projection, wantCols)
	}
	if scan.PushedPredicate == nil || scan.PushedPredicate.String() != "(pred > 10)" {
		t.Fatalf("pushed predicate=%v", scan.PushedPredicate)
	}
	if contains(scan.Projection, "unused") {
		t.Fatalf("unused column was not pruned: %v", scan.Projection)
	}
}

func TestFilterMergePreservesResidualAndPushdown(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)}
	plan := &logical.Aggregate{
		Input: &logical.Filter{
			Input: &logical.Filter{
				Input:     &logical.Scan{Source: src},
				Predicate: expr.GT(expr.Col("pred"), expr.Lit(10)),
			},
			Predicate: expr.LT(expr.Col("pred"), expr.Lit(20)),
		},
		GroupBy:    []string{"key"},
		Aggregates: []expr.Aggregate{expr.Count("rows")},
	}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	agg := got.Plan.(*logical.Aggregate)
	filter, ok := agg.Input.(*logical.Filter)
	if !ok {
		t.Fatalf("aggregate input=%T want *logical.Filter", agg.Input)
	}
	if _, nested := filter.Input.(*logical.Filter); nested {
		t.Fatal("adjacent filters were not merged")
	}
	scan := findScan(t, got.Plan)
	if scan.PushedPredicate == nil {
		t.Fatal("expected pushed predicate")
	}
	if got := scan.PushedPredicate.String(); got != "((pred > 10) AND (pred < 20))" {
		t.Fatalf("predicate=%s", got)
	}
}

func findScan(t *testing.T, plan logical.Plan) *logical.Scan {
	t.Helper()
	for {
		switch n := plan.(type) {
		case *logical.Scan:
			return n
		case *logical.Filter:
			plan = n.Input
		case *logical.Projection:
			plan = n.Input
		case *logical.ExprProjection:
			plan = n.Input
		case *logical.Sort:
			plan = n.Input
		case *logical.TopK:
			plan = n.Input
		case *logical.Limit:
			plan = n.Input
		case *logical.Aggregate:
			plan = n.Input
		default:
			t.Fatalf("unexpected plan %T", plan)
		}
	}
}

func contains(in []string, value string) bool {
	for _, s := range in {
		if s == value {
			return true
		}
	}
	return false
}

func TestExpressionProjectionPushesDependenciesAndPrunesUnused(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64},
		memory.Field{Name: "b", Type: memory.TypeInt64},
		memory.Field{Name: "c", Type: memory.TypeInt64},
	)}
	plan := &logical.ExprProjection{
		Input: &logical.Scan{Source: src},
		Exprs: []expr.NamedExpr{
			expr.Named(expr.Col("a").Add(expr.Col("b")), "score"),
			expr.Named(expr.Col("c").Mul(expr.Lit(2)), "unused_score"),
		},
	}
	outer := &logical.Projection{Input: plan, Columns: []string{"score"}}
	got, err := Optimize(outer)
	if err != nil {
		t.Fatal(err)
	}
	scan := findScan(t, got.Plan)
	if want := []string{"a", "b"}; !equalStrings(scan.Projection, want) {
		t.Fatalf("scan projection=%v want=%v", scan.Projection, want)
	}
	if contains(scan.Projection, "c") {
		t.Fatalf("unused expression dependency c was not pruned: %v", scan.Projection)
	}
	if !contains(got.AppliedRules, "expression-pruning") {
		t.Fatalf("rules=%v missing expression-pruning", got.AppliedRules)
	}
}

func TestProjectionPushdownNarrowsIntermediateProjection(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "c0", Type: memory.TypeInt64},
		memory.Field{Name: "c1", Type: memory.TypeInt64},
		memory.Field{Name: "c2", Type: memory.TypeInt64},
		memory.Field{Name: "c3", Type: memory.TypeInt64},
	)}
	plan := &logical.Aggregate{
		Input:      &logical.Projection{Input: &logical.Scan{Source: src}, Columns: []string{"c0", "c1", "c2", "c3"}},
		GroupBy:    []string{"c1", "c2"},
		Aggregates: []expr.Aggregate{expr.Sum("c3", "total")},
	}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	scan := findScan(t, got.Plan)
	if want := []string{"c1", "c2", "c3"}; !equalStrings(scan.Projection, want) {
		t.Fatalf("scan projection=%v want=%v", scan.Projection, want)
	}
	schema, err := logical.InferSchema(got.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"c1", "c2", "total"}; !equalStrings(schemaNames(schema), want) {
		t.Fatalf("schema=%v want=%v", schemaNames(schema), want)
	}
}

func TestStackedProjectionsSeparatedByFilterStayConsistent(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "c0", Type: memory.TypeInt64},
		memory.Field{Name: "c1", Type: memory.TypeInt64},
		memory.Field{Name: "c2", Type: memory.TypeInt64},
		memory.Field{Name: "c3", Type: memory.TypeInt64},
	)}
	plan := &logical.Projection{
		Input: &logical.Filter{
			Input:     &logical.Projection{Input: &logical.Scan{Source: src}, Columns: []string{"c0", "c1", "c2", "c3"}},
			Predicate: expr.GT(expr.Col("c0"), expr.Lit(0)),
		},
		Columns: []string{"c0", "c1"},
	}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	scan := findScan(t, got.Plan)
	if want := []string{"c0", "c1"}; !equalStrings(scan.Projection, want) {
		t.Fatalf("scan projection=%v want=%v", scan.Projection, want)
	}
	schema, err := logical.InferSchema(got.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"c0", "c1"}; !equalStrings(schemaNames(schema), want) {
		t.Fatalf("schema=%v want=%v", schemaNames(schema), want)
	}
}

func TestWithColumnsReplacementPreservesColumnOrder(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64},
		memory.Field{Name: "b", Type: memory.TypeInt64},
		memory.Field{Name: "c", Type: memory.TypeInt64},
	)}
	plan := &logical.ExprProjection{
		Input:        &logical.Scan{Source: src},
		IncludeInput: true,
		Exprs:        []expr.NamedExpr{expr.Named(expr.Col("b").Add(expr.Lit(1)), "a")},
	}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := logical.InferSchema(got.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b", "c"}; !equalStrings(schemaNames(schema), want) {
		t.Fatalf("schema=%v want=%v: replaced column must keep its original position", schemaNames(schema), want)
	}
	scan := findScan(t, got.Plan)
	if want := []string{"b", "c"}; !equalStrings(scan.Projection, want) {
		t.Fatalf("scan projection=%v want=%v: overwritten input column must stay pruned", scan.Projection, want)
	}
}

func TestChainedWithColumnsReplacementKeepsOriginalPosition(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64},
		memory.Field{Name: "b", Type: memory.TypeInt64},
		memory.Field{Name: "c", Type: memory.TypeInt64},
	)}
	plan := &logical.ExprProjection{
		Input: &logical.ExprProjection{
			Input:        &logical.Scan{Source: src},
			IncludeInput: true,
			Exprs:        []expr.NamedExpr{expr.Named(expr.Col("b").Add(expr.Lit(1)), "a")},
		},
		IncludeInput: true,
		Exprs:        []expr.NamedExpr{expr.Named(expr.Col("c").Mul(expr.Lit(2)), "a")},
	}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := logical.InferSchema(got.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b", "c"}; !equalStrings(schemaNames(schema), want) {
		t.Fatalf("schema=%v want=%v: chained replacement must keep the original position", schemaNames(schema), want)
	}
	scan := findScan(t, got.Plan)
	if want := []string{"b", "c"}; !equalStrings(scan.Projection, want) {
		t.Fatalf("scan projection=%v want=%v", scan.Projection, want)
	}
	projection, ok := got.Plan.(*logical.Projection)
	if !ok {
		t.Fatalf("plan=%T want *logical.Projection restoring replaced column order", got.Plan)
	}
	replacement, ok := projection.Input.(*logical.ExprProjection)
	if !ok {
		t.Fatalf("projection input=%T want *logical.ExprProjection", projection.Input)
	}
	if len(replacement.Exprs) != 1 || replacement.Exprs[0].Expr.String() != "(c * 2)" {
		t.Fatalf("replacement exprs=%v: dead first replacement must be eliminated", replacement.Exprs)
	}
	if !contains(got.AppliedRules, "expression-projection-elimination") {
		t.Fatalf("rules=%v missing expression-projection-elimination", got.AppliedRules)
	}
}

func TestSelectOverWithColumnsReplacementPreservesOrder(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64},
		memory.Field{Name: "b", Type: memory.TypeInt64},
		memory.Field{Name: "c", Type: memory.TypeInt64},
	)}
	plan := &logical.Projection{
		Input: &logical.ExprProjection{
			Input:        &logical.Scan{Source: src},
			IncludeInput: true,
			Exprs:        []expr.NamedExpr{expr.Named(expr.Col("b").Add(expr.Lit(1)), "a")},
		},
		Columns: []string{"a", "b"},
	}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := logical.InferSchema(got.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b"}; !equalStrings(schemaNames(schema), want) {
		t.Fatalf("schema=%v want=%v", schemaNames(schema), want)
	}
}

func TestPredicateAboveComputedProjectionRemainsResidual(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64},
		memory.Field{Name: "b", Type: memory.TypeInt64},
	)}
	plan := &logical.Filter{
		Input: &logical.ExprProjection{
			Input: &logical.Scan{Source: src},
			Exprs: []expr.NamedExpr{expr.Named(expr.Col("a").Add(expr.Col("b")), "score")},
		},
		Predicate: expr.GT(expr.Col("score"), expr.Lit(10)),
	}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	scan := findScan(t, got.Plan)
	if scan.PushedPredicate != nil {
		t.Fatalf("computed predicate was pushed below expression boundary: %v", scan.PushedPredicate)
	}
	if want := []string{"a", "b"}; !equalStrings(scan.Projection, want) {
		t.Fatalf("scan projection=%v want=%v", scan.Projection, want)
	}
}

func TestJoinExpressionDependenciesPruneBothInputs(t *testing.T) {
	left := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt32},
		memory.Field{Name: "amount", Type: memory.TypeInt64},
		memory.Field{Name: "unused_left", Type: memory.TypeInt64},
	)}
	right := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt32},
		memory.Field{Name: "bonus", Type: memory.TypeInt64},
		memory.Field{Name: "unused_right", Type: memory.TypeInt64},
	)}
	joinPlan := &logical.Join{
		Left:  &logical.Scan{Source: left},
		Right: &logical.Scan{Source: right},
		Type:  join.Inner,
		Keys:  []logical.JoinKey{{Left: "id", Right: "id"}},
	}
	plan := &logical.Aggregate{
		Input: &logical.ExprProjection{
			Input:        joinPlan,
			IncludeInput: true,
			Exprs:        []expr.NamedExpr{expr.Named(expr.Col("amount").Add(expr.Col("bonus")), "total")},
		},
		GroupBy:    []string{"id"},
		Aggregates: []expr.Aggregate{expr.Sum("total", "sum")},
	}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	agg := got.Plan.(*logical.Aggregate)
	proj := agg.Input.(*logical.ExprProjection)
	joined := proj.Input.(*logical.Join)
	ls := findScan(t, joined.Left)
	rs := findScan(t, joined.Right)
	if want := []string{"id", "amount"}; !equalStrings(ls.Projection, want) {
		t.Fatalf("left projection=%v want=%v", ls.Projection, want)
	}
	if want := []string{"id", "bonus"}; !equalStrings(rs.Projection, want) {
		t.Fatalf("right projection=%v want=%v", rs.Projection, want)
	}
}

func TestSortProjectionKeepsOrderingKeyAndPrunesUnused(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "score", Type: memory.TypeInt64},
		memory.Field{Name: "unused", Type: memory.TypeInt64},
	)}
	plan := &logical.Projection{Input: &logical.Sort{Input: &logical.Scan{Source: src}, By: []logical.SortKey{{Expr: expr.Col("score"), Descending: true, NullsLast: true}}}, Columns: []string{"id"}}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	scan := findScan(t, got.Plan)
	if want := []string{"id", "score"}; !equalStrings(scan.Projection, want) {
		t.Fatalf("scan projection=%v want=%v", scan.Projection, want)
	}
	if contains(scan.Projection, "unused") {
		t.Fatalf("unused survived: %v", scan.Projection)
	}
}

func TestExpressionSortProjectionKeepsDependenciesAndPrunesUnused(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "a", Type: memory.TypeInt64},
		memory.Field{Name: "b", Type: memory.TypeInt64},
		memory.Field{Name: "unused", Type: memory.TypeInt64},
	)}
	plan := &logical.Projection{Input: &logical.Sort{
		Input: &logical.Scan{Source: src},
		By:    []logical.SortKey{{Expr: expr.Add(expr.Col("a"), expr.Col("b")), Descending: true, NullsLast: true}},
	}, Columns: []string{"id"}}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	scan := findScan(t, got.Plan)
	if want := []string{"id", "a", "b"}; !equalStrings(scan.Projection, want) {
		t.Fatalf("scan projection=%v want=%v", scan.Projection, want)
	}
	if contains(scan.Projection, "unused") {
		t.Fatalf("unused survived: %v", scan.Projection)
	}
}

func TestSortLimitRewritesToTopK(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(memory.Field{Name: "score", Type: memory.TypeInt64})}
	plan := &logical.Limit{Input: &logical.Sort{Input: &logical.Scan{Source: src}, By: []logical.SortKey{{Expr: expr.Col("score"), Descending: true, NullsLast: true}}}, N: 10}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Plan.(*logical.TopK); !ok {
		t.Fatalf("plan=%T want *logical.TopK", got.Plan)
	}
	if !contains(got.AppliedRules, "sort-limit-to-topk") {
		t.Fatalf("rules=%v", got.AppliedRules)
	}
}

func TestExpressionSortLimitRewritesToTopK(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(memory.Field{Name: "score", Type: memory.TypeInt64})}
	plan := &logical.Limit{Input: &logical.Sort{
		Input: &logical.Scan{Source: src},
		By:    []logical.SortKey{{Expr: expr.Add(expr.Col("score"), expr.Lit(1)), Descending: true, NullsLast: true}},
	}, N: 10}
	got, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	top, ok := got.Plan.(*logical.TopK)
	if !ok {
		t.Fatalf("plan=%T want *logical.TopK", got.Plan)
	}
	if len(top.By) != 1 || top.By[0].Expr == nil || top.By[0].Expr.String() != "(score + 1)" {
		t.Fatalf("top-k keys=%+v", top.By)
	}
}

func TestPredicateDoesNotCrossLimitOrTopK(t *testing.T) {
	src := &testSource{schema: memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})}
	for _, plan := range []logical.Plan{
		&logical.Filter{Input: &logical.Limit{Input: &logical.Scan{Source: src}, N: 5}, Predicate: expr.GT(expr.Col("x"), expr.Lit(1))},
		&logical.Filter{Input: &logical.TopK{Input: &logical.Scan{Source: src}, K: 5, By: []logical.SortKey{{Expr: expr.Col("x"), NullsLast: true}}}, Predicate: expr.GT(expr.Col("x"), expr.Lit(1))},
	} {
		got, err := Optimize(plan)
		if err != nil {
			t.Fatal(err)
		}
		if scan := findScan(t, got.Plan); scan.PushedPredicate != nil {
			t.Fatalf("predicate crossed cardinality boundary: %v", scan.PushedPredicate)
		}
	}
}
