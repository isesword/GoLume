package optimizer

import (
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/logical"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func predicateJoin(typ join.Type, suffix string) *logical.Join {
	scan := func(side string) *logical.Scan {
		return &logical.Scan{Source: &testSource{schema: memory.NewSchema(
			memory.Field{Name: "id", Type: memory.TypeInt64},
			memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true},
			memory.Field{Name: side, Type: memory.TypeBool, Nullable: true},
			memory.Field{Name: "unused_" + side, Type: memory.TypeInt64},
		)}}
	}
	return &logical.Join{Left: scan("l"), Right: scan("r"), Type: typ, Keys: []logical.JoinKey{{Left: "id", Right: "id"}}, Suffix: suffix}
}

func optimizedPredicateJoin(t *testing.T, plan logical.Plan) (Result, *logical.Join) {
	t.Helper()
	before, err := logical.InferSchema(plan)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	after, err := logical.InferSchema(result.Plan)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("schema changed: before=%+v after=%+v error=%v", before, after, err)
	}
	var find func(logical.Plan) *logical.Join
	find = func(p logical.Plan) *logical.Join {
		switch n := p.(type) {
		case *logical.Join:
			return n
		case *logical.Filter:
			return find(n.Input)
		case *logical.Projection:
			return find(n.Input)
		case *logical.ExprProjection:
			return find(n.Input)
		}
		return nil
	}
	n := find(result.Plan)
	if n == nil {
		t.Fatal("join missing")
	}
	return result, n
}

func TestInnerJoinPredicateOwnershipAndResidual(t *testing.T) {
	for _, tc := range []struct {
		name, suffix, left, right string
		predicate                 expr.Expr
	}{
		{"left", "", "(x > 1)", "", expr.GT(expr.Col("x"), expr.I64(1))},
		{"right_default_suffix", "", "", "(x > 1)", expr.GT(expr.Col("x_right"), expr.I64(1))},
		{"right_custom_suffix", "_r", "", "(x > 1)", expr.GT(expr.Col("x_r"), expr.I64(1))},
		{"both_conjuncts", "", "l", "r", expr.And(expr.Col("l"), expr.Col("r"))},
		{"cross_side_residual", "", "l", "", expr.And(expr.Col("l"), expr.GT(expr.Col("x"), expr.Col("x_right")))},
		{"cross_side_or", "", "", "", expr.Or(expr.Col("l"), expr.Col("r"))},
		{"same_side_or", "", "(l OR (x > 1))", "", expr.Or(expr.Col("l"), expr.GT(expr.Col("x"), expr.I64(1)))},
		{"constant", "", "", "", expr.Bool(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := &logical.Filter{Input: predicateJoin(join.Inner, tc.suffix), Predicate: tc.predicate}
			before := logical.Explain(plan)
			result, n := optimizedPredicateJoin(t, plan)
			if result.Plan.(*logical.Filter).Predicate.String() != tc.predicate.String() {
				t.Fatal("original residual was removed or changed")
			}
			for i, side := range []logical.Plan{n.Left, n.Right} {
				want := []string{tc.left, tc.right}[i]
				filter, ok := side.(*logical.Filter)
				if want == "" {
					if ok {
						t.Fatalf("unexpected side %d filter: %s", i, filter.Predicate)
					}
					continue
				}
				if !ok || filter.Predicate.String() != want {
					t.Fatalf("side %d=%v want %s", i, side, want)
				}
				if pushed := findScan(t, side).PushedPredicate; pushed == nil || pushed.String() != want {
					t.Fatalf("side %d missing source annotation: %v", i, pushed)
				}
			}
			if logical.Explain(plan) != before {
				t.Fatal("caller-owned plan mutated")
			}
		})
	}
}

func TestJoinPredicateNativeDecimalBoundary(t *testing.T) {
	for _, native := range []bool{false, true} {
		n := predicateJoin(join.Inner, "")
		scan := n.Right.(*logical.Scan)
		field := memory.Field{Name: "amount", Type: memory.TypeDecimal}
		if native {
			field.DecimalPrecision, field.DecimalScale = 9, 2
		}
		schema := scan.Source.Schema()
		scan.Source = &testSource{schema: memory.NewSchema(append(schema.Fields, field)...)}
		_, right := predicatesThroughJoin(n, expr.GT(expr.Col("id_right"), expr.I64(0)))
		if (right == nil) != native {
			t.Fatalf("native=%v right predicate=%v", native, right)
		}
		// An intermediate projection must not hide an unsafe input node.
		input := &logical.Projection{Input: scan, Columns: []string{"id"}}
		if joinInputInfallible(input) == native {
			t.Fatalf("native=%v hidden payload boundary was lost", native)
		}
	}
}

func TestJoinPredicateAliasesAndCollisionPruning(t *testing.T) {
	n := predicateJoin(join.Inner, "")
	n.Right = &logical.ExprProjection{Input: n.Right, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.Col("x"), "x_right")}}
	// x -> x_right -> x_right_right is a right-output collision chain.
	plan := &logical.Projection{Input: &logical.Filter{
		Input:     &logical.ExprProjection{Input: n, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.Col("x_right_right"), "alias")}},
		Predicate: expr.GT(expr.Col("alias"), expr.I64(1)),
	}, Columns: []string{"id", "alias"}}
	_, optimized := optimizedPredicateJoin(t, plan)
	if got := findScan(t, optimized.Right).PushedPredicate; got == nil || got.String() != "(x > 1)" {
		t.Fatalf("alias/right collision mapping: %v", got)
	}
	for _, side := range []logical.Plan{optimized.Left, optimized.Right} {
		if got := findScan(t, side).Projection; !reflect.DeepEqual(got, []string{"id", "x"}) {
			t.Fatalf("collision/key/predicate dependency lost or unused retained: %v", got)
		}
	}
}

func TestJoinPredicateOuterSemiAntiBoundaries(t *testing.T) {
	for _, typ := range []join.Type{join.Left, join.Right, join.Full, join.Semi, join.Anti} {
		t.Run(typ.String(), func(t *testing.T) {
			n := predicateJoin(typ, "")
			n.Left = &logical.Filter{Input: n.Left, Predicate: expr.GT(expr.Col("x"), expr.I64(1))}
			result, optimized := optimizedPredicateJoin(t, &logical.Filter{Input: n, Predicate: expr.Col("l")})
			if contains(result.AppliedRules, "inner-join-predicate-pushdown") {
				t.Fatal("crossed non-inner join")
			}
			if got := findScan(t, optimized.Left).PushedPredicate; got == nil || got.String() != "(x > 1)" {
				t.Fatalf("independent inner filter=%v", got)
			}
			if got := findScan(t, optimized.Right).PushedPredicate; got != nil {
				t.Fatalf("right annotation=%v", got)
			}
		})
	}
}

func TestJoinPredicateFallibleBoundaries(t *testing.T) {
	checked := expr.GT(expr.CastTo(expr.Col("x"), memory.TypeInt8), expr.I64(0))
	for _, tc := range []struct {
		name      string
		change    func(*logical.Join) logical.Plan
		predicate expr.Expr
	}{
		{"fallible_predicate", func(n *logical.Join) logical.Plan { return n }, expr.And(checked, expr.Col("r"))},
		{"guarded_fallible_predicate", func(n *logical.Join) logical.Plan { return n }, expr.And(expr.Col("l"), checked)},
		{"left_projection", func(n *logical.Join) logical.Plan {
			n.Left = &logical.ExprProjection{Input: n.Left, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.CastTo(expr.Col("x"), memory.TypeInt8), "checked")}}
			return n
		}, expr.Col("r")},
		{"right_filter", func(n *logical.Join) logical.Plan {
			n.Right = &logical.Filter{Input: n.Right, Predicate: checked}
			return n
		}, expr.Col("l")},
		{"fallible_above_join", func(n *logical.Join) logical.Plan {
			return &logical.ExprProjection{Input: n, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.CastTo(expr.Col("x"), memory.TypeInt8), "checked")}}
		}, expr.Col("r")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, n := optimizedPredicateJoin(t, &logical.Filter{Input: tc.change(predicateJoin(join.Inner, "")), Predicate: tc.predicate})
			if contains(result.AppliedRules, "inner-join-predicate-pushdown") {
				t.Fatal("fallible expression crossed")
			}
			for _, side := range []logical.Plan{n.Left, n.Right} {
				if got := findScan(t, side).PushedPredicate; got != nil {
					t.Fatalf("fallible domain pruned: %v", got)
				}
			}
		})
	}
}
