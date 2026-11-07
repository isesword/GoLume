package optimizer

import (
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func reviewPredicateScan() *logical.Scan {
	return &logical.Scan{Source: &testSource{schema: memory.NewSchema(
		memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "y", Type: memory.TypeInt64},
		memory.Field{Name: "keep", Type: memory.TypeBool, Nullable: true},
	)}}
}

func TestReviewExpressionIdentityMapping(t *testing.T) {
	tests := []struct {
		name      string
		input     logical.Plan
		predicate expr.Expr
		want      string
	}{
		{"mixed computed conjunction", &logical.ExprProjection{Input: reviewPredicateScan(), IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.Col("keep"), "alias"), expr.Named(expr.GT(expr.Col("x"), expr.I64(0)), "flag")}}, expr.And(expr.Col("alias"), expr.Col("flag")), ""},
		{"swap", &logical.ExprProjection{Input: reviewPredicateScan(), IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.Col("y"), "x"), expr.Named(expr.Col("x"), "y")}}, expr.GT(expr.Col("x"), expr.Col("y")), "(y > x)"},
		{"chain", &logical.ExprProjection{Input: &logical.ExprProjection{Input: reviewPredicateScan(), Exprs: []expr.NamedExpr{expr.Named(expr.Col("x"), "a")}}, Exprs: []expr.NamedExpr{expr.Named(expr.Col("a"), "b")}}, expr.GT(expr.Col("b"), expr.I64(0)), "(x > 0)"},
		{"safe unrelated output", &logical.ExprProjection{Input: reviewPredicateScan(), IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.GT(expr.Col("x"), expr.I64(0)), "flag")}}, expr.Col("keep"), "keep"},
		{"computed overwrite", &logical.ExprProjection{Input: reviewPredicateScan(), IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.Bool(false), "keep")}}, expr.Col("keep"), ""},
		{"unrelated fallible output", &logical.ExprProjection{Input: reviewPredicateScan(), IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.CastTo(expr.Col("x"), memory.TypeInt8), "checked")}}, expr.Col("keep"), ""},
		{"fallible predicate", &logical.ExprProjection{Input: reviewPredicateScan(), Exprs: []expr.NamedExpr{expr.Named(expr.Col("x"), "a")}}, expr.GT(expr.CastTo(expr.Col("a"), memory.TypeInt8), expr.I64(0)), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := tt.predicate.String()
			result, err := Optimize(&logical.Filter{Input: tt.input, Predicate: tt.predicate})
			if err != nil {
				t.Fatal(err)
			}
			scan := findScan(t, result.Plan)
			got := ""
			if scan.PushedPredicate != nil {
				got = scan.PushedPredicate.String()
			}
			if got != tt.want {
				t.Fatalf("pushed=%q want %q", got, tt.want)
			}
			residual, ok := result.Plan.(*logical.Filter)
			if !ok || residual.Predicate.String() != original {
				t.Fatalf("residual changed: %v", result.Plan)
			}
		})
	}
}

func TestReviewPredicateASTSlicesAreIndependent(t *testing.T) {
	original := expr.CaseExpr{Whens: []expr.WhenThen{{When: expr.Col("a"), Then: expr.InExpr{Value: expr.Col("b"), Values: []expr.Expr{expr.Col("b"), expr.I64(1)}}}}, Else: expr.CoalesceExpr{Values: []expr.Expr{expr.Col("a"), expr.Bool(false)}}}
	before := original.String()
	rewritten, ok := renamePredicateColumns(original, map[string]string{"a": "keep", "b": "x"})
	if !ok {
		t.Fatal("rewrite failed")
	}
	if original.String() != before {
		t.Fatal("rewrite mutated original")
	}
	c := rewritten.(expr.CaseExpr)
	in := c.Whens[0].Then.(expr.InExpr)
	in.Values[0] = expr.I64(99)
	co := c.Else.(expr.CoalesceExpr)
	co.Values[0] = expr.Bool(true)
	c.Whens[0].When = expr.Bool(false)
	if original.String() != before {
		t.Fatal("rewritten slice aliases original")
	}
	if _, ok := renamePredicateColumns(expr.Col("missing"), map[string]string{}); ok {
		t.Fatal("missing mapping accepted")
	}
	if _, ok := renamePredicateColumns(&expr.Column{Name: "a"}, map[string]string{"a": "keep"}); ok {
		t.Fatal("unsupported node accepted")
	}
}

func TestReviewAliasBindingPreservesNativeMetadata(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeUint64, memory.TypeFloat32, memory.TypeString} {
		t.Run(typ.String(), func(t *testing.T) {
			field := memory.Field{Name: "x", Type: typ, Nullable: true, ArrowType: "review-logical-metadata"}
			if typ == memory.TypeString {
				field.Encoding = memory.EncodingDictionary
				field.DictionaryDomain = "review-domain"
			}
			scan := &logical.Scan{Source: &testSource{schema: memory.NewSchema(field)}}
			projection := &logical.ExprProjection{Input: scan, Exprs: []expr.NamedExpr{expr.Named(expr.Col("x"), "alias")}}
			predicate := expr.IsNull(expr.Col("alias"))
			rewritten := predicateThroughExpressions(projection, predicate)
			if rewritten == nil {
				t.Fatal("alias was blocked")
			}
			output, err := logical.InferSchema(projection)
			if err != nil {
				t.Fatal(err)
			}
			before, err := expr.Bind(output, predicate)
			if err != nil {
				t.Fatal(err)
			}
			after, err := expr.Bind(scan.Source.Schema(), rewritten)
			if err != nil {
				t.Fatal(err)
			}
			a, b := before.Children[0].Field, after.Children[0].Field
			a.Name = ""
			b.Name = ""
			if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(before.Field, after.Field) {
				t.Fatalf("binding metadata changed: %+v vs %+v", before, after)
			}
		})
	}
}

func TestReviewExpressionTranslationStopsAtBarriers(t *testing.T) {
	barriers := map[string]func(logical.Plan) logical.Plan{
		"aggregate": func(p logical.Plan) logical.Plan {
			return &logical.Aggregate{Input: p, GroupBy: []string{"keep"}, Aggregates: []expr.Aggregate{expr.Count("n")}}
		},
		"limit": func(p logical.Plan) logical.Plan { return &logical.Limit{Input: p, N: 1} },
		"topk": func(p logical.Plan) logical.Plan {
			return &logical.TopK{Input: p, K: 1, By: []logical.SortKey{{Expr: expr.Col("x")}}}
		},
		"fallible sort": func(p logical.Plan) logical.Plan {
			return &logical.Sort{Input: p, By: []logical.SortKey{{Expr: expr.CastTo(expr.Col("x"), memory.TypeInt8)}}}
		},
		"window": func(p logical.Plan) logical.Plan {
			return &logical.Window{Input: p, Spec: logical.WindowSpec{By: []logical.SortKey{{Expr: expr.Col("x")}}}, Exprs: []logical.WindowExpr{{Function: logical.WindowRowNumber, Alias: "rank"}}}
		},
	}
	for name, barrier := range barriers {
		for _, inner := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "", true: " inner filter"}[inner], func(t *testing.T) {
				var child logical.Plan = reviewPredicateScan()
				if inner {
					child = &logical.Filter{Input: child, Predicate: expr.GT(expr.Col("y"), expr.I64(0))}
				}
				p := &logical.Filter{Input: &logical.ExprProjection{Input: barrier(child), Exprs: []expr.NamedExpr{expr.Named(expr.Col("keep"), "alias")}}, Predicate: expr.Col("alias")}
				result, err := Optimize(p)
				if err != nil {
					t.Fatal(err)
				}
				pushed := findScan(t, result.Plan).PushedPredicate
				if inner {
					if pushed == nil || pushed.String() != "(y > 0)" {
						t.Fatalf("inner filter lost: %v", pushed)
					}
				} else if pushed != nil {
					t.Fatalf("crossed barrier: %v", pushed)
				}
			})
		}
	}
}
