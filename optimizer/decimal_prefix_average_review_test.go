package optimizer

import (
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"strings"
	"testing"
)

type n28OptimizerReviewSource struct{ *testSource }

func (*n28OptimizerReviewSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "key"}}
}
func (*n28OptimizerReviewSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Operator: "Scan", Class: source.StreamPipeline}
}
func n28OptimizerNode(t *testing.T, extra bool) *logical.DecimalPrefixAverage {
	t.Helper()
	d, e := memory.Decimal128Field("d", true, 38, 7)
	if e != nil {
		t.Fatal(e)
	}
	fields := []memory.Field{{Name: "key", Type: memory.TypeInt64}, d}
	if extra {
		fields = append(fields, memory.Field{Name: "unused", Type: memory.TypeInt64})
	}
	var input logical.Plan = &logical.Scan{Source: &n28OptimizerReviewSource{&testSource{schema: memory.NewSchema(fields...)}}}
	if extra {
		input = &logical.Projection{Input: input, Columns: []string{"d", "key"}}
	}
	return &logical.DecimalPrefixAverage{Input: input, Column: "d", OrderBy: "key", Alias: "mean", Precision: 17, Scale: 4, Rounding: aggregate.DecimalAverageHalfEven}
}
func n28ReviewFind(plan logical.Plan) (*logical.DecimalPrefixAverage, *logical.Scan) {
	switch n := plan.(type) {
	case *logical.DecimalPrefixAverage:
		_, scan := n28ReviewFind(n.Input)
		return n, scan
	case *logical.Scan:
		return nil, n
	case *logical.Filter:
		return n28ReviewFind(n.Input)
	case *logical.Projection:
		return n28ReviewFind(n.Input)
	case *logical.ExprProjection:
		return n28ReviewFind(n.Input)
	case *logical.Limit:
		return n28ReviewFind(n.Input)
	default:
		return nil, nil
	}
}
func TestN28ReviewOptimizerUnusedAverageStillValidates(t *testing.T) {
	for _, cols := range [][]string{{"key"}, {"d"}, {"mean"}, {"mean", "key"}} {
		n := n28OptimizerNode(t, true)
		p := &logical.Projection{Input: n, Columns: cols}
		result, e := Optimize(p)
		if e != nil {
			t.Fatal(cols, e)
		}
		avg, scan := n28ReviewFind(result.Plan)
		if avg == nil || scan == nil {
			t.Fatal("prefix barrier removed", logical.Explain(result.Plan))
		}
		if avg.Column != "d" || avg.OrderBy != "key" || avg.Precision != 17 || avg.Scale != 4 || avg.Rounding != aggregate.DecimalAverageHalfEven {
			t.Fatal("config lost", avg)
		}
		input, e := logical.InferSchema(avg.Input)
		if e != nil || len(input.Fields) != 2 {
			t.Fatal(input, e)
		}
		if _, ok := input.Field("key"); !ok {
			t.Fatal("order dependency pruned")
		}
		if _, ok := input.Field("d"); !ok {
			t.Fatal("value validation dependency pruned")
		}
		if contains(scan.Projection, "unused") {
			t.Fatal("unneeded source column retained", scan.Projection)
		}
		output, e := logical.InferSchema(result.Plan)
		if e != nil {
			t.Fatal(e)
		}
		names := []string{}
		for _, f := range output.Fields {
			names = append(names, f.Name)
		}
		if !reflect.DeepEqual(names, cols) {
			t.Fatal("projection order", names, cols)
		}
		// Reoptimization must not widen the source or erase the prefix validation.
		twice, e := Optimize(result.Plan)
		if e != nil {
			t.Fatal(e)
		}
		avg2, scan2 := n28ReviewFind(twice.Plan)
		if avg2 == nil || scan2 == nil || !reflect.DeepEqual(scan.Projection, scan2.Projection) {
			t.Fatal("unstable optimizer", logical.Explain(twice.Plan))
		}
	}
}
func TestN28ReviewOptimizerPredicateBarrier(t *testing.T) {
	for _, predicate := range []expr.Expr{expr.Col("key").Gt(expr.Lit(4)), expr.Bool(false), expr.GT(expr.Col("mean"), expr.Decimal128(decimal128.FromI64(0), 17, 4))} {
		n := n28OptimizerNode(t, false)
		plan := &logical.Filter{Input: n, Predicate: predicate}
		result, e := Optimize(plan)
		if e != nil {
			t.Fatal(e)
		}
		filter, ok := result.Plan.(*logical.Filter)
		if !ok {
			t.Fatal("filter disappeared", logical.Explain(result.Plan))
		}
		avg, ok := filter.Input.(*logical.DecimalPrefixAverage)
		if !ok {
			t.Fatal("filter crossed prefix", logical.Explain(result.Plan))
		}
		_, scan := n28ReviewFind(avg)
		if scan == nil || scan.PushedPredicate != nil {
			t.Fatal("prefix input predicate annotated")
		}
		if _, ok := avg.Input.(*logical.Filter); ok {
			t.Fatal("filter pushed into prefix input")
		}
	}
}
func TestN28ReviewOptimizerInvalidUnusedAverage(t *testing.T) {
	for _, mutate := range []func(*logical.DecimalPrefixAverage){
		func(n *logical.DecimalPrefixAverage) { n.Rounding = 0 },
		func(n *logical.DecimalPrefixAverage) { n.Alias = "key" },
		func(n *logical.DecimalPrefixAverage) { n.Precision = 0 },
		func(n *logical.DecimalPrefixAverage) {
			n.Input = &logical.Filter{Input: n.Input, Predicate: expr.Bool(false)}
		},
	} {
		n := n28OptimizerNode(t, false)
		mutate(n)
		if _, e := Optimize(&logical.Projection{Input: n, Columns: []string{"key"}}); e == nil {
			t.Fatal("invalid unused average optimized away")
		}
	}
}
func TestN28ReviewOptimizerExplainAndCopy(t *testing.T) {
	n := n28OptimizerNode(t, true)
	result, e := Optimize(&logical.Projection{Input: n, Columns: []string{"mean"}})
	if e != nil {
		t.Fatal(e)
	}
	explain := logical.Explain(result.Plan)
	if !strings.Contains(explain, "DECIMAL_PREFIX_AVG") || strings.Contains(explain, "WINDOW") || strings.Contains(explain, "SORT ") {
		t.Fatal(explain)
	}
	avg, _ := n28ReviewFind(result.Plan)
	if avg == n {
		t.Fatal("optimizer retained mutable node alias")
	}
	avg.Alias = "changed"
	avg.Precision = 2
	avg.Rounding = aggregate.DecimalAverageCeil
	if n.Alias != "mean" || n.Precision != 17 || n.Rounding != aggregate.DecimalAverageHalfEven {
		t.Fatal("optimizer mutated original configuration")
	}
}

// COUNT(*) consumes row cardinality rather than named columns. The intervening
// Select must still carry a column through lowering, and prefix validation is
// observable even when its AVG result is unused.
func TestN28ReviewOptimizerPrefixSelectCountCarrier(t *testing.T) {
	for _, count := range []expr.Aggregate{expr.Count("n"), expr.CountColumn("key", "n")} {
		prefix := n28OptimizerNode(t, true)
		selectKey := &logical.Projection{Input: prefix, Columns: []string{"key"}}
		input := &logical.Aggregate{Input: selectKey, Aggregates: []expr.Aggregate{count}}
		var plan logical.Plan = input
		for pass := 0; pass < 2; pass++ {
			result, e := Optimize(plan)
			if e != nil {
				t.Fatal(e)
			}
			agg, ok := result.Plan.(*logical.Aggregate)
			if !ok {
				t.Fatalf("count changed to %T", result.Plan)
			}
			if len(agg.GroupBy) != 0 || len(agg.Aggregates) != 1 || agg.Aggregates[0] != count {
				t.Fatal("count semantics changed", agg)
			}
			avg, scan := n28ReviewFind(agg.Input)
			if avg == nil || scan == nil {
				t.Fatal("COUNT removed prefix validation", logical.Explain(result.Plan))
			}
			// Any surviving physical projection must be lowerable, never an empty
			// Select. The optimizer may instead safely remove the projection itself.
			var check func(logical.Plan)
			check = func(p logical.Plan) {
				switch n := p.(type) {
				case *logical.Projection:
					if len(n.Columns) == 0 {
						t.Fatal("empty projection above prefix", logical.Explain(result.Plan))
					}
					check(n.Input)
				case *logical.ExprProjection:
					if !n.IncludeInput && len(n.Exprs) == 0 {
						t.Fatal("empty expression projection")
					}
					check(n.Input)
				case *logical.DecimalPrefixAverage:
					check(n.Input)
				case *logical.Scan:
				default:
					t.Fatalf("unexpected COUNT input %T", p)
				}
			}
			check(agg.Input)
			fields, e := logical.InferSchema(avg.Input)
			if e != nil || len(fields.Fields) != 2 {
				t.Fatal(fields, e)
			}
			for _, name := range []string{"key", "d"} {
				if _, ok := fields.Field(name); !ok {
					t.Fatal("prefix dependency pruned", name)
				}
			}
			out, e := logical.InferSchema(result.Plan)
			if e != nil || len(out.Fields) != 1 || out.Fields[0].Name != "n" || out.Fields[0].Type != memory.TypeUint64 || out.Fields[0].Nullable {
				t.Fatal("count result schema", out, e)
			}
			if len(selectKey.Columns) != 1 || selectKey.Columns[0] != "key" {
				t.Fatal("input Select mutated")
			}
			plan = result.Plan
		}
	}
}
