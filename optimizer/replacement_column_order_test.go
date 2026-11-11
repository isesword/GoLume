package optimizer

import (
	"fmt"
	"reflect"
	"testing"

	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
)

func TestReplacementColumnOrderInterleavedOutputs(t *testing.T) {
	// Enumerate expression orders and downstream demand independently. Old
	// columns keep their original positions; new ones append in expression order.
	aliases := []string{"a", "x", "b", "y"}
	var visit func(int)
	visit = func(start int) {
		if start < len(aliases) {
			for i := start; i < len(aliases); i++ {
				aliases[start], aliases[i] = aliases[i], aliases[start]
				visit(start + 1)
				aliases[start], aliases[i] = aliases[i], aliases[start]
			}
			return
		}
		for _, window := range []bool{true, false} {
			for demand := 0; demand < 32; demand++ {
				t.Run(fmt.Sprintf("window=%t/%v/demand=%d", window, aliases, demand), func(t *testing.T) {
					schema := memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeInt64}, memory.Field{Name: "a", Type: memory.TypeString}, memory.Field{Name: "b", Type: memory.TypeBool})
					scan := &logical.Scan{Source: &testSource{schema: schema}}
					var plan logical.Plan
					if window {
						node := &logical.Window{Input: scan, Spec: logical.WindowSpec{By: []logical.SortKey{{Expr: expr.Col("value")}}}}
						for _, alias := range aliases {
							node.Exprs = append(node.Exprs, logical.WindowExpr{Function: logical.WindowRowNumber, Alias: alias})
						}
						plan = node
					} else {
						node := &logical.ExprProjection{Input: scan, IncludeInput: true}
						for _, alias := range aliases {
							node.Exprs = append(node.Exprs, expr.NamedExpr{Expr: expr.Col("value"), Alias: alias})
						}
						plan = node
					}
					// Zero denotes the unprojected full schema; every nonzero bitmask
					// selects a deliberately reversed subset to verify parent ownership.
					if demand != 0 {
						var columns []string
						for i, name := range []string{"y", "x", "b", "a", "value"} {
							if demand&(1<<i) != 0 {
								columns = append(columns, name)
							}
						}
						plan = &logical.Projection{Input: plan, Columns: columns}
					}
					want, err := logical.InferSchema(plan)
					if err != nil {
						t.Fatal(err)
					}
					result, err := Optimize(plan)
					if err != nil {
						t.Fatal(err)
					}
					got, err := logical.InferSchema(result.Plan)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("schema=%v want=%v", schemaNames(got), schemaNames(want))
					}
					// Pruning replaced storage must stay effective after reordering.
					var input logical.Plan = result.Plan
					for {
						switch n := input.(type) {
						case *logical.Projection:
							input = n.Input
						case *logical.Window:
							input = n.Input
						case *logical.ExprProjection:
							input = n.Input
						case *logical.Scan:
							if !reflect.DeepEqual(n.Projection, []string{"value"}) {
								t.Fatalf("scan=%v want [value]", n.Projection)
							}
							input = nil
						default:
							t.Fatalf("unexpected node %T", input)
						}
						if input == nil {
							break
						}
					}
				})
			}
		}
	}
	visit(0)
}

func TestReplacementColumnOrderFastPath(t *testing.T) {
	input := memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeInt64}, memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64})
	for _, names := range [][]string{{"value", "a", "b", "x", "y"}, {"value", "a", "x", "b", "y"}, {"x", "value", "a", "b", "y"}, {"value", "b", "a", "x", "y"}, {"x", "y"}, {"a", "x"}} {
		t.Run(fmt.Sprint(names), func(t *testing.T) {
			fields := make([]memory.Field, len(names))
			for i, name := range names {
				fields[i] = memory.Field{Name: name, Type: memory.TypeInt64}
			}
			node := &logical.Scan{Source: &testSource{schema: memory.NewSchema(fields...)}}
			var rules []string
			out, err := restoreReplacementOrder(node, input, "reorder", &rules)
			if err != nil {
				t.Fatal(err)
			}
			var want []string
			present := map[string]bool{}
			for _, name := range names {
				present[name] = true
			}
			for _, field := range input.Fields {
				if present[field.Name] {
					want = append(want, field.Name)
				}
			}
			for _, name := range names {
				if _, ok := input.Field(name); !ok {
					want = append(want, name)
				}
			}
			got, err := logical.InferSchema(out)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(schemaNames(got), want) {
				t.Fatalf("got=%v want=%v", schemaNames(got), want)
			}
			stable := reflect.DeepEqual(names, want)
			if stable != (out == node) || stable != (len(rules) == 0) {
				t.Fatalf("stable=%t node retained=%t rules=%v", stable, out == node, rules)
			}
		})
	}
}
