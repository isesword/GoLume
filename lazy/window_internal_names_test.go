package lazy

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

// Internal names are an implementation detail, not a reserved user namespace.
// Check both the original candidate and suffixes occupied by other outputs.
func TestWindowInternalNamesPublicAPI(t *testing.T) {
	cases := []struct {
		name            string
		inputs, outputs []string
		computed        bool
	}{
		{"computed", nil, []string{"__gocolumn_order_key_0"}, true},
		{"computed_suffixes", []string{"__gocolumn_order_key_0"}, []string{"__gocolumn_order_key_0_1", "__gocolumn_order_key_0_2"}, true},
		{"ordinal", nil, []string{"__golume_window_ordinal"}, false},
		{"ordinal_suffixes", []string{"__golume_window_ordinal"}, []string{"__golume_window_ordinal_1", "__golume_window_ordinal_2"}, false},
		{"combined_replacement", []string{"__gocolumn_order_key_0", "__golume_window_ordinal"}, []string{"__gocolumn_order_key_0", "__golume_window_ordinal", "__gocolumn_order_key_0_1", "__golume_window_ordinal_1"}, true},
	}
	for _, tc := range cases {
		for _, mode := range []string{"resident", "external"} {
			for _, batchRows := range []int{1, 2, 5} {
				t.Run(fmt.Sprintf("%s/%s/batch%d", tc.name, mode, batchRows), func(t *testing.T) {
					values := []int64{30, 10, 40, 20, 10}
					fields := []memory.Field{{Name: "value", Type: memory.TypeInt64}}
					for _, name := range tc.inputs {
						fields = append(fields, memory.Field{Name: name, Type: memory.TypeInt64})
					}
					var batches []*memory.Batch
					for start := 0; start < len(values); start += batchRows {
						end := min(start+batchRows, len(values))
						b := memory.NewBatch()
						b.Rows = end - start
						b.Int64s["value"] = &memory.Int64Vector{Values: append([]int64(nil), values[start:end]...)}
						for _, name := range tc.inputs {
							b.Int64s[name] = &memory.Int64Vector{Values: append([]int64(nil), values[start:end]...)}
						}
						batches = append(batches, b)
					}
					src, err := inmemory.New(memory.NewSchema(fields...), batches...)
					if err != nil {
						t.Fatal(err)
					}
					opts := engine.DefaultOptions()
					opts.MemoryLimitBytes = 8 << 20
					if mode == "external" {
						opts.ExecutionPolicy = engine.ExecutionBounded
						opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20}
					}
					session := NewSession(opts)
					defer session.Close()
					keys := OrderBy(Asc("value"))
					if tc.computed {
						keys = OrderBy(AscExpr(Col("value").Add(Lit(1))))
					}
					exprs := make([]WindowExpr, len(tc.outputs))
					for i, name := range tc.outputs {
						exprs[i] = RowNumber(name)
					}
					query := session.FromSource(src).WithWindows(NewWindow(nil, keys), exprs...)
					expectedSchema, err := query.Schema()
					if err != nil {
						t.Fatal(err)
					}
					plan, err := query.ExplainPhysical(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					if !strings.Contains(plan, "strategy="+map[string]string{"resident": "resident", "external": "external-sort-restore"}[mode]) {
						t.Fatalf("wrong strategy: %s", plan)
					}
					prepared, err := query.Prepare(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer prepared.Close()
					for repeat := 0; repeat < 2; repeat++ {
						result, err := prepared.Execute(context.Background())
						if err != nil {
							t.Fatal(err)
						}
						func() {
							defer result.Close()
							if !reflect.DeepEqual(result.Schema(), expectedSchema) {
								t.Errorf("schema=%+v want %+v", result.Schema(), expectedSchema)
							}
							if got := frameInt64Column(t, result, "value"); !reflect.DeepEqual(got, values) {
								t.Errorf("original rows=%v want %v", got, values)
							}
							for _, name := range tc.outputs {
								if got := frameInt64Column(t, result, name); !reflect.DeepEqual(got, []int64{4, 1, 5, 3, 2}) {
									t.Errorf("output %s=%v", name, got)
								}
							}
							for _, name := range tc.inputs {
								replaced := false
								for _, out := range tc.outputs {
									replaced = replaced || out == name
								}
								if !replaced {
									if got := frameInt64Column(t, result, name); !reflect.DeepEqual(got, values) {
										t.Errorf("input %s=%v", name, got)
									}
								}
							}
							for _, b := range result.batches() {
								if len(b.Int64s) != len(expectedSchema.Fields) {
									t.Errorf("batch contains %d columns, want %d", len(b.Int64s), len(expectedSchema.Fields))
								}
							}
						}()
					}
					prepared.Close()
					if used := session.Engine().Memory().Used(); used != 0 {
						t.Errorf("leaked %d bytes", used)
					}
				})
			}
		}
	}
}

// Reserving result names must not make results visible to expressions in the
// same window. This also checks multiple computed keys use one private scope.
func TestOrderKeyPlanReservedOutputScope(t *testing.T) {
	input := memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeInt64}, memory.Field{Name: "__gocolumn_order_key_0", Type: memory.TypeInt64})
	outputs := memory.NewSchema(memory.Field{Name: "result", Type: memory.TypeInt64}, memory.Field{Name: "__gocolumn_order_key_0_1", Type: memory.TypeInt64}, memory.Field{Name: "__gocolumn_order_key_0_2", Type: memory.TypeInt64}, memory.Field{Name: "__gocolumn_order_key_1", Type: memory.TypeInt64})
	plan, err := newOrderKeyPlan(input, NewWindow(nil, OrderBy(AscExpr(Col("value").Add(Lit(1))), AscExpr(Col("value").Add(Lit(2))))).By, outputs)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.hiddenColumns, []string{"__gocolumn_order_key_0_3", "__gocolumn_order_key_1_1"}) {
		t.Fatalf("allocated %v", plan.hiddenColumns)
	}
	if _, err := newOrderKeyPlan(input, NewWindow(nil, OrderBy(AscExpr(Col("result").Add(Lit(1))))).By, outputs); err == nil {
		t.Fatal("same-window output became visible to expression binding")
	}
}
