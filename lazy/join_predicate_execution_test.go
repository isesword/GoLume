package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/optimizer"
	"gocolumn/physical"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Compare the complete ordered output, including NULLs and column metadata, to
// direct lowering of the original plan (which never invokes the optimizer).
func joinPredicateRows(plan logical.Plan, mode kernel.Mode) (memory.Schema, []string, error) {
	ctx := context.Background()
	manager := memory.NewBudgetManager(64 << 20)
	kernels := kernel.New(mode)
	src, pending, closers, err := lowerSourcePipeline(ctx, plan, manager, kernels, nil)
	defer func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i].Close()
		}
	}()
	if err != nil {
		return memory.Schema{}, nil, err
	}
	if pending != nil {
		src, err = physical.NewFilterSource(src, pending)
		if err != nil {
			return memory.Schema{}, nil, err
		}
	}
	schema := src.Schema()
	stream, err := source.OpenBatchStream(ctx, src, source.ReadOptions{Kernels: kernels, MemoryManager: manager, BatchRows: 2})
	if err != nil {
		return schema, nil, err
	}
	defer stream.Close()
	var rows []string
	batch := memory.NewBatch()
	for {
		ok, err := stream.Next(ctx, batch)
		if err != nil {
			return schema, nil, err
		}
		if !ok {
			break
		}
		for row := 0; row < batch.Rows; row++ {
			var out strings.Builder
			for _, field := range schema.Fields {
				var value any
				var valid memory.Bitmap
				switch field.Type {
				case memory.TypeInt64:
					v := batch.Int64s[field.Name]
					value, valid = v.Values[row], v.Validity
				case memory.TypeInt8:
					v := batch.Int8s[field.Name]
					value, valid = v.Values[row], v.Validity
				case memory.TypeBool:
					v := batch.Uint32s[field.Name]
					value, valid = v.Values[row], v.Validity
				default:
					return schema, nil, fmt.Errorf("test unsupported field type %s", field.Type)
				}
				if valid != nil && !memory.IsSet(valid, row) {
					value = "NULL"
				}
				fmt.Fprintf(&out, "%s=%v;", field.Name, value)
			}
			rows = append(rows, out.String())
		}
	}
	return schema, rows, nil
}

func executionPredicateJoin(t testing.TB, typ join.Type, ordered bool) *logical.Join {
	t.Helper()
	makeSource := func(right bool) source.Source {
		keys, values, keeps := []int64{1, 1, 2, 4, 9}, []int64{1, 300, 3, 4, 5}, []int64{1, 0, 1, 1, 0}
		name := "l"
		if right {
			keys, values, keeps, name = []int64{1, 1, 3, 4, 9}, []int64{10, 20, 300, 40, 50}, []int64{0, 1, 1, 1, 0}, "r"
		}
		batch := memory.NewBatch()
		batch.Reset(len(keys))
		batch.Int64s["id"] = &memory.Int64Vector{Values: keys, Validity: memory.Bitmap{15}}
		validity := memory.Bitmap{27}
		if right {
			validity = memory.Bitmap{23}
		}
		batch.Int64s["x"] = &memory.Int64Vector{Values: values, Validity: validity}
		batch.Int64s[name] = &memory.Int64Vector{Values: keeps, Validity: memory.Bitmap{23}}
		src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: name, Type: memory.TypeInt64, Nullable: true}), batch)
		if err != nil {
			t.Fatal(err)
		}
		if ordered {
			src.WithOrdering(source.OrderingColumn{Name: "id", NullsLast: true})
		}
		return src
	}
	return &logical.Join{Left: &logical.Scan{Source: makeSource(false)}, Right: &logical.Scan{Source: makeSource(true)}, Type: typ, Keys: []logical.JoinKey{{Left: "id", Right: "id"}}}
}

func compareJoinPredicatePlans(t *testing.T, original logical.Plan, wantError bool, mode kernel.Mode) *optimizer.Result {
	t.Helper()
	optimized, err := optimizer.Optimize(original)
	if err != nil {
		t.Fatal(err)
	}
	optimized.Plan, _, err = applyPushdown(context.Background(), optimized.Plan)
	if err != nil {
		t.Fatal(err)
	}
	beforeSchema, beforeRows, beforeErr := joinPredicateRows(original, mode)
	afterSchema, afterRows, afterErr := joinPredicateRows(optimized.Plan, mode)
	if beforeErr != nil || afterErr != nil {
		if !wantError || beforeErr == nil || afterErr == nil || beforeErr.Error() != afterErr.Error() {
			t.Fatalf("errors original=%v optimized=%v", beforeErr, afterErr)
		}
		return &optimized
	}
	if wantError {
		t.Fatal("expected evaluation error")
	}
	if !reflect.DeepEqual(beforeSchema, afterSchema) || !reflect.DeepEqual(beforeRows, afterRows) {
		t.Fatalf("original=%+v %v optimized=%+v %v", beforeSchema, beforeRows, afterSchema, afterRows)
	}
	return &optimized
}

func TestJoinPredicateExecutionDifferential(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, ordered := range []bool{false, true} {
			for _, typ := range []join.Type{join.Inner, join.Left, join.Right, join.Full, join.Semi, join.Anti} {
				t.Run(fmt.Sprintf("%s/ordered%v/%s", mode, ordered, typ), func(t *testing.T) {
					predicates := []expr.Expr{expr.EQ(expr.Col("l"), expr.I64(1)), expr.IsNull(expr.Col("x")), expr.Or(expr.EQ(expr.Col("l"), expr.I64(1)), expr.GT(expr.Col("x"), expr.I64(2)))}
					if typ != join.Semi && typ != join.Anti {
						predicates = append(predicates, expr.And(expr.EQ(expr.Col("l"), expr.I64(1)), expr.EQ(expr.Col("r"), expr.I64(1))), expr.GT(expr.Col("x_right"), expr.I64(15)), expr.IsNull(expr.Col("x_right")), expr.Or(expr.EQ(expr.Col("l"), expr.I64(1)), expr.EQ(expr.Col("r"), expr.I64(1))), expr.And(expr.EQ(expr.Col("r"), expr.I64(1)), expr.GT(expr.Col("x"), expr.Col("x_right"))))
					}
					for _, predicate := range predicates {
						t.Run(predicate.String(), func(t *testing.T) {
							plan := &logical.Filter{Input: executionPredicateJoin(t, typ, ordered), Predicate: predicate}
							compareJoinPredicatePlans(t, plan, false, mode)
						})
					}
				})
			}
		}
	}
}

func TestJoinPredicateExecutionAliasesSuffixesAndLimits(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		n := executionPredicateJoin(t, join.Inner, ordered)
		n.Suffix = "_r"
		n.Right = &logical.ExprProjection{Input: n.Right, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.Col("x"), "x_r")}}
		plan := &logical.Projection{Input: &logical.Limit{Input: &logical.Filter{
			Input:     &logical.ExprProjection{Input: n, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.Col("x_r_r"), "alias")}},
			Predicate: expr.GT(expr.Col("alias"), expr.I64(15)),
		}, N: 1}, Columns: []string{"id", "alias"}}
		compareJoinPredicatePlans(t, plan, false, kernel.ModeScalar)
	}
}

func TestJoinPredicateExecutionNativeDecimalErrorOrder(t *testing.T) {
	makeSource := func(name string) source.Source {
		field, err := memory.Decimal128Field(name, true, 9, 2)
		if err != nil {
			t.Fatal(err)
		}
		src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, field))
		if err != nil {
			t.Fatal(err)
		}
		return src
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, predicate := range []expr.Expr{
			expr.GT(expr.Col("id"), expr.I64(0)),
			expr.GT(expr.Col("id_right"), expr.I64(0)),
			expr.And(expr.GT(expr.Col("id"), expr.I64(0)), expr.GT(expr.Col("id_right"), expr.I64(0))),
		} {
			n := &logical.Join{Left: &logical.Scan{Source: makeSource("left_amount")}, Right: &logical.Scan{Source: makeSource("right_amount")}, Type: join.Inner, Keys: []logical.JoinKey{{Left: "id", Right: "id"}}}
			plan := &logical.Filter{Input: n, Predicate: predicate}
			_, _, err := joinPredicateRows(plan, mode)
			if err == nil || !strings.Contains(err.Error(), `column "left_amount"`) {
				t.Fatalf("expected left native-decimal rejection, got %v", err)
			}
			result := compareJoinPredicatePlans(t, plan, true, mode)
			joined := result.Plan.(*logical.Filter).Input.(*logical.Join)
			for _, side := range []logical.Plan{joined.Left, joined.Right} {
				if _, ok := side.(*logical.Scan); !ok {
					t.Fatalf("native-decimal input acquired pushed predicate: %T", side)
				}
			}
		}
	}
}

func TestJoinPredicateExecutionBoundedStringKeys(t *testing.T) {
	left, _ := plainJoinParquet(t, "lid", 137)
	right, _ := plainJoinParquet(t, "rid", 75)
	collect := func(plan logical.Plan, mode kernel.Mode, columns []string) []string {
		t.Helper()
		opts := engine.DefaultOptions()
		opts.Workers, opts.KernelMode = 1, mode
		opts.ExecutionPolicy = engine.ExecutionBounded
		opts.MemoryLimitBytes = 4 << 20
		opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 128 << 20}
		eng := engine.New(opts)
		defer eng.Close()
		budget, err := eng.BeginQueryMemory()
		if err != nil {
			t.Fatal(err)
		}
		defer budget.Close()
		ctx := engine.WithExecutionPolicy(memory.WithQueryBudget(context.Background(), budget), engine.ExecutionBounded)
		src, pending, closers, err := lowerSourcePipeline(ctx, plan, eng.Memory(), eng.KernelSet(), eng)
		defer func() {
			for i := len(closers) - 1; i >= 0; i-- {
				closers[i].Close()
			}
		}()
		if err != nil {
			t.Fatal(err)
		}
		if pending != nil {
			src, err = physical.NewFilterSource(src, pending)
			if err != nil {
				t.Fatal(err)
			}
		}
		stream, err := source.OpenBatchStream(ctx, src, source.ReadOptions{Kernels: eng.KernelSet(), MemoryManager: eng.Memory(), BatchRows: 7})
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		var rows []string
		spilled := false
		batch := memory.NewBatch()
		for {
			ok, err := stream.Next(ctx, batch)
			if err != nil {
				t.Fatal(err)
			}
			store, err := budget.SpillManager()
			if err != nil {
				t.Fatal(err)
			}
			if files, ok := store.(*memory.QuerySpillStore); ok && files.Used() > 0 {
				spilled = true
			}
			if !ok {
				break
			}
			for row := 0; row < batch.Rows; row++ {
				var values []string
				for _, name := range columns {
					value := batch.Int64s[name]
					if value.IsValid(row) {
						values = append(values, fmt.Sprint(value.Values[row]))
					} else {
						values = append(values, "NULL")
					}
				}
				rows = append(rows, strings.Join(values, "/"))
			}
		}
		if !spilled {
			t.Fatal("bounded string join did not exercise spill files")
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		for i := len(closers) - 1; i >= 0; i-- {
			if err := closers[i].Close(); err != nil {
				t.Fatal(err)
			}
		}
		closers = nil
		if snapshot := budget.Snapshot(); snapshot.UsedBytes != 0 || snapshot.PeakBytes > opts.MemoryLimitBytes {
			t.Fatalf("join memory budget=%+v", snapshot)
		}
		sort.Strings(rows)
		return rows
	}
	names := []string{"s", "x", "n"}
	for count := 1; count <= len(names); count++ {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			for _, typ := range []join.Type{join.Inner, join.Left, join.Right, join.Full, join.Semi, join.Anti} {
				t.Run(fmt.Sprintf("keys%d/%s/%s", count, mode, typ), func(t *testing.T) {
					keys := make([]logical.JoinKey, count)
					for i := range keys {
						keys[i] = logical.JoinKey{Left: names[i], Right: names[i]}
					}
					n := &logical.Join{Left: &logical.Scan{Source: left}, Right: &logical.Scan{Source: right}, Type: typ, Keys: keys}
					predicate := expr.GT(expr.Col("lid"), expr.I64(10))
					columns := []string{"lid"}
					if typ != join.Semi && typ != join.Anti {
						predicate = expr.And(predicate, expr.LT(expr.Col("rid"), expr.I64(20)))
						columns = append(columns, "rid")
					}
					plan := &logical.Projection{Input: &logical.Filter{Input: n, Predicate: predicate}, Columns: columns}
					optimized, err := optimizer.Optimize(plan)
					if err != nil {
						t.Fatal(err)
					}
					pushed := false
					for _, rule := range optimized.AppliedRules {
						pushed = pushed || rule == "inner-join-predicate-pushdown"
					}
					if pushed != (typ == join.Inner) {
						t.Fatalf("join=%s pushdown=%v rules=%v", typ, pushed, optimized.AppliedRules)
					}
					optimized.Plan, _, err = applyPushdown(context.Background(), optimized.Plan)
					if err != nil {
						t.Fatal(err)
					}
					before, after := collect(plan, mode, columns), collect(optimized.Plan, mode, columns)
					if !reflect.DeepEqual(before, after) {
						t.Fatalf("bounded join rows original=%v optimized=%v", before, after)
					}
				})
			}
		}
	}
}

func TestJoinPredicateExecutionErrorDomains(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		for _, kind := range []string{"unmatched_fallible_predicate", "guarded_null", "left_projection", "right_filter", "above_join", "fallible_before_safe"} {
			t.Run(fmt.Sprintf("ordered%v/%s", ordered, kind), func(t *testing.T) {
				n := executionPredicateJoin(t, join.Inner, ordered)
				checked := expr.GT(expr.CastTo(expr.Col("x"), memory.TypeInt8), expr.I64(0))
				var input logical.Plan = n
				predicate, wantError := expr.Expr(expr.EQ(expr.Col("r"), expr.I64(1))), true
				switch kind {
				case "unmatched_fallible_predicate":
					// Right row 3 overflows but never joins. It must not be newly evaluated.
					predicate = expr.GT(expr.CastTo(expr.Col("x_right"), memory.TypeInt8), expr.I64(0))
					wantError = false
				case "guarded_null":
					predicate = expr.And(expr.EQ(expr.Col("l"), expr.I64(1)), checked)
				case "left_projection":
					n.Left = &logical.ExprProjection{Input: n.Left, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.CastTo(expr.CastTo(expr.Col("x"), memory.TypeInt8), memory.TypeInt64), "checked")}}
				case "right_filter":
					n.Right = &logical.Filter{Input: n.Right, Predicate: expr.GT(expr.CastTo(expr.Col("x"), memory.TypeInt8), expr.I64(0))}
					// Use a multi-group input whose demanded overflows must survive FALSE.
					scan := n.Right.(*logical.Filter).Input.(*logical.Scan)
					scan.Source = expressionPushdownParquet(t, 3, 100)
					predicate = expr.Bool(false)
				case "above_join":
					input = &logical.ExprProjection{Input: n, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(expr.CastTo(expr.CastTo(expr.Col("x"), memory.TypeInt8), memory.TypeInt64), "checked")}}
				case "fallible_before_safe":
					predicate = expr.And(checked, expr.EQ(expr.Col("r"), expr.I64(1)))
				}
				compareJoinPredicatePlans(t, &logical.Filter{Input: input, Predicate: predicate}, wantError, kernel.ModeScalar)
			})
		}
	}
}

func TestJoinPredicateParquetPruningKeepsBothResiduals(t *testing.T) {
	src := expressionPushdownParquet(t, 3, 100)
	n := &logical.Join{Left: &logical.Scan{Source: src}, Right: &logical.Scan{Source: src}, Type: join.Inner, Keys: []logical.JoinKey{{Left: "id", Right: "id"}}}
	plan := &logical.Filter{Input: n, Predicate: expr.And(expr.LT(expr.Col("x"), expr.I64(50)), expr.LT(expr.Col("x_right"), expr.I64(25)))}
	result := compareJoinPredicatePlans(t, plan, false, kernel.ModeScalar)
	outer, ok := result.Plan.(*logical.Filter)
	if !ok {
		t.Fatal("missing post-join residual")
	}
	joined := outer.Input.(*logical.Join)
	for _, side := range []logical.Plan{joined.Left, joined.Right} {
		filter, ok := side.(*logical.Filter)
		if !ok {
			t.Fatal("missing pre-join row residual")
		}
		scan := filter.Input.(*logical.Scan)
		if scan.Pushdown.PrunedPartitions != 2 || scan.Pushdown.SelectedRows != 100 {
			t.Fatalf("source pruning=%+v", scan.Pushdown)
		}
	}
	_, rows, err := joinPredicateRows(result.Plan, kernel.ModeScalar)
	if err != nil || len(rows) != 25 {
		t.Fatalf("rows=%d error=%v", len(rows), err)
	}
}

func BenchmarkInnerJoinPredicatePushdown(b *testing.B) {
	const rows = 16384
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i)
	}
	batch := memory.NewBatch()
	batch.Reset(rows)
	batch.Int64s["id"] = &memory.Int64Vector{Values: values}
	batch.Int64s["x"] = &memory.Int64Vector{Values: values}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "x", Type: memory.TypeInt64}), batch)
	if err != nil {
		b.Fatal(err)
	}
	plan := &logical.Filter{Input: &logical.Join{Left: &logical.Scan{Source: src}, Right: &logical.Scan{Source: src}, Type: join.Inner, Keys: []logical.JoinKey{{Left: "id", Right: "id"}}}, Predicate: expr.LT(expr.Col("x_right"), expr.I64(128))}
	optimized, err := optimizer.Optimize(plan)
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		plan      logical.Plan
		buildRows int
	}{{"original", plan, rows}, {"optimized", optimized.Plan, 128}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(tc.buildRows), "build_rows/op")
			for i := 0; i < b.N; i++ {
				ids, err := expressionPushdownIDs(tc.plan)
				if err != nil || len(ids) != 128 || ids[127] != 127 {
					b.Fatalf("rows=%d error=%v", len(ids), err)
				}
			}
		})
	}
}

func TestJoinPredicatePreservesFallibleAncestorBatchDemand(t *testing.T) {
	// Merge emits at most 4096 joined rows per batch. Selecting one right row
	// from a larger duplicate group originally emits x=1 and x=300 in separate
	// batches. Moving that filter to the build input puts them in one batch:
	// a later cast would then fail before LIMIT(1) can return the first row.
	const rightRows = 4097
	left := memory.NewBatch()
	left.Reset(2)
	left.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 1}}
	left.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 300}}
	leftSource, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "x", Type: memory.TypeInt64}), left)
	if err != nil {
		t.Fatal(err)
	}
	right := memory.NewBatch()
	right.Reset(rightRows)
	keys, keep := make([]int64, rightRows), make([]int64, rightRows)
	for i := range keys {
		keys[i] = 1
	}
	keep[0] = 1
	right.Int64s["id"] = &memory.Int64Vector{Values: keys}
	right.Int64s["keep"] = &memory.Int64Vector{Values: keep}
	rightSource, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "keep", Type: memory.TypeInt64}), right)
	if err != nil {
		t.Fatal(err)
	}
	leftSource.WithOrdering(source.OrderingColumn{Name: "id"})
	rightSource.WithOrdering(source.OrderingColumn{Name: "id"})
	joined := &logical.Join{Left: &logical.Scan{Source: leftSource}, Right: &logical.Scan{Source: rightSource}, Type: join.Inner, Keys: []logical.JoinKey{{Left: "id", Right: "id"}}}
	safe := &logical.Filter{Input: joined, Predicate: expr.EQ(expr.Col("keep"), expr.I64(1))}
	checked := expr.CastTo(expr.Col("x"), memory.TypeInt8)
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, kind := range []string{"filter", "projection", "sort", "topk", "window", "filter_after_limit"} {
			for _, limited := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/limit%v", mode, kind, limited), func(t *testing.T) {
					var plan logical.Plan
					wantError := true
					switch kind {
					case "filter":
						plan = &logical.Filter{Input: safe, Predicate: expr.GT(checked, expr.I64(0))}
						wantError = !limited
					case "projection":
						plan = &logical.ExprProjection{Input: safe, IncludeInput: true, Exprs: []expr.NamedExpr{expr.Named(checked, "checked")}}
						wantError = !limited
					case "sort":
						plan = &logical.Sort{Input: safe, By: []logical.SortKey{{Expr: checked}}}
					case "topk":
						plan = &logical.TopK{Input: safe, K: 1, By: []logical.SortKey{{Expr: checked}}}
					case "window":
						plan = &logical.Window{Input: safe, Spec: logical.WindowSpec{By: []logical.SortKey{{Expr: checked}}}, Exprs: []logical.WindowExpr{{Function: logical.WindowRowNumber, Alias: "rn"}}}
					case "filter_after_limit":
						plan = &logical.Filter{Input: &logical.Limit{Input: safe, N: 1}, Predicate: expr.GT(checked, expr.I64(0))}
						wantError = false
					}
					if limited {
						plan = &logical.Limit{Input: plan, N: 1}
					}
					optimized := compareJoinPredicatePlans(t, plan, wantError, mode)
					for _, rule := range optimized.AppliedRules {
						if rule == "inner-join-predicate-pushdown" {
							t.Fatal("changed batching before a fallible ancestor")
						}
					}
				})
			}
		}
		// The same large group remains eligible when no partial expression
		// consumes its output; the selected row order must still be identical.
		compareJoinPredicatePlans(t, safe, false, mode)
	}
}
