package lazy

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestWindowColumnOrderPublicAPI(t *testing.T) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, batchRows := range []int{1, 2, 7} {
			for _, variant := range []string{"interleaved", "new_first", "narrowed", "cascade", "computed"} {
				if mode == "ordered" && variant == "computed" {
					continue
				}
				t.Run(fmt.Sprintf("%s/batch%d/%s", mode, batchRows, variant), func(t *testing.T) {
					values := []int64{3, 0, 1, 3, 2, 0, 0}
					valid := []bool{true, false, true, true, true, false, true}
					if mode == "ordered" {
						values = []int64{0, 1, 2, 3, 3, 0, 0}
						valid = []bool{true, true, true, true, true, false, false}
					}
					src := windowColumnOrderInput(t, values, valid, batchRows, mode == "ordered")
					opts := engine.DefaultOptions()
					opts.MemoryLimitBytes = 8 << 20
					if mode == "external" {
						opts.ExecutionPolicy = engine.ExecutionBounded
						opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20}
					}
					session := NewSession(opts)
					defer session.Close()
					keys := OrderBy(Asc("value"))
					if variant == "computed" {
						keys = OrderBy(AscExpr(Col("value").Add(Lit(1))))
					}
					expressions := []WindowExpr{RowNumber("a"), Shift("value", 0, "x"), Rank("b"), DenseRank("y")}
					if variant == "new_first" {
						expressions = []WindowExpr{Shift("value", 0, "x"), RowNumber("a"), DenseRank("y"), Rank("b")}
					}
					query := session.FromSource(src).WithWindows(NewWindow(nil, keys), expressions...)
					expectedNames := []string{"value", "a", "b", "x", "y"}
					if variant == "narrowed" {
						query = query.Select("y", "b", "x", "a")
						expectedNames = []string{"y", "b", "x", "a"}
					}
					if variant == "cascade" {
						// The same aliases in successive windows are replacements, not
						// duplicates within a single output scope.
						query = query.WithWindows(NewWindow(nil, OrderBy(Asc("value"))), RowNumber("a"), Shift("x", 0, "z"), Rank("b"), DenseRank("w"))
						expectedNames = append(expectedNames, "z", "w")
					}
					expected, err := query.Schema()
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(schemaColumnNames(expected), expectedNames) {
						t.Fatalf("declared order=%v want=%v", schemaColumnNames(expected), expectedNames)
					}
					plan, err := query.ExplainPhysical(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					strategy := map[string]string{"resident": "resident", "ordered": "ordered-state", "external": "external-sort-restore"}[mode]
					if !strings.Contains(plan, "strategy="+strategy) {
						t.Fatalf("missing %s: %s", strategy, plan)
					}
					prepared, err := query.Prepare(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer prepared.Close()
					if !reflect.DeepEqual(prepared.Schema(), expected) {
						t.Errorf("prepared order=%v want=%v", schemaColumnNames(prepared.Schema()), expectedNames)
					}
					expectedValues := windowColumnOrderExpected(values, valid)
					if variant == "cascade" {
						expectedValues["z"] = expectedValues["x"]
						expectedValues["w"] = expectedValues["y"]
					}
					for repeat := 0; repeat < 2; repeat++ {
						result, err := prepared.Execute(context.Background())
						if err != nil {
							t.Fatal(err)
						}
						func() {
							defer result.Close()
							if !reflect.DeepEqual(result.Schema(), expected) {
								t.Errorf("result order=%v want=%v", result.Columns(), expectedNames)
							}
							for _, name := range expectedNames {
								column, err := result.Column(name)
								if err != nil {
									t.Fatal(err)
								}
								got, err := column.ToAnySlice()
								if err != nil {
									t.Fatal(err)
								}
								if !reflect.DeepEqual(got, expectedValues[name]) {
									t.Errorf("%s=%v want=%v", name, got, expectedValues[name])
								}
							}
							for _, b := range result.batches() {
								if b.Strings["a"] != nil || b.Int32s["b"] != nil {
									t.Fatal("replaced input binding escaped")
								}
							}
						}()
					}
					prepared.Close()
					if used := session.Engine().Memory().Used(); used != 0 {
						t.Fatalf("leaked %d bytes", used)
					}
				})
			}
		}
	}
}

func windowColumnOrderInput(t *testing.T, values []int64, valid []bool, batchRows int, ordered bool) *inmemory.Source {
	t.Helper()
	var batches []*memory.Batch
	for start := 0; start < len(values); start += batchRows {
		rows := min(batchRows, len(values)-start)
		b := memory.NewBatch()
		b.Rows = rows
		bitmap := memory.EnsureBitmap(nil, rows)
		for i := 0; i < rows; i++ {
			if valid[start+i] {
				memory.Set(bitmap, i)
			}
		}
		b.Int64s["value"] = &memory.Int64Vector{Values: append([]int64(nil), values[start:start+rows]...), Validity: bitmap}
		b.Strings["a"] = &memory.StringViewVector{Offsets: make([]uint32, rows+1), Encoding: memory.EncodingView}
		b.Int32s["b"] = &memory.Int32Vector{Values: make([]int32, rows)}
		batches = append(batches, b)
	}
	schema := memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "a", Type: memory.TypeString}, memory.Field{Name: "b", Type: memory.TypeInt32})
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	if ordered {
		src.WithOrdering(source.OrderingColumn{Name: "value", NullsLast: true})
	}
	return src
}

func windowColumnOrderExpected(values []int64, valid []bool) map[string][]any {
	positions := make([]int, len(values))
	for i := range positions {
		positions[i] = i
	}
	sort.SliceStable(positions, func(i, j int) bool {
		a, b := positions[i], positions[j]
		if valid[a] != valid[b] {
			return valid[a]
		}
		return valid[a] && values[a] < values[b]
	})
	out := map[string][]any{}
	for _, name := range []string{"value", "a", "b", "x", "y"} {
		out[name] = make([]any, len(values))
	}
	rank, dense := 0, 0
	for i, row := range positions {
		if i == 0 || valid[row] != valid[positions[i-1]] || valid[row] && values[row] != values[positions[i-1]] {
			rank = i + 1
			dense++
		}
		out["a"][row] = int64(i + 1)
		out["b"][row] = int64(rank)
		out["y"][row] = int64(dense)
		if valid[row] {
			out["value"][row] = values[row]
			out["x"][row] = values[row]
		}
	}
	return out
}

func TestWindowColumnOrderDuplicateAliases(t *testing.T) {
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	src := windowColumnOrderInput(t, []int64{1}, []bool{true}, 1, false)
	query := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("value"))), RowNumber("a"), Rank("x"), DenseRank("a"))
	if _, err := query.Schema(); err == nil || !strings.Contains(err.Error(), "duplicate output") {
		t.Fatalf("schema error=%v", err)
	}
	if prepared, err := query.Prepare(context.Background()); err == nil {
		prepared.Close()
		t.Fatal("duplicate alias prepared")
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("prepare leaked %d bytes", used)
	}
}

func TestWindowColumnOrderWithColumns(t *testing.T) {
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	src := windowColumnOrderInput(t, []int64{3, 0, 1}, []bool{true, false, true}, 1, false)
	query := session.FromSource(src).WithColumns(expr.NamedExpr{Expr: Col("value"), Alias: "a"}, expr.NamedExpr{Expr: Col("value"), Alias: "x"}, expr.NamedExpr{Expr: Col("value"), Alias: "b"}, expr.NamedExpr{Expr: Col("value"), Alias: "y"})
	expected, err := query.Schema()
	if err != nil {
		t.Fatal(err)
	}
	result, err := query.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if !reflect.DeepEqual(result.Schema(), expected) {
		t.Fatalf("actual=%v want=%v", result.Columns(), schemaColumnNames(expected))
	}
	for _, name := range result.Columns() {
		column, _ := result.Column(name)
		got, err := column.ToAnySlice()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, []any{int64(3), nil, int64(1)}) {
			t.Fatalf("%s=%v", name, got)
		}
	}
}

func TestWindowColumnOrderSpillLifecycle(t *testing.T) {
	for _, stop := range []string{"complete", "early", "cancel"} {
		t.Run(stop, func(t *testing.T) {
			const rows = 31 * 1024
			values := make([]int64, rows)
			valid := make([]bool, rows)
			for i := range values {
				values[i] = int64(rows - i)
				valid[i] = i%17 != 0
			}
			src := windowColumnOrderInput(t, values, valid, 127, false)
			dir := t.TempDir()
			opts := engine.DefaultOptions()
			opts.ExecutionPolicy = engine.ExecutionBounded
			opts.MemoryLimitBytes = 8 << 20
			opts.QueryMemorySoftLimitBytes = 48 << 10
			opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
			session := NewSession(opts)
			defer session.Close()
			query := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("value"))), RowNumber("a"), Shift("value", 0, "x"), Rank("b"), DenseRank("y"))
			expected, _ := query.Schema()
			expectedValues := windowColumnOrderExpected(values, valid)
			budget, err := session.Engine().BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), budget))
			defer cancel()
			reader, err := query.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 8 << 10, MaxBatchBytes: 32 << 10})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			seen, batches := 0, 0
			for reader.Next(ctx) {
				batches++
				view := reader.Batch()
				if !reflect.DeepEqual(view.Schema(), expected) {
					t.Errorf("batch order=%v want=%v", view.Columns(), schemaColumnNames(expected))
				}
				for _, name := range schemaColumnNames(expected) {
					col, err := view.Column(name)
					if err != nil {
						t.Fatal(err)
					}
					got, err := col.ToAnySlice()
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, expectedValues[name][seen:seen+int(view.Len())]) {
						t.Fatalf("batch%d %s values differ", batches, name)
					}
				}
				seen += int(view.Len())
				if stop == "early" {
					break
				}
				if stop == "cancel" {
					cancel()
					break
				}
			}
			if stop == "complete" && (reader.Err() != nil || seen != rows || batches < 2) {
				t.Fatalf("seen=%d batches=%d err=%v", seen, batches, reader.Err())
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			snapshot := budget.Snapshot()
			if snapshot.SpilledBytes == 0 || snapshot.PeakBytes > budget.Limit() || budget.Used() != 0 {
				t.Fatalf("spill accounting=%+v used=%d", snapshot, budget.Used())
			}
			if err := budget.Close(); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("spill leftovers=%v err=%v", entries, err)
			}
			if used := session.Engine().Memory().Used(); used != 0 {
				t.Fatalf("leaked %d bytes", used)
			}
			t.Logf("seen=%d batches=%d spill bytes=%d", seen, batches, snapshot.SpilledBytes)
		})
	}
}
