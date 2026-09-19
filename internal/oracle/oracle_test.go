package oracle

import (
	"math"
	"strings"
	"testing"

	"gocolumn/expr"
)

// mustTable builds a table and fails the test on validation errors.
func mustTable(t *testing.T, columns []Column, rows [][]Value) Table {
	t.Helper()
	table, err := NewTable(columns, rows)
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	return table
}

// mustExecute runs a query and fails the test on evaluation errors.
func mustExecute(t *testing.T, table Table, ops ...Op) Table {
	t.Helper()
	out, err := Query{Ops: ops}.Execute(table)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return out
}

// intColumn extracts the non-NULL int64 cells of column i in row order, using
// -1 as the NULL marker for compact table-driven assertions.
func intColumn(t *testing.T, table Table, col int) []int64 {
	t.Helper()
	out := make([]int64, len(table.Rows))
	for i, row := range table.Rows {
		if row[col].Null {
			out[i] = -1
			continue
		}
		if row[col].Kind != KindInt64 {
			t.Fatalf("column %d kind %s, want int64", col, row[col].Kind)
		}
		out[i] = row[col].I64
	}
	return out
}

func equalInts(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFilterThreeValuedLogic(t *testing.T) {
	// Rows: (5), (NULL), (12), (-3). SQL three-valued logic never selects a
	// row whose predicate evaluates to NULL.
	table := mustTable(t,
		[]Column{{Name: "v", Kind: KindInt64}},
		[][]Value{{Int64(5)}, {Null(KindInt64)}, {Int64(12)}, {Int64(-3)}},
	)
	tests := []struct {
		name      string
		predicate expr.Expr
		want      []int64
	}{
		{"gt", expr.GT(expr.Col("v"), expr.Lit(10)), []int64{12}},
		{"le", expr.LE(expr.Col("v"), expr.Lit(5)), []int64{5, -3}},
		{"eq", expr.EQ(expr.Col("v"), expr.Lit(5)), []int64{5}},
		{"ne_via_not_eq", expr.Not(expr.EQ(expr.Col("v"), expr.Lit(5))), []int64{12, -3}},
		{"and_null_arm", expr.And(expr.GT(expr.Col("v"), expr.Lit(4)), expr.LT(expr.Col("v"), expr.Lit(10))), []int64{5}},
		{"or_null_arm", expr.Or(expr.LT(expr.Col("v"), expr.Lit(0)), expr.GT(expr.Col("v"), expr.Lit(10))), []int64{12, -3}},
		{"not_null_comparison", expr.Not(expr.GT(expr.Col("v"), expr.Lit(10))), []int64{5, -3}},
		{"is_null", expr.IsNull(expr.Col("v")), []int64{-1}},
		{"is_not_null", expr.IsNotNull(expr.Col("v")), []int64{5, 12, -3}},
		{"null_and_false_is_false", expr.And(expr.IsNull(expr.Col("v")), expr.GT(expr.Col("v"), expr.Lit(10))), nil},
		{"null_or_true_is_true", expr.Or(expr.IsNull(expr.Col("v")), expr.GT(expr.Col("v"), expr.Lit(10))), []int64{-1, 12}},
		{"col_vs_col", expr.LT(expr.Col("v"), expr.Col("v")), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mustExecute(t, table, FilterOp{Predicate: tt.predicate})
			if want := tt.want; !equalInts(intColumn(t, got, 0), want) {
				t.Fatalf("got %v, want %v (NULL shown as -1)", intColumn(t, got, 0), want)
			}
		})
	}
}

func TestFilterStringBoolAndNaN(t *testing.T) {
	table := mustTable(t,
		[]Column{{Name: "s", Kind: KindString}, {Name: "b", Kind: KindBool}, {Name: "f", Kind: KindFloat64}},
		[][]Value{
			{String(""), Bool(false), Float64(math.NaN())},
			{String("abc"), Bool(true), Float64(1.5)},
			{String("张三"), Bool(false), Float64(math.Inf(1))},
			{Null(KindString), Null(KindBool), Null(KindFloat64)},
		},
	)
	tests := []struct {
		name      string
		predicate expr.Expr
		wantRows  []int
	}{
		// Byte order: "abc" and "张三" (0xE5...) both exceed "a"; "" and NULL do not.
		{"string_gt_ascii", expr.GT(expr.Col("s"), expr.Str("a")), []int{1, 2}},
		{"string_eq_empty", expr.EQ(expr.Col("s"), expr.Str("")), []int{0}},
		{"bool_eq_true", expr.EQ(expr.Col("b"), expr.Lit(1)), []int{1}},
		{"bool_eq_false", expr.EQ(expr.Col("b"), expr.Lit(0)), []int{0, 2}},
		// Every comparison against a NaN cell is false.
		{"nan_never_matches", expr.GE(expr.Col("f"), expr.F64(math.Inf(-1))), []int{1, 2}},
		{"inf_eq", expr.EQ(expr.Col("f"), expr.F64(math.Inf(1))), []int{2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mustExecute(t, table, FilterOp{Predicate: tt.predicate})
			if len(got.Rows) != len(tt.wantRows) {
				t.Fatalf("rows=%d, want %v", len(got.Rows), tt.wantRows)
			}
			// Verify the kept rows by their string cell.
			for i, want := range tt.wantRows {
				if !valuesEqual(got.Rows[i][0], table.Rows[want][0]) {
					t.Fatalf("row %d: got %s, want row %d", i, got.Rows[i][0], want)
				}
			}
		})
	}
}

func TestProjectArithmeticAndCoercion(t *testing.T) {
	table := mustTable(t,
		[]Column{{Name: "i", Kind: KindInt64, Bounded: true}, {Name: "f", Kind: KindFloat64}, {Name: "s", Kind: KindString}},
		[][]Value{
			{Int64(4), Float64(0.5), String("ab")},
			{Null(KindInt64), Float64(-2), Null(KindString)},
			{Int64(-7), Float64(3), String("")},
		},
	)
	out := mustExecute(t, table, ProjectOp{Exprs: []expr.NamedExpr{
		expr.Named(expr.Add(expr.Col("i"), expr.Lit(6)), "plus"),
		expr.Named(expr.Mul(expr.Col("i"), expr.Col("i")), "square"),
		expr.Named(expr.Div(expr.Col("i"), expr.Lit(2)), "half"),
		expr.Named(expr.Add(expr.Col("f"), expr.Col("i")), "mixed"),
		expr.Named(expr.Concat(expr.Col("s"), expr.Str("!")), "bang"),
		expr.Named(expr.Col("s"), "renamed"),
	}})
	wantColumns := []Column{
		{Name: "plus", Kind: KindInt64}, {Name: "square", Kind: KindInt64},
		{Name: "half", Kind: KindFloat64}, {Name: "mixed", Kind: KindFloat64},
		{Name: "bang", Kind: KindString}, {Name: "renamed", Kind: KindString},
	}
	if len(out.Columns) != len(wantColumns) {
		t.Fatalf("columns=%v", out.Columns)
	}
	for i, c := range wantColumns {
		if out.Columns[i].Name != c.Name || out.Columns[i].Kind != c.Kind {
			t.Fatalf("column %d: got %v, want %v", i, out.Columns[i], c)
		}
	}
	want := [][]Value{
		{Int64(10), Int64(16), Float64(2), Float64(4.5), String("ab!"), String("ab")},
		{Null(KindInt64), Null(KindInt64), Null(KindFloat64), Null(KindFloat64), Null(KindString), Null(KindString)},
		{Int64(-1), Int64(49), Float64(-3.5), Float64(-4), String("!"), String("")},
	}
	if err := CompareTables(mustTable(t, wantColumns, want), out, true); err != nil {
		t.Fatalf("projection mismatch: %v", err)
	}
}

func TestProjectIntegerOverflowAndDivisionByZero(t *testing.T) {
	table := mustTable(t,
		[]Column{{Name: "i", Kind: KindInt64}},
		[][]Value{{Int64(math.MaxInt64)}, {Int64(0)}},
	)
	if _, err := (Query{Ops: []Op{ProjectOp{Exprs: []expr.NamedExpr{
		expr.Named(expr.Add(expr.Col("i"), expr.Lit(1)), "boom"),
	}}}}).Execute(table); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("expected overflow error, got %v", err)
	}
	// Division always produces float64, so a zero divisor yields IEEE results
	// instead of an error.
	out := mustExecute(t, table, ProjectOp{Exprs: []expr.NamedExpr{
		expr.Named(expr.Div(expr.Col("i"), expr.Col("i")), "q"),
	}})
	if out.Rows[0][0].F64 != 1 {
		t.Fatalf("MaxInt64/MaxInt64=%v", out.Rows[0][0].F64)
	}
	if !math.IsNaN(out.Rows[1][0].F64) {
		t.Fatalf("0/0=%v, want NaN", out.Rows[1][0].F64)
	}
}

func TestProjectWithColumnsReplacesAndAppends(t *testing.T) {
	table := mustTable(t,
		[]Column{{Name: "a", Kind: KindInt64, Bounded: true}, {Name: "b", Kind: KindInt64, Bounded: true}},
		[][]Value{{Int64(1), Int64(2)}, {Int64(3), Int64(4)}},
	)
	out := mustExecute(t, table, ProjectOp{IncludeInput: true, Exprs: []expr.NamedExpr{
		expr.Named(expr.Add(expr.Col("a"), expr.Lit(10)), "a"),
		expr.Named(expr.Add(expr.Col("a"), expr.Col("b")), "c"),
	}})
	want := mustTable(t,
		[]Column{{Name: "a", Kind: KindInt64}, {Name: "b", Kind: KindInt64}, {Name: "c", Kind: KindInt64}},
		[][]Value{{Int64(11), Int64(2), Int64(3)}, {Int64(13), Int64(4), Int64(7)}},
	)
	if err := CompareTables(want, out, true); err != nil {
		t.Fatalf("with-columns mismatch: %v", err)
	}
}

func TestGroupByKnownAnswers(t *testing.T) {
	// Groups in first-seen order: k=1 (rows 0,1), NULL key (row 2), k=2 (row 3).
	// The NULL-key group has only NULL values; k=1 mixes a NULL value.
	table := mustTable(t,
		[]Column{{Name: "k", Kind: KindInt64}, {Name: "v", Kind: KindInt64, Bounded: true}},
		[][]Value{
			{Int64(1), Int64(10)},
			{Int64(1), Null(KindInt64)},
			{Null(KindInt64), Null(KindInt64)},
			{Int64(2), Int64(-4)},
		},
	)
	out := mustExecute(t, table, GroupByOp{
		Keys: []string{"k"},
		Aggregates: []expr.Aggregate{
			expr.Count("n"), expr.CountColumn("v", "nn"), expr.Sum("v", "s"),
			expr.Min("v", "lo"), expr.Max("v", "hi"), expr.Avg("v", "avg"),
		},
	})
	want := mustTable(t,
		[]Column{
			{Name: "k", Kind: KindInt64}, {Name: "n", Kind: KindUint64}, {Name: "nn", Kind: KindUint64},
			{Name: "s", Kind: KindInt64}, {Name: "lo", Kind: KindInt64}, {Name: "hi", Kind: KindInt64},
			{Name: "avg", Kind: KindFloat64},
		},
		[][]Value{
			{Int64(1), Uint64(2), Uint64(1), Int64(10), Int64(10), Int64(10), Float64(10)},
			{Null(KindInt64), Uint64(1), Uint64(0), Null(KindInt64), Null(KindInt64), Null(KindInt64), Null(KindFloat64)},
			{Int64(2), Uint64(1), Uint64(1), Int64(-4), Int64(-4), Int64(-4), Float64(-4)},
		},
	)
	// First-seen order is the oracle's deterministic contract.
	if err := CompareTables(want, out, true); err != nil {
		t.Fatalf("group by mismatch: %v\ngot:\n%s", err, out)
	}
}

func TestGroupByStringKeysAndEmptyInput(t *testing.T) {
	table := mustTable(t,
		[]Column{{Name: "k", Kind: KindString}, {Name: "v", Kind: KindInt64, Bounded: true}},
		[][]Value{
			{String(""), Int64(1)},
			{String(""), Int64(2)},
			{String("张三"), Int64(5)},
			{Null(KindString), Int64(7)},
		},
	)
	out := mustExecute(t, table, GroupByOp{Keys: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("v", "s")}})
	want := mustTable(t,
		[]Column{{Name: "k", Kind: KindString}, {Name: "s", Kind: KindInt64}},
		[][]Value{{String(""), Int64(3)}, {String("张三"), Int64(5)}, {Null(KindString), Int64(7)}},
	)
	if err := CompareTables(want, out, true); err != nil {
		t.Fatalf("string group by mismatch: %v", err)
	}

	empty := mustTable(t, table.Columns, nil)
	out = mustExecute(t, empty, GroupByOp{Keys: []string{"k"}, Aggregates: []expr.Aggregate{expr.Count("n")}})
	if out.NumRows() != 0 {
		t.Fatalf("empty input produced %d groups", out.NumRows())
	}
}

func TestSortNullPlacementDirectionStabilityAndNaN(t *testing.T) {
	// payload preserves the input order so stability is observable.
	table := mustTable(t,
		[]Column{{Name: "v", Kind: KindFloat64}, {Name: "payload", Kind: KindInt64}},
		[][]Value{
			{Float64(2), Int64(0)},
			{Null(KindFloat64), Int64(1)},
			{Float64(math.NaN()), Int64(2)},
			{Float64(2), Int64(3)},
			{Float64(-1), Int64(4)},
			{Null(KindFloat64), Int64(5)},
		},
	)
	tests := []struct {
		name string
		key  SortKey
		want []int64
	}{
		// NULL placement is direction-independent; NaN is greatest among
		// non-NULL values; equal keys keep input order (payload 0 before 3).
		{"asc_nulls_first", SortKey{Column: "v"}, []int64{1, 5, 4, 0, 3, 2}},
		{"asc_nulls_last", SortKey{Column: "v", NullsLast: true}, []int64{4, 0, 3, 2, 1, 5}},
		{"desc_nulls_first", SortKey{Column: "v", Descending: true}, []int64{1, 5, 2, 0, 3, 4}},
		{"desc_nulls_last", SortKey{Column: "v", Descending: true, NullsLast: true}, []int64{2, 0, 3, 4, 1, 5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := mustExecute(t, table, SortOp{Keys: []SortKey{tt.key}})
			if got := intColumn(t, out, 1); !equalInts(got, tt.want) {
				t.Fatalf("payload order=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestSortMultiKeyAndStringByteOrder(t *testing.T) {
	table := mustTable(t,
		[]Column{{Name: "a", Kind: KindInt64}, {Name: "s", Kind: KindString}},
		[][]Value{
			{Int64(1), String("张三")},
			{Int64(0), String("z")},
			{Int64(1), String("")},
			{Int64(1), String("abc")},
		},
	)
	out := mustExecute(t, table, SortOp{Keys: []SortKey{{Column: "a"}, {Column: "s", Descending: true}}})
	var got []string
	for _, row := range out.Rows {
		got = append(got, row[1].Str)
	}
	// a=0 first; within a=1 strings descend in byte order: 张三 > abc > "".
	want := []string{"z", "张三", "abc", ""}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order=%q, want %q", got, want)
		}
	}
}

func TestTopKAndLimitEdges(t *testing.T) {
	table := mustTable(t,
		[]Column{{Name: "v", Kind: KindInt64}},
		[][]Value{{Int64(3)}, {Int64(1)}, {Int64(2)}, {Int64(1)}},
	)
	// Ties (two rows equal to 1) resolve by input order: rows 1 then 3.
	out := mustExecute(t, table, TopKOp{K: 2, Keys: []SortKey{{Column: "v"}}})
	if got := intColumn(t, out, 0); !equalInts(got, []int64{1, 1}) {
		t.Fatalf("top-2=%v", got)
	}
	out = mustExecute(t, table, TopKOp{K: 99, Keys: []SortKey{{Column: "v"}}})
	if got := intColumn(t, out, 0); !equalInts(got, []int64{1, 1, 2, 3}) {
		t.Fatalf("top-99=%v", got)
	}
	out = mustExecute(t, table, TopKOp{K: 0, Keys: []SortKey{{Column: "v"}}})
	if out.NumRows() != 0 {
		t.Fatalf("top-0 rows=%d", out.NumRows())
	}
	out = mustExecute(t, table, LimitOp{N: 0})
	if out.NumRows() != 0 {
		t.Fatalf("limit 0 rows=%d", out.NumRows())
	}
	out = mustExecute(t, table, LimitOp{N: 99})
	if out.NumRows() != 4 {
		t.Fatalf("limit 99 rows=%d", out.NumRows())
	}
	if _, err := (Query{Ops: []Op{LimitOp{N: -1}}}).Execute(table); err == nil {
		t.Fatal("negative limit succeeded")
	}
	if _, err := (Query{Ops: []Op{TopKOp{K: -1, Keys: []SortKey{{Column: "v"}}}}}).Execute(table); err == nil {
		t.Fatal("negative top-k succeeded")
	}
}

func TestCompareTablesRules(t *testing.T) {
	columns := []Column{{Name: "v", Kind: KindFloat64}}
	base := mustTable(t, columns, [][]Value{{Float64(math.NaN())}, {Float64(1)}, {Null(KindFloat64)}})
	same := mustTable(t, columns, [][]Value{{Null(KindFloat64)}, {Float64(math.NaN())}, {Float64(1)}})
	if err := CompareTables(base, same, true); err == nil {
		t.Fatal("ordered compare of shuffled rows unexpectedly passed")
	}
	if err := CompareTables(base, same, false); err != nil {
		t.Fatalf("canonical compare failed: %v", err)
	}
	// -0.0 equals +0.0; the relative tolerance absorbs tiny drift.
	signed := mustTable(t, columns, [][]Value{{Float64(1)}, {Float64(math.Copysign(0, -1))}})
	near := mustTable(t, columns, [][]Value{{Float64(1 + 1e-13)}, {Float64(0)}})
	if err := CompareTables(signed, near, false); err != nil {
		t.Fatalf("tolerance compare failed: %v", err)
	}
	far := mustTable(t, columns, [][]Value{{Float64(1.1)}, {Float64(0)}})
	if err := CompareTables(signed, far, false); err == nil {
		t.Fatal("1.1 vs 1 passed tolerance")
	}
	// NULL position mismatches are detected.
	moved := mustTable(t, columns, [][]Value{{Float64(math.NaN())}, {Null(KindFloat64)}, {Float64(1)}})
	if err := CompareTables(base, moved, true); err == nil {
		t.Fatal("NULL position mismatch passed")
	}
}

func TestTableValidateAndValueRendering(t *testing.T) {
	if _, err := NewTable([]Column{{Name: "a", Kind: KindInt64}, {Name: "a", Kind: KindInt64}}, nil); err == nil {
		t.Fatal("duplicate columns accepted")
	}
	if _, err := NewTable([]Column{{Name: "a", Kind: KindInt64}}, [][]Value{{Int64(1), Int64(2)}}); err == nil {
		t.Fatal("short row accepted")
	}
	if _, err := NewTable([]Column{{Name: "a", Kind: KindInt64}}, [][]Value{{String("x")}}); err == nil {
		t.Fatal("kind mismatch accepted")
	}
	table := mustTable(t, []Column{{Name: "a", Kind: KindInt64}}, [][]Value{{Int64(-9)}, {Null(KindInt64)}})
	if got := table.String(); !strings.Contains(got, "-9") || !strings.Contains(got, "NULL") {
		t.Fatalf("table dump missing values: %s", got)
	}
	if got := Null(KindString).String(); got != "NULL" {
		t.Fatalf("null renders %q", got)
	}
}

func TestQueryOrderedContract(t *testing.T) {
	if !(Query{}).Ordered() {
		t.Fatal("empty query must be ordered")
	}
	q := Query{Ops: []Op{GroupByOp{Keys: []string{"a"}, Aggregates: []expr.Aggregate{expr.Count("n")}}}}
	if q.Ordered() {
		t.Fatal("group-by output is not ordered")
	}
	q.Ops = append(q.Ops, SortOp{Keys: []SortKey{{Column: "a"}}}, LimitOp{N: 3})
	if !q.Ordered() {
		t.Fatal("sort after group-by re-establishes order")
	}
}
