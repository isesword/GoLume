package oracle

import (
	"context"
	"math"
	"strconv"
	"testing"

	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/lazy"
)

// regressionSeeds is the fixed differential regression corpus. Each seed
// produces one random table and several random queries; every (seed, query,
// kernel mode) triple must produce exactly the oracle's answer. Add a seed
// here whenever a fixed bug was first caught by a random run.
var regressionSeeds = []int64{1, 2, 3, 7, 42, 97, 256, 999, 1336, 4096, 4245, 1239, 31337, 123456, 999983}

// kernelModes exercises the full execution matrix: forced scalar, gate
// -approved auto, and forced SIMD. Under toolchains without SIMD support the
// forced-SIMD mode legally degenerates to scalar.
var kernelModes = []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD}

// queriesPerSeed balances coverage against runtime; each query runs under
// every kernel mode.
const queriesPerSeed = 6

// batchShapes cycles the in-memory source through single-batch, one-row
// batches, and small multi-row batches so results are checked across
// partition boundaries.
var batchShapes = []int{0, 1, 7}

func runEngine(t *testing.T, mode kernel.Mode, table Table, batchRows int, q Query, label string) Table {
	t.Helper()
	src, err := table.Source(batchRows)
	if err != nil {
		t.Fatalf("%s: source: %v", label, err)
	}
	opts := engine.DefaultOptions()
	opts.KernelMode = mode
	eng := engine.New(opts)
	defer func() { _ = eng.Close() }()
	session := lazy.NewSessionWithEngine(eng)
	frame, err := q.Apply(session.FromSource(src)).Collect(context.Background())
	if err != nil {
		t.Fatalf("%s: engine rejected query (mode=%s): %v\nquery: %s\ntable:\n%s", label, mode, err, q, table)
	}
	defer func() { _ = frame.Close() }()
	got, err := TableFromFrame(frame)
	if err != nil {
		t.Fatalf("%s: extract engine result (mode=%s): %v\nquery: %s", label, mode, err, q)
	}
	return got
}

// runDifferential executes q on the oracle and on the engine under every
// kernel mode and requires the documented equality.
func runDifferential(t *testing.T, label string, table Table, batchRows int, q Query) {
	t.Helper()
	want, err := q.Execute(table)
	if err != nil {
		t.Fatalf("%s: oracle rejected query: %v\nquery: %s\ntable:\n%s", label, err, q, table)
	}
	for _, mode := range kernelModes {
		got := runEngine(t, mode, table, batchRows, q, label)
		if err := CompareTables(want, got, q.Ordered()); err != nil {
			t.Fatalf("%s mode=%s ordered=%v: %v\nquery: %s\noracle:\n%s\nengine:\n%s\ninput:\n%s",
				label, mode, q.Ordered(), err, q, want, got, table)
		}
	}
}

func TestDifferentialRegressionSeeds(t *testing.T) {
	for _, seed := range regressionSeeds {
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			g := NewGenerator(seed)
			table := g.Table(TableOptions{})
			for qi := 0; qi < queriesPerSeed; qi++ {
				q := g.Query(table)
				runDifferential(t, Describe(seed, qi, q), table, batchShapes[qi%len(batchShapes)], q)
			}
		})
	}
}

// TestScanRoundTrip pins the bridge itself: an empty pipeline must return the
// input table unchanged, including NULL bitmaps, bools, and empty strings.
func TestScanRoundTrip(t *testing.T) {
	table := mustTable(t,
		[]Column{
			{Name: "i", Kind: KindInt64}, {Name: "f", Kind: KindFloat64},
			{Name: "s", Kind: KindString}, {Name: "b", Kind: KindBool},
		},
		[][]Value{
			{Int64(math.MinInt64), Float64(math.NaN()), String(""), Bool(false)},
			{Null(KindInt64), Null(KindFloat64), Null(KindString), Null(KindBool)},
			{Int64(math.MaxInt64), Float64(math.Inf(-1)), String("张三"), Bool(true)},
		},
	)
	runDifferential(t, "scan-round-trip", table, 0, Query{})
	runDifferential(t, "scan-round-trip-1-row-batches", table, 1, Query{})
	empty := mustTable(t, table.Columns, nil)
	runDifferential(t, "scan-empty-table", empty, 0, Query{})
}

// TestDifferentialLargeTables exercises parallel scan, partition-local
// aggregation with merge, and multi-worker sort on larger inputs than the
// random generator produces.
func TestDifferentialLargeTables(t *testing.T) {
	g := NewGenerator(20260919)
	table := g.Table(TableOptions{MinRows: 1500, MaxRows: 4000, MaxColumns: 5})
	if table.NumRows() < 1000 {
		t.Fatalf("large table fixture has only %d rows", table.NumRows())
	}
	for qi := 0; qi < 4; qi++ {
		q := g.Query(table)
		runDifferential(t, Describe(20260919, qi, q), table, batchShapes[qi%len(batchShapes)], q)
	}
}

// TestDifferentialTargetedScenarios anchors the riskiest semantics with
// hand-written tables so a regression points at a named scenario rather than
// a random seed.
func TestDifferentialTargetedScenarios(t *testing.T) {
	groups := mustTable(t,
		[]Column{{Name: "k", Kind: KindInt64, Bounded: true}, {Name: "v", Kind: KindInt64, Bounded: true}, {Name: "b", Kind: KindBool}},
		[][]Value{
			{Int64(1), Int64(10), Bool(true)},
			{Int64(1), Null(KindInt64), Bool(false)},
			{Null(KindInt64), Null(KindInt64), Null(KindBool)},
			{Int64(2), Int64(-4), Bool(true)},
		},
	)
	runDifferential(t, "group-all-null-values", groups, 0, Query{Ops: []Op{
		GroupByOp{Keys: []string{"k"}, Aggregates: []expr.Aggregate{
			expr.Count("n"), expr.CountColumn("v", "nn"), expr.Sum("v", "s"),
			expr.Min("v", "lo"), expr.Max("v", "hi"), expr.Avg("v", "avg"),
		}},
	}})
	// HAVING-style filter on the COUNT(*) uint64 output.
	runDifferential(t, "group-having-count", groups, 2, Query{Ops: []Op{
		GroupByOp{Keys: []string{"k"}, Aggregates: []expr.Aggregate{expr.Count("n"), expr.Sum("v", "s")}},
		FilterOp{Predicate: expr.GT(expr.Col("n"), expr.Lit(1))},
	}})
	// Sort followed by limit over grouped output: ordered comparison.
	runDifferential(t, "group-sort-limit", groups, 0, Query{Ops: []Op{
		GroupByOp{Keys: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("v", "s")}},
		SortOp{Keys: []SortKey{{Column: "s", Descending: true}, {Column: "k"}}},
		LimitOp{N: 2},
	}})

	strings_ := mustTable(t,
		[]Column{{Name: "k", Kind: KindString}, {Name: "v", Kind: KindInt64, Bounded: true}},
		[][]Value{
			{String(""), Int64(1)}, {String(""), Int64(2)},
			{String("张三"), Int64(5)}, {Null(KindString), Int64(7)},
		},
	)
	runDifferential(t, "group-string-key-with-null", strings_, 0, Query{Ops: []Op{
		GroupByOp{Keys: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("v", "s"), expr.Avg("v", "avg")}},
	}})

	floats := mustTable(t,
		[]Column{{Name: "v", Kind: KindFloat64}, {Name: "i", Kind: KindInt64}},
		[][]Value{
			{Float64(math.NaN()), Int64(0)},
			{Float64(2), Int64(1)},
			{Null(KindFloat64), Int64(2)},
			{Float64(math.Inf(1)), Int64(3)},
			{Float64(-1), Int64(4)},
		},
	)
	runDifferential(t, "sort-float-nan-desc", floats, 0, Query{Ops: []Op{
		SelectOp{Columns: []string{"v", "i"}},
		SortOp{Keys: []SortKey{{Column: "v", Descending: true}}},
	}})
	runDifferential(t, "filter-nan-literal", floats, 0, Query{Ops: []Op{
		FilterOp{Predicate: expr.LE(expr.Col("v"), expr.F64(10))},
	}})
	// Integer division produces float64; 0/0 yields NaN.
	runDifferential(t, "project-div-by-zero", floats, 0, Query{Ops: []Op{
		ProjectOp{Exprs: []expr.NamedExpr{expr.Named(expr.Div(expr.Col("i"), expr.Lit(2)), "half")}},
	}})

	bools := mustTable(t,
		[]Column{{Name: "b", Kind: KindBool}, {Name: "v", Kind: KindInt64, Bounded: true}},
		[][]Value{
			{Bool(true), Int64(1)}, {Bool(false), Int64(2)}, {Null(KindBool), Int64(3)},
		},
	)
	runDifferential(t, "filter-bool-literal", bools, 1, Query{Ops: []Op{
		FilterOp{Predicate: expr.EQ(expr.Col("b"), expr.Lit(1))},
	}})

	empty := mustTable(t, []Column{{Name: "k", Kind: KindInt64}, {Name: "v", Kind: KindInt64, Bounded: true}}, nil)
	runDifferential(t, "group-empty-input", empty, 0, Query{Ops: []Op{
		GroupByOp{Keys: []string{"k"}, Aggregates: []expr.Aggregate{expr.Count("n")}},
	}})
	runDifferential(t, "topk-empty-input", empty, 0, Query{Ops: []Op{
		TopKOp{K: 3, Keys: []SortKey{{Column: "k"}}},
	}})

	// Grouping over sources with a declared ordering (sort, top-k) must give
	// identical groups regardless of ordering direction.
	ordered := mustTable(t,
		[]Column{{Name: "k", Kind: KindInt64, Bounded: true}},
		[][]Value{{Int64(3)}, {Int64(1)}, {Null(KindInt64)}, {Int64(2)}, {Int64(3)}, {Int64(1)}},
	)
	aggs := []expr.Aggregate{expr.Count("n"), expr.Sum("k", "s"), expr.Min("k", "lo"), expr.Max("k", "hi")}
	runDifferential(t, "group-over-sort-asc", ordered, 1, Query{Ops: []Op{
		SortOp{Keys: []SortKey{{Column: "k"}}},
		GroupByOp{Keys: []string{"k"}, Aggregates: aggs},
	}})
	runDifferential(t, "group-over-sort-desc", ordered, 1, Query{Ops: []Op{
		SortOp{Keys: []SortKey{{Column: "k", Descending: true, NullsLast: true}}},
		GroupByOp{Keys: []string{"k"}, Aggregates: aggs},
	}})
	runDifferential(t, "group-over-topk-desc", ordered, 0, Query{Ops: []Op{
		TopKOp{K: 5, Keys: []SortKey{{Column: "k", Descending: true, NullsLast: true}}},
		GroupByOp{Keys: []string{"k"}, Aggregates: aggs},
	}})
}

// TestDifferentialRegressionFixedBugs pins the shapes that earlier engine
// bugs forced the generator to avoid: bool sort payloads and keys, TopK over
// sorted/top-k input, Limit over a Sort with an upstream sort (the
// sort-limit-to-topk rewrite), Select subsets below GroupBy, non-adjacent
// Selects below TopK's split-path key projection, and in-place WithColumns
// column replacement.
func TestDifferentialRegressionFixedBugs(t *testing.T) {
	mixed := mustTable(t,
		[]Column{
			{Name: "c0", Kind: KindInt64, Bounded: true}, {Name: "c1", Kind: KindString},
			{Name: "c2", Kind: KindInt64, Bounded: true}, {Name: "c3", Kind: KindBool},
		},
		[][]Value{
			{Int64(2), String("x"), Int64(5), Bool(true)},
			{Int64(1), String("y"), Int64(3), Null(KindBool)},
			{Int64(3), String("x"), Int64(5), Bool(false)},
			{Int64(1), Null(KindString), Int64(4), Bool(true)},
			{Int64(2), String(""), Int64(1), Bool(false)},
		},
	)
	// Bool columns as sort payload and as sort keys (false < true), in both
	// directions and with NULL keys.
	runDifferential(t, "bool-sort-payload-and-key", mixed, 2, Query{Ops: []Op{
		SortOp{Keys: []SortKey{{Column: "c3"}, {Column: "c0", Descending: true}}},
	}})
	runDifferential(t, "bool-sort-desc-nulls", mixed, 0, Query{Ops: []Op{
		SortOp{Keys: []SortKey{{Column: "c3", Descending: true, NullsLast: true}}},
	}})
	runDifferential(t, "bool-topk-key", mixed, 1, Query{Ops: []Op{
		TopKOp{K: 3, Keys: []SortKey{{Column: "c3", NullsLast: true}, {Column: "c0"}}},
	}})
	// TopK over already-sorted and already-top-k inputs, and the
	// Limit-over-Sort rewrite with a sort upstream.
	runDifferential(t, "topk-over-sort", mixed, 0, Query{Ops: []Op{
		SortOp{Keys: []SortKey{{Column: "c0", Descending: true}}},
		TopKOp{K: 2, Keys: []SortKey{{Column: "c2"}}},
	}})
	runDifferential(t, "topk-over-topk", mixed, 0, Query{Ops: []Op{
		TopKOp{K: 4, Keys: []SortKey{{Column: "c0"}}},
		TopKOp{K: 2, Keys: []SortKey{{Column: "c2", Descending: true}}},
	}})
	runDifferential(t, "limit-over-sort-with-upstream-sort", mixed, 2, Query{Ops: []Op{
		SortOp{Keys: []SortKey{{Column: "c0"}}},
		SortOp{Keys: []SortKey{{Column: "c2", Descending: true}}},
		LimitOp{N: 2},
	}})
	// A Select subset below a GroupBy (optimizer projection pushdown).
	runDifferential(t, "select-subset-below-groupby", mixed, 0, Query{Ops: []Op{
		SelectOp{Columns: []string{"c0", "c1", "c2"}},
		GroupByOp{Keys: []string{"c0", "c2"}, Aggregates: []expr.Aggregate{expr.Count("n"), expr.Sum("c0", "s")}},
	}})
	// Two Selects separated by a Filter below a TopK (split-path key
	// projection replay).
	runDifferential(t, "selects-filter-topk-split", mixed, 1, Query{Ops: []Op{
		SelectOp{Columns: []string{"c0", "c2", "c3"}},
		FilterOp{Predicate: expr.IsNotNull(expr.Col("c0"))},
		SelectOp{Columns: []string{"c2", "c0"}},
		TopKOp{K: 2, Keys: []SortKey{{Column: "c0"}}},
	}})
	// WithColumns replaces a column of an earlier WithColumns in place; the
	// output column order must not move.
	runDifferential(t, "withcolumns-replace-in-place", mixed, 0, Query{Ops: []Op{
		ProjectOp{IncludeInput: true, Exprs: []expr.NamedExpr{
			expr.Named(expr.Add(expr.Col("c0"), expr.Lit(1)), "p0"),
			expr.Named(expr.Col("c1"), "p1"),
		}},
		ProjectOp{IncludeInput: true, Exprs: []expr.NamedExpr{
			expr.Named(expr.Mul(expr.Col("c2"), expr.Lit(2)), "p0"),
		}},
	}})
}
