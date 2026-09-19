package proptest_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gocolumn/expr"
	"gocolumn/internal/proptest"
	"gocolumn/kernel"
	"gocolumn/lazy"
	"gocolumn/memory"
)

// consistencyTable builds the shared 513-row input (a deliberately
// non-power-of-two length) with NULLs in every column type.
func consistencyTable(t *testing.T) *proptest.Table {
	t.Helper()
	table, err := proptest.NewGen(20260919).Table(513,
		proptest.ColumnSpec{Name: "v", Type: memory.TypeInt64, NullProbability: 0.1, Cardinality: 400},
		proptest.ColumnSpec{Name: "w", Type: memory.TypeInt64, NullProbability: 0.1},
		proptest.ColumnSpec{Name: "f", Type: memory.TypeFloat64, NullProbability: 0.1},
		proptest.ColumnSpec{Name: "k", Type: memory.TypeString, NullProbability: 0.05, Cardinality: 17},
		proptest.ColumnSpec{Name: "b", Type: memory.TypeBool, NullProbability: 0.1},
	)
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func defaultHarness(t *testing.T) *proptest.Harness {
	t.Helper()
	h, err := proptest.NewHarness(proptest.DefaultBackends()...)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestHarnessConsistencyFilter(t *testing.T) {
	defaultHarness(t).Run(t, proptest.Query{
		Name:  "filter-int-and-string-prefix",
		Table: consistencyTable(t),
		Build: func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.Filter(expr.And(
				lazy.Col("v").Ge(lazy.Lit(50)),
				lazy.Col("k").StartsWith(lazy.Str("k0")),
			))
		},
	})
}

func TestHarnessConsistencyArithmetic(t *testing.T) {
	// Bounded integer domains keep the checked int64 arithmetic overflow-free.
	table, err := proptest.NewGen(31337).Table(513,
		proptest.ColumnSpec{Name: "v", Type: memory.TypeInt64, NullProbability: 0.1, Cardinality: 1000},
		proptest.ColumnSpec{Name: "w", Type: memory.TypeInt64, NullProbability: 0.1, Cardinality: 100},
		proptest.ColumnSpec{Name: "f", Type: memory.TypeFloat64, NullProbability: 0.1},
	)
	if err != nil {
		t.Fatal(err)
	}
	defaultHarness(t).Run(t, proptest.Query{
		Name:  "int-and-float-arithmetic-projection",
		Table: table,
		Build: func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.WithColumns(
				lazy.Named(lazy.Col("v").Add(lazy.Col("w").Mul(lazy.Lit(7))), "calc"),
				lazy.Named(lazy.Col("f").Mul(lazy.F64(1.5)).Add(lazy.F64(0.25)), "scaled"),
			)
		},
	})
}

func TestHarnessConsistencyAggregate(t *testing.T) {
	defaultHarness(t).Run(t, proptest.Query{
		Name:  "grouped-sum-count-avg",
		Table: consistencyTable(t),
		Build: func(lf lazy.LazyFrame) lazy.LazyFrame {
			// The engine requires every value aggregate in one query to read
			// the same int64 column. Sorting the aggregate output keeps group
			// order deterministic so row-by-row comparison is meaningful.
			return lf.GroupBy("k").Agg(
				lazy.Sum("v", "total"),
				lazy.Count("rows"),
				lazy.Avg("v", "mean"),
			).Sort(lazy.Asc("k"))
		},
	})
}

func TestHarnessConsistencySortAndTopK(t *testing.T) {
	table := consistencyTable(t)
	h := defaultHarness(t)
	h.Run(t, proptest.Query{
		Name:  "multi-key-sort",
		Table: table,
		Build: func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.Sort(lazy.Desc("v"), lazy.Asc("k"))
		},
	})
	h.Run(t, proptest.Query{
		Name:  "topk-desc",
		Table: table,
		Build: func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.TopK(25, lazy.Desc("w"))
		},
	})
}

func TestHarnessConsistencyEmptyResult(t *testing.T) {
	defaultHarness(t).Run(t, proptest.Query{
		Name:  "filter-matches-nothing",
		Table: consistencyTable(t),
		Build: func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.Filter(lazy.Col("v").Gt(lazy.Lit(1_000_000_000)))
		},
	})
}

func TestHarnessRejectsInvalidQueries(t *testing.T) {
	h := defaultHarness(t)
	table := consistencyTable(t)
	ctx := context.Background()
	if err := h.Check(ctx, proptest.Query{Name: "", Table: table, Build: func(lf lazy.LazyFrame) lazy.LazyFrame { return lf }}); err == nil {
		t.Fatal("empty query name accepted")
	}
	if err := h.Check(ctx, proptest.Query{Name: "x", Build: func(lf lazy.LazyFrame) lazy.LazyFrame { return lf }}); err == nil {
		t.Fatal("nil table accepted")
	}
	if err := h.Check(ctx, proptest.Query{Name: "x", Table: table}); err == nil {
		t.Fatal("nil build accepted")
	}
}

func TestNewHarnessValidation(t *testing.T) {
	backend := proptest.EngineBackend("scalar", kernel.ModeScalar)
	if _, err := proptest.NewHarness(); err == nil {
		t.Fatal("zero backends accepted")
	}
	if _, err := proptest.NewHarness(backend); err == nil {
		t.Fatal("single backend accepted")
	}
	if _, err := proptest.NewHarness(backend, backend); err == nil {
		t.Fatal("duplicate backend names accepted")
	}
	unnamed := proptest.Backend{Name: "", Run: backend.Run}
	if _, err := proptest.NewHarness(backend, unnamed); err == nil {
		t.Fatal("empty backend name accepted")
	}
	noRun := proptest.Backend{Name: "noop"}
	if _, err := proptest.NewHarness(backend, noRun); err == nil {
		t.Fatal("nil Run accepted")
	}
	h, err := proptest.NewHarness(backend, proptest.EngineBackend("auto", kernel.ModeAuto))
	if err != nil {
		t.Fatal(err)
	}
	names := h.Backends()
	if len(names) != 2 || names[0] != "scalar" || names[1] != "auto" {
		t.Fatalf("unexpected backend names %v", names)
	}
}

// fixedBackend returns a mock backend that always answers rel.
func fixedBackend(name string, rel *proptest.Relation) proptest.Backend {
	return proptest.Backend{
		Name: name,
		Run: func(context.Context, *proptest.Table, func(lazy.LazyFrame) lazy.LazyFrame) (*proptest.Relation, error) {
			return rel, nil
		},
	}
}

func mockQuery(t *testing.T) proptest.Query {
	t.Helper()
	table, err := proptest.NewGen(1).Table(4, proptest.ColumnSpec{Name: "v", Type: memory.TypeInt64})
	if err != nil {
		t.Fatal(err)
	}
	return proptest.Query{
		Name:  "mock-query",
		Table: table,
		Build: func(lf lazy.LazyFrame) lazy.LazyFrame { return lf },
	}
}

func mockRelation() *proptest.Relation {
	return rel([]memory.Field{intField, strField},
		[]proptest.Cell{i64(1), str("a")},
		[]proptest.Cell{i64(2), str("b")},
		[]proptest.Cell{null(), str("c")},
	)
}

// TestHarnessReportsInjectedDivergence feeds the harness a deliberately
// corrupted mock backend and asserts the mismatch error is readable: it must
// name the query, both backends, and the exact row, column, and cell values.
func TestHarnessReportsInjectedDivergence(t *testing.T) {
	reference := mockRelation()

	mutatedValue := mockRelation()
	mutatedValue.Rows[1][1] = str("zzz")

	mutatedNull := mockRelation()
	mutatedNull.Rows[2][0] = i64(0)

	mutatedRowCount := mockRelation()
	mutatedRowCount.Rows = mutatedRowCount.Rows[:2]

	mutatedColumn := mockRelation()
	mutatedColumn.Fields[1] = memory.Field{Name: "renamed", Type: memory.TypeString}

	tests := []struct {
		name     string
		mutant   *proptest.Relation
		contains []string
	}{
		{
			name:   "value divergence",
			mutant: mutatedValue,
			contains: []string{
				`query "mock-query"`, `backend "mutant"`, `from "reference"`,
				`row 1 column "s"`, `"b"`, `"zzz"`,
			},
		},
		{
			name:   "NULL bitmap divergence",
			mutant: mutatedNull,
			contains: []string{
				`query "mock-query"`, `row 2 column "v"`, "want NULL", "got 0",
			},
		},
		{
			name:   "row count divergence",
			mutant: mutatedRowCount,
			contains: []string{
				`query "mock-query"`, "row count mismatch", "want 3 rows, got 2 rows",
			},
		},
		{
			name:   "schema divergence",
			mutant: mutatedColumn,
			contains: []string{
				`query "mock-query"`, `column 1 mismatch`, `"s"`, `"renamed"`,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, err := proptest.NewHarness(
				fixedBackend("reference", reference),
				fixedBackend("mutant", tc.mutant),
			)
			if err != nil {
				t.Fatal(err)
			}
			err = h.Check(context.Background(), mockQuery(t))
			if err == nil {
				t.Fatal("injected divergence was not reported")
			}
			for _, fragment := range tc.contains {
				if !strings.Contains(err.Error(), fragment) {
					t.Fatalf("error %q is missing fragment %q", err, fragment)
				}
			}
		})
	}
}

// TestHarnessReportsBackendFailure ensures a backend execution error surfaces
// with query and backend context rather than a bare failure.
func TestHarnessReportsBackendFailure(t *testing.T) {
	broken := proptest.Backend{
		Name: "broken",
		Run: func(context.Context, *proptest.Table, func(lazy.LazyFrame) lazy.LazyFrame) (*proptest.Relation, error) {
			return nil, fmt.Errorf("simulated execution failure")
		},
	}
	h, err := proptest.NewHarness(fixedBackend("reference", mockRelation()), broken)
	if err != nil {
		t.Fatal(err)
	}
	err = h.Check(context.Background(), mockQuery(t))
	if err == nil {
		t.Fatal("backend failure was not reported")
	}
	for _, fragment := range []string{`query "mock-query"`, `backend "broken"`, "simulated execution failure"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error %q is missing fragment %q", err, fragment)
		}
	}
}
