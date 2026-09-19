package proptest_test

import (
	"context"
	"testing"

	"gocolumn/internal/proptest"
	"gocolumn/kernel"
	"gocolumn/lazy"
	"gocolumn/memory"
)

// scalarBackend collects queries on a single forced-scalar engine for
// property assertions that do not need the multi-backend matrix.
func scalarBackend() proptest.Backend {
	return proptest.EngineBackend("scalar", kernel.ModeScalar)
}

func collect(t *testing.T, table *proptest.Table, build func(lazy.LazyFrame) lazy.LazyFrame) *proptest.Relation {
	t.Helper()
	rel, err := scalarBackend().Run(context.Background(), table, build)
	if err != nil {
		t.Fatal(err)
	}
	return rel
}

// TestPropertyFilterIsIdempotent verifies Filter(p) ∘ Filter(p) == Filter(p)
// on generated data with NULLs in every predicate column.
func TestPropertyFilterIsIdempotent(t *testing.T) {
	table := consistencyTable(t)
	once := collect(t, table, func(lf lazy.LazyFrame) lazy.LazyFrame {
		return lf.Filter(lazy.Col("v").Ge(lazy.Lit(100)))
	})
	twice := collect(t, table, func(lf lazy.LazyFrame) lazy.LazyFrame {
		return lf.Filter(lazy.Col("v").Ge(lazy.Lit(100))).Filter(lazy.Col("v").Ge(lazy.Lit(100)))
	})
	if err := proptest.CheckEqual(once, twice); err != nil {
		t.Fatalf("filter is not idempotent: %v", err)
	}
}

// TestPropertyCollectIsDeterministic verifies that collecting the same plan
// twice yields byte-identical relations.
func TestPropertyCollectIsDeterministic(t *testing.T) {
	table := consistencyTable(t)
	build := func(lf lazy.LazyFrame) lazy.LazyFrame {
		return lf.Filter(lazy.Col("f").Lt(lazy.F64(0))).Select("v", "k")
	}
	first := collect(t, table, build)
	second := collect(t, table, build)
	if err := proptest.CheckEqual(first, second); err != nil {
		t.Fatalf("repeated collect diverged: %v", err)
	}
}

// TestPropertySortIsOrderedPermutation verifies the two halves of sort
// correctness on generated data: the output is a permutation of the input
// and it is ordered on the sort key with NULLs last. The full row including
// the bool column is sorted, exercising bool as payload and as sort key.
func TestPropertySortIsOrderedPermutation(t *testing.T) {
	table := consistencyTable(t)
	input := collect(t, table, func(lf lazy.LazyFrame) lazy.LazyFrame {
		return lf
	})
	sorted := collect(t, table, func(lf lazy.LazyFrame) lazy.LazyFrame {
		return lf.Sort(lazy.Asc("v"))
	})
	if err := proptest.CheckRowPermutation(input, sorted); err != nil {
		t.Fatalf("sorted output is not a permutation of the input: %v", err)
	}
	if err := proptest.CheckSorted(sorted, "v", false, true); err != nil {
		t.Fatalf("sorted output is not ascending with NULLs last: %v", err)
	}
	desc := collect(t, table, func(lf lazy.LazyFrame) lazy.LazyFrame {
		return lf.Sort(lazy.Desc("v"))
	})
	if err := proptest.CheckSorted(desc, "v", true, true); err != nil {
		t.Fatalf("descending output violates ordering: %v", err)
	}
	byBool := collect(t, table, func(lf lazy.LazyFrame) lazy.LazyFrame {
		return lf.Sort(lazy.Asc("b"), lazy.Asc("v"))
	})
	if err := proptest.CheckRowPermutation(input, byBool); err != nil {
		t.Fatalf("bool-keyed sort is not a permutation of the input: %v", err)
	}
	if err := proptest.CheckSorted(byBool, "b", false, true); err != nil {
		t.Fatalf("bool-keyed sort violates ordering: %v", err)
	}
}

// TestPropertyTopKMatchesFullSortPrefix verifies TopK(k) equals the first k
// rows of a full sort on a unique ordering key (no tie ambiguity).
func TestPropertyTopKMatchesFullSortPrefix(t *testing.T) {
	gen := proptest.NewGen(77)
	const rows = 400
	perm := gen.Perm(rows)
	unique := make([]int64, rows)
	for i, p := range perm {
		unique[i] = int64(p)
	}
	table, err := gen.Table(rows,
		proptest.ColumnSpec{Name: "k", Type: memory.TypeString, Cardinality: 9},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Attach the unique ordering key as an extra column.
	table.Columns = append([]proptest.Column{{
		Name:   "u",
		Type:   memory.TypeInt64,
		Int64s: unique,
	}}, table.Columns...)

	full := collect(t, table, func(lf lazy.LazyFrame) lazy.LazyFrame {
		return lf.Sort(lazy.Desc("u"))
	})
	top := collect(t, table, func(lf lazy.LazyFrame) lazy.LazyFrame {
		return lf.TopK(37, lazy.Desc("u"))
	})
	if err := proptest.CheckTopKPrefix(full, top, 37); err != nil {
		t.Fatalf("top-37 diverges from full sort prefix: %v", err)
	}
	if err := proptest.CheckSorted(top, "u", true, true); err != nil {
		t.Fatalf("top-37 output is not ordered: %v", err)
	}
}
