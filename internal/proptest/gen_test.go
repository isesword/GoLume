package proptest_test

import (
	"testing"

	"gocolumn/internal/proptest"
	"gocolumn/memory"
)

func standardSpecs() []proptest.ColumnSpec {
	return []proptest.ColumnSpec{
		{Name: "v", Type: memory.TypeInt64, NullProbability: 0.2},
		{Name: "f", Type: memory.TypeFloat64, NullProbability: 0.2},
		{Name: "k", Type: memory.TypeString, NullProbability: 0.2, Cardinality: 8},
		{Name: "b", Type: memory.TypeBool, NullProbability: 0.2},
	}
}

func TestGenTableIsDeterministicPerSeed(t *testing.T) {
	a, err := proptest.NewGen(99).Table(257, standardSpecs()...)
	if err != nil {
		t.Fatal(err)
	}
	b, err := proptest.NewGen(99).Table(257, standardSpecs()...)
	if err != nil {
		t.Fatal(err)
	}
	if a.Seed != 99 || b.Seed != 99 {
		t.Fatalf("unexpected seeds %d, %d", a.Seed, b.Seed)
	}
	if len(a.Columns) != len(b.Columns) {
		t.Fatalf("column counts differ: %d vs %d", len(a.Columns), len(b.Columns))
	}
	for i := range a.Columns {
		ca, cb := a.Columns[i], b.Columns[i]
		if ca.Name != cb.Name || ca.Type != cb.Type {
			t.Fatalf("column %d header differs: %q/%s vs %q/%s", i, ca.Name, ca.Type, cb.Name, cb.Type)
		}
		if !equalSlice(ca.Int64s, cb.Int64s) || !equalSlice(ca.Float64s, cb.Float64s) ||
			!equalSlice(ca.Strings, cb.Strings) || !equalSlice(ca.Bools, cb.Bools) || !equalSlice(ca.Nulls, cb.Nulls) {
			t.Fatalf("column %q differs between identical seeds", ca.Name)
		}
	}
}

func TestGenDifferentSeedsDiffer(t *testing.T) {
	spec := proptest.ColumnSpec{Name: "v", Type: memory.TypeInt64}
	a, err := proptest.NewGen(1).Table(64, spec)
	if err != nil {
		t.Fatal(err)
	}
	b, err := proptest.NewGen(2).Table(64, spec)
	if err != nil {
		t.Fatal(err)
	}
	if equalSlice(a.Columns[0].Int64s, b.Columns[0].Int64s) {
		t.Fatal("distinct seeds produced identical int64 columns")
	}
}

func TestGenNullProbabilityExtremes(t *testing.T) {
	never, err := proptest.NewGen(7).Table(100, proptest.ColumnSpec{Name: "v", Type: memory.TypeInt64, NullProbability: 0})
	if err != nil {
		t.Fatal(err)
	}
	if never.Columns[0].Nulls != nil {
		t.Fatal("null probability 0 must not allocate a NULL bitmap")
	}
	always, err := proptest.NewGen(7).Table(100, proptest.ColumnSpec{Name: "v", Type: memory.TypeInt64, NullProbability: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i, null := range always.Columns[0].Nulls {
		if !null {
			t.Fatalf("row %d is valid under null probability 1", i)
		}
	}
}

func TestGenCardinalityBoundsValues(t *testing.T) {
	gen := proptest.NewGen(3)
	if gen.Seed() != 3 {
		t.Fatalf("Seed() = %d, want 3", gen.Seed())
	}
	table, err := gen.Table(1000, proptest.ColumnSpec{Name: "g", Type: memory.TypeInt64, Cardinality: 6})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int64]struct{})
	for _, v := range table.Columns[0].Int64s {
		if v < 0 || v >= 6 {
			t.Fatalf("value %d outside cardinality domain [0,6)", v)
		}
		seen[v] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("expected duplicate-producing domain, saw %d distinct values", len(seen))
	}
	floats, err := proptest.NewGen(4).Table(500, proptest.ColumnSpec{Name: "f", Type: memory.TypeFloat64, Cardinality: 4})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range floats.Columns[0].Float64s {
		if v < 0.5 || v >= 4.5 {
			t.Fatalf("float %v outside cardinality domain", v)
		}
	}
}

func TestGenRejectsInvalidSpecs(t *testing.T) {
	tests := []struct {
		name  string
		rows  int
		specs []proptest.ColumnSpec
	}{
		{name: "no columns", rows: 1, specs: nil},
		{name: "negative rows", rows: -1, specs: []proptest.ColumnSpec{{Name: "v", Type: memory.TypeInt64}}},
		{name: "empty name", rows: 1, specs: []proptest.ColumnSpec{{Type: memory.TypeInt64}}},
		{name: "duplicate name", rows: 1, specs: []proptest.ColumnSpec{{Name: "v", Type: memory.TypeInt64}, {Name: "v", Type: memory.TypeBool}}},
		{name: "unsupported type", rows: 1, specs: []proptest.ColumnSpec{{Name: "v", Type: memory.TypeInt32}}},
		{name: "null probability below range", rows: 1, specs: []proptest.ColumnSpec{{Name: "v", Type: memory.TypeInt64, NullProbability: -0.1}}},
		{name: "null probability above range", rows: 1, specs: []proptest.ColumnSpec{{Name: "v", Type: memory.TypeInt64, NullProbability: 1.1}}},
		{name: "negative cardinality", rows: 1, specs: []proptest.ColumnSpec{{Name: "v", Type: memory.TypeInt64, Cardinality: -2}}},
		{name: "negative string length", rows: 1, specs: []proptest.ColumnSpec{{Name: "v", Type: memory.TypeString, MaxStringLength: -1}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := proptest.NewGen(1).Table(tc.rows, tc.specs...); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestGenZeroRows(t *testing.T) {
	table, err := proptest.NewGen(5).Table(0, standardSpecs()...)
	if err != nil {
		t.Fatal(err)
	}
	if table.Rows() != 0 {
		t.Fatalf("rows = %d, want 0", table.Rows())
	}
	if _, err := table.Source(); err != nil {
		t.Fatal(err)
	}
}

func TestTableSourceRoundTrip(t *testing.T) {
	gen := proptest.NewGen(11)
	table, err := gen.Table(130,
		proptest.ColumnSpec{Name: "v", Type: memory.TypeInt64, NullProbability: 0.25},
		proptest.ColumnSpec{Name: "s", Type: memory.TypeString, NullProbability: 0.25, MaxStringLength: 6},
		proptest.ColumnSpec{Name: "f", Type: memory.TypeFloat64},
		proptest.ColumnSpec{Name: "b", Type: memory.TypeBool, NullProbability: 0.25},
	)
	if err != nil {
		t.Fatal(err)
	}
	src, err := table.Source()
	if err != nil {
		t.Fatal(err)
	}
	schema := src.Schema()
	if len(schema.Fields) != 4 {
		t.Fatalf("schema has %d fields, want 4", len(schema.Fields))
	}
	for i, name := range []string{"v", "s", "f", "b"} {
		if schema.Fields[i].Name != name {
			t.Fatalf("field %d is %q, want %q", i, schema.Fields[i].Name, name)
		}
	}
	// Spot-check the batch vectors against the generated columns, including
	// NULL bitmap positions.
	batch := table.Batch()
	if batch.Rows != 130 {
		t.Fatalf("batch rows = %d, want 130", batch.Rows)
	}
	ints := batch.Int64s["v"]
	strings_ := batch.Strings["s"]
	bools := batch.Uint32s["b"]
	colV, _ := table.Column("v")
	colS, _ := table.Column("s")
	colB, _ := table.Column("b")
	for row := 0; row < 130; row++ {
		if ints.IsValid(row) == colV.IsNull(row) {
			t.Fatalf("row %d: int64 validity disagrees with NULL flag", row)
		}
		if colV.IsNull(row) {
			continue
		}
		if ints.Values[row] != colV.Int64s[row] {
			t.Fatalf("row %d: int64 value mismatch", row)
		}
		if string(strings_.ValueBytes(row)) != colS.Strings[row] {
			t.Fatalf("row %d: string value mismatch", row)
		}
		if (bools.Values[row] != 0) != colB.Bools[row] {
			t.Fatalf("row %d: bool value mismatch", row)
		}
	}
}

func TestGenPermShufflesFullRange(t *testing.T) {
	gen := proptest.NewGen(21)
	perm := gen.Perm(200)
	if len(perm) != 200 {
		t.Fatalf("perm length = %d, want 200", len(perm))
	}
	seen := make(map[int]struct{}, 200)
	identity := true
	for i, v := range perm {
		if v < 0 || v >= 200 {
			t.Fatalf("perm value %d outside [0,200)", v)
		}
		seen[v] = struct{}{}
		if v != i {
			identity = false
		}
	}
	if len(seen) != 200 {
		t.Fatal("perm contains duplicates")
	}
	if identity {
		t.Fatal("perm is the identity permutation")
	}
}

func equalSlice[T comparable](a, b []T) bool {
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
