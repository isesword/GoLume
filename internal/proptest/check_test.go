package proptest_test

import (
	"math"
	"strings"
	"testing"

	"gocolumn/internal/proptest"
	"gocolumn/memory"
)

// rel builds a relation directly from typed columns for known-answer tests.
func rel(fields []memory.Field, rows ...[]proptest.Cell) *proptest.Relation {
	return &proptest.Relation{Fields: fields, Rows: rows}
}

func i64(v int64) proptest.Cell   { return proptest.Cell{Valid: true, Value: v} }
func f64(v float64) proptest.Cell { return proptest.Cell{Valid: true, Value: v} }
func str(v string) proptest.Cell  { return proptest.Cell{Valid: true, Value: v} }
func null() proptest.Cell         { return proptest.Cell{} }

var intField = memory.Field{Name: "v", Type: memory.TypeInt64}
var strField = memory.Field{Name: "s", Type: memory.TypeString}

func TestCheckEqualKnownAnswers(t *testing.T) {
	base := rel([]memory.Field{intField, strField},
		[]proptest.Cell{i64(1), str("a")},
		[]proptest.Cell{null(), str("b")},
	)
	same := rel([]memory.Field{intField, strField},
		[]proptest.Cell{i64(1), str("a")},
		[]proptest.Cell{null(), str("b")},
	)
	if err := proptest.CheckEqual(base, same); err != nil {
		t.Fatalf("identical relations rejected: %v", err)
	}

	tests := []struct {
		name    string
		other   *proptest.Relation
		message string
	}{
		{
			name: "value differs",
			other: rel([]memory.Field{intField, strField},
				[]proptest.Cell{i64(1), str("a")},
				[]proptest.Cell{null(), str("c")}),
			message: `row 1 column "s"`,
		},
		{
			name: "NULL flag differs",
			other: rel([]memory.Field{intField, strField},
				[]proptest.Cell{i64(1), str("a")},
				[]proptest.Cell{i64(0), str("b")}),
			message: `row 1 column "v"`,
		},
		{
			name: "row count differs",
			other: rel([]memory.Field{intField, strField},
				[]proptest.Cell{i64(1), str("a")}),
			message: "row count mismatch",
		},
		{
			name: "column name differs",
			other: rel([]memory.Field{intField, {Name: "x", Type: memory.TypeString}},
				[]proptest.Cell{i64(1), str("a")},
				[]proptest.Cell{null(), str("b")}),
			message: "column 1 mismatch",
		},
		{
			name: "column count differs",
			other: rel([]memory.Field{intField},
				[]proptest.Cell{i64(1)},
				[]proptest.Cell{null()}),
			message: "column count mismatch",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := proptest.CheckEqual(base, tc.other)
			if err == nil {
				t.Fatal("expected a mismatch error")
			}
			if !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("error %q does not contain %q", err, tc.message)
			}
		})
	}
}

func TestCheckEqualNilRelations(t *testing.T) {
	nonNil := rel([]memory.Field{intField})
	if err := proptest.CheckEqual(nil, nonNil); err == nil {
		t.Fatal("nil want relation accepted")
	}
	if err := proptest.CheckEqual(nonNil, nil); err == nil {
		t.Fatal("nil got relation accepted")
	}
}

func TestCheckRowPermutationKnownAnswers(t *testing.T) {
	input := rel([]memory.Field{intField},
		[]proptest.Cell{i64(2)},
		[]proptest.Cell{i64(2)},
		[]proptest.Cell{null()},
	)
	reordered := rel([]memory.Field{intField},
		[]proptest.Cell{null()},
		[]proptest.Cell{i64(2)},
		[]proptest.Cell{i64(2)},
	)
	if err := proptest.CheckRowPermutation(input, reordered); err != nil {
		t.Fatalf("reordered duplicate rows rejected: %v", err)
	}

	tests := []struct {
		name    string
		other   *proptest.Relation
		message string
	}{
		{
			name: "row removed",
			other: rel([]memory.Field{intField},
				[]proptest.Cell{i64(2)},
				[]proptest.Cell{null()}),
			message: "row count mismatch",
		},
		{
			name: "row value changed",
			other: rel([]memory.Field{intField},
				[]proptest.Cell{i64(2)},
				[]proptest.Cell{i64(2)},
				[]proptest.Cell{i64(7)}),
			message: "missing",
		},
		{
			name: "duplicate count changed",
			other: rel([]memory.Field{intField},
				[]proptest.Cell{i64(2)},
				[]proptest.Cell{i64(2)},
				[]proptest.Cell{i64(2)}),
			message: "missing",
		},
		{
			name: "NULL became value",
			other: rel([]memory.Field{intField},
				[]proptest.Cell{i64(2)},
				[]proptest.Cell{i64(2)},
				[]proptest.Cell{i64(0)}),
			message: "missing",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := proptest.CheckRowPermutation(input, tc.other)
			if err == nil {
				t.Fatal("expected a permutation error")
			}
			if !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("error %q does not contain %q", err, tc.message)
			}
		})
	}
}

func TestCheckSortedKnownAnswers(t *testing.T) {
	ascending := rel([]memory.Field{intField},
		[]proptest.Cell{i64(1)},
		[]proptest.Cell{i64(2)},
		[]proptest.Cell{i64(2)},
		[]proptest.Cell{i64(5)},
	)
	if err := proptest.CheckSorted(ascending, "v", false, true); err != nil {
		t.Fatalf("non-decreasing input rejected: %v", err)
	}
	if err := proptest.CheckSorted(ascending, "v", true, true); err == nil {
		t.Fatal("ascending input accepted as descending")
	}

	descending := rel([]memory.Field{intField},
		[]proptest.Cell{i64(5)},
		[]proptest.Cell{i64(2)},
		[]proptest.Cell{i64(2)},
		[]proptest.Cell{i64(1)},
	)
	if err := proptest.CheckSorted(descending, "v", true, true); err != nil {
		t.Fatalf("non-increasing input rejected: %v", err)
	}
	if err := proptest.CheckSorted(descending, "v", false, true); err == nil {
		t.Fatal("descending input accepted as ascending")
	}

	unsorted := rel([]memory.Field{intField},
		[]proptest.Cell{i64(1)},
		[]proptest.Cell{i64(3)},
		[]proptest.Cell{i64(2)},
	)
	err := proptest.CheckSorted(unsorted, "v", false, true)
	if err == nil || !strings.Contains(err.Error(), "row 2") {
		t.Fatalf("unsorted input not reported at row 2: %v", err)
	}

	nullsLast := rel([]memory.Field{intField},
		[]proptest.Cell{i64(1)},
		[]proptest.Cell{i64(9)},
		[]proptest.Cell{null()},
		[]proptest.Cell{null()},
	)
	if err := proptest.CheckSorted(nullsLast, "v", false, true); err != nil {
		t.Fatalf("nulls-last input rejected: %v", err)
	}
	if err := proptest.CheckSorted(nullsLast, "v", false, false); err == nil {
		t.Fatal("nulls-last input accepted as nulls-first")
	}

	nullsFirst := rel([]memory.Field{intField},
		[]proptest.Cell{null()},
		[]proptest.Cell{i64(1)},
		[]proptest.Cell{i64(9)},
	)
	if err := proptest.CheckSorted(nullsFirst, "v", false, false); err != nil {
		t.Fatalf("nulls-first input rejected: %v", err)
	}
	if err := proptest.CheckSorted(nullsFirst, "v", false, true); err == nil {
		t.Fatal("nulls-first input accepted as nulls-last")
	}

	strings_ := rel([]memory.Field{strField},
		[]proptest.Cell{str("apple")},
		[]proptest.Cell{str("banana")},
		[]proptest.Cell{str("cherry")},
	)
	if err := proptest.CheckSorted(strings_, "s", false, true); err != nil {
		t.Fatalf("sorted strings rejected: %v", err)
	}
	if err := proptest.CheckSorted(strings_, "s", true, true); err == nil {
		t.Fatal("ascending strings accepted as descending")
	}

	if err := proptest.CheckSorted(ascending, "missing", false, true); err == nil {
		t.Fatal("missing sort column accepted")
	}
	if err := proptest.CheckSorted(nil, "v", false, true); err == nil {
		t.Fatal("nil relation accepted")
	}
}

func TestCheckTopKPrefixKnownAnswers(t *testing.T) {
	full := rel([]memory.Field{intField},
		[]proptest.Cell{i64(9)},
		[]proptest.Cell{i64(7)},
		[]proptest.Cell{i64(5)},
		[]proptest.Cell{i64(3)},
	)
	top2 := rel([]memory.Field{intField},
		[]proptest.Cell{i64(9)},
		[]proptest.Cell{i64(7)},
	)
	if err := proptest.CheckTopKPrefix(full, top2, 2); err != nil {
		t.Fatalf("correct top-2 prefix rejected: %v", err)
	}
	// k larger than the input clamps to the full relation.
	if err := proptest.CheckTopKPrefix(full, full, 10); err != nil {
		t.Fatalf("clamped top-k rejected: %v", err)
	}

	wrongValue := rel([]memory.Field{intField},
		[]proptest.Cell{i64(9)},
		[]proptest.Cell{i64(5)},
	)
	err := proptest.CheckTopKPrefix(full, wrongValue, 2)
	if err == nil || !strings.Contains(err.Error(), "row 1") {
		t.Fatalf("wrong top-k value not reported at row 1: %v", err)
	}

	tooMany := rel([]memory.Field{intField},
		[]proptest.Cell{i64(9)},
		[]proptest.Cell{i64(7)},
		[]proptest.Cell{i64(5)},
	)
	err = proptest.CheckTopKPrefix(full, tooMany, 2)
	if err == nil || !strings.Contains(err.Error(), "3 rows, want 2") {
		t.Fatalf("oversized top-k not reported: %v", err)
	}

	if err := proptest.CheckTopKPrefix(full, top2, -1); err == nil {
		t.Fatal("negative k accepted")
	}
}

func TestCheckSortedAllCellTypes(t *testing.T) {
	u32Field := memory.Field{Name: "u", Type: memory.TypeUint32}
	u64Field := memory.Field{Name: "n", Type: memory.TypeUint64}
	boolField := memory.Field{Name: "b", Type: memory.TypeBool}
	boolCell := func(v bool) proptest.Cell { return proptest.Cell{Valid: true, Value: v} }
	u32 := func(v uint32) proptest.Cell { return proptest.Cell{Valid: true, Value: v} }
	u64 := func(v uint64) proptest.Cell { return proptest.Cell{Valid: true, Value: v} }

	tests := []struct {
		name    string
		rel     *proptest.Relation
		column  string
		desc    bool
		wantErr string
	}{
		{
			name: "uint32 ascending",
			rel: rel([]memory.Field{u32Field},
				[]proptest.Cell{u32(1)}, []proptest.Cell{u32(2)}, []proptest.Cell{u32(2)}),
			column: "u",
		},
		{
			name: "uint32 violation",
			rel: rel([]memory.Field{u32Field},
				[]proptest.Cell{u32(3)}, []proptest.Cell{u32(1)}),
			column: "u", wantErr: "row 1",
		},
		{
			name: "uint64 descending",
			rel: rel([]memory.Field{u64Field},
				[]proptest.Cell{u64(9)}, []proptest.Cell{u64(4)}, []proptest.Cell{u64(0)}),
			column: "n", desc: true,
		},
		{
			name: "uint64 descending violation",
			rel: rel([]memory.Field{u64Field},
				[]proptest.Cell{u64(9)}, []proptest.Cell{u64(10)}),
			column: "n", desc: true, wantErr: "row 1",
		},
		{
			name: "bool ascending",
			rel: rel([]memory.Field{boolField},
				[]proptest.Cell{boolCell(false)}, []proptest.Cell{boolCell(true)}),
			column: "b",
		},
		{
			name: "bool violation",
			rel: rel([]memory.Field{boolField},
				[]proptest.Cell{boolCell(true)}, []proptest.Cell{boolCell(false)}),
			column: "b", wantErr: "row 1",
		},
		{
			name: "float ascending",
			rel: rel([]memory.Field{{Name: "f", Type: memory.TypeFloat64}},
				[]proptest.Cell{f64(-1.5)}, []proptest.Cell{f64(0)}, []proptest.Cell{f64(2.5)}),
			column: "f",
		},
		{
			name: "mixed cell types",
			rel: rel([]memory.Field{intField},
				[]proptest.Cell{i64(1)}, []proptest.Cell{str("oops")}),
			column: "v", wantErr: "type mismatch",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := proptest.CheckSorted(tc.rel, tc.column, tc.desc, true)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestCheckEqualFloatsAreBitwise(t *testing.T) {
	floatField := memory.Field{Name: "f", Type: memory.TypeFloat64}
	a := rel([]memory.Field{floatField}, []proptest.Cell{f64(0)})
	b := rel([]memory.Field{floatField}, []proptest.Cell{f64(math.Copysign(0, -1))})
	if err := proptest.CheckEqual(a, b); err == nil {
		t.Fatal("+0 and -0 must not compare equal")
	}
	nan1 := rel([]memory.Field{floatField}, []proptest.Cell{f64(math.NaN())})
	nan2 := rel([]memory.Field{floatField}, []proptest.Cell{f64(math.NaN())})
	if err := proptest.CheckEqual(nan1, nan2); err != nil {
		t.Fatalf("identical NaN bit patterns must compare equal: %v", err)
	}
}
