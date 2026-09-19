package oracle

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// FloatRelTolerance is the documented relative-error tolerance applied when
// comparing float64 result cells between the oracle and the engine.
//
// Element-wise float arithmetic (+, -, *, /) is exactly rounded IEEE-754 in
// both implementations, so projection results compare bit-exactly. The
// tolerance exists for aggregate-style reductions (AVG) whose accumulation
// order may differ between the oracle's single left-to-right pass and the
// engine's per-partition partial sums: with the generator's bounded int64
// inputs every partial sum is exact below 2^53, so in practice even AVG
// compares bit-exactly and the tolerance is a safety net, not a requirement.
// NaN cells only ever equal other NaN cells.
const FloatRelTolerance = 1e-12

// Kind identifies one logical scalar type in the oracle's bounded subset.
type Kind int

const (
	// KindInvalid marks an unspecified type.
	KindInvalid Kind = iota
	// KindInt64 identifies signed 64-bit integers.
	KindInt64
	// KindUint64 identifies unsigned 64-bit integers (engine COUNT outputs).
	KindUint64
	// KindFloat64 identifies IEEE-754 double-precision values.
	KindFloat64
	// KindString identifies UTF-8 strings.
	KindString
	// KindBool identifies booleans.
	KindBool
)

// String returns the stable textual name of the kind.
func (k Kind) String() string {
	switch k {
	case KindInt64:
		return "int64"
	case KindUint64:
		return "uint64"
	case KindFloat64:
		return "float64"
	case KindString:
		return "string"
	case KindBool:
		return "bool"
	default:
		return "invalid"
	}
}

// Value is one nullable scalar cell. The typed field matching Kind is
// meaningful only when Null is false; a NULL Value still carries its Kind so
// result schemas stay well typed.
type Value struct {
	Kind Kind
	Null bool
	I64  int64
	U64  uint64
	F64  float64
	Str  string
	Bool bool
}

// Int64 constructs a non-NULL int64 value.
func Int64(v int64) Value { return Value{Kind: KindInt64, I64: v} }

// Uint64 constructs a non-NULL uint64 value.
func Uint64(v uint64) Value { return Value{Kind: KindUint64, U64: v} }

// Float64 constructs a non-NULL float64 value.
func Float64(v float64) Value { return Value{Kind: KindFloat64, F64: v} }

// String constructs a non-NULL string value.
func String(v string) Value { return Value{Kind: KindString, Str: v} }

// Bool constructs a non-NULL bool value.
func Bool(v bool) Value { return Value{Kind: KindBool, Bool: v} }

// Null constructs a NULL value of the given kind.
func Null(k Kind) Value { return Value{Kind: k, Null: true} }

// String renders the value for failure diagnostics.
func (v Value) String() string {
	if v.Null {
		return "NULL"
	}
	switch v.Kind {
	case KindInt64:
		return strconv.FormatInt(v.I64, 10)
	case KindUint64:
		return strconv.FormatUint(v.U64, 10)
	case KindFloat64:
		return strconv.FormatFloat(v.F64, 'g', -1, 64)
	case KindString:
		return strconv.Quote(v.Str)
	case KindBool:
		return strconv.FormatBool(v.Bool)
	default:
		return "<?>"
	}
}

// Column describes one oracle table column. Bounded is generator metadata: it
// reports that every generated non-NULL value has magnitude at most
// generatorBoundedMagnitude, which keeps checked int64 arithmetic and SUM/AVG
// accumulations overflow-free. Hand-built tables should leave it false unless
// the invariant holds.
type Column struct {
	Name    string
	Kind    Kind
	Bounded bool
}

// Table is a row-major in-memory relation: Rows[i][j] is the cell of row i in
// Columns[j]. The row-major layout is intentional — this is the reference
// implementation, not the fast one.
type Table struct {
	Columns []Column
	Rows    [][]Value
}

// NewTable validates shapes and kinds and returns the table.
func NewTable(columns []Column, rows [][]Value) (Table, error) {
	t := Table{Columns: columns, Rows: rows}
	return t, t.Validate()
}

// Validate checks that every row matches the column list in length and kind.
func (t Table) Validate() error {
	seen := make(map[string]struct{}, len(t.Columns))
	for _, c := range t.Columns {
		if c.Name == "" {
			return fmt.Errorf("oracle: table has an unnamed column")
		}
		if c.Kind == KindInvalid {
			return fmt.Errorf("oracle: column %q has invalid kind", c.Name)
		}
		if _, dup := seen[c.Name]; dup {
			return fmt.Errorf("oracle: duplicate column %q", c.Name)
		}
		seen[c.Name] = struct{}{}
	}
	for i, row := range t.Rows {
		if len(row) != len(t.Columns) {
			return fmt.Errorf("oracle: row %d has %d cells, want %d", i, len(row), len(t.Columns))
		}
		for j, cell := range row {
			if cell.Kind != t.Columns[j].Kind {
				return fmt.Errorf("oracle: row %d column %q has kind %s, want %s", i, t.Columns[j].Name, cell.Kind, t.Columns[j].Kind)
			}
		}
	}
	return nil
}

// ColumnIndex returns the position of the named column.
func (t Table) ColumnIndex(name string) (int, bool) {
	for i, c := range t.Columns {
		if c.Name == name {
			return i, true
		}
	}
	return -1, false
}

// NumRows reports the row count.
func (t Table) NumRows() int { return len(t.Rows) }

// String renders a bounded textual dump for failure diagnostics.
func (t Table) String() string {
	var b strings.Builder
	names := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		names[i] = c.Name + ":" + c.Kind.String()
	}
	fmt.Fprintf(&b, "(%d rows) %s", len(t.Rows), strings.Join(names, ", "))
	const maxRows = 12
	for i, row := range t.Rows {
		if i == maxRows {
			fmt.Fprintf(&b, "\n  ... %d more rows", len(t.Rows)-maxRows)
			break
		}
		cells := make([]string, len(row))
		for j, cell := range row {
			cells[j] = cell.String()
		}
		fmt.Fprintf(&b, "\n  %s", strings.Join(cells, " | "))
	}
	return b.String()
}

// valuesEqual compares two cells under the documented float rule: NaN equals
// NaN, numeric equality uses Go == (so -0.0 equals +0.0), and otherwise a
// relative error of at most FloatRelTolerance is accepted. Kinds must match.
func valuesEqual(a, b Value) bool {
	if a.Kind != b.Kind {
		return false
	}
	if a.Null || b.Null {
		return a.Null && b.Null
	}
	switch a.Kind {
	case KindInt64:
		return a.I64 == b.I64
	case KindUint64:
		return a.U64 == b.U64
	case KindString:
		return a.Str == b.Str
	case KindBool:
		return a.Bool == b.Bool
	case KindFloat64:
		if math.IsNaN(a.F64) || math.IsNaN(b.F64) {
			return math.IsNaN(a.F64) && math.IsNaN(b.F64)
		}
		if a.F64 == b.F64 {
			return true
		}
		if math.IsInf(a.F64, 0) || math.IsInf(b.F64, 0) {
			return false
		}
		denom := math.Max(math.Abs(a.F64), math.Abs(b.F64))
		return math.Abs(a.F64-b.F64) <= FloatRelTolerance*denom
	default:
		return false
	}
}

// CompareTables verifies that got (engine) equals want (oracle): identical
// column names and kinds, identical row counts, and identical cells including
// NULL positions. When ordered is false both tables are first canonicalized
// with SortCanonical, which makes the comparison order-insensitive; pass
// ordered=true only when the query defines the row order (Query.Ordered). A
// nil return means equality; otherwise the error describes the first
// difference.
func CompareTables(want, got Table, ordered bool) error {
	if !ordered {
		want = SortCanonical(want)
		got = SortCanonical(got)
	}
	if len(want.Columns) != len(got.Columns) {
		return fmt.Errorf("column count: oracle %d, engine %d", len(want.Columns), len(got.Columns))
	}
	for i := range want.Columns {
		if want.Columns[i].Name != got.Columns[i].Name {
			return fmt.Errorf("column %d name: oracle %q, engine %q", i, want.Columns[i].Name, got.Columns[i].Name)
		}
		if want.Columns[i].Kind != got.Columns[i].Kind {
			return fmt.Errorf("column %q kind: oracle %s, engine %s", want.Columns[i].Name, want.Columns[i].Kind, got.Columns[i].Kind)
		}
	}
	if len(want.Rows) != len(got.Rows) {
		return fmt.Errorf("row count: oracle %d, engine %d", len(want.Rows), len(got.Rows))
	}
	for i := range want.Rows {
		for j := range want.Columns {
			a, b := want.Rows[i][j], got.Rows[i][j]
			if a.Null != b.Null {
				return fmt.Errorf("row %d column %q NULL mismatch: oracle %s, engine %s", i, want.Columns[j].Name, a, b)
			}
			if !valuesEqual(a, b) {
				return fmt.Errorf("row %d column %q: oracle %s, engine %s", i, want.Columns[j].Name, a, b)
			}
		}
	}
	return nil
}

// compareCellsCanonical defines the total order used by SortCanonical. It is
// unrelated to SQL comparison semantics; it only needs to be total and
// deterministic so that two equal multisets canonicalize identically.
func compareCellsCanonical(a, b Value) int {
	if a.Null != b.Null {
		if a.Null {
			return -1
		}
		return 1
	}
	switch a.Kind {
	case KindInt64:
		return cmpOrdered(a.I64, b.I64)
	case KindUint64:
		return cmpOrdered(a.U64, b.U64)
	case KindFloat64:
		// Total order: non-NaN sorts numerically (-0.0 below +0.0 via bit
		// tie-break), NaN sorts last by bit pattern.
		an, bn := math.IsNaN(a.F64), math.IsNaN(b.F64)
		if an || bn {
			switch {
			case an && bn:
				return 0
			case an:
				return 1
			default:
				return -1
			}
		}
		if a.F64 != b.F64 {
			return cmpOrdered(a.F64, b.F64)
		}
		return cmpOrdered(math.Float64bits(a.F64), math.Float64bits(b.F64))
	case KindString:
		return strings.Compare(a.Str, b.Str)
	case KindBool:
		if a.Bool == b.Bool {
			return 0
		}
		if !a.Bool {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func cmpOrdered[T int64 | uint64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// SortCanonical returns a copy of t with rows in the deterministic total
// order used for order-insensitive comparison. The input is not modified.
func SortCanonical(t Table) Table {
	out := Table{Columns: t.Columns, Rows: append([][]Value(nil), t.Rows...)}
	sort.SliceStable(out.Rows, func(i, j int) bool {
		for c := range out.Columns {
			if cmp := compareCellsCanonical(out.Rows[i][c], out.Rows[j][c]); cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
	return out
}
