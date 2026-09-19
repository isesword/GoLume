package proptest

import (
	"fmt"
	"math/rand"

	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

// DefaultMaxStringLength bounds generated string values when ColumnSpec does
// not set an explicit limit.
const DefaultMaxStringLength = 12

// Column is one materialized logical column with explicit per-row NULL flags.
// Exactly one of Int64s, Float64s, Strings, or Bools is populated, matching
// Type. Nulls marks NULL rows; a nil Nulls slice means every row is valid.
type Column struct {
	Name     string
	Type     memory.DataType
	Int64s   []int64
	Float64s []float64
	Strings  []string
	Bools    []bool
	Nulls    []bool
}

// Rows returns the logical row count of c.
func (c Column) Rows() int {
	switch c.Type {
	case memory.TypeInt64:
		return len(c.Int64s)
	case memory.TypeFloat64:
		return len(c.Float64s)
	case memory.TypeString:
		return len(c.Strings)
	case memory.TypeBool:
		return len(c.Bools)
	default:
		return 0
	}
}

// IsNull reports whether row i is NULL.
func (c Column) IsNull(i int) bool { return c.Nulls != nil && c.Nulls[i] }

func (c Column) field() memory.Field {
	return memory.Field{Name: c.Name, Type: c.Type, Nullable: c.Nulls != nil, Encoding: memory.EncodingPlain}
}

func (c Column) validity() memory.Bitmap {
	if c.Nulls == nil {
		return nil
	}
	rows := c.Rows()
	bits := memory.EnsureBitmap(nil, rows)
	for i := 0; i < rows; i++ {
		if !c.Nulls[i] {
			memory.Set(bits, i)
		}
	}
	return bits
}

func (c Column) bind(batch *memory.Batch) {
	switch c.Type {
	case memory.TypeInt64:
		batch.Int64s[c.Name] = &memory.Int64Vector{Values: c.Int64s, Validity: c.validity(), Encoding: memory.EncodingPlain}
	case memory.TypeFloat64:
		batch.Float64s[c.Name] = &memory.Float64Vector{Values: c.Float64s, Validity: c.validity(), Encoding: memory.EncodingPlain}
	case memory.TypeString:
		offsets := make([]uint32, 0, len(c.Strings)+1)
		offsets = append(offsets, 0)
		var data []byte
		for _, s := range c.Strings {
			data = append(data, s...)
			offsets = append(offsets, uint32(len(data)))
		}
		batch.Strings[c.Name] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: c.validity(), Encoding: memory.EncodingView}
	case memory.TypeBool:
		values := make([]uint32, len(c.Bools))
		for i, b := range c.Bools {
			if b {
				values[i] = 1
			}
		}
		batch.Uint32s[c.Name] = &memory.Uint32Vector{Values: values, Validity: c.validity(), Encoding: memory.EncodingPlain}
	}
}

// Table is a generated set of equal-length columns plus the seed that
// produced it. Tables are immutable after generation and safe to share
// between backends.
type Table struct {
	Seed    int64
	Columns []Column
}

// Rows returns the shared logical row count of every column.
func (t *Table) Rows() int {
	if t == nil || len(t.Columns) == 0 {
		return 0
	}
	return t.Columns[0].Rows()
}

// Column returns the column named name and whether it exists.
func (t *Table) Column(name string) (Column, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// Schema returns the logical source schema for t.
func (t *Table) Schema() memory.Schema {
	fields := make([]memory.Field, len(t.Columns))
	for i, c := range t.Columns {
		fields[i] = c.field()
	}
	return memory.NewSchema(fields...)
}

// Batch builds a fresh in-memory batch binding t's columns. Vectors alias the
// immutable column slices, so the batch must be treated as read-only.
func (t *Table) Batch() *memory.Batch {
	batch := memory.NewBatch()
	for _, c := range t.Columns {
		c.bind(batch)
	}
	batch.Reset(t.Rows())
	return batch
}

// Source constructs an immutable in-memory source over t.
func (t *Table) Source() (*inmemory.Source, error) {
	if t == nil {
		return nil, fmt.Errorf("proptest: table is nil")
	}
	return inmemory.New(t.Schema(), t.Batch())
}

// ColumnSpec describes one generated column.
type ColumnSpec struct {
	// Name is the logical column name and must be non-empty and unique.
	Name string
	// Type must be TypeInt64, TypeFloat64, TypeString, or TypeBool.
	Type memory.DataType
	// NullProbability in [0,1] is the per-row probability of a NULL.
	NullProbability float64
	// Cardinality, when positive, restricts the value domain: integers are
	// drawn from [0, Cardinality) and strings from Cardinality distinct keys.
	// Small cardinalities force duplicates for grouping and ordering tests.
	Cardinality int
	// MaxStringLength bounds generated string length; zero uses
	// DefaultMaxStringLength.
	MaxStringLength int
}

// Gen is a seeded deterministic generator of columns and tables.
type Gen struct {
	seed int64
	rng  *rand.Rand
}

// NewGen returns a generator rooted at seed. Identical seeds reproduce
// identical tables.
func NewGen(seed int64) *Gen {
	return &Gen{seed: seed, rng: rand.New(rand.NewSource(seed))}
}

// Seed returns the seed used to construct g.
func (g *Gen) Seed() int64 { return g.seed }

// Perm returns a random permutation of [0, n), useful for unique ordering
// keys that avoid sort-tie ambiguity.
func (g *Gen) Perm(n int) []int { return g.rng.Perm(n) }

// Column generates one column from spec.
func (g *Gen) Column(spec ColumnSpec, rows int) (Column, error) {
	if err := validateColumnSpec(spec); err != nil {
		return Column{}, err
	}
	if rows < 0 {
		return Column{}, fmt.Errorf("proptest: row count %d is negative", rows)
	}
	col := Column{Name: spec.Name, Type: spec.Type}
	switch spec.Type {
	case memory.TypeInt64:
		col.Int64s = make([]int64, rows)
		for i := range col.Int64s {
			col.Int64s[i] = g.int64Value(spec.Cardinality)
		}
	case memory.TypeFloat64:
		col.Float64s = make([]float64, rows)
		for i := range col.Float64s {
			col.Float64s[i] = g.float64Value(spec.Cardinality)
		}
	case memory.TypeString:
		col.Strings = make([]string, rows)
		for i := range col.Strings {
			col.Strings[i] = g.stringValue(spec)
		}
	case memory.TypeBool:
		col.Bools = make([]bool, rows)
		for i := range col.Bools {
			col.Bools[i] = g.rng.Intn(2) == 0
		}
	}
	if spec.NullProbability > 0 {
		col.Nulls = make([]bool, rows)
		for i := range col.Nulls {
			col.Nulls[i] = g.rng.Float64() < spec.NullProbability
		}
	}
	return col, nil
}

// Table generates rows rows with one column per spec. All columns share the
// row count; each column draws independently from g's stream.
func (g *Gen) Table(rows int, specs ...ColumnSpec) (*Table, error) {
	if rows < 0 {
		return nil, fmt.Errorf("proptest: row count %d is negative", rows)
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("proptest: table requires at least one column spec")
	}
	seen := make(map[string]struct{}, len(specs))
	table := &Table{Seed: g.seed, Columns: make([]Column, len(specs))}
	for i, spec := range specs {
		if _, dup := seen[spec.Name]; dup {
			return nil, fmt.Errorf("proptest: duplicate column name %q", spec.Name)
		}
		seen[spec.Name] = struct{}{}
		col, err := g.Column(spec, rows)
		if err != nil {
			return nil, err
		}
		table.Columns[i] = col
	}
	return table, nil
}

func validateColumnSpec(spec ColumnSpec) error {
	if spec.Name == "" {
		return fmt.Errorf("proptest: column spec requires a name")
	}
	switch spec.Type {
	case memory.TypeInt64, memory.TypeFloat64, memory.TypeString, memory.TypeBool:
	default:
		return fmt.Errorf("proptest: column %q has unsupported generator type %s", spec.Name, spec.Type)
	}
	if spec.NullProbability < 0 || spec.NullProbability > 1 {
		return fmt.Errorf("proptest: column %q null probability %v outside [0,1]", spec.Name, spec.NullProbability)
	}
	if spec.Cardinality < 0 {
		return fmt.Errorf("proptest: column %q cardinality %d is negative", spec.Name, spec.Cardinality)
	}
	if spec.MaxStringLength < 0 {
		return fmt.Errorf("proptest: column %q max string length %d is negative", spec.Name, spec.MaxStringLength)
	}
	return nil
}

func (g *Gen) int64Value(cardinality int) int64 {
	if cardinality > 0 {
		return g.rng.Int63n(int64(cardinality))
	}
	// Mix small values with wide-range values to exercise both dense
	// comparisons and high-bit patterns without risking overflow downstream.
	if g.rng.Intn(4) == 0 {
		return g.rng.Int63()
	}
	return g.rng.Int63n(2_000_001) - 1_000_000
}

func (g *Gen) float64Value(cardinality int) float64 {
	if cardinality > 0 {
		return float64(g.rng.Int63n(int64(cardinality))) + 0.5
	}
	// Finite, moderate-magnitude values keep sums and products exact-comparable
	// across backends; NaN and infinities are intentionally never generated.
	return g.rng.NormFloat64() * 1000
}

const stringAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func (g *Gen) stringValue(spec ColumnSpec) string {
	if spec.Cardinality > 0 {
		return fmt.Sprintf("k%0*d", len(fmt.Sprint(spec.Cardinality-1)), g.rng.Intn(spec.Cardinality))
	}
	maxLen := spec.MaxStringLength
	if maxLen <= 0 {
		maxLen = DefaultMaxStringLength
	}
	n := g.rng.Intn(maxLen + 1)
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = stringAlphabet[g.rng.Intn(len(stringAlphabet))]
	}
	return string(buf)
}
