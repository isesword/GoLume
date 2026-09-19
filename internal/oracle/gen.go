package oracle

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
)

// GeneratorBoundedMagnitude caps the absolute value of int64 columns marked
// Bounded. Checked int64 arithmetic (+, -, *) over bounded operands and small
// literals cannot overflow, and SUM/AVG over at most 2^20 bounded rows stays
// far below 2^63.
const GeneratorBoundedMagnitude = int64(1) << 20

// Generator produces deterministic pseudo-random tables and queries from a
// single seed. The generated value domains deliberately include the
// adversarial edges the engine must get right: NULLs (including all-NULL
// columns), empty and single-row tables, empty strings, unicode strings,
// zero, negative numbers, math.MinInt64/MaxInt64, NaN, and infinities.
type Generator struct {
	r *rand.Rand
}

// NewGenerator returns a generator seeded by seed. Two generators created
// with the same seed produce byte-identical outputs.
func NewGenerator(seed int64) *Generator {
	return &Generator{r: rand.New(rand.NewSource(seed))}
}

// TableOptions bounds random table generation. Zero fields take defaults.
type TableOptions struct {
	// MaxRows caps the generated row count (default 96). Zero-row and
	// single-row tables are always possible outcomes unless MinRows rules
	// them out.
	MaxRows int
	// MinRows raises the row-count lower bound (default 0), for tests that
	// need enough rows to exercise parallel and multi-partition paths.
	MinRows int
	// MaxColumns caps the generated column count (default 6).
	MaxColumns int
}

func (o TableOptions) normalize() TableOptions {
	if o.MaxRows <= 0 {
		o.MaxRows = 96
	}
	if o.MaxColumns <= 0 {
		o.MaxColumns = 6
	}
	if o.MinRows < 0 || o.MinRows > o.MaxRows {
		o.MinRows = 0
	}
	return o
}

// stringPool is the string value domain. The leading entries form a
// low-cardinality set so group keys and equality predicates collide often;
// the tail covers empty, unicode, and long values.
var stringPool = []string{
	"", "a", "b", "ab", "abc", "banana", "Hello",
	"world", "张三", "🙂🚀", "null", "0", "-1",
	"The quick brown fox jumps over the lazy dog",
}

// int64Extremes are the adversarial special values mixed into unbounded int64
// columns.
var int64Extremes = []int64{0, 1, -1, math.MinInt64, math.MaxInt64, math.MinInt64 + 1, math.MaxInt64 - 1}

// float64Specials are the adversarial special values mixed into float64
// columns: signed zeros, subnormal-scale values, huge magnitudes, NaN, and
// infinities.
var float64Specials = []float64{
	0, math.Copysign(0, -1), 1.5, -2.75, 1e-300, -1e-300,
	1e300, -1e300, math.MaxFloat64, -math.MaxFloat64,
	math.SmallestNonzeroFloat64, math.NaN(), math.Inf(1), math.Inf(-1),
}

// nullRates are the candidate per-column NULL probabilities; 1.0 produces
// all-NULL columns.
var nullRates = []float64{0, 0, 0.1, 0.25, 0.5, 1.0}

// Table generates one random table. Column names are c0, c1, .... Every
// column records whether its values stay within ±GeneratorBoundedMagnitude
// (Column.Bounded), which the query generator uses to keep checked int64
// arithmetic overflow-free.
func (g *Generator) Table(opts TableOptions) Table {
	opts = opts.normalize()
	cols := 1 + g.r.Intn(opts.MaxColumns)
	table := Table{Columns: make([]Column, cols)}
	for i := range table.Columns {
		table.Columns[i] = Column{Name: "c" + strconv.Itoa(i), Kind: g.columnKind()}
	}
	rows := g.rowCount(opts.MinRows, opts.MaxRows)
	table.Rows = make([][]Value, rows)
	for i := range table.Rows {
		table.Rows[i] = make([]Value, cols)
	}
	for col := range table.Columns {
		nullRate := nullRates[g.r.Intn(len(nullRates))]
		lowCardinality := g.r.Intn(2) == 0
		bounded := table.Columns[col].Kind == KindInt64 && g.r.Intn(5) < 3
		table.Columns[col].Bounded = bounded
		for row := 0; row < rows; row++ {
			table.Rows[row][col] = g.value(table.Columns[col].Kind, nullRate, lowCardinality, bounded)
		}
	}
	return table
}

func (g *Generator) columnKind() Kind {
	switch n := g.r.Intn(20); {
	case n < 7:
		return KindInt64
	case n < 12:
		return KindFloat64
	case n < 17:
		return KindString
	default:
		return KindBool
	}
}

// rowCount biases toward edge sizes: empty and single-row tables are
// frequent, and the remainder spread over the full range. A positive minRows
// disables the edge sizes.
func (g *Generator) rowCount(minRows, maxRows int) int {
	if minRows > 0 {
		return minRows + g.r.Intn(maxRows-minRows+1)
	}
	switch n := g.r.Intn(10); {
	case n == 0:
		return 0
	case n == 1:
		return 1
	case n == 2:
		return 2
	default:
		return 3 + g.r.Intn(maxRows-2)
	}
}

// value generates one cell of the given kind under the column's null rate,
// cardinality, and boundedness contract.
func (g *Generator) value(kind Kind, nullRate float64, lowCardinality, bounded bool) Value {
	if g.r.Float64() < nullRate {
		return Null(kind)
	}
	switch kind {
	case KindInt64:
		switch {
		case lowCardinality:
			return Int64(int64(g.r.Intn(17) - 8))
		case bounded:
			if g.r.Intn(10) == 0 {
				return Int64(int64Extremes[g.r.Intn(3)]) // 0, 1, -1 only
			}
			return Int64(g.r.Int63n(2*GeneratorBoundedMagnitude+1) - GeneratorBoundedMagnitude)
		default:
			if g.r.Intn(10) < 3 {
				return Int64(int64Extremes[g.r.Intn(len(int64Extremes))])
			}
			return Int64(g.r.Int63n(2_000_001) - 1_000_000)
		}
	case KindFloat64:
		if lowCardinality {
			return Float64([]float64{0, 1.5, -2.5, 3.25, -0.5}[g.r.Intn(5)])
		}
		if g.r.Intn(20) < 9 {
			return Float64(float64Specials[g.r.Intn(len(float64Specials))])
		}
		return Float64(g.r.NormFloat64() * 300)
	case KindString:
		if lowCardinality {
			return String(stringPool[g.r.Intn(7)])
		}
		v := stringPool[g.r.Intn(len(stringPool))]
		if g.r.Intn(4) == 0 {
			v += "-" + strconv.Itoa(g.r.Intn(1000))
		}
		return String(v)
	case KindBool:
		return Bool(g.r.Intn(2) == 0)
	default:
		panic(fmt.Sprintf("oracle: cannot generate kind %s", kind))
	}
}
