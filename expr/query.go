package expr

// AggregateKind identifies a logical aggregate function.
type AggregateKind uint8

// Aggregate kinds supported by the logical query API.
const (
	AggInvalid AggregateKind = iota
	// AggSum computes SQL SUM over non-NULL int64 input values.
	AggSum
	// AggCountRows computes COUNT(*).
	AggCountRows
	// AggCountValues computes COUNT(column), ignoring SQL NULL input values.
	AggCountValues
	// AggMin computes SQL MIN over non-NULL int64 input values.
	AggMin
	// AggMax computes SQL MAX over non-NULL int64 input values.
	AggMax
	// AggAvg computes SQL AVG over non-NULL int64 input values and finalizes to float64.
	AggAvg
)

// AggCount is retained as a source-compatible alias for COUNT(*).
const AggCount = AggCountRows

// Aggregate describes one logical aggregate expression.
type Aggregate struct {
	Kind   AggregateKind
	Column string
	Alias  string
}

// Query is the logical query shape accepted by Engine.Prepare.
type Query struct {
	Filter     Expr
	GroupBy    []string
	Aggregates []Aggregate
	Hints      Hints
}

// Hints supplies optional physical-planning estimates and test overrides.
type Hints struct {
	Selectivity      float64
	Cardinality      int
	ReuseSelection   bool
	ForceStrategy    string
	ForceRadixBits   uint
	ForceWorkerCount int
	// AverageKeyBytes guides string arena/memory costing. Zero means 16 bytes.
	AverageKeyBytes int
}

// Sum constructs a SUM(column) aggregate.
func Sum(column, alias string) Aggregate {
	return Aggregate{Kind: AggSum, Column: column, Alias: alias}
}

// Count constructs a COUNT(*) aggregate.
func Count(alias string) Aggregate { return Aggregate{Kind: AggCountRows, Alias: alias} }

// CountColumn constructs a COUNT(column) aggregate that ignores SQL NULL values.
func CountColumn(column, alias string) Aggregate {
	return Aggregate{Kind: AggCountValues, Column: column, Alias: alias}
}

// Min constructs a MIN(column) aggregate.
func Min(column, alias string) Aggregate {
	return Aggregate{Kind: AggMin, Column: column, Alias: alias}
}

// Max constructs a MAX(column) aggregate.
func Max(column, alias string) Aggregate {
	return Aggregate{Kind: AggMax, Column: column, Alias: alias}
}

// Avg constructs an AVG(column) aggregate.
func Avg(column, alias string) Aggregate {
	return Aggregate{Kind: AggAvg, Column: column, Alias: alias}
}
