package logical

import (
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/source"
)

// Plan is one immutable node in a lazy logical query plan.
type Plan interface {
	planNode()
}

// Scan reads rows from a columnar source. Projection and PushedPredicate are
// optimizer annotations; Sources still evaluate residual row-level predicates
// in the execution layer unless they explicitly guarantee full predicate
// execution.
type Scan struct {
	Source          source.Source
	Projection      []string
	PushedPredicate expr.Expr
	Pushdown        source.PushdownReport
}

func (*Scan) planNode() {}

// Filter retains rows for which Predicate evaluates to SQL TRUE.
type Filter struct {
	Input     Plan
	Predicate expr.Expr
}

func (*Filter) planNode() {}

// Projection restricts the logical output to a set of existing columns.
// ExprProjection represents computed SelectExprs/WithColumns operations.
type Projection struct {
	Input   Plan
	Columns []string
}

func (*Projection) planNode() {}

// ExprProjection computes named scalar expressions. IncludeInput implements
// WithColumns semantics by retaining input columns and replacing any column
// whose name is produced by Exprs; false implements SelectExprs semantics.
type ExprProjection struct {
	Input        Plan
	Exprs        []expr.NamedExpr
	IncludeInput bool
}

func (*ExprProjection) planNode() {}

// SortKey describes one column ordering key. NullsLast controls NULL placement
// independently from ascending/descending value order.
type SortKey struct {
	Column     string
	Descending bool
	NullsLast  bool
}

// Sort globally orders all input rows by one or more keys.
type Sort struct {
	Input Plan
	By    []SortKey
}

func (*Sort) planNode() {}

// TopK returns only the first K rows under the requested ordering without
// requiring a full global sort.
type TopK struct {
	Input Plan
	K     int
	By    []SortKey
}

func (*TopK) planNode() {}

// Limit retains at most the first N rows while preserving input order.
type Limit struct {
	Input Plan
	N     int64
}

func (*Limit) planNode() {}

// Aggregate groups input rows and computes aggregate expressions.
type Aggregate struct {
	Input      Plan
	GroupBy    []string
	Aggregates []expr.Aggregate
	Hints      expr.Hints
}

func (*Aggregate) planNode() {}

// JoinKey maps one left input column to one right input column for an equality join.
type JoinKey struct {
	Left  string
	Right string
}

// Join combines two logical inputs using one or two equality keys. Physical
// lowering can either return terminal row references or stream materialized
// columnar join batches into downstream lazy operators.
type Join struct {
	Left   Plan
	Right  Plan
	Type   join.Type
	Keys   []JoinKey
	Suffix string
	// RadixBits explicitly enables the experimental fixed-key radix join path.
	// Zero keeps the default single-table hash join.
	RadixBits uint8
	// Workers controls radix bucket build/probe parallelism when RadixBits is set.
	Workers int
}

func (*Join) planNode() {}
