package engine

import (
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/planner"
	"time"
)

// KeyPart is one materialized group-key component. Valid is false for SQL NULL.
type KeyPart struct {
	Valid            bool
	Type             memory.DataType
	Int64            int64
	Uint64           uint64
	String           string
	DictionaryDomain string
}

// AggregateValue is one materialized aggregate result in query order.
type AggregateValue struct {
	Alias   string
	Kind    expr.AggregateKind
	Valid   bool
	Int64   int64
	Float64 float64
	IsFloat bool
}

// AggregateRow is one materialized grouped aggregate result.
type AggregateRow struct {
	Keys       []KeyPart
	Sum        int64
	Count      uint64
	Aggregates []AggregateValue
}

// Result contains summary counters and optional materialized aggregate rows.
// Rows and their key/value data are caller-owned after Execute returns and do
// not retain source buffers or query-budget reservations.
type Result struct {
	Rows         []AggregateRow
	GroupCount   int
	SelectedRows int
	Checksum     int64
	Strategy     planner.Strategy
	Elapsed      time.Duration
}

// ExecuteOptions controls per-execution result materialization.
// Materialize must fit the execution budget while rows are collected. Returned
// rows remain ordinary caller-owned Go allocations after the query completes.
type ExecuteOptions struct{ Materialize bool }

// GroupShape identifies the specialized physical group-key representation.
type GroupShape uint8

// GroupShape values describe specialized physical key layouts.
const (
	GroupShapeInvalid GroupShape = iota
	GroupShapeI32
	GroupShapeI64
	GroupShapeU32
	GroupShapeDictionaryString
	GroupShapeStringView
	GroupShapePairFixed
)

// CompiledGroupColumn records physical metadata for one group key.
type CompiledGroupColumn struct {
	Name             string
	Type             memory.DataType
	Encoding         memory.Encoding
	DictionaryDomain string
	Nullable         bool
}

// CompiledQuery is the engine-internal compiled query description.
type CompiledQuery struct {
	Predicate        expr.Program
	SimpleComparison *expr.SimpleComparison
	GroupBy          []CompiledGroupColumn
	GroupShape       GroupShape
	ValueColumn      string
	ValueNullable    bool
	Aggregates       []expr.Aggregate
	AggregateLayout  aggregate.Layout
	CardinalityHint  int
	SelectivityHint  float64
	AverageKeyBytes  int
	ReuseSelection   bool
	ForceStrategy    string
	FusedCapable     bool
	ComparisonID     kernel.KernelID
	HasComparisonID  bool
	KeyWidthBytes    int
}
