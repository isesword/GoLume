package engine

import (
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/planner"
	"time"
)

// Group is the backward-compatible single-int32 materialization shape.
// KeyValid is false for the SQL NULL group.
type Group struct {
	Key      int32
	KeyValid bool
	Sum      int64
	Count    uint64
}

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

// Result contains summary counters and optional materialized groups.
type Result struct {
	Groups       []Group        // populated for single int32 group-by
	Rows         []AggregateRow // generic materialized result
	GroupCount   int
	SelectedRows int
	Checksum     int64
	Strategy     planner.Strategy
	Elapsed      time.Duration
}

// ExecuteOptions controls per-execution result materialization.
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
	Predicate       expr.Program
	SimpleGT        *expr.SimpleGT
	GroupBy         []CompiledGroupColumn
	GroupShape      GroupShape
	ValueColumn     string
	ValueNullable   bool
	Aggregates      []expr.Aggregate
	AggregateLayout aggregate.Layout
	CardinalityHint int
	SelectivityHint float64
	AverageKeyBytes int
	ReuseSelection  bool
	ForceStrategy   string
	ForceRadixBits  uint
	FusedCapable    bool
	RadixCapable    bool
	KeyWidthBytes   int
}
