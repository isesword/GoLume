package source

import (
	"context"
	"gocolumn/expr"
)

// PushdownRequest describes scan-level work discovered by the lazy optimizer.
// Projection contains every column required by the residual logical plan.
// Predicate is conservative: sources may use it for metadata pruning while the
// execution engine still evaluates the residual row-level predicate.
type PushdownRequest struct {
	Projection []string
	Predicate  expr.Expr
}

// PushdownReport describes work eliminated by a source after applying a
// pushdown request. Byte counts are compressed-source estimates when the source
// can provide them and zero otherwise.
type PushdownReport struct {
	Projection         []string
	Predicate          string
	TotalPartitions    int
	SelectedPartitions int
	PrunedPartitions   int
	TotalRows          int64
	SelectedRows       int64
	PrunedRows         int64
	TotalBytes         int64
	SelectedBytes      int64
	PrunedBytes        int64
}

// PushdownSource is implemented by sources that can apply projection and
// predicate information before physical execution. Implementations must treat
// Predicate conservatively: a partition may be pruned only when it cannot
// produce a SQL TRUE result for the predicate.
type PushdownSource interface {
	Source
	ApplyPushdown(context.Context, PushdownRequest) (Source, PushdownReport, error)
}

// Describer is implemented by sources that expose a stable human-readable
// description for logical and physical explain output.
type Describer interface {
	Description() string
}
