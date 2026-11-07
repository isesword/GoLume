package logical

// WindowSpec describes one shared partitioning and ordering definition for a
// Window node. PartitionBy contains direct input-column names; By uses the
// same expression ordering contract as Sort.
type WindowSpec struct {
	PartitionBy []string
	By          []SortKey
}

// WindowFunction identifies a supported analytic window calculation.
type WindowFunction uint8

const (
	// WindowRowNumber numbers rows from one within each partition.
	WindowRowNumber WindowFunction = iota
	// WindowRank assigns peer-aware ranks with gaps.
	WindowRank
	// WindowDenseRank assigns peer-aware ranks without gaps.
	WindowDenseRank
	// WindowCountRows counts rows in the frame.
	WindowCountRows
	// WindowCountValues counts non-NULL values in the frame.
	WindowCountValues
	// WindowSum sums non-NULL values in the frame.
	WindowSum
	// WindowMin selects the minimum non-NULL frame value.
	WindowMin
	// WindowMax selects the maximum non-NULL frame value.
	WindowMax
	// WindowAvg averages non-NULL frame values.
	WindowAvg
	// WindowShift reads one partition-relative row without changing cardinality.
	WindowShift
	// WindowDiff subtracts a partition-relative value from the current value.
	WindowDiff
)

// RowBoundKind identifies one side of a ROWS frame.
type RowBoundKind uint8

const (
	// RowUnboundedPreceding starts at the first partition row.
	RowUnboundedPreceding RowBoundKind = iota
	// RowPreceding selects a constant number of preceding rows.
	RowPreceding
	// RowCurrent selects the current row.
	RowCurrent
	// RowFollowing selects a constant number of following rows.
	RowFollowing
	// RowUnboundedFollowing ends at the final partition row.
	RowUnboundedFollowing
)

// RowBound is one constant ROWS frame boundary. Offset is used only for
// preceding and following bounds and must be non-negative.
type RowBound struct {
	Kind   RowBoundKind
	Offset int64
}

// RowsFrame is an inclusive ROWS interval evaluated inside each partition.
type RowsFrame struct {
	Start RowBound
	End   RowBound
}

// WindowExpr describes one named window output. Column is required by value
// aggregates and is ignored by ranking and CountRows.
type WindowExpr struct {
	Function WindowFunction
	Column   string
	Frame    RowsFrame
	Alias    string
}

// Window adds named analytic outputs while retaining the input columns and
// their original row order.
type Window struct {
	Input Plan
	Spec  WindowSpec
	Exprs []WindowExpr
}

func (*Window) planNode() {}
