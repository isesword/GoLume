// Package costmodel contains lightweight statistics used by the physical planner.
package costmodel

// ColumnStats describes runtime statistics available to the planner.
type ColumnStats struct {
	// RowCount is the number of rows in the input column.
	RowCount int64
	// NullCount is the number of NULL values.
	NullCount int64
	// Cardinality is the estimated number of distinct values.
	Cardinality int64
	// AverageWidth is the average encoded value width in bytes.
	AverageWidth int
}

// Selectivity returns the estimated fraction of rows surviving a predicate.
func (s ColumnStats) Selectivity() float64 {
	if s.RowCount <= 0 {
		return 1
	}
	v := float64(s.RowCount-s.NullCount) / float64(s.RowCount)
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
