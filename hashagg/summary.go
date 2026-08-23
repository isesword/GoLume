package hashagg

// Summary contains non-materialized aggregation counters used by execution
// operators and benchmark diagnostics.
type Summary struct {
	Groups   int
	Checksum int64
	Rows     int
}
