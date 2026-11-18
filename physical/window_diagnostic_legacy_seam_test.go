package physical

import "strconv"

// windowDiagnostics adapts the old observer fixture to the execution-private
// typed seam. Its public map method, original values, Close poisoning, and all
// snapshot/lifecycle assertions remain unchanged. No public formatter is called.
func (s *percentRankDiagnosticTestStream) windowDiagnostics() windowDiagnosticValues {
	var out windowDiagnosticValues
	runs, e1 := strconv.ParseInt(s.values["spill_runs"], 10, 64)
	bytes, e2 := strconv.ParseInt(s.values["spill_bytes"], 10, 64)
	if e1 == nil && e2 == nil {
		out.hasSort = true
		out.sortRuns = runs
		out.sortBytes = bytes
	}
	for i, key := range windowDiagnosticKeys {
		if value, err := strconv.ParseInt(s.values[key], 10, 64); err == nil {
			out.values[i] = value
			out.present |= 1 << i
		}
	}
	return out
}
