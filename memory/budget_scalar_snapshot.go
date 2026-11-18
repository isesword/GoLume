package memory

// BudgetScalarSnapshot contains fixed-size query diagnostics without class or
// operator maps. Spill counters are independently sampled monotonic totals, not
// a transactionally consistent record of a particular reservation or release.
type BudgetScalarSnapshot struct {
	UsedBytes      int64
	PeakBytes      int64
	SoftLimitBytes int64
	HardLimitBytes int64
	SpillRequests  int64
	SpilledBytes   int64
	Closed         bool
}

// ScalarSnapshot reads fixed-size budget diagnostics without allocating or
// enumerating operators. It serializes with admission and Close so an admission
// cannot appear in UsedBytes before PeakBytes. Releases and spill counters remain
// independent atomic updates; the result is not a globally linearizable snapshot
// across those updates. A nil receiver returns the zero value.
func (q *QueryBudget) ScalarSnapshot() BudgetScalarSnapshot {
	if q == nil {
		return BudgetScalarSnapshot{}
	}
	q.reserveMu.Lock()
	out := BudgetScalarSnapshot{
		UsedBytes: q.used.Load(), PeakBytes: q.peak.Load(),
		SoftLimitBytes: q.soft, HardLimitBytes: q.hard, Closed: q.closed.Load(),
	}
	if q.arbiter != nil {
		out.SpillRequests = q.arbiter.requests.Load()
		out.SpilledBytes = q.arbiter.spilled.Load()
	}
	q.reserveMu.Unlock()
	return out
}
