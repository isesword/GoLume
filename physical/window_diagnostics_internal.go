package physical

var windowDiagnosticKeys = [...]string{"output_rows", "spill_bytes", "peak_memory_bytes", "front_sort_spill_runs", "front_sort_spill_bytes", "restore_sort_spill_runs", "restore_sort_spill_bytes", "partition_spool_pages", "partition_spool_bytes", "partition_spool_partitions", "peer_lookahead_pages", "partition_replay_pages"}

// windowDiagnosticValues is an allocation-free execution snapshot. Public map
// formatting remains an explicitly requested cold-path operation.
type windowDiagnosticValues struct {
	values                    [len(windowDiagnosticKeys)]int64
	present                   uint16
	strategy                  uint8
	observed, observedPresent uint8
	queryScope                bool
	sortRuns, sortBytes       int64
	hasSort                   bool
}
type windowDiagnosticProvider interface{ windowDiagnostics() windowDiagnosticValues }

func (d *percentRankDiagnostics) fixedSnapshot() windowDiagnosticValues {
	d.mu.Lock()
	values := d.values
	d.mu.Unlock()
	var out windowDiagnosticValues
	for i, v := range values {
		a, b := 3, 4
		switch i {
		case 1:
			a, b = 7, 8
		case 2:
			a, b = 5, 6
		}
		out.values[a], out.values[b] = v.units, v.bytes
		out.present |= 1<<a | 1<<b
		out.observedPresent |= 1 << i
		if v.observed {
			out.observed |= 1 << i
		}
	}
	out.values[9] = values[1].partitions
	out.present |= 1 << 9
	if values[1].cumeObserved {
		out.values[10] = values[1].peekPages
		out.present |= 1 << 10
	}
	if values[1].replayObserved {
		out.values[11] = values[1].replayPages
		out.present |= 1 << 11
	}
	return out
}
func (d *percentRankDiagnostics) captureFixed(stage int, v windowDiagnosticValues) {
	var next percentRankStageValues
	if stage == 1 {
		if v.present&(1<<7|1<<8|1<<9) != (1<<7 | 1<<8 | 1<<9) {
			return
		}
		next.units, next.bytes, next.partitions = v.values[7], v.values[8], v.values[9]
		next.peekPages, next.replayPages = v.values[10], v.values[11]
		next.cumeObserved = v.present&(1<<10) != 0
		next.replayObserved = v.present&(1<<11) != 0
	} else {
		if !v.hasSort {
			return
		}
		next.units, next.bytes = v.sortRuns, v.sortBytes
	}
	if next.units < 0 || next.bytes < 0 || next.partitions < 0 {
		return
	}
	if next.peekPages < 0 || next.replayPages < 0 {
		next.cumeObserved, next.replayObserved = false, false
	}
	next.observed = true
	d.mu.Lock()
	old := &d.values[stage]
	old.units = max(old.units, next.units)
	old.bytes = max(old.bytes, next.bytes)
	old.partitions = max(old.partitions, next.partitions)
	old.peekPages = max(old.peekPages, next.peekPages)
	old.replayPages = max(old.replayPages, next.replayPages)
	old.observed = true
	old.cumeObserved = old.cumeObserved || next.cumeObserved
	old.replayObserved = old.replayObserved || next.replayObserved
	d.mu.Unlock()
}
func (s *externalSortStream) windowDiagnostics() windowDiagnosticValues {
	return windowDiagnosticValues{sortRuns: s.spillRuns, sortBytes: s.spillBytes, hasSort: true}
}
func (s *percentRankSource) windowDiagnostics() windowDiagnosticValues {
	var out windowDiagnosticValues
	if s.metrics == nil {
		return out
	}
	out.strategy = 2
	out.present = 1<<7 | 1<<8 | 1<<9
	out.values[7], out.values[8], out.values[9] = s.metrics.pages.Load(), s.metrics.bytes.Load(), s.metrics.partitions.Load()
	if s.cumeHidden != "" {
		out.present |= 1 << 10
		out.values[10] = s.metrics.peekPages.Load()
	}
	if s.cumeHidden != "" || len(s.ntilePlans) > 0 {
		out.present |= 1 << 11
		out.values[11] = s.metrics.replayPages.Load()
	}
	return out
}
func (s *percentRankPartitionStream) windowDiagnostics() windowDiagnosticValues {
	out := windowDiagnosticValues{strategy: 2, present: 1<<7 | 1<<8 | 1<<9}
	out.values[7], out.values[8], out.values[9] = s.coldPages, s.coldBytes, s.coldPartitions
	out.values[10], out.values[11] = s.coldPeekPages, s.coldReplayPages
	if s.metrics != nil {
		out.values[7], out.values[8], out.values[9] = s.metrics.pages.Load(), s.metrics.bytes.Load(), s.metrics.partitions.Load()
		out.values[10], out.values[11] = s.metrics.peekPages.Load(), s.metrics.replayPages.Load()
	}
	if s.hasCume {
		out.present |= 1 << 10
	}
	if s.hasReplay {
		out.present |= 1 << 11
	}
	return out
}
func (s *orderedWindowStream) windowDiagnostics() windowDiagnosticValues {
	if p, ok := s.percentRankProvider.(windowDiagnosticProvider); ok {
		return p.windowDiagnostics()
	}
	if p, ok := s.child.(windowDiagnosticProvider); ok {
		return p.windowDiagnostics()
	}
	return windowDiagnosticValues{}
}
func (s *percentRankDiagnosticStream) windowDiagnostics() windowDiagnosticValues {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.child.(windowDiagnosticProvider); ok {
		return p.windowDiagnostics()
	}
	return windowDiagnosticValues{}
}
func (s *externalWindowStream) windowDiagnostics() windowDiagnosticValues {
	var out windowDiagnosticValues
	if s.percentRankDiagnostics != nil {
		out = s.percentRankDiagnostics.fixedSnapshot()
	}
	q := s.query.ScalarSnapshot()
	out.strategy = 1
	out.queryScope = true
	out.present |= 1<<0 | 1<<1 | 1<<2
	out.values[0], out.values[1], out.values[2] = s.rows, q.SpilledBytes, q.PeakBytes
	return out
}
