package engine

import "context"

// canonicalKey borrows stored keys, or encodes a bound input row into the
// table-owned scratch. The index copies only newly admitted keys into its arena.
func (m *multiAggregateState) canonicalKey(record aggregateRecord) ([]byte, error) {
	if record.canonical == nil {
		return record.stringKey, nil
	}
	n, err := record.canonical.KeySize(int(record.a))
	if err != nil {
		return nil, err
	}
	if n > cap(m.scratch) {
		size := max(n, max(64, cap(m.scratch)*2))
		lease, err := m.table.reserveGrowth(int64(size))
		if err != nil {
			return nil, err
		}
		m.scratch = make([]byte, size)
		if m.scratchLease != nil {
			m.scratchLease.Release()
		}
		m.scratchLease = lease
	}
	return record.canonical.appendSizedKey(m.scratch[:0], int(record.a), n)
}

func (s *sortedAggregateStream) canonicalRowKey(ctx context.Context, row int) ([]byte, error) {
	n, err := s.binding.canonical.KeySize(row)
	if err != nil {
		return nil, err
	}
	if n > cap(s.canonicalScratch) {
		size := max(n, max(64, cap(s.canonicalScratch)*2))
		lease, err := s.stateBudget.ReserveGrowth(ctx, int64(size))
		if err != nil {
			return nil, err
		}
		s.canonicalScratch = make([]byte, size)
		if s.canonicalLease != nil {
			s.canonicalLease.Release()
		}
		s.canonicalLease = lease
	}
	return s.binding.canonical.appendSizedKey(s.canonicalScratch[:0], row, n)
}
