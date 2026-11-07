package engine

import (
	"context"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"math"
	"unsafe"
)

func byteStatisticsType(t memory.DataType) bool {
	return t == memory.TypeString || t == memory.TypeBinary
}
func (p *PreparedQuery) hasByteStatistics() bool {
	if s := p.statistics(); s != nil {
		for _, t := range s.Types {
			if byteStatisticsType(t) {
				return true
			}
		}
	}
	return false
}
func (p *PreparedQuery) byteStatisticsOutput(index int) bool {
	s := p.statistics()
	return s != nil && expr.IsStatistics(p.cq.Aggregates[index].Kind) && byteStatisticsType(s.Types[s.OutputLane[index]])
}
func (p *PreparedQuery) byteStatisticsValue(r aggregateRecord, index int) ([]byte, bool) {
	s := p.statistics()
	lane := s.OutputLane[index]
	state := bytesAggregateRecordState(r, lane, s.Types, s.Layouts)
	if p.cq.Aggregates[index].Kind == expr.AggFirst {
		return state.First, state.Seen && state.FirstValid
	}
	return state.Last, state.Seen && state.LastValid
}
func (p *PreparedQuery) byteStatisticsPayloadSize(r aggregateRecord) (int64, error) {
	var n int64
	for i := range p.cq.Aggregates {
		if !p.byteStatisticsOutput(i) {
			continue
		}
		v, valid := p.byteStatisticsValue(r, i)
		if valid {
			if int64(len(v)) > math.MaxInt64-n {
				return 0, fmt.Errorf("endpoint output payload overflow")
			}
			n += int64(len(v))
		}
	}
	return n, nil
}
func statisticsInputBytes(batch *memory.Batch, name string, row int) ([]byte, bool, error) {
	var offsets []uint32
	var data []byte
	var index int
	if v := batch.Strings[name]; v != nil {
		if row < 0 || row >= v.Len() {
			return nil, false, fmt.Errorf("endpoint row out of range")
		}
		if !v.IsValid(row) {
			return nil, false, nil
		}
		offsets, data, index = v.Offsets, v.Data, row
	} else if v := batch.DictStrings[name]; v != nil {
		if row < 0 || row >= v.Len() {
			return nil, false, fmt.Errorf("endpoint row out of range")
		}
		if !v.IsValid(row) {
			return nil, false, nil
		}
		index = int(v.Indices[row])
		offsets, data = v.Offsets, v.Data
	} else {
		return nil, false, fmt.Errorf("endpoint column %q missing string/binary vector", name)
	}
	if index < 0 || index >= len(offsets)-1 || offsets[index] > offsets[index+1] || uint64(offsets[index+1]) > uint64(len(data)) {
		return nil, false, fmt.Errorf("endpoint column %q has invalid or unavailable dictionary/offsets", name)
	}
	return data[offsets[index]:offsets[index+1]], true, nil
}

// bytesAggregateRecordState is extended by the V5 decoder for spilled payloads.
func bytesAggregateRecordState(r aggregateRecord, lane int, types []memory.DataType, layouts []aggregate.StatisticsLayout) aggregate.BytesEndpointResult {
	if r.statisticsBundle != nil {
		return r.statisticsBundle.BytesResult(r.groupID, lane)
	}
	return decodeBytesStatisticsFrame(bytesStatisticsFrame(r.bytesStatisticsRaw, lane, types), types[lane])
}

type byteStatisticsWorkspace struct {
	pending []aggregate.BytesEndpointMutation
	lease   memory.Reservation
}

func (w *byteStatisticsWorkspace) init(st *CompiledStatistics, reserve aggregate.BundleReserve) error {
	if st == nil {
		return nil
	}
	has := false
	for _, t := range st.Types {
		has = has || byteStatisticsType(t)
	}
	if !has {
		return nil
	}
	width := int64(unsafe.Sizeof(aggregate.BytesEndpointMutation{}))
	if int64(len(st.Types)) > math.MaxInt64/width {
		return fmt.Errorf("endpoint transaction scratch overflow")
	}
	lease, err := reserve(int64(len(st.Types)) * width)
	if err != nil {
		return err
	}
	if lease == nil {
		return fmt.Errorf("endpoint scratch reservation missing")
	}
	w.lease = lease
	w.pending = make([]aggregate.BytesEndpointMutation, len(st.Types))
	return nil
}
func (w *byteStatisticsWorkspace) abort() {
	for i := range w.pending {
		w.pending[i].Abort()
	}
}
func (w *byteStatisticsWorkspace) commit() {
	for i := range w.pending {
		w.pending[i].Commit()
	}
}
func (w *byteStatisticsWorkspace) close() {
	w.abort()
	w.pending = nil
	if w.lease != nil {
		w.lease.Release()
		w.lease = nil
	}
}
func (w *byteStatisticsWorkspace) prepare(b *aggregate.StatisticsBundle, st *CompiledStatistics, group int, batch *memory.Batch, row int, ordinal aggregate.SourceOrdinal) error {
	for lane, t := range st.Types {
		if !byteStatisticsType(t) {
			continue
		}
		data, valid, err := statisticsInputBytes(batch, st.Columns[lane], row)
		if err != nil {
			w.abort()
			return err
		}
		change, err := b.PrepareBytes(group, lane, data, valid, ordinal)
		if err != nil {
			w.abort()
			return err
		}
		w.pending[lane] = change
	}
	return nil
}
func (w *byteStatisticsWorkspace) prepareMerge(b *aggregate.StatisticsBundle, st *CompiledStatistics, group int, r aggregateRecord) error {
	for lane, t := range st.Types {
		if !byteStatisticsType(t) {
			continue
		}
		change, err := b.PrepareMergeBytes(group, lane, bytesAggregateRecordState(r, lane, st.Types, st.Layouts))
		if err != nil {
			w.abort()
			return err
		}
		w.pending[lane] = change
	}
	return nil
}

func (s *sortedAggregateStream) ensureEndpointOutput(ctx context.Context, dst *memory.Batch, r aggregateRecord) error {
	for i, a := range s.query.cq.Aggregates {
		if !s.query.byteStatisticsOutput(i) {
			continue
		}
		data, valid := s.query.byteStatisticsValue(r, i)
		if !valid {
			continue
		}
		name := a.Alias
		if name == "" {
			name = fmt.Sprintf("agg_%d", i)
		}
		v := dst.Strings[name]
		if len(data) > math.MaxUint32-len(v.Data) {
			return fmt.Errorf("endpoint output exceeds Uint32 offsets")
		}
		need := len(v.Data) + len(data)
		if need <= cap(v.Data) {
			continue
		}
		capacity := max(need, 64)
		if cap(v.Data) <= math.MaxInt/2 {
			capacity = max(capacity, cap(v.Data)*2)
		}
		lease, err := s.outputBudget.ReserveGrowth(ctx, int64(capacity))
		if err != nil {
			return err
		}
		next := make([]byte, len(v.Data), capacity)
		copy(next, v.Data)
		v.Data = next
		if s.endpointOutputLeases[i] != nil {
			s.endpointOutputLeases[i].Release()
		}
		s.endpointOutputLeases[i] = lease
	}
	return nil
}
