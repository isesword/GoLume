package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"math"
	"unsafe"
)

type quantileOutputState struct {
	Plan                 aggregate.QuantileRanks
	Lower, Upper         uint64
	HaveLower, HaveUpper bool
}
type quantileLaneState struct {
	Count, Seen uint64
	Planned     bool
}

func quantileStreamBytes(q *CompiledQuantiles) (int64, error) {
	if q == nil {
		return 0, nil
	}
	a, b := uint64(len(q.Outputs)), uint64(len(q.Fields))
	wa, wb := uint64(unsafe.Sizeof(quantileOutputState{}))+9, uint64(unsafe.Sizeof(quantileLaneState{}))
	if a > math.MaxInt64/wa || b > math.MaxInt64/wb || a*wa > math.MaxInt64-b*wb {
		return 0, fmt.Errorf("quantile state size overflow")
	}
	return int64(a*wa + b*wb), nil
}
func (s *distinctAggregateStream) addQuantileTuples(ctx context.Context, batch *memory.Batch, row, prefix int, ordinal aggregate.SourceOrdinal) error {
	q := s.p.quantiles()
	if q == nil {
		return nil
	}
	for lane, f := range q.Fields {
		value, valid := nativeAggregateBits(batch, f.Name, f.Type, aggregate.Layout{Min: true}, row)
		if !valid {
			continue
		}
		key, err := aggregate.QuantileOrderKey(f.Type, value)
		if err != nil {
			return err
		}
		if err = s.grow(ctx, &s.scratch, &s.scratchLease, prefix+37); err != nil {
			return err
		}
		body := s.scratch[prefix : prefix+37]
		body[0] = 3
		binary.BigEndian.PutUint32(body[1:5], uint32(lane))
		binary.BigEndian.PutUint64(body[5:13], key)
		binary.BigEndian.PutUint64(body[13:21], ordinal.Partition)
		binary.BigEndian.PutUint64(body[21:29], ordinal.Row)
		binary.LittleEndian.PutUint64(body[29:37], value)
		if err = s.sorter.Add(ctx, s.scratch[:prefix+37]); err != nil {
			return err
		}
	}
	return nil
}
func (s *distinctAggregateStream) applyQuantile(body []byte) error {
	bad := func() error { return fmt.Errorf("%w: invalid quantile tuple", memory.ErrSpillCorrupt) }
	q := s.p.quantiles()
	if q == nil || len(body) != 37 {
		return bad()
	}
	lane := uint64(binary.BigEndian.Uint32(body[1:5]))
	if lane >= uint64(len(q.Fields)) {
		return bad()
	}
	value := binary.LittleEndian.Uint64(body[29:37])
	key, err := aggregate.QuantileOrderKey(q.Fields[lane].Type, value)
	if err != nil || key != binary.BigEndian.Uint64(body[5:13]) {
		return bad()
	}
	state := &s.quantileLanes[lane]
	if !state.Planned {

		for i, o := range q.Outputs {
			if o.Lane == int(lane) {
				p, e := aggregate.PlanQuantile(state.Count, o.Probability, o.Mode)
				if e != nil {
					return e
				}
				s.quantileOutputs[i].Plan = p
			}
		}
		state.Planned = true
	}
	if state.Seen >= state.Count {
		return bad()
	}
	for i, o := range q.Outputs {
		if o.Lane != int(lane) {
			continue
		}
		out := &s.quantileOutputs[i]
		if state.Seen == out.Plan.Lower {
			out.Lower = value
			out.HaveLower = true
		}
		if state.Seen == out.Plan.Upper {
			out.Upper = value
			out.HaveUpper = true
		}
	}
	state.Seen++
	return nil
}
func (s *distinctAggregateStream) finishQuantiles() error {
	q := s.p.quantiles()
	if q == nil {
		return nil
	}
	for _, state := range s.quantileLanes {
		count := state.Count
		if state.Seen != count {
			return fmt.Errorf("%w: quantile value count mismatch", memory.ErrSpillCorrupt)
		}
	}
	base := len(s.p.distinct().Fields) * 8
	for i, o := range q.Outputs {
		state := s.quantileOutputs[i]
		if !state.Plan.Valid {
			continue
		}
		if !state.HaveLower || !state.HaveUpper {
			return fmt.Errorf("%w: quantile rank missing", memory.ErrSpillCorrupt)
		}
		value, valid, err := aggregate.InterpolateQuantile(q.Fields[o.Lane].Type, state.Lower, state.Upper, state.Plan)
		if err != nil {
			return err
		}
		if valid {
			s.counts[base+i*9] = 1
			binary.LittleEndian.PutUint64(s.counts[base+i*9+1:base+i*9+9], value)
		}
	}
	return nil
}

func (s *distinctAggregateStream) countQuantileValue(numericLane int, valid bool) error {
	if !valid {
		return nil
	}
	q := s.p.quantiles()
	if q == nil {
		return nil
	}
	for lane, countLane := range q.CountLane {
		if countLane == numericLane {
			state := &s.quantileLanes[lane]
			if state.Planned || state.Count == math.MaxUint64 {
				return fmt.Errorf("%w: invalid quantile count or tuple order", memory.ErrSpillCorrupt)
			}
			state.Count++
		}
	}
	return nil
}
