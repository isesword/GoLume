package aggregate

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"unsafe"
)

// StatisticsLayout selects independent fixed-size statistics for one input.
// Moments can serve both variance/std and different ddof finalizations. Either
// endpoint flag retains the endpoint pair, without changing the legacy Layout.
type StatisticsLayout struct{ Moments, First, Last bool }

// StatisticsResult carries raw mergeable state and its exact descriptor.
// Unselected state fields are zero. It is not a finalized scalar result.
type StatisticsResult struct {
	Type      memory.DataType
	Layout    StatisticsLayout
	Moments   MomentsResult
	Endpoints EndpointResult
}

type statisticsLane struct {
	moments   []MomentsState
	endpoints []EndpointState
}

// StatisticsBundle owns fixed-size statistic lanes indexed by stable group ID.
// It is separate from NumericBundle so a variance-only query needs no fake SUM
// state and existing Layout/V1/V2/V3 wire contracts remain unchanged.
type StatisticsBundle struct {
	types           []memory.DataType
	layouts         []StatisticsLayout
	lanes           []statisticsLane
	capacity        int
	reserve         BundleReserve
	metadata, lease memory.Reservation
	closed          bool
}

func validateStatisticsDescriptors(types []memory.DataType, layouts []StatisticsLayout) error {
	if len(types) == 0 || len(types) != len(layouts) {
		return fmt.Errorf("aggregate: statistics require matching nonempty descriptors")
	}
	for i, t := range types {
		l := layouts[i]
		if !l.Moments && !l.First && !l.Last {
			return fmt.Errorf("aggregate: empty statistics lane %d", i)
		}
		if l.Moments && !isAggregateNumeric(t) {
			return fmt.Errorf("aggregate: moments lane %d is not numeric", i)
		}
		if (l.First || l.Last) && !endpointType(t) {
			return fmt.Errorf("aggregate: endpoint lane %d is not fixed-width", i)
		}
	}
	return nil
}

// NewStatisticsBundle copies descriptors after admitting their exact backing
// sizes. Reserve is mandatory; returned tokens are owned until replacement/Close.
func NewStatisticsBundle(types []memory.DataType, layouts []StatisticsLayout, reserve BundleReserve) (*StatisticsBundle, error) {
	if reserve == nil {
		return nil, fmt.Errorf("aggregate: statistics bundle requires reservations")
	}
	if err := validateStatisticsDescriptors(types, layouts); err != nil {
		return nil, err
	}
	per := uint64(unsafe.Sizeof(memory.DataType(0)) + unsafe.Sizeof(StatisticsLayout{}))
	limit := min(uint64(math.MaxInt64), uint64(^uint(0)>>1))
	if uint64(len(types)) > limit/per {
		return nil, fmt.Errorf("aggregate: statistics descriptor size overflow")
	}
	metadata, err := reserveBundle(reserve, int64(uint64(len(types))*per))
	if err != nil {
		return nil, err
	}
	ownedTypes := make([]memory.DataType, len(types))
	copy(ownedTypes, types)
	ownedLayouts := make([]StatisticsLayout, len(layouts))
	copy(ownedLayouts, layouts)
	return &StatisticsBundle{types: ownedTypes, layouts: ownedLayouts, reserve: reserve, metadata: metadata}, nil
}

// StatisticsBundleBytes returns full replacement arrays/headers, excluding
// immutable descriptors. Overflow is rejected before any backing allocation.
func StatisticsBundleBytes(capacity int, types []memory.DataType, layouts []StatisticsLayout) (int64, error) {
	if capacity < 0 {
		return 0, fmt.Errorf("aggregate: negative statistics capacity")
	}
	if err := validateStatisticsDescriptors(types, layouts); err != nil {
		return 0, err
	}
	limit := min(uint64(math.MaxInt64), uint64(^uint(0)>>1))
	header := uint64(unsafe.Sizeof(statisticsLane{}))
	if uint64(len(types)) > limit/header {
		return 0, fmt.Errorf("aggregate: statistics headers overflow")
	}
	bytes := uint64(len(types)) * header
	for _, l := range layouts {
		per := uint64(0)
		if l.Moments {
			per += uint64(unsafe.Sizeof(MomentsState{}))
		}
		if l.First || l.Last {
			per += uint64(unsafe.Sizeof(EndpointState{}))
		}
		if uint64(capacity) > (limit-bytes)/per {
			return 0, fmt.Errorf("aggregate: statistics backing size overflow")
		}
		bytes += uint64(capacity) * per
	}
	return int64(bytes), nil
}

// EnsureGroups admits complete replacement state while the old state remains
// charged. Failure preserves the previous groups, capacity and reservations.
func (b *StatisticsBundle) EnsureGroups(groups int) error {
	if b == nil || b.closed {
		return fmt.Errorf("aggregate: statistics bundle closed")
	}
	if groups < 0 {
		return fmt.Errorf("aggregate: negative statistics groups")
	}
	if groups <= b.capacity {
		return nil
	}
	capacity := max(8, b.capacity)
	for capacity < groups {
		if capacity > int(^uint(0)>>1)/2 {
			capacity = groups
			break
		}
		capacity *= 2
	}
	bytes, err := StatisticsBundleBytes(capacity, b.types, b.layouts)
	if err != nil {
		return err
	}
	lease, err := reserveBundle(b.reserve, bytes)
	if err != nil {
		return err
	}
	lanes := make([]statisticsLane, len(b.types))
	for i, t := range b.types {
		l := b.layouts[i]
		if l.Moments {
			lanes[i].moments = make([]MomentsState, capacity)
			for g := range lanes[i].moments {
				lanes[i].moments[g].result.Type = t
			}
			if b.capacity > 0 {
				copy(lanes[i].moments, b.lanes[i].moments)
			}
		}
		if l.First || l.Last {
			lanes[i].endpoints = make([]EndpointState, capacity)
			for g := range lanes[i].endpoints {
				lanes[i].endpoints[g].result.Type = t
			}
			if b.capacity > 0 {
				copy(lanes[i].endpoints, b.lanes[i].endpoints)
			}
		}
	}
	previous := b.lease
	b.lanes, b.capacity, b.lease = lanes, capacity, lease
	if previous != nil {
		previous.Release()
	}
	return nil
}

func (b *StatisticsBundle) check(group, column int) error {
	if b == nil || b.closed {
		return fmt.Errorf("aggregate: statistics bundle closed")
	}
	if group < 0 || group >= b.capacity || column < 0 || column >= len(b.types) {
		return fmt.Errorf("aggregate: statistics group/lane out of range")
	}
	return nil
}

// AddBits updates an addressable lane atomically. NULL is skipped by moments
// but retained by endpoints. Ordinal must identify the original logical row.
func (b *StatisticsBundle) AddBits(group, column int, bits uint64, valid bool, ordinal SourceOrdinal) error {
	if err := b.check(group, column); err != nil {
		return err
	}
	lane := &b.lanes[column]
	var m MomentsState
	var e EndpointState
	if lane.moments != nil {
		m = lane.moments[group]
		if err := m.AddBits(bits, valid); err != nil {
			return err
		}
	}
	if lane.endpoints != nil {
		e = lane.endpoints[group]
		if err := e.AddBits(bits, valid, ordinal); err != nil {
			return err
		}
	}
	if lane.moments != nil {
		lane.moments[group] = m
	}
	if lane.endpoints != nil {
		lane.endpoints[group] = e
	}
	return nil
}

// Result returns raw state for a live addressable group/lane without allocating.
// Invalid indices panic, matching the numeric bundle's hot-path read contract.
func (b *StatisticsBundle) Result(group, column int) StatisticsResult {
	r := StatisticsResult{Type: b.types[column], Layout: b.layouts[column]}
	lane := &b.lanes[column]
	if lane.moments != nil {
		r.Moments = lane.moments[group].Snapshot()
	}
	if lane.endpoints != nil {
		r.Endpoints = lane.endpoints[group].Snapshot()
	}
	return r
}

// Merge validates descriptor and raw state before atomically combining a lane.
func (b *StatisticsBundle) Merge(group, column int, r StatisticsResult) error {
	if err := b.check(group, column); err != nil {
		return err
	}
	if r.Type != b.types[column] || r.Layout != b.layouts[column] {
		return fmt.Errorf("aggregate: incompatible statistics descriptor")
	}
	lane := &b.lanes[column]
	var m MomentsState
	var e EndpointState
	if lane.moments != nil {
		if r.Moments.Type != r.Type {
			return fmt.Errorf("aggregate: moments payload type mismatch")
		}
		other, err := RestoreMoments(r.Moments)
		if err != nil {
			return err
		}
		m = lane.moments[group]
		if err = m.Merge(other); err != nil {
			return err
		}
	} else if r.Moments != (MomentsResult{}) {
		return fmt.Errorf("aggregate: undeclared moments payload")
	}
	if lane.endpoints != nil {
		if r.Endpoints.Type != r.Type {
			return fmt.Errorf("aggregate: endpoint payload type mismatch")
		}
		other, err := RestoreEndpoints(r.Endpoints)
		if err != nil {
			return err
		}
		e = lane.endpoints[group]
		if err = e.Merge(other); err != nil {
			return err
		}
	} else if r.Endpoints != (EndpointResult{}) {
		return fmt.Errorf("aggregate: undeclared endpoint payload")
	}
	if lane.moments != nil {
		lane.moments[group] = m
	}
	if lane.endpoints != nil {
		lane.endpoints[group] = e
	}
	return nil
}

// ClearGroup restores empty typed identities for every lane in one group.
func (b *StatisticsBundle) ClearGroup(group int) error {
	if err := b.check(group, 0); err != nil {
		return err
	}
	for i := range b.lanes {
		if b.lanes[i].moments != nil {
			b.lanes[i].moments[group].Reset()
		}
		if b.lanes[i].endpoints != nil {
			b.lanes[i].endpoints[group].Reset()
		}
	}
	return nil
}

// Capacity reports the number of addressable group IDs.
func (b *StatisticsBundle) Capacity() int {
	if b == nil {
		return 0
	}
	return b.capacity
}

// Columns reports the number of independent statistics input lanes.
func (b *StatisticsBundle) Columns() int {
	if b == nil {
		return 0
	}
	return len(b.types)
}

// RetainedBytes reports live reservations including descriptor/header arrays.
func (b *StatisticsBundle) RetainedBytes() int64 {
	if b == nil {
		return 0
	}
	var n int64
	if b.metadata != nil {
		n += b.metadata.Bytes()
	}
	if b.lease != nil {
		n += b.lease.Bytes()
	}
	return n
}

// Close releases every backing array and token exactly once.
func (b *StatisticsBundle) Close() {
	if b == nil || b.closed {
		return
	}
	b.closed = true
	if b.lease != nil {
		b.lease.Release()
	}
	if b.metadata != nil {
		b.metadata.Release()
	}
	b.types, b.layouts, b.lanes = nil, nil, nil
	b.metadata, b.lease = nil, nil
	b.capacity = 0
}
