package aggregate

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"unsafe"
)

// NumericBundle owns one NumericVector per immutable input descriptor, indexed
// by stable group IDs. All columns share those IDs; the bundle owns no keys.
// Column state, descriptor copies and vector headers are charged before their
// backing allocations. External partition ownership must serialize updates.
type NumericBundle struct {
	types    []memory.DataType
	layouts  []Layout
	lanes    []NumericVector
	capacity int
	reserve  BundleReserve
	metadata memory.Reservation
	lease    memory.Reservation
	closed   bool
}

func validateNumericDescriptors(types []memory.DataType, layouts []Layout) error {
	if len(types) == 0 || len(types) != len(layouts) {
		return fmt.Errorf("aggregate: numeric bundle requires matching nonempty type and layout descriptors")
	}
	for lane, typ := range types {
		if err := validateNumericLayout(typ, layouts[lane]); err != nil {
			return fmt.Errorf("aggregate: numeric bundle lane %d: %w", lane, err)
		}
	}
	return nil
}

// NewNumericBundle constructs an empty budgeted collection of native lanes.
// Types/layouts must have equal nonzero length and are copied at exact capacity.
// Every layout is explicit; reserve must return exact, owned reservation tokens.
func NewNumericBundle(types []memory.DataType, layouts []Layout, reserve BundleReserve) (*NumericBundle, error) {
	if reserve == nil {
		return nil, fmt.Errorf("aggregate: numeric bundle requires a reservation callback")
	}
	if err := validateNumericDescriptors(types, layouts); err != nil {
		return nil, err
	}
	per := uint64(unsafe.Sizeof(memory.DataType(0))) + uint64(unsafe.Sizeof(Layout{}))
	limit := min(uint64(math.MaxInt64), uint64(^uint(0)>>1))
	if uint64(len(types)) > limit/per {
		return nil, fmt.Errorf("aggregate: numeric bundle descriptor size overflow")
	}
	metadata, err := reserveBundle(reserve, int64(uint64(len(types))*per))
	if err != nil {
		return nil, err
	}
	ownedTypes := make([]memory.DataType, len(types))
	copy(ownedTypes, types)
	ownedLayouts := make([]Layout, len(layouts))
	copy(ownedLayouts, layouts)
	return &NumericBundle{types: ownedTypes, layouts: ownedLayouts, reserve: reserve, metadata: metadata}, nil
}

// NumericBundleBytes returns the complete requested replacement backing size
// for the given capacity, including the per-lane NumericVector headers. It
// excludes immutable descriptor copies, which NewNumericBundle charges once,
// and runtime/allocator overhead. Invalid descriptors and overflow are rejected.
func NumericBundleBytes(capacity int, types []memory.DataType, layouts []Layout) (int64, error) {
	if capacity < 0 {
		return 0, fmt.Errorf("aggregate: negative numeric bundle capacity")
	}
	if err := validateNumericDescriptors(types, layouts); err != nil {
		return 0, err
	}
	header := uint64(unsafe.Sizeof(NumericVector{}))
	limit := min(uint64(math.MaxInt64), uint64(^uint(0)>>1))
	if uint64(len(types)) > limit/header {
		return 0, fmt.Errorf("aggregate: numeric bundle header size overflow")
	}
	bytes := uint64(len(types)) * header
	for lane, typ := range types {
		n, err := NumericVectorBytesForSlots(capacity, typ, layouts[lane])
		if err != nil {
			return 0, err
		}
		if uint64(n) > limit-bytes {
			return 0, fmt.Errorf("aggregate: numeric bundle backing size overflow")
		}
		bytes += uint64(n)
	}
	return int64(bytes), nil
}

// EnsureGroups makes at least groups stable IDs addressable, preserving all
// existing raw states. New groups have empty identities. Replacement reserves
// full new headers/arrays while old arrays remain charged; any returned error
// leaves the previous capacity, values and reservations unchanged.
func (b *NumericBundle) EnsureGroups(groups int) error {
	if b == nil || b.closed {
		return fmt.Errorf("aggregate: numeric bundle is closed")
	}
	if groups < 0 {
		return fmt.Errorf("aggregate: negative numeric bundle group count")
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
	bytes, err := NumericBundleBytes(capacity, b.types, b.layouts)
	if err != nil {
		return err
	}
	lease, err := reserveBundle(b.reserve, bytes)
	if err != nil {
		return err
	}
	lanes := make([]NumericVector, len(b.types))
	for column, typ := range b.types {
		lanes[column], err = NewNumericVector(capacity, typ, b.layouts[column])
		if err != nil {
			lease.Release()
			return err
		}
		for group := 0; group < b.capacity; group++ {
			if err = b.lanes[column].Copy(&lanes[column], group, group); err != nil {
				lease.Release()
				return err
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

// Capacity returns the number of addressable stable group IDs.
func (b *NumericBundle) Capacity() int {
	if b == nil {
		return 0
	}
	return b.capacity
}

// Columns returns the number of independent input-column lanes.
func (b *NumericBundle) Columns() int {
	if b == nil {
		return 0
	}
	return len(b.types)
}

// AddBits updates one existing group and lane using its native input bits.
// The bundle must be live, and group/column must be addressable. Count-only
// lanes ignore bits and use validity only. Updates allocate no memory.
func (b *NumericBundle) AddBits(group, column int, bits uint64, valid bool) {
	b.lanes[column].AddBits(group, bits, valid)
}

// Result returns one lane's raw mergeable state without allocating. The bundle
// must be live, and group/column must be addressable.
func (b *NumericBundle) Result(group, column int) NumericResult { return b.lanes[column].Result(group) }

// Merge combines one compatible raw partial state into an addressable lane.
// Type/layout mismatches leave the lane unchanged and return an error.
func (b *NumericBundle) Merge(group, column int, state NumericResult) error {
	return b.lanes[column].MergeResult(group, state)
}

// ClearGroup restores empty identities for all columns of an existing group.
// Closed bundles and out-of-range IDs are rejected before any state changes.
func (b *NumericBundle) ClearGroup(group int) error {
	if b == nil || b.closed {
		return fmt.Errorf("aggregate: numeric bundle is closed")
	}
	if group < 0 || group >= b.capacity {
		return fmt.Errorf("aggregate: numeric bundle group %d out of range", group)
	}
	for column, typ := range b.types {
		if err := b.lanes[column].InitResult(group, NumericResult{Type: typ, Layout: b.layouts[column]}); err != nil {
			return err
		}
	}
	return nil
}

// RetainedBytes reports all live bundle-owned reservation bytes, including
// copied descriptors and vector headers but excluding allocator overhead.
func (b *NumericBundle) RetainedBytes() int64 {
	if b == nil {
		return 0
	}
	var bytes int64
	if b.metadata != nil {
		bytes += b.metadata.Bytes()
	}
	if b.lease != nil {
		bytes += b.lease.Bytes()
	}
	return bytes
}

// Close releases every bundle-owned backing array and reservation exactly once.
// Other operations except inspection must not be used afterward.
func (b *NumericBundle) Close() {
	if b == nil || b.closed {
		return
	}
	b.closed = true
	if b.lease != nil {
		b.lease.Release()
		b.lease = nil
	}
	if b.metadata != nil {
		b.metadata.Release()
		b.metadata = nil
	}
	b.types, b.layouts, b.lanes = nil, nil, nil
	b.capacity = 0
}
