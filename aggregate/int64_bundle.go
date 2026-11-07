package aggregate

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"unsafe"
)

// BundleReserve reserves a complete backing allocation before it is created.
// The bundle owns successful reservations and releases them on replacement or
// Close. During growth both the old and new backing arrays remain charged.
type BundleReserve func(bytes int64) (memory.Reservation, error)

// Int64Bundle keeps one structure-of-arrays state vector per input column,
// indexed by stable group IDs rather than hash-table slots. It is the shared
// mergeable state contract for multi-column int64 aggregation. It does not own
// keys and must not be used concurrently without external partition ownership.
type Int64Bundle struct {
	layouts  []Layout
	lanes    []Int64Vector
	capacity int
	reserve  BundleReserve
	lease    memory.Reservation
	metadata memory.Reservation
	closed   bool
}

// NewInt64Bundle constructs an empty, budgeted multi-column state bundle.
// Every lane must request an explicit nonzero layout. Reserve is mandatory;
// callers that do not need a finite limit can use an unlimited budget manager.
func NewInt64Bundle(layouts []Layout, reserve BundleReserve) (*Int64Bundle, error) {
	if len(layouts) == 0 || reserve == nil {
		return nil, fmt.Errorf("aggregate bundle requires layouts and reservation callback")
	}
	for _, layout := range layouts {
		if layout.IsZero() {
			return nil, fmt.Errorf("aggregate bundle layout must not be empty")
		}
	}
	bytesPer := int64(unsafe.Sizeof(Layout{}))
	if uint64(len(layouts)) > uint64(math.MaxInt64/bytesPer) {
		return nil, fmt.Errorf("aggregate bundle metadata size overflow")
	}
	metadata, err := reserveBundle(reserve, int64(len(layouts))*bytesPer)
	if err != nil {
		return nil, err
	}
	ownedLayouts := make([]Layout, len(layouts))
	copy(ownedLayouts, layouts)
	return &Int64Bundle{layouts: ownedLayouts, reserve: reserve, metadata: metadata}, nil
}

func reserveBundle(reserve BundleReserve, bytes int64) (memory.Reservation, error) {
	if bytes == 0 {
		return nil, nil
	}
	lease, err := reserve(bytes)
	if err != nil {
		if lease != nil {
			lease.Release()
		}
		return nil, fmt.Errorf("aggregate bundle reserve %d bytes: %w", bytes, err)
	}
	if lease == nil {
		return nil, fmt.Errorf("aggregate bundle reservation is nil")
	}
	if lease.Bytes() != bytes {
		lease.Release()
		return nil, fmt.Errorf("aggregate bundle reservation size differs from %d", bytes)
	}
	return lease, nil
}

// Int64BundleBytes computes backing-array bytes for a group capacity. The
// estimate includes the temporary vector-header array allocated on growth;
// immutable layout metadata is owned and charged separately by the bundle.
func Int64BundleBytes(capacity int, layouts []Layout) (int64, error) {
	if capacity < 0 {
		return 0, fmt.Errorf("aggregate bundle capacity must be nonnegative")
	}
	per := int64(0)
	for _, layout := range layouts {
		if layout.IsZero() {
			return 0, fmt.Errorf("aggregate bundle layout must not be empty")
		}
		next := Int64VectorBytesForSlots(1, layout)
		if per > math.MaxInt64-next {
			return 0, fmt.Errorf("aggregate bundle size overflow")
		}
		per += next
	}
	headers := uint64(len(layouts)) * uint64(unsafe.Sizeof(Int64Vector{}))
	if headers > math.MaxInt64 || (per > 0 && uint64(capacity) > uint64((math.MaxInt64-int64(headers))/per)) {
		return 0, fmt.Errorf("aggregate bundle size overflow")
	}
	return int64(capacity)*per + int64(headers), nil
}

// EnsureGroups grows state to contain at least groups stable IDs. Growth is
// transactional: reservation failure leaves every previously stored state
// unchanged. New groups start with empty aggregate identities, not a fake row.
func (b *Int64Bundle) EnsureGroups(groups int) error {
	if b == nil || b.closed {
		return fmt.Errorf("aggregate bundle is closed")
	}
	if groups < 0 {
		return fmt.Errorf("aggregate bundle group count must be nonnegative")
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
	bytes, err := Int64BundleBytes(capacity, b.layouts)
	if err != nil {
		return err
	}
	lease, err := reserveBundle(b.reserve, bytes)
	if err != nil {
		return err
	}
	lanes := make([]Int64Vector, len(b.layouts))
	for column, layout := range b.layouts {
		lanes[column] = NewInt64Vector(capacity, layout)
		for group := 0; group < b.capacity; group++ {
			b.lanes[column].Copy(&lanes[column], group, group)
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
func (b *Int64Bundle) Capacity() int {
	if b == nil {
		return 0
	}
	return b.capacity
}

// Columns returns the number of independent input-column state lanes.
func (b *Int64Bundle) Columns() int {
	if b == nil {
		return 0
	}
	return len(b.layouts)
}

// AddValue updates one existing group and input-column lane. EnsureGroups must
// have made the ID addressable. A NULL value still contributes to COUNT(*).
func (b *Int64Bundle) AddValue(group, column int, value int64, valid bool) {
	b.lanes[column].AddValue(group, value, valid)
}

// Result returns one lane's mergeable state. Average is finalized as a convenience
// but merges always use the raw Sum and CountValues fields.
func (b *Int64Bundle) Result(group, column int) Int64Result { return b.lanes[column].Result(group) }

// Merge combines one compatible partial lane into an existing group. The caller
// verifies matching descriptors and layouts before invoking this hot-path method.
func (b *Int64Bundle) Merge(group, column int, state Int64Result) {
	b.lanes[column].MergeResult(group, state)
}

// ClearGroup restores empty identities for all lanes of an existing group.
func (b *Int64Bundle) ClearGroup(group int) {
	for i := range b.lanes {
		b.lanes[i].InitResult(group, Int64Result{})
	}
}

// RetainedBytes reports live bundle-owned reservation bytes, including headers.
func (b *Int64Bundle) RetainedBytes() int64 {
	if b == nil {
		return 0
	}
	var n int64
	if b.lease != nil {
		n += b.lease.Bytes()
	}
	if b.metadata != nil {
		n += b.metadata.Bytes()
	}
	return n
}

// Close releases all bundle-owned buffers and reservations. It is idempotent.
func (b *Int64Bundle) Close() {
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
	b.lanes, b.layouts = nil, nil
	b.capacity = 0
}
