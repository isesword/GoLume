// Package hashagg implements cache-conscious hash aggregation tables used by
// the columnar execution engine.
package hashagg

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
)

const defaultMaxLoadFactor = 0.70

// ReserveFunc reserves one complete backing allocation before it is created.
// A successful call must return a non-nil reservation for exactly bytes. Tables
// own these reservations and release them after replacement or Close. During
// growth the old and complete new allocation remain charged simultaneously.
type ReserveFunc func(bytes int64) (memory.Reservation, error)

// TableOptions controls adaptive hash-table growth and aggregate-state layout.
type TableOptions struct {
	// MaxLoadFactor is the maximum occupied-slot ratio before the table grows.
	// Values outside (0, 0.90] use the default of 0.70.
	MaxLoadFactor float64
	// Reserve is called before initial allocation and replacement of each
	// owned component. A nil callback opts out of external memory accounting.
	Reserve ReserveFunc
	// Aggregates selects the structure-of-arrays state stored per occupied slot.
	// A zero Layout preserves the default SUM(value)+COUNT(*) behavior.
	Aggregates aggregate.Layout
}

func normalizeTableOptions(opts TableOptions) TableOptions {
	if opts.MaxLoadFactor <= 0 || opts.MaxLoadFactor > 0.90 {
		opts.MaxLoadFactor = defaultMaxLoadFactor
	}
	if opts.Aggregates.IsZero() {
		opts.Aggregates = aggregate.DefaultLayout()
	}
	return opts
}

func loadLimit(capacity int, factor float64) int {
	limit := int(float64(capacity) * factor)
	if limit < 1 {
		return 1
	}
	if limit >= capacity {
		return capacity - 1
	}
	return limit
}

func reserveAllocation(reserve ReserveFunc, bytes int64) (memory.Reservation, error) {
	if bytes < 0 {
		return nil, fmt.Errorf("hash table allocation size overflow")
	}
	if reserve == nil || bytes == 0 {
		return nil, nil
	}
	lease, err := reserve(bytes)
	if err != nil {
		if lease != nil {
			lease.Release()
		}
		return nil, fmt.Errorf("hash table reserve %d bytes: %w", bytes, err)
	}
	if lease == nil {
		return nil, fmt.Errorf("hash table reservation for %d bytes is nil", bytes)
	}
	if lease.Bytes() != bytes {
		lease.Release()
		return nil, fmt.Errorf("hash table reservation size differs from %d bytes", bytes)
	}
	return lease, nil
}

func releaseAllocation(lease memory.Reservation) {
	if lease != nil {
		lease.Release()
	}
}

// checkedTableCapacity rejects unrepresentable backing sizes before computing
// a power-of-two capacity or asking an allocator to reserve memory.
func checkedTableCapacity(hint int, bytesPerSlot int64) (int, error) {
	if hint < 8 {
		hint = 8
	}
	maxInt := int(^uint(0) >> 1)
	maxSlots := int64(maxInt) / bytesPerSlot
	if int64(hint) > maxSlots/2 {
		return 0, fmt.Errorf("hash table capacity overflow")
	}
	n := tableCapacity(hint)
	if int64(n) > maxSlots {
		return 0, fmt.Errorf("hash table backing size overflow")
	}
	return n, nil
}

func checkedGrowCapacity(old int, bytesPerSlot int64) (int, error) {
	if old < 1 || int64(old) > int64(int(^uint(0)>>1))/bytesPerSlot/2 {
		return 0, fmt.Errorf("hash table growth capacity overflow")
	}
	return old * 2, nil
}
