// Package hashagg implements cache-conscious hash aggregation tables used by
// the columnar execution engine.
package hashagg

import (
	"fmt"
	"gocolumn/aggregate"
)

const defaultMaxLoadFactor = 0.70

// ReserveFunc reserves additional retained bytes before a hash table grows.
//
// The callback is invoked only for capacity growth beyond the table's initial
// allocation. Callers that enforce a query memory budget should retain the
// reservation until the owning workspace is released.
type ReserveFunc func(bytes int64) error

// TableOptions controls adaptive hash-table growth and aggregate-state layout.
type TableOptions struct {
	// MaxLoadFactor is the maximum occupied-slot ratio before the table grows.
	// Values outside (0, 0.90] use the default of 0.70.
	MaxLoadFactor float64
	// Reserve is called before retained table capacity grows. A nil callback
	// permits growth without external memory accounting.
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

func reserveGrowth(reserve ReserveFunc, oldBytes, newBytes int64) error {
	if newBytes <= oldBytes || reserve == nil {
		return nil
	}
	if err := reserve(newBytes - oldBytes); err != nil {
		return fmt.Errorf("hash table growth reserve %d bytes: %w", newBytes-oldBytes, err)
	}
	return nil
}
