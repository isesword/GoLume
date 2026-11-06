package hashagg

import (
	"context"
	"gocolumn/aggregate"
	"gocolumn/memory"
)

func mustHashTable[T any](table T, err error) T {
	if err != nil {
		panic(err)
	}
	return table
}

// Fixed-width tables reserve slots and nullable state before any growth.
func denyFixedGrowth(denied error) ReserveFunc {
	maximumInitialComponent := u64TableBytesForCapacity(tableCapacity(1), aggregate.DefaultLayout())
	manager := memory.NewBudgetManager(0)
	return func(bytes int64) (memory.Reservation, error) {
		if bytes > maximumInitialComponent {
			return nil, denied
		}
		return manager.Reserve(context.Background(), bytes)
	}
}
