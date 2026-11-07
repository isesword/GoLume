package engine

import (
	"context"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"testing"
)

func TestAggregateStateTableTracksLiveComponentReservations(t *testing.T) {
	for _, shape := range []GroupShape{GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapeDictionaryString, GroupShapePairFixed, GroupShapeStringView} {
		t.Run(fmt.Sprint(shape), func(t *testing.T) {
			manager := memory.NewBudgetManager(1 << 24)
			peak := int64(0)
			state, err := newAggregateStateTable(shape, aggregate.Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}, 8, 1, 0, func(bytes int64) (memory.Reservation, error) {
				lease, err := manager.Reserve(context.Background(), bytes)
				peak = max(peak, manager.Used())
				return lease, err
			})
			if err != nil {
				t.Fatal(err)
			}
			actual := func() int64 {
				switch shape {
				case GroupShapeI32:
					return state.i32.RetainedBytes()
				case GroupShapeI64:
					return state.u64.RetainedBytes()
				case GroupShapeU32, GroupShapeDictionaryString:
					return state.u32.RetainedBytes()
				case GroupShapePairFixed:
					return state.pair.RetainedBytes()
				default:
					return state.str.RetainedBytes()
				}
			}
			for key := 0; key < 300; key++ {
				record := aggregateRecord{a: uint64(key), b: uint64(key * 13), validMask: 3, stringKey: []byte(fmt.Sprintf("key-%08d-contents", key))}
				if _, err := state.add(record, int64(key), key%3 != 0); err != nil {
					t.Fatal(err)
				}
				if state.retained != actual() || manager.Used() != actual() {
					t.Fatalf("key=%d tracked=%d actual=%d charged=%d", key, state.retained, actual(), manager.Used())
				}
			}
			if peak <= state.retained {
				t.Fatalf("replacement overlap not charged: peak=%d live=%d", peak, state.retained)
			}
			state.close()
			state.close()
			if state.retained != 0 || manager.Used() != 0 {
				t.Fatalf("closed tracked=%d charged=%d", state.retained, manager.Used())
			}
		})
	}
}

func TestAggregateStateReservationRejectsInvalidCallbacks(t *testing.T) {
	for _, mode := range []string{"nil", "wrong-size", "lease-with-error"} {
		t.Run(mode, func(t *testing.T) {
			manager := memory.NewBudgetManager(1 << 20)
			state := &aggregateStateTable{reserve: func(bytes int64) (memory.Reservation, error) {
				switch mode {
				case "nil":
					return nil, nil
				case "wrong-size":
					return manager.Reserve(context.Background(), bytes+1)
				default:
					lease, err := manager.Reserve(context.Background(), bytes)
					if err != nil {
						t.Fatal(err)
					}
					return lease, fmt.Errorf("injected reservation failure")
				}
			}}
			if _, err := state.reserveGrowth(128); err == nil {
				t.Fatal("invalid callback accepted")
			}
			if manager.Used() != 0 || state.retained != 0 {
				t.Fatalf("charged=%d retained=%d", manager.Used(), state.retained)
			}
		})
	}
}
