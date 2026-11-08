package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"reflect"
	"testing"
	"unsafe"
)

// Compare every range to the scalar hash oracle and full-table visitor. This
// covers sparse physical slots, NULL sentinels, dense multi-state IDs, scratch
// flush boundaries, and every radix level used by recursive repartitioning.
func TestPartitionDirectoryIndependentAllShapes(t *testing.T) {
	for _, multi := range []bool{false, true} {
		for _, shape := range []GroupShape{GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapeDictionaryString, GroupShapePairFixed, GroupShapeStringView, GroupShapeGeneric} {
			if shape == GroupShapeGeneric && !multi {
				continue
			}
			for _, count := range []int{0, 1, 255, 256, 257, 777} {
				t.Run(fmt.Sprintf("multi=%v/shape=%v/count=%d", multi, shape, count), func(t *testing.T) {
					s, b, _ := aggregateMetadataStream(t, 32<<20, 1, 1)
					ctx := memory.WithQueryBudget(context.Background(), b)
					manager := memory.NewBudgetManager(32 << 20)
					layout := aggregate.DefaultLayout()
					var compiled *CompiledMultiAggregate
					if multi {
						compiled = &CompiledMultiAggregate{Columns: []string{"v", "w"}, Types: []memory.DataType{memory.TypeInt64, memory.TypeInt64}, Layouts: []aggregate.Layout{layout, layout}}
					}
					table, err := newAggregateStateTable(shape, layout, 4096, 20, 0, func(n int64) (memory.Reservation, error) { return manager.Reserve(ctx, n) }, compiled)
					if err != nil {
						t.Fatal(err)
					}
					defer table.close()
					for i := 0; i < count; i++ {
						r := aggregateRecord{a: uint64(int64(i) - 400), b: uint64(i * 17), validMask: 1, stringKey: []byte(fmt.Sprintf("key-%d-中", i)), state: aggregate.Int64Result{Sum: int64(i + 1), SumValid: true, CountRows: 1}}
						if shape == GroupShapePairFixed {
							r.validMask = uint8(i % 4)
							if r.validMask&1 == 0 {
								r.a = 0
							}
							if r.validMask&2 == 0 {
								r.b = 0
							}
						}
						if i == 0 && shape != GroupShapeGeneric {
							r.a = 0
							r.b = 0
							r.validMask = 0
							r.stringKey = nil
						}
						r.states = []aggregate.Int64Result{r.state, {Sum: -int64(i + 1), SumValid: true, CountRows: 1}}
						if _, err := table.merge(r); err != nil {
							t.Fatal(err)
						}
					}
					key := func(r aggregateRecord) string { return fmt.Sprintf("%d/%d/%d/%x", r.a, r.b, r.validMask, r.stringKey) }
					expected := map[string]aggregate.Int64Result{}
					if err := table.visit(func(r aggregateRecord) error { expected[key(r)] = r.state; return nil }); err != nil {
						t.Fatal(err)
					}
					for _, parts := range []int{1, 2, 4, 8, 16} {
						for _, offset := range []uint{0, 4, 28, 60} {
							before := b.Used()
							d, err := s.partitionTable(ctx, table, offset, parts)
							if err != nil {
								t.Fatal(err)
							}
							wantBytes := int64(unsafe.Sizeof(aggregatePartitionDirectory{})) + int64(table.len())*8
							if b.Used()-before != wantBytes {
								t.Fatalf("directory charge=%d want=%d", b.Used()-before, wantBytes)
							}
							got := map[string]aggregate.Int64Result{}
							for p := 0; p < parts; p++ {
								if err := d.visit(ctx, table, p, func(r aggregateRecord) error {
									if int(aggregateRecordHash(shape, r)>>offset&uint64(parts-1)) != p {
										t.Fatalf("wrong partition %d", p)
									}
									k := key(r)
									if _, ok := got[k]; ok {
										t.Fatalf("duplicate %s", k)
									}
									got[k] = r.state
									if multi && r.bundle.Result(r.groupID, 1).Sum != -r.state.Sum {
										t.Fatal("multi-state lane changed")
									}
									return nil
								}); err != nil {
									t.Fatal(err)
								}
							}
							if !reflect.DeepEqual(got, expected) {
								t.Fatalf("range records differ: got=%d want=%d", len(got), len(expected))
							}
							d.close()
							d.close()
							if b.Used() != before {
								t.Fatal("directory leaked")
							}
						}
					}
					before := b.Used()
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					if d, err := s.partitionTable(cancelled, table, 0, 16); !errors.Is(err, context.Canceled) || d != nil {
						t.Fatalf("cancel result=%v %v", d, err)
					}
					if b.Used() != before {
						t.Fatal("cancel leaked")
					}
					for _, parts := range []int{0, 3, 32} {
						if d, err := s.partitionTable(ctx, table, 0, parts); err == nil || d != nil {
							t.Fatal("invalid count accepted")
						}
					}
					table.close()
					if manager.Used() != 0 {
						t.Fatal("table leaked")
					}
				})
			}
		}
	}
}

func TestPartitionDirectoryIndependentRetry(t *testing.T) {
	s, b, dir := aggregateMetadataStream(t, 1<<20, 300, 300)
	ctx := memory.WithQueryBudget(context.Background(), b)
	if err := s.ensureTableLocked(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if _, err := s.table.table.add(aggregateRecord{a: uint64(i), validMask: 1}, int64(i), true); err != nil {
			t.Fatal(err)
		}
	}
	before := b.Used()
	// Exhaust the query's remaining hard allowance: admission must be atomic.
	hold, err := s.encoder.ReserveGrowth(ctx, b.Limit()-before)
	if err != nil {
		t.Fatal(err)
	}
	if d, err := s.partitionTable(ctx, s.table.table, 0, 16); !errors.Is(err, memory.ErrBudgetExceeded) || d != nil {
		t.Fatalf("budget admission=%v %v", d, err)
	}
	hold.Release()
	if b.Used() != before || s.table.table.len() != 300 {
		t.Fatal("failed admission mutated state")
	}
	// One partition is the maximally skewed case. A callback failure must not
	// consume any directory cursor, and repeated visits must be identical.
	d, err := s.partitionTable(ctx, s.table.table, 60, 1)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("callback failure")
	calls := 0
	err = d.visit(ctx, s.table.table, 0, func(aggregateRecord) error {
		calls++
		if calls == 257 {
			return injected
		}
		return nil
	})
	if !errors.Is(err, injected) || calls != 257 {
		t.Fatal("callback error not propagated")
	}
	var previous []uint64
	for attempt := 0; attempt < 2; attempt++ {
		var keys []uint64
		if err := d.visit(ctx, s.table.table, 0, func(r aggregateRecord) error { keys = append(keys, r.a); return nil }); err != nil {
			t.Fatal(err)
		}
		if len(keys) != 300 {
			t.Fatal("retry lost records")
		}
		if attempt > 0 && !reflect.DeepEqual(keys, previous) {
			t.Fatal("repeat visit changed")
		}
		previous = keys
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	calls = 0
	if err := d.visit(cancelled, s.table.table, 0, func(aggregateRecord) error { calls++; return nil }); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatal("cancelled visit emitted records")
	}
	d.close()
	if b.Used() != before {
		t.Fatal("retry leaked")
	}
	if _, err := s.Spill(ctx, memory.SpillRequest{}); err != nil {
		t.Fatal(err)
	}
	groups := 0
	for _, p := range s.partitions {
		for _, r := range p.Values {
			groups += r.groups
		}
	}
	if groups != 300 || s.table != nil {
		t.Fatal("spill after retry lost or duplicated groups")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, b, dir)
}
