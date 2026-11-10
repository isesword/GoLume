package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"math"
	"testing"
)

func compactTestRecord(key uint64, value int64) aggregateRecord {
	return aggregateRecord{a: key, validMask: 1, state: aggregate.Int64Result{
		Sum: value, SumValid: true, CountRows: 1,
	}}
}

func compactTestSeed(t *testing.T, s *spillableAggregateStream, partition int, records []aggregateRecord) aggregateRunRef {
	t.Helper()
	ctx := context.Background()
	encoded, err := s.query.encodeAggregateRun(ctx, func(fn func(aggregateRecord) error) error {
		for _, record := range records {
			if err := fn(record); err != nil {
				return err
			}
		}
		return nil
	}, nil, spillmeta.GrowthManager(s.encoder))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.close()
	h, err := s.store.Spill(ctx, memory.SpillObject{
		Kind: memory.SpillAggregateState, Partition: int32(partition), Codec: s.query.aggregateRunCodec(),
		SchemaFingerprint: s.fingerprint, Payload: encoded.payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := aggregateRunRef{handle: h, groups: encoded.groups, partition: partition}
	if err := s.partitions[partition].Append(ctx, s.metadata, ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

func compactTestOriginals(t *testing.T, s *spillableAggregateStream, refs ...aggregateRunRef) {
	t.Helper()
	if len(s.partitions[0].Values) != len(refs) {
		t.Fatalf("source run count=%d want=%d", len(s.partitions[0].Values), len(refs))
	}
	for i, ref := range refs {
		if s.partitions[0].Values[i].handle != ref.handle {
			t.Fatal("source owner changed before commit")
		}
		if err := s.restoreRun(context.Background(), ref, func(aggregateRecord) error { return nil }); err != nil {
			t.Fatalf("original source is not retryable: %v", err)
		}
	}
	if s.state.Used() != 0 || s.encoder.Used() != 0 || s.restorer.Used() != 0 || s.table != nil {
		t.Fatal("compaction retained a temporary table or buffer")
	}
}

func TestAggregateInputCompactionPrefixAndCounters(t *testing.T) {
	s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
	ctx := context.Background()
	first := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 2), compactTestRecord(9, 3)})
	second := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 5), compactTestRecord(9, 11)})
	third := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 13)})
	fourth := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(9, 17)})
	s.selectedRows, s.checksum, s.groupCount = 19, 23, 29
	s.maxDepth, s.maxPartitionGroups = 2, 31
	s.spillRuns, s.spillBytes = 4, 100
	if err := s.compactInputRuns(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.partitions[0].Values) != 3 || s.partitions[0].Values[1].handle != third.handle || s.partitions[0].Values[2].handle != fourth.handle {
		t.Fatal("compaction did not replace exactly the oldest two runs, preserving suffix order")
	}
	ref := s.partitions[0].Values[0]
	if ref.groups != 2 || ref.partition != 0 {
		t.Fatalf("new run=%+v", ref)
	}
	seen := 0
	if err := s.restoreRun(ctx, ref, func(record aggregateRecord) error {
		want := int64(7)
		if record.a == 9 {
			want = 14
		} else if record.a != 7 {
			t.Fatalf("unexpected key %d", record.a)
		}
		if record.state.Sum != want || record.state.CountRows != 2 {
			t.Fatalf("wrong merged state %+v", record.state)
		}
		seen++
		return nil
	}); err != nil || seen != 2 {
		t.Fatalf("restore: seen=%d err=%v", seen, err)
	}
	for _, old := range []aggregateRunRef{first, second} {
		if err := s.restoreRun(ctx, old, func(aggregateRecord) error { return nil }); !errors.Is(err, memory.ErrInvalidSpillHandle) {
			t.Fatalf("old run was not deleted: %v", err)
		}
	}
	if s.selectedRows != 19 || s.checksum != 23 || s.groupCount != 29 || s.maxDepth != 2 || s.maxPartitionGroups != 31 {
		t.Fatal("compaction changed logical counters")
	}
	if s.spillRuns != 5 || s.spillBytes != 100+ref.handle.Bytes() {
		t.Fatal("compaction did not count its real spill I/O")
	}
	if s.state.Used() != 0 || s.encoder.Used() != 0 || s.restorer.Used() != 0 {
		t.Fatal("temporary workspace remains")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, budget, dir)
}

func TestAggregateInputCompactionPressureLeavesSources(t *testing.T) {
	for _, mode := range []string{"query", "state"} {
		t.Run(mode, func(t *testing.T) {
			s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
			first := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 2)})
			second := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 3)})
			before := budget.Used()
			oldMax := s.maxState
			var blocker memory.Reservation
			if mode == "query" {
				var err error
				blocker, err = budget.Operator("compaction-test-blocker", memory.MemoryWorkspace).ReserveGrowth(context.Background(), budget.Limit()-before)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				s.maxState = 1
			}
			if err := s.compactInputRuns(context.Background()); err != nil {
				t.Fatal(err)
			}
			if blocker != nil {
				blocker.Release()
			}
			s.maxState = oldMax
			if budget.Used() != before {
				t.Fatalf("failed attempt changed retained bytes: %d want %d", budget.Used(), before)
			}
			compactTestOriginals(t, s, first, second)
			if err := s.compactInputRuns(context.Background()); err != nil || len(s.partitions[0].Values) != 1 {
				t.Fatalf("retry failed: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			assertAggregateMetadataClean(t, budget, dir)
		})
	}
}

type compactTestManager struct {
	memory.Manager
	calls, fail int
	failure     error
}

func (m *compactTestManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls == m.fail {
		return nil, m.failure
	}
	return m.Manager.Reserve(ctx, n)
}

type compactTestStore struct {
	memory.SpillManager
	writeFailure          error
	deleteFailure         error
	deleteID              string
	last                  memory.SpillHandle
	writes, deletes       int
	restoreFailure        error
	failRestore, restores int
	afterWrite            func()
}

func (s *compactTestStore) Spill(ctx context.Context, object memory.SpillObject) (memory.SpillHandle, error) {
	s.writes++
	if s.writeFailure != nil {
		return nil, s.writeFailure
	}
	h, err := s.SpillManager.Spill(ctx, object)
	s.last = h
	if err == nil && s.afterWrite != nil {
		s.afterWrite()
	}
	return h, err
}

func (s *compactTestStore) Restore(ctx context.Context, h memory.SpillHandle, schema [32]byte, manager memory.Manager) (*memory.RestoredSpill, error) {
	s.restores++
	if s.restores == s.failRestore {
		return nil, s.restoreFailure
	}
	return s.SpillManager.Restore(ctx, h, schema, manager)
}

func (s *compactTestStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	s.deletes++
	if s.deleteFailure != nil && (s.deleteID == "" || h.ID() == s.deleteID) {
		return s.deleteFailure
	}
	return s.SpillManager.Delete(ctx, h)
}

func TestAggregateInputCompactionPrecommitFailuresRetry(t *testing.T) {
	for _, mode := range []string{"write", "refs", "refs-pressure", "cleanup"} {
		t.Run(mode, func(t *testing.T) {
			s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
			first := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 2)})
			second := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 3)})
			before := budget.Used()
			failure := errors.New("injected compaction failure")
			cleanup := errors.New("injected compaction cleanup failure")
			store := &compactTestStore{SpillManager: s.store}
			s.store = store
			metadata := &compactTestManager{Manager: s.metadata, fail: 2, failure: failure}
			if mode == "write" {
				store.writeFailure = failure
			} else {
				s.metadata = metadata
				if mode == "refs-pressure" || mode == "cleanup" {
					metadata.failure = memory.ErrBudgetExceeded
				}
				if mode == "cleanup" {
					store.deleteFailure = cleanup
				}
			}
			err := s.compactInputRuns(context.Background())
			switch mode {
			case "refs-pressure":
				if err != nil {
					t.Fatalf("pressure was not a safe skip: %v", err)
				}
			case "cleanup":
				if !errors.Is(err, cleanup) {
					t.Fatalf("cleanup failure was swallowed by pressure: %v", err)
				}
			default:
				if !errors.Is(err, failure) {
					t.Fatalf("want injected failure: %v", err)
				}
			}
			compactTestOriginals(t, s, first, second)
			if mode == "cleanup" {
				if budget.Used() <= before {
					t.Fatal("failed temporary-file delete lost accounting")
				}
				store.deleteFailure = nil
				if err := store.Delete(context.Background(), store.last); err != nil {
					t.Fatal(err)
				}
			}
			if budget.Used() != before {
				t.Fatalf("precommit attempt leaked bytes: %d want %d", budget.Used(), before)
			}
			store.writeFailure = nil
			metadata.fail = 0
			if err := s.compactInputRuns(context.Background()); err != nil || len(s.partitions[0].Values) != 1 {
				t.Fatalf("retry failed: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			assertAggregateMetadataClean(t, budget, dir)
		})
	}
}

func TestAggregateInputCompactionPartialWorkIsNeverPublished(t *testing.T) {
	for _, mode := range []string{"restore-pressure", "restore-error", "cancel-after-write"} {
		t.Run(mode, func(t *testing.T) {
			s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
			first := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 2)})
			second := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 3)})
			before := budget.Used()
			failure := errors.New("second restore failed")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &compactTestStore{SpillManager: s.store, failRestore: 2, restoreFailure: failure}
			if mode == "restore-pressure" {
				store.restoreFailure = memory.ErrBudgetExceeded
			} else if mode == "cancel-after-write" {
				store.failRestore = 0
				store.afterWrite = cancel
				failure = context.Canceled
			}
			s.store = store
			err := s.compactInputRuns(ctx)
			if mode == "restore-pressure" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, failure) {
				t.Fatalf("want %v, got %v", failure, err)
			}
			compactTestOriginals(t, s, first, second)
			if budget.Used() != before {
				t.Fatalf("partial work retained %d bytes, want %d", budget.Used(), before)
			}
			store.failRestore, store.afterWrite = 0, nil
			if err := s.compactInputRuns(context.Background()); err != nil || len(s.partitions[0].Values) != 1 {
				t.Fatalf("retry failed: %v", err)
			}
			if err := s.restoreRun(context.Background(), s.partitions[0].Values[0], func(record aggregateRecord) error {
				if record.state.Sum != 5 || record.state.CountRows != 2 {
					t.Fatal("partial work was double-counted")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			assertAggregateMetadataClean(t, budget, dir)
		})
	}
}

func TestAggregateInputCompactionPostcommitDeleteFailure(t *testing.T) {
	s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
	first := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 2)})
	second := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 3)})
	failure := errors.New("first old file cannot be deleted")
	store := &compactTestStore{SpillManager: s.store, deleteFailure: failure, deleteID: first.handle.ID()}
	s.store = store
	if err := s.compactInputRuns(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("want deletion failure: %v", err)
	}
	if store.deletes != 2 || len(s.partitions[0].Values) != 1 || s.partitions[0].Values[0].handle == first.handle || s.partitions[0].Values[0].handle == second.handle {
		t.Fatal("partial deletion rolled back the authoritative replacement")
	}
	if err := s.restoreRun(context.Background(), s.partitions[0].Values[0], func(record aggregateRecord) error {
		if record.a != 7 || record.state.Sum != 5 || record.state.CountRows != 2 {
			t.Fatalf("authoritative replacement is incomplete: %+v", record)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.restoreRun(context.Background(), first, func(aggregateRecord) error { return nil }); err != nil {
		t.Fatalf("undeleted old file not retained: %v", err)
	}
	if err := s.restoreRun(context.Background(), second, func(aggregateRecord) error { return nil }); !errors.Is(err, memory.ErrInvalidSpillHandle) {
		t.Fatalf("second old file was not deleted: %v", err)
	}
	beforeDelete := budget.Used()
	store.deleteFailure = nil
	if err := store.Delete(context.Background(), first.handle); err != nil {
		t.Fatal(err)
	}
	if budget.Used() >= beforeDelete {
		t.Fatal("old file retained no real metadata charge")
	}
	if err := s.compactInputRuns(context.Background()); err != nil || store.writes != 1 {
		t.Fatal("single authoritative run was compacted again")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, budget, dir)
}

func TestAggregateInputCompactionCancellationAndResidentGuards(t *testing.T) {
	s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
	first := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 2)})
	second := compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 3)})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.compactInputRuns(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	compactTestOriginals(t, s, first, second)
	s.residentOutput = true
	if err := s.compactInputRuns(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.residentOutput = false
	compactTestOriginals(t, s, first, second)
	if err := s.ensureTableLocked(context.Background()); err != nil {
		t.Fatal(err)
	}
	owner := s.table
	if err := s.compactInputRuns(context.Background()); err != nil || s.table != owner || len(s.partitions[0].Values) != 2 {
		t.Fatal("compaction changed a live input table")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, budget, dir)
}

// Configure all five existing codec families without finalizing their state.
// The second run rotates fixture groups so endpoints carry different source
// ordinals (including NULLs), rather than merely duplicating the first run.
func compactTestCodecRecords(t *testing.T, s *spillableAggregateStream, family string) [2][]aggregateRecord {
	t.Helper()
	var out [2][]aggregateRecord
	groups := 3
	var numeric *aggregate.NumericBundle
	var statistics *aggregate.StatisticsBundle
	switch family {
	case "scalar":
	case "multi":
		layout := aggregate.Layout{Sum: true, CountRows: true, CountValues: true, Avg: true}
		s.query.cq.MultiAggregate = &CompiledMultiAggregate{
			Columns: []string{"a", "b"}, Types: []memory.DataType{memory.TypeInt64, memory.TypeInt64},
			Layouts: []aggregate.Layout{layout, layout},
		}
	case "numeric", "statistics", "bytes":
		types, layouts, bundle := numericCodecFixture(t)
		numeric = bundle
		s.query.cq.MultiAggregate = &CompiledMultiAggregate{
			Columns: make([]string, len(types)), Types: types, Layouts: layouts, Native: true,
		}
		if family == "numeric" {
			groups = 5
		} else {
			var types []memory.DataType
			var layouts []aggregate.StatisticsLayout
			if family == "statistics" {
				types, layouts, statistics = statisticsCodecFixture(t)
			} else {
				types, layouts, statistics = bytesCodecFixture(t)
			}
			s.query.cq.MultiAggregate.Statistics = &CompiledStatistics{
				Columns: make([]string, len(types)), Types: types, Layouts: layouts, Endpoints: true,
			}
		}
	default:
		t.Fatal(family)
	}
	for run := range out {
		for key := 0; key < groups; key++ {
			id := (key + run*2) % groups
			record := compactTestRecord(uint64(key), int64(id+1))
			record.groupID, record.numericBundle, record.statisticsBundle = id, numeric, statistics
			if family == "multi" {
				record.states = []aggregate.Int64Result{
					{Sum: int64(id + 1), SumValid: true, CountRows: 2, CountValues: 2},
					{Sum: int64(id*3 + 7), SumValid: true, CountRows: 2, CountValues: 1},
				}
			}
			out[run] = append(out[run], record)
		}
	}
	return out
}

func TestAggregateInputCompactionAllCodecIntermediateStates(t *testing.T) {
	for _, family := range []string{"scalar", "multi", "numeric", "statistics", "bytes"} {
		t.Run(family, func(t *testing.T) {
			s, budget, dir := aggregateMetadataStream(t, 4<<20, 1, 1)
			records := compactTestCodecRecords(t, s, family)
			ctx := context.Background()
			expected, err := newAggregateTableOwner(ctx, s.query, s.state, s.maxState)
			if err != nil {
				t.Fatal(err)
			}
			defer expected.close()
			for _, run := range records {
				compactTestSeed(t, s, 0, run)
				for _, record := range run {
					if _, err := expected.table.merge(record); err != nil {
						t.Fatal(err)
					}
				}
			}
			want, err := s.query.encodeAggregateRun(ctx, expected.table.visit, nil, spillmeta.GrowthManager(s.encoder))
			if err != nil {
				t.Fatal(err)
			}
			expected.close()
			if err := s.compactInputRuns(ctx); err != nil {
				want.close()
				t.Fatal(err)
			}
			if len(s.partitions[0].Values) != 1 {
				want.close()
				t.Fatal("runs were not compacted")
			}
			restored, err := s.store.Restore(ctx, s.partitions[0].Values[0].handle, s.fingerprint, spillmeta.GrowthManager(s.restorer))
			if err != nil {
				want.close()
				t.Fatal(err)
			}
			if !bytes.Equal(restored.Object.Payload, want.payload) {
				t.Error("compaction changed raw intermediate state or source ordinals")
			}
			want.close()
			if err := restored.Close(); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			assertAggregateMetadataClean(t, budget, dir)
		})
	}
}

func TestAggregateCompactionPayloadCapAllCodecs(t *testing.T) {
	for _, family := range []string{"scalar", "multi", "numeric", "statistics", "bytes"} {
		t.Run(family, func(t *testing.T) {
			s, budget, dir := aggregateMetadataStream(t, 4<<20, 1, 1)
			records := compactTestCodecRecords(t, s, family)[0]
			visit := func(fn func(aggregateRecord) error) error {
				for _, record := range records {
					if err := fn(record); err != nil {
						return err
					}
				}
				return nil
			}
			ctx := context.Background()
			encoded, err := s.query.encodeAggregateRun(ctx, visit, nil, spillmeta.GrowthManager(s.encoder))
			if err != nil {
				t.Fatal(err)
			}
			size := int64(len(encoded.payload))
			encoded.close()
			for _, limit := range []int64{size - 1, size} {
				buffer := &aggregateCompactionBuffer{Manager: spillmeta.GrowthManager(s.encoder), maxPayload: limit}
				run, err := s.query.encodeAggregateRun(ctx, visit, nil, buffer)
				if limit < size {
					if run != nil || !errors.Is(err, errAggregateStateTarget) || s.encoder.Used() != 0 {
						t.Fatalf("oversized payload allocated: run=%v err=%v bytes=%d", run != nil, err, s.encoder.Used())
					}
				} else {
					if err != nil || run == nil || s.encoder.Used() != size {
						t.Fatalf("exact cap rejected: err=%v bytes=%d size=%d", err, s.encoder.Used(), size)
					}
					run.close()
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			assertAggregateMetadataClean(t, budget, dir)
		})
	}
}

func TestAggregateCompactionPressureDoesNotHideOtherErrors(t *testing.T) {
	cleanup := errors.New("cleanup")
	for _, err := range []error{context.Canceled, cleanup, errors.Join(memory.ErrBudgetExceeded, cleanup), errors.Join(errAggregateStateTarget, context.Canceled)} {
		if aggregateCompactionPressure(err) {
			t.Fatalf("non-pressure error hidden: %v", err)
		}
	}
	for _, err := range []error{memory.ErrBudgetExceeded, fmt.Errorf("wrapped: %w", errAggregateStateTarget), errors.Join(memory.ErrBudgetExceeded, errAggregateStateTarget)} {
		if !aggregateCompactionPressure(err) {
			t.Fatalf("pressure not recognized: %v", err)
		}
	}
}

// Repeated prefix compaction must preserve the original left fold. In
// particular, merging the middle runs first would round -1e16+1 back to -1e16
// and turn the final SUM into zero. Endpoint ordinals deliberately disagree
// with run order, and one earliest endpoint belongs to a NULL-key group.
func TestAggregateInputCompactionRepeatedPrefixOrder(t *testing.T) {
	const runs, groups = 5, 2
	s, budget, dir := aggregateMetadataStream(t, 4<<20, 1, 1)
	ctx := context.Background()
	numericTypes := []memory.DataType{memory.TypeFloat64}
	numericLayouts := []aggregate.Layout{{Sum: true, CountRows: true, CountValues: true, Avg: true}}
	statisticsTypes := []memory.DataType{memory.TypeInt64}
	statisticsLayouts := []aggregate.StatisticsLayout{{First: true, Last: true}}
	s.query.cq.MultiAggregate = &CompiledMultiAggregate{
		Columns: []string{"floating"}, Types: numericTypes, Layouts: numericLayouts, Native: true,
		Statistics: &CompiledStatistics{
			Columns: []string{"ordered"}, Types: statisticsTypes, Layouts: statisticsLayouts, Endpoints: true,
		},
	}
	fixtureBudget := memory.NewBudgetManager(1 << 20)
	reserve := func(n int64) (memory.Reservation, error) { return fixtureBudget.Reserve(ctx, n) }
	numeric, err := aggregate.NewNumericBundle(numericTypes, numericLayouts, reserve)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { numeric.Close() })
	statistics, err := aggregate.NewStatisticsBundle(statisticsTypes, statisticsLayouts, reserve)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { statistics.Close() })
	if err := numeric.EnsureGroups(runs * groups); err != nil {
		t.Fatal(err)
	}
	if err := statistics.EnsureGroups(runs * groups); err != nil {
		t.Fatal(err)
	}

	values := [runs]float64{1e16, -1e16, 1, 0, 0}
	ordinals := [runs]aggregate.SourceOrdinal{
		{Partition: 1, Row: 7}, {Partition: 0, Row: 9}, {Partition: 2, Row: 1},
		{Partition: 0, Row: 2}, {Partition: 1, Row: 3},
	}
	var records [runs][groups]aggregateRecord
	var originals [runs]aggregateRunRef
	for run := range records {
		for group := range records[run] {
			id := run*groups + group
			numeric.AddBits(id, 0, math.Float64bits(values[run]), true)
			if err := statistics.AddBits(id, 0, uint64((run+1)*10+group), group != 0 || run != 3, ordinals[run]); err != nil {
				t.Fatal(err)
			}
			records[run][group] = aggregateRecord{
				groupID: id, numericBundle: numeric, statisticsBundle: statistics,
				ordinal: ordinals[run],
			}
			if group == 1 {
				records[run][group].a, records[run][group].validMask = 7, 1
			}
		}
		originals[run] = compactTestSeed(t, s, 0, records[run][:])
	}
	s.selectedRows, s.checksum, s.groupCount = 101, 103, 107
	for round := 1; round < runs; round++ {
		// Independently fold the original records, never the compacted output,
		// to detect reassociation, duplicate state or missing source ordinals.
		expected, err := newAggregateTableOwner(ctx, s.query, s.state, s.maxState)
		if err != nil {
			t.Fatal(err)
		}
		for run := 0; run <= round; run++ {
			for _, record := range records[run] {
				if _, err := expected.table.merge(record); err != nil {
					expected.close()
					t.Fatal(err)
				}
			}
		}
		want, err := s.query.encodeAggregateRun(ctx, expected.table.visit, nil, spillmeta.GrowthManager(s.encoder))
		expected.close()
		if err != nil {
			t.Fatal(err)
		}
		previousPrefix := s.partitions[0].Values[0]
		if err := s.compactInputRuns(ctx); err != nil {
			want.close()
			t.Fatal(err)
		}
		if len(s.partitions[0].Values) != runs-round {
			want.close()
			t.Fatalf("round %d: run count=%d want=%d", round, len(s.partitions[0].Values), runs-round)
		}
		prefix := s.partitions[0].Values[0]
		if prefix.groups != groups || prefix.handle == previousPrefix.handle || prefix.handle == originals[round].handle {
			want.close()
			t.Fatalf("round %d: replacement is not authoritative", round)
		}
		for suffix := 1; suffix < len(s.partitions[0].Values); suffix++ {
			if s.partitions[0].Values[suffix].handle != originals[round+suffix].handle {
				want.close()
				t.Fatalf("round %d: suffix %d reordered or duplicated", round, suffix)
			}
		}
		restored, err := s.store.Restore(ctx, prefix.handle, s.fingerprint, spillmeta.GrowthManager(s.restorer))
		if err != nil {
			want.close()
			t.Fatal(err)
		}
		if !bytes.Equal(restored.Object.Payload, want.payload) {
			t.Errorf("round %d: prefix differs from original-order raw state", round)
		}
		want.close()
		if err := restored.Close(); err != nil {
			t.Fatal(err)
		}
		for _, consumed := range []aggregateRunRef{previousPrefix, originals[round]} {
			if err := s.restoreRun(ctx, consumed, func(aggregateRecord) error { return nil }); !errors.Is(err, memory.ErrInvalidSpillHandle) {
				t.Fatalf("round %d: consumed run remains: %v", round, err)
			}
		}
		if s.selectedRows != 101 || s.checksum != 103 || s.groupCount != 107 {
			t.Fatalf("round %d: logical counters changed", round)
		}
		if s.state.Used() != 0 || s.encoder.Used() != 0 || s.restorer.Used() != 0 || s.table != nil {
			t.Fatalf("round %d: temporary owner remains", round)
		}
	}
	var seen [groups]bool
	if err := s.restoreRun(ctx, s.partitions[0].Values[0], func(record aggregateRecord) error {
		group := 0
		if record.validMask == 1 && record.a == 7 {
			group = 1
		} else if record.validMask != 0 {
			t.Fatalf("unexpected key: mask=%d key=%d", record.validMask, record.a)
		}
		if seen[group] {
			t.Fatalf("duplicate group %d", group)
		}
		seen[group] = true
		n := numericAggregateRecordState(record, 0, numericTypes[0], numericLayouts[0])
		if !n.SumValid || math.Float64frombits(n.SumBits) != 1 || n.AvgSum != 1 || n.CountRows != runs || n.CountValues != runs {
			t.Fatalf("group %d: floating left fold changed: %+v", group, n)
		}
		e := statisticsAggregateRecordState(record, 0, statisticsTypes[0], statisticsLayouts[0]).Endpoints
		if !e.Seen || e.FirstOrdinal != ordinals[3] || e.LastOrdinal != ordinals[2] || e.FirstValid != (group == 1) || !e.LastValid || e.LastBits != uint64(30+group) {
			t.Fatalf("group %d: source endpoints changed: %+v", group, e)
		}
		if group == 1 && e.FirstBits != 41 {
			t.Fatalf("first endpoint=%d want=41", e.FirstBits)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !seen[0] || !seen[1] {
		t.Fatal("NULL or non-NULL group missing")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, budget, dir)
	numeric.Close()
	statistics.Close()
	if fixtureBudget.Used() != 0 {
		t.Fatalf("fixture retained %d bytes", fixtureBudget.Used())
	}
}

// A successful file admission can permanently grow the live query handle map
// before publication of the operator's replacement refs is rejected. Rolling
// back that file must not erase the still-live map's high-water reservation.
func TestAggregateInputCompactionMapGrowthPublicationFailure(t *testing.T) {
	for _, boundary := range []int{8, 16} {
		t.Run(fmt.Sprintf("handles=%d", boundary), func(t *testing.T) {
			s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
			ctx := context.Background()
			if err := s.store.Close(); err != nil {
				t.Fatal(err)
			}
			base, err := budget.SpillManager()
			if err != nil {
				t.Fatal(err)
			}
			queryStore, ok := base.(*memory.QuerySpillStore)
			if !ok {
				t.Fatalf("query spill store is %T", base)
			}
			// Both operator refs and the Store must capture this manager. Merely
			// changing s.metadata would miss handle/path/map admission calls.
			metadata := &failNthMetadataReview{Manager: s.metadata}
			s.metadata = metadata
			s.store = spillmeta.NewStore(base, metadata)
			var originals [16]aggregateRunRef
			originals[0] = compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 2)})
			originals[1] = compactTestSeed(t, s, 0, []aggregateRecord{compactTestRecord(7, 3)})
			for i := 2; i < boundary; i++ {
				originals[i] = compactTestSeed(t, s, i-1, []aggregateRecord{compactTestRecord(uint64(i), int64(i+10))})
			}
			oldArray := &s.partitions[0].Values[0]
			oldCapacity := cap(s.partitions[0].Values)
			beforeMemory, beforeDisk := budget.Used(), queryStore.Used()
			store := &compactTestStore{SpillManager: s.store}
			s.store = store
			// Scratch, private handle, query handle/path, replacement query map,
			// then replacement operator refs. Reject only the fifth admission.
			metadata.n, metadata.fail = 0, 5
			if err := s.compactInputRuns(ctx); err != nil {
				t.Fatalf("publication pressure was not a safe skip: %v", err)
			}
			if metadata.n != 5 || store.writes != 1 || store.last == nil || store.deletes != 1 {
				t.Fatalf("failure did not follow successful map/file admission: reserves=%d writes=%d handle=%v deletes=%d", metadata.n, store.writes, store.last != nil, store.deletes)
			}
			// QuerySpillStore admits 128+capacity*128 for its map. Growing
			// boundary -> 2*boundary keeps exactly this extra map capacity live
			// after the new file is removed; the old nonempty map cannot shrink.
			wantGrowth := int64(boundary) * 128
			if got := budget.Used() - beforeMemory; got != wantGrowth {
				t.Fatalf("retained map growth=%d want=%d; failure must retain real high-water capacity", got, wantGrowth)
			}
			if queryStore.Used() != beforeDisk {
				t.Fatalf("temporary file retained disk bytes: %d want=%d", queryStore.Used(), beforeDisk)
			}
			if &s.partitions[0].Values[0] != oldArray || cap(s.partitions[0].Values) != oldCapacity {
				t.Fatal("rejected publication replaced the original slice owner")
			}
			compactTestOriginals(t, s, originals[0], originals[1])
			if err := s.restoreRun(ctx, aggregateRunRef{handle: store.last, groups: 1, partition: 0}, func(aggregateRecord) error { return nil }); !errors.Is(err, memory.ErrInvalidSpillHandle) {
				t.Fatalf("unpublished temporary run still exists: %v", err)
			}
			checkSiblings := func() {
				t.Helper()
				for i := 2; i < boundary; i++ {
					if len(s.partitions[i-1].Values) != 1 || s.partitions[i-1].Values[0].handle != originals[i].handle {
						t.Fatalf("sibling partition %d owner changed", i-1)
					}
					seen := 0
					if err := s.restoreRun(ctx, originals[i], func(record aggregateRecord) error {
						if record.a != uint64(i) || record.state.Sum != int64(i+10) || record.state.CountRows != 1 {
							t.Fatalf("sibling %d state changed: %+v", i, record)
						}
						seen++
						return nil
					}); err != nil || seen != 1 {
						t.Fatalf("sibling %d unavailable: seen=%d err=%v", i, seen, err)
					}
				}
			}
			checkSiblings()
			metadata.fail = 0
			if err := s.compactInputRuns(ctx); err != nil || len(s.partitions[0].Values) != 1 {
				t.Fatalf("retry failed: %v", err)
			}
			if err := s.restoreRun(ctx, s.partitions[0].Values[0], func(record aggregateRecord) error {
				if record.a != 7 || record.state.Sum != 5 || record.state.CountRows != 2 {
					t.Fatalf("retry lost or duplicated state: %+v", record)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			checkSiblings()
			if s.state.Used() != 0 || s.encoder.Used() != 0 || s.restorer.Used() != 0 || s.table != nil {
				t.Fatal("retry retained a temporary table or buffer")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if queryStore.Used() != 0 {
				t.Fatalf("Close retained %d disk bytes", queryStore.Used())
			}
			assertAggregateMetadataClean(t, budget, dir)
			if err := budget.Close(); err != nil {
				t.Fatal(err)
			}
			assertAggregateMetadataClean(t, budget, dir)
		})
	}
}
