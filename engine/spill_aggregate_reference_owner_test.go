package engine

import (
	"context"
	"errors"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"testing"
)

type aggregateReferenceLease struct {
	memory.Reservation
	released  bool
	onRelease func()
}

func (r *aggregateReferenceLease) Release() {
	if r.released {
		return
	}
	if r.onRelease != nil {
		r.onRelease()
	}
	r.released = true
	r.Reservation.Release()
}

type aggregateReferenceManager struct {
	memory.Manager
	leases []*aggregateReferenceLease
}

func (m *aggregateReferenceManager) Reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	r, err := m.Manager.Reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	lease := &aggregateReferenceLease{Reservation: r}
	m.leases = append(m.leases, lease)
	return lease, nil
}

type aggregateReferenceStore struct {
	memory.SpillManager
	beforeSpill   func() error
	beforeRestore func(memory.SpillHandle) error
	beforeDelete  func(memory.SpillHandle) error
	afterDelete   func(memory.SpillHandle, error)
}

func (s *aggregateReferenceStore) Spill(ctx context.Context, object memory.SpillObject) (memory.SpillHandle, error) {
	if s.beforeSpill != nil {
		if err := s.beforeSpill(); err != nil {
			return nil, err
		}
	}
	return s.SpillManager.Spill(ctx, object)
}

func (s *aggregateReferenceStore) Restore(ctx context.Context, h memory.SpillHandle, fingerprint [32]byte, manager memory.Manager) (*memory.RestoredSpill, error) {
	if s.beforeRestore != nil {
		if err := s.beforeRestore(h); err != nil {
			return nil, err
		}
	}
	return s.SpillManager.Restore(ctx, h, fingerprint, manager)
}

func (s *aggregateReferenceStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	if s.beforeDelete != nil {
		if err := s.beforeDelete(h); err != nil {
			return err
		}
	}
	err := s.SpillManager.Delete(ctx, h)
	if s.afterDelete != nil {
		s.afterDelete(h, err)
	}
	return err
}

// addAggregateReferenceRun creates a real encoded run and exactly one admitted
// array owner. It returns only diagnostics, never a copied owner or slice view.
func addAggregateReferenceRun(t *testing.T, s *spillableAggregateStream, refs *spillmeta.Slice[aggregateRunRef], keys []uint64) (string, *aggregateReferenceLease) {
	t.Helper()
	ctx := memory.WithQueryBudget(context.Background(), s.budget)
	owner, err := newAggregateTableOwner(ctx, s.query, s.state, s.maxState)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.close()
	for _, key := range keys {
		if _, err := owner.table.add(aggregateRecord{a: key, validMask: 1}, 1, true); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := s.query.encodeAggregateRun(ctx, owner.table.visit, nil, spillmeta.GrowthManager(s.encoder))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.close()
	h, err := s.store.Spill(ctx, memory.SpillObject{
		Kind: memory.SpillAggregateState, Codec: s.query.aggregateRunCodec(),
		SchemaFingerprint: s.fingerprint, Payload: encoded.payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	manager := &aggregateReferenceManager{Manager: s.metadata}
	if err := refs.Append(ctx, manager, aggregateRunRef{handle: h, groups: encoded.groups}); err != nil {
		t.Fatal(err)
	}
	if len(manager.leases) != 1 {
		t.Fatalf("source needs exactly one array allocation, got %d", len(manager.leases))
	}
	return h.ID(), manager.leases[0]
}

func TestAggregateReferenceOwnerReleasedBeforeOutput(t *testing.T) {
	s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
	ctx := memory.WithQueryBudget(context.Background(), budget)
	refs := &s.partitions[0]
	id, lease := addAggregateReferenceRun(t, s, refs, []uint64{1, 2, 3})
	deleted, writes := false, 0
	lease.onRelease = func() {
		if refs.Values != nil || !deleted {
			t.Errorf("source array released before caller was cleared and file deleted")
		}
	}
	observed := &aggregateReferenceStore{SpillManager: s.store}
	observed.beforeRestore = func(h memory.SpillHandle) error {
		if h.ID() == id && (refs.Values != nil || lease.released) {
			t.Error("restore did not exclusively own the live source array")
		}
		return nil
	}
	observed.afterDelete = func(h memory.SpillHandle, err error) {
		if h.ID() == id && err == nil {
			if lease.released {
				t.Error("source allowance released while its file was live")
			}
			deleted = true
		}
	}
	observed.beforeSpill = func() error {
		writes++
		if refs.Values != nil || !lease.released || !deleted || s.restorer.Used() != 0 {
			t.Errorf("output retained source owner/payload: caller=%v released=%v deleted=%v restored=%d", refs.Values != nil, lease.released, deleted, s.restorer.Used())
		}
		return nil
	}
	s.store = observed
	if err := s.mergePartition(ctx, refs, 1); err != nil {
		t.Fatal(err)
	}
	if writes != 3 || s.groupCount != 3 || len(s.outputRuns) != 3 {
		t.Fatalf("writes=%d groups=%d outputs=%d", writes, s.groupCount, len(s.outputRuns))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, budget, dir)
}

func aggregateReferenceRadixKeys(t *testing.T, s *spillableAggregateStream) []uint64 {
	t.Helper()
	var counts [1 << aggregateRadixBits]int
	keys := make([]uint64, 0, 2*len(counts))
	for key := uint64(0); len(keys) < cap(keys) && key < 1<<20; key++ {
		hash := aggregateRecordHash(s.query.cq.GroupShape, aggregateRecord{a: key, validMask: 1})
		if hash&((1<<aggregateRadixBits)-1) != 0 {
			continue
		}
		child := hash >> aggregateRadixBits & ((1 << aggregateRadixBits) - 1)
		if counts[child] < 2 {
			counts[child]++
			keys = append(keys, key)
		}
	}
	if len(keys) != cap(keys) {
		t.Fatal("could not build recursive radix fixture")
	}
	return keys
}

func TestAggregateReferenceOwnerReleasedBeforeRecursiveRestore(t *testing.T) {
	s, budget, dir := aggregateMetadataStream(t, 4<<20, 1, 1)
	ctx := memory.WithQueryBudget(context.Background(), budget)
	keys := aggregateReferenceRadixKeys(t, s)
	refs := &s.partitions[0]
	id, lease := addAggregateReferenceRun(t, s, refs, keys)
	owner, err := newAggregateTableOwner(ctx, s.query, s.state, s.maxState)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh table fits two groups but cannot grow to fit the parent run.
	s.maxState = owner.retainedBytes()
	owner.close()
	deleted, childRestores := false, 0
	lease.onRelease = func() {
		if refs.Values != nil || !deleted {
			t.Error("recursive parent released before ownership transfer/deletion")
		}
	}
	observed := &aggregateReferenceStore{SpillManager: s.store}
	observed.afterDelete = func(h memory.SpillHandle, err error) {
		if h.ID() == id && err == nil {
			deleted = true
		}
	}
	observed.beforeRestore = func(h memory.SpillHandle) error {
		if refs.Values != nil {
			t.Error("recursive caller retained moved source array")
		}
		if h.ID() != id {
			childRestores++
			if !deleted || !lease.released {
				t.Error("recursive restore admitted with a live ancestor source owner")
			}
		}
		return nil
	}
	s.store = observed
	if err := s.mergePartition(ctx, refs, 1); err != nil {
		t.Fatal(err)
	}
	if childRestores != 1<<aggregateRadixBits || s.maxDepth != 2 || s.groupCount != len(keys) {
		t.Fatalf("child restores=%d depth=%d groups=%d", childRestores, s.maxDepth, s.groupCount)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, budget, dir)
}

func TestAggregateReferenceRepartitionConsumesChildOwner(t *testing.T) {
	s, budget, dir := aggregateMetadataStream(t, 4<<20, 1, 1)
	ctx := memory.WithQueryBudget(context.Background(), budget)
	_, lease := addAggregateReferenceRun(t, s, &s.partitions[0], aggregateReferenceRadixKeys(t, s))
	children, err := s.repartitionRuns(ctx, &s.partitions[0], 1)
	if err != nil {
		t.Fatal(err)
	}
	defer children.release()
	if s.partitions[0].Values != nil || !lease.released {
		t.Fatal("successful repartition retained its parent array")
	}
	for i := range children.runs {
		child := &children.runs[i]
		observed := &aggregateReferenceStore{SpillManager: s.store}
		observed.beforeSpill = func() error {
			if child.Values != nil {
				t.Error("child owner still aliases consumed array during output")
			}
			return nil
		}
		s.store = observed
		if err := s.mergePartition(ctx, child, 2); err != nil {
			t.Fatal(err)
		}
		s.store = observed.SpillManager
		if child.Values != nil {
			t.Fatal("child owner was not cleared")
		}
	}
	children.release()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, budget, dir)
}

func TestAggregateReferenceOwnerDeleteFailureKeepsFileAccounting(t *testing.T) {
	for _, operation := range []string{"merge", "repartition"} {
		t.Run(operation, func(t *testing.T) {
			s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
			ctx := memory.WithQueryBudget(context.Background(), budget)
			base, err := budget.SpillManager()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.store.Close(); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("source delete failed")
			failing := &aggregateCleanupErrorStore{QuerySpillStore: base.(*memory.QuerySpillStore), failure: failure}
			s.store = spillmeta.NewStore(failing, s.metadata)
			refs := &s.partitions[0]
			id, lease := addAggregateReferenceRun(t, s, refs, []uint64{1, 2})
			failing.firstID = id
			beforeMemory, beforeDisk := budget.Used(), failing.Used()
			if operation == "merge" {
				err = s.mergePartition(ctx, refs, 1)
			} else {
				var children *aggregateRunPartitions
				children, err = s.repartitionRuns(ctx, refs, 1)
				if children != nil {
					children.release()
					t.Error("failed repartition returned children")
				}
			}
			if !errors.Is(err, failure) || refs.Values != nil || !lease.released {
				t.Fatalf("failure=%v source=%v released=%v", err, refs.Values != nil, lease.released)
			}
			// Repartition can raise the shared store map's charged high-water
			// capacity. The source file's original charge must all remain.
			if budget.Used() < beforeMemory-lease.Bytes() || failing.Used() != beforeDisk {
				t.Fatalf("failed delete lost file accounting: memory=%d minimum=%d disk=%d want=%d", budget.Used(), beforeMemory-lease.Bytes(), failing.Used(), beforeDisk)
			}
			if len(s.outputRuns) != 0 {
				t.Fatal("failed deletion published output")
			}
			if ok, err := s.Next(ctx, memory.NewBatch()); ok || !errors.Is(err, failure) {
				t.Fatalf("consumed source was retried: ok=%v error=%v", ok, err)
			}
			failing.firstID = ""
			calls := failing.calls
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if failing.calls != calls+1 {
				t.Fatalf("private owner did not retry failed file: deletes=%d want=%d", failing.calls, calls+1)
			}
			assertAggregateMetadataClean(t, budget, dir)
		})
	}
}

func TestAggregateReferenceOwnerFailureAndCancellationCleanup(t *testing.T) {
	for _, mode := range []string{"partial-output", "partial-repartition", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
			ctx := memory.WithQueryBudget(context.Background(), budget)
			refs := &s.partitions[0]
			_, lease := addAggregateReferenceRun(t, s, refs, aggregateReferenceRadixKeys(t, s))
			injected := errors.New("injected write failure")
			writes := 0
			observed := &aggregateReferenceStore{SpillManager: s.store, beforeSpill: func() error {
				writes++
				if writes == 2 {
					return injected
				}
				return nil
			}}
			s.store = observed
			var err error
			if mode == "cancel" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				err = s.mergePartition(cancelled, refs, 1)
				injected = context.Canceled
			} else if mode == "partial-repartition" {
				_, err = s.repartitionRuns(ctx, refs, 1)
			} else {
				err = s.mergePartition(ctx, refs, 1)
			}
			if !errors.Is(err, injected) || refs.Values != nil || !lease.released || len(s.outputRuns) != 0 {
				t.Fatalf("failure=%v caller=%v released=%v outputs=%d", err, refs.Values != nil, lease.released, len(s.outputRuns))
			}
			if mode != "cancel" && writes != 2 {
				t.Fatalf("did not fail after a committed partial write: %d", writes)
			}
			if ok, err := s.Next(ctx, memory.NewBatch()); ok || !errors.Is(err, injected) {
				t.Fatalf("terminal retry: %v %v", ok, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			assertAggregateMetadataClean(t, budget, dir)
		})
	}
}

func TestAggregateRestoreRunWithManagerLifetime(t *testing.T) {
	s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
	ctx := memory.WithQueryBudget(context.Background(), budget)
	_, _ = addAggregateReferenceRun(t, s, &s.partitions[0], []uint64{1, 2})
	manager := &aggregateReferenceManager{Manager: spillmeta.GrowthManager(s.restorer)}
	injected := errors.New("stop visiting restored state")
	visited := 0
	err := s.restoreRunWithManager(ctx, s.partitions[0].Values[0], manager, func(aggregateRecord) error {
		visited++
		if len(manager.leases) != 1 || manager.leases[0].released || s.restorer.Used() == 0 {
			t.Error("visitor did not borrow a live payload from the supplied manager")
		}
		return injected
	})
	if !errors.Is(err, injected) || visited != 1 || len(manager.leases) != 1 || !manager.leases[0].released || s.restorer.Used() != 0 {
		t.Fatalf("restore visitor error=%v visits=%d leases=%d restored=%d", err, visited, len(manager.leases), s.restorer.Used())
	}
	invalid := s.partitions[0].Values[0]
	invalid.groups++
	err = s.restoreRunWithManager(ctx, invalid, manager, func(aggregateRecord) error {
		t.Error("group-count mismatch reached visitor")
		return nil
	})
	invalid = aggregateRunRef{}
	if !errors.Is(err, memory.ErrSpillCorrupt) || len(manager.leases) != 2 || !manager.leases[1].released || s.restorer.Used() != 0 {
		t.Fatalf("invalid restore error=%v leases=%d restored=%d", err, len(manager.leases), s.restorer.Used())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, budget, dir)
}
