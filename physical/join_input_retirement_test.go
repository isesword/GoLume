package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/hashagg"
	"gocolumn/internal/spillmeta"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type joinRetiredInput struct {
	ref      joinRunRef
	left     bool
	schema   memory.Schema
	restored *memory.RestoredSpill
	charge   int64
	restores int
	deletes  int
	attempts int
}

// Observe the real store and operator budgets without imposing pressure or
// replacing the decoder. RestoredSpill is captured only to verify that its
// temporary payload is closed; the live P+M charge must belong to the decoded
// batch that the join still needs after deleting its input file.
type joinRetirementStore struct {
	memory.SpillManager
	t            *testing.T
	stream       *spillableJoinStream
	query        *memory.QueryBudget
	directory    string
	inputs       map[string]*joinRetiredInput
	lastProbe    map[int]*joinRetiredInput
	rightArrays  map[int]*joinArrayTracker
	outputWrites int
	armed        bool
	failID       string
	failErr      error
	failed       bool
	failedHandle memory.SpillHandle
}

func (s *joinRetirementStore) fileCount() int {
	s.t.Helper()
	count := 0
	if err := filepath.WalkDir(s.directory, func(_ string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			count++
		}
		return err
	}); err != nil {
		s.t.Fatal(err)
	}
	return count
}

func (s *joinRetirementStore) buildCharge(partition int, requireDeleted bool) int64 {
	s.t.Helper()
	if tracker := s.rightArrays[partition]; requireDeleted && tracker != nil && tracker.used != 0 {
		s.t.Fatalf("probe/output retained %d bytes of retired build reference backing", tracker.used)
	}
	var charge int64
	rows := 0
	for id, input := range s.inputs {
		if input.left || input.ref.partition != partition {
			continue
		}
		if input.restores != 1 || input.charge == 0 {
			s.t.Fatalf("build input %s was not fully restored before retirement: restores=%d charge=%d", id, input.restores, input.charge)
		}
		if input.restored.Object.Payload != nil {
			s.t.Fatalf("build input %s still owns a temporary restore payload", id)
		}
		if requireDeleted && input.deletes != 1 {
			s.t.Fatalf("probe/output began before build input %s was deleted", id)
		}
		charge += input.charge
		rows += input.ref.rows
	}
	if s.stream.maxBuildRows < rows || s.stream.state.Used() == 0 || s.stream.maxDepth != 1 {
		s.t.Fatalf("not a successfully loaded build: rows=%d maxBuild=%d state=%d depth=%d", rows, s.stream.maxBuildRows, s.stream.state.Used(), s.stream.maxDepth)
	}
	return charge
}

func (s *joinRetirementStore) checkDecodedCharge(want int64) {
	s.t.Helper()
	if got := s.stream.restorer.Used(); got != want {
		s.t.Fatalf("live decoded P+M charge=%d, want %d", got, want)
	}
}

func (s *joinRetirementStore) Restore(ctx context.Context, handle memory.SpillHandle, fingerprint [32]byte, manager memory.Manager) (*memory.RestoredSpill, error) {
	input := s.inputs[handle.ID()]
	if input != nil {
		if input.restores != 0 || s.failed {
			s.t.Fatalf("input %s restored again or after failure", handle.ID())
		}
		if input.left {
			// The previous probe must have closed, while every retired build
			// batch remains charged and available for this next probe.
			s.checkDecodedCharge(s.buildCharge(input.ref.partition, true))
			s.lastProbe[input.ref.partition] = input
		}
	}
	restored, err := s.SpillManager.Restore(ctx, handle, fingerprint, manager)
	if err != nil {
		return nil, err
	}
	if input != nil {
		metadata, err := memory.ColumnarRunDecodeMetadataBytes(input.schema)
		if err != nil {
			s.t.Fatal(err)
		}
		if metadata <= 0 || len(restored.Object.Payload) == 0 {
			s.t.Fatal("fixture must charge both payload and decoder metadata")
		}
		input.restored = restored
		input.charge = int64(len(restored.Object.Payload)) + metadata
		input.restores++
	}
	return restored, nil
}

func (s *joinRetirementStore) SpillWithMetadata(ctx context.Context, object memory.SpillObject, manager memory.Manager) (memory.SpillHandle, error) {
	if s.armed {
		if s.failed {
			s.t.Fatal("output was retried after input retirement failed")
		}
		partition := int(object.Partition)
		want := s.buildCharge(partition, true)
		// Inspect this output with a separate test-owned decoder. Unmatched
		// right rows are emitted after all probe decoders have closed; every
		// other output requires the currently retired probe to remain alive.
		metadata, err := memory.ColumnarRunDecodeMetadataBytes(s.stream.owner.schema)
		if err != nil {
			s.t.Fatal(err)
		}
		inspection := memory.NewBudgetManager(int64(len(object.Payload)) + metadata)
		decoded, err := memory.DecodeColumnarRun(ctx, s.stream.owner.schema, &memory.RestoredSpill{Object: object}, inspection)
		if err != nil {
			s.t.Fatal(err)
		}
		probeOutput := false
		for row := 0; row < decoded.Batch.Rows; row++ {
			if decoded.Batch.Int64s["row"].IsValid(row) {
				probeOutput = true
			}
		}
		_ = decoded.Close()
		if inspection.Used() != 0 {
			s.t.Fatal("test inspection retained a decoded reservation")
		}
		if probeOutput {
			probe := s.lastProbe[partition]
			if probe == nil || probe.deletes != 1 || probe.restores != 1 {
				s.t.Fatal("probe output was written before its source input was retired")
			}
			want += probe.charge
		}
		s.checkDecodedCharge(want)
		s.outputWrites++
	}
	return s.SpillManager.(memory.MetadataSpillManager).SpillWithMetadata(ctx, object, manager)
}

func (s *joinRetirementStore) Delete(ctx context.Context, handle memory.SpillHandle) error {
	input := s.inputs[handle.ID()]
	if input != nil {
		input.attempts++
		if input.deletes != 0 {
			s.t.Fatalf("successfully retired input %s deleted twice", handle.ID())
		}
		if !s.failed {
			want := s.buildCharge(input.ref.partition, input.left)
			if input.left {
				if input.restores != 1 || input.restored.Object.Payload != nil {
					s.t.Fatal("probe retirement preceded a complete decode")
				}
				want += input.charge
			} else if s.lastProbe[input.ref.partition] != nil {
				s.t.Fatal("build retirement was deferred until after probing")
			}
			s.checkDecodedCharge(want)
		}
	}
	before, decodedBefore, filesBefore := s.query.Used(), s.stream.restorer.Used(), s.fileCount()
	if handle.ID() == s.failID && !s.failed {
		s.failed, s.failedHandle = true, handle
		if s.query.Used() != before || s.fileCount() != filesBefore {
			s.t.Fatal("failed deletion released file ownership or metadata")
		}
		return s.failErr
	}
	if err := s.SpillManager.Delete(ctx, handle); err != nil {
		return err
	}
	if input != nil {
		input.deletes++
		if s.query.Used() >= before || s.fileCount() != filesBefore-1 {
			s.t.Fatal("successful input deletion did not release its file and metadata")
		}
		if s.stream.restorer.Used() != decodedBefore {
			s.t.Fatal("deleting input metadata also released live decoded buffers")
		}
	}
	return nil
}

func installJoinRetirementStore(t *testing.T, stream *spillableJoinStream, query *memory.QueryBudget, directory string) *joinRetirementStore {
	t.Helper()
	if !stream.owner.spillable || stream.owner.boundedBuild || stream.owner.Strategy() != "partitioned-hash-spill" {
		t.Fatalf("fixture requires an ordinary spillable owner, got %s", stream.owner.Strategy())
	}
	base, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.store.Close(); err != nil {
		t.Fatal(err)
	}
	store := &joinRetirementStore{SpillManager: base, t: t, stream: stream, query: query, directory: directory, inputs: make(map[string]*joinRetiredInput), lastProbe: make(map[int]*joinRetiredInput), rightArrays: make(map[int]*joinArrayTracker)}
	stream.store = spillmeta.NewStore(store, stream.metadata)
	return store
}

func (s *joinRetirementStore) arm() {
	for partition := range s.stream.rightRuns {
		// Move the prepared build array to a forwarding reservation tracker.
		// Preserve its full capacity and exact charge, including unused slots.
		// This distinguishes early Release from the processRefs defer.
		original := &s.stream.rightRuns[partition]
		if len(original.Values) != 0 {
			tracker := &joinArrayTracker{Manager: s.stream.metadata}
			var tracked spillmeta.Slice[joinRunRef]
			if err := tracked.Append(context.Background(), tracker, original.Values[:cap(original.Values)]...); err != nil {
				s.t.Fatal(err)
			}
			tracked.Values = tracked.Values[:len(original.Values)]
			original.Release()
			*original = tracked
			s.rightArrays[partition] = tracker
		}
		for _, ref := range s.stream.rightRuns[partition].Values {
			s.inputs[ref.handle.ID()] = &joinRetiredInput{ref: ref, schema: s.stream.rightSchema}
		}
		for _, ref := range s.stream.leftRuns[partition].Values {
			s.inputs[ref.handle.ID()] = &joinRetiredInput{ref: ref, left: true, schema: s.stream.leftSchema}
		}
	}
	s.armed = true
}

// These fixtures have no retained generic dictionaries or broadcast build.
// Their only prepared-source reservation is reserveBytePartitions(ctx, 1):
// 32 bytes in GenericJoinBuild that outlive the stream until owner.Close.
func joinRetirementOwnerCharge(t *testing.T, owner *JoinSource) int64 {
	t.Helper()
	if owner.shape != joinBytes {
		if len(owner.reservations) != 0 || owner.keyManager.Used() != 0 {
			t.Fatal("fixed/dictionary fixture unexpectedly retains a prepared build")
		}
		return 0
	}
	if owner.closed || len(owner.parts) != 1 || owner.parts[0].ID != 0 || len(owner.leftParts) != 0 || len(owner.reservations) != 1 {
		t.Fatal("generic fixture must retain only its single streaming partition plan")
	}
	const partitionPlanBytes = int64(32)
	if owner.reservations[0].Bytes() != partitionPlanBytes || owner.keyManager.Used() != partitionPlanBytes {
		t.Fatalf("generic owner partition reservation=%d operator=%d, want %d", owner.reservations[0].Bytes(), owner.keyManager.Used(), partitionPlanBytes)
	}
	return partitionPlanBytes
}

// Check stream cleanup before owner.Close can hide any stream leak. The exact
// prepared-source charge was checked before processing, and must be the only
// remaining reservation in the whole query, not merely an allowed tolerance.
func (s *joinRetirementStore) assertReleased() {
	s.t.Helper()
	ownerBytes := joinRetirementOwnerCharge(s.t, s.stream.owner)
	snapshot := s.query.Snapshot()
	if snapshot.UsedBytes != ownerBytes || snapshot.PeakBytes > s.query.Limit() {
		s.t.Fatalf("stream cleanup: owner=%d query=%+v", ownerBytes, snapshot)
	}
	for name, used := range snapshot.Operators {
		var want int64
		if name == "GenericJoinBuild" {
			want = ownerBytes
		}
		if used != want {
			s.t.Fatalf("stream cleanup operator %s=%d, want %d", name, used, want)
		}
	}
	for partition := range s.stream.leftRuns {
		if s.stream.leftRuns[partition].Values != nil || s.stream.rightRuns[partition].Values != nil {
			s.t.Fatalf("partition %d retained input reference backing", partition)
		}
		if tracker := s.rightArrays[partition]; tracker != nil && tracker.used != 0 {
			s.t.Fatalf("partition %d retained %d build reference bytes", partition, tracker.used)
		}
	}
	if s.stream.outputRuns.Values != nil || s.stream.decoded != nil || s.stream.byteOutput != nil || s.stream.dictionaryReservation != nil || s.stream.baseReservation != nil || s.stream.pairReservation != nil {
		s.t.Fatal("stream cleanup retained output, decoded, or reservation ownership")
	}
	if files := s.fileCount(); files != 0 {
		s.t.Fatalf("stream cleanup retained %d spill files", files)
	}
}

func joinRetirementSource(t *testing.T, rows []skewBudgetRow, representation string, left bool) (source.Source, string) {
	t.Helper()
	if representation == "bytes" {
		generic := make([]genericJoinRow, len(rows))
		for i, row := range rows {
			generic[i] = genericJoinRow{s: fmt.Sprint(row.key), x: fmt.Sprintf("payload-%d", i)}
			if row.null {
				generic[i].null = 1
			}
		}
		src := genericJoinSource(t, generic, false, 1)
		if !left {
			return skewBudgetUnknownStatsSource{src}, "s"
		}
		return src, "s"
	}
	dictionaryKeys := 0
	if representation == "dictionary" {
		dictionaryKeys = 32
	}
	src := skewBudgetSource(t, rows, 1, dictionaryKeys, left)
	if !left {
		return skewBudgetUnknownStatsSource{src}, "id"
	}
	return src, "id"
}

func openJoinRetirement(t *testing.T, left, right []skewBudgetRow, representation string, typ join.Type) (context.Context, *JoinSource, *spillableJoinStream, *memory.QueryBudget, string, *joinRetirementStore) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), strings.Repeat("retired-input-long-directory-", 4))
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	lsrc, key := joinRetirementSource(t, left, representation, true)
	rsrc, _ := joinRetirementSource(t, right, representation, false)
	ctx, owner, stream, query, directory := openByteSpill(skewBudgetTempDir{TB: t, directory: directory}, lsrc, rsrc, typ, []JoinKey{{key, key}}, 160<<10, 64<<20)
	joinRetirementOwnerCharge(t, owner)
	store := installJoinRetirementStore(t, stream, query, directory)
	if err := stream.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	store.arm()
	return ctx, owner, stream, query, directory, store
}

func TestJoinInputRetirementKeepsDecodedAllModes(t *testing.T) {
	left := []skewBudgetRow{{key: 7}, {key: 7}, {key: 8}, {key: 7, null: true}}
	right := []skewBudgetRow{{key: 7}, {key: 7}, {key: 7}, {key: 9}, {key: 7, null: true}}
	for _, representation := range []string{"numeric", "dictionary", "bytes"} {
		for _, typ := range []join.Type{join.Inner, join.Left, join.Right, join.Full, join.Semi, join.Anti} {
			t.Run(representation+"/"+typ.String(), func(t *testing.T) {
				ctx, owner, stream, query, directory, store := openJoinRetirement(t, left, right, representation, typ)
				got, err := drainByteSpill(ctx, stream)
				if err != nil {
					t.Fatal(err)
				}
				want := skewBudgetExpected(left, right, typ)
				sortBytePairs(got)
				sortBytePairs(want)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("retired-input pair oracle: got=%v want=%v", got, want)
				}
				for id, input := range store.inputs {
					if input.restores != 1 || input.deletes != 1 || input.attempts != 1 {
						t.Fatalf("input %s was not consumed exactly once: %+v", id, input)
					}
				}
				if stream.maxDepth != 1 || stream.maxBuildRows < 3 || stream.maxMultiplicity != 3 || store.outputWrites == 0 {
					t.Fatalf("fixture missed loaded-build output: depth=%d build=%d multiplicity=%d writes=%d", stream.maxDepth, stream.maxBuildRows, stream.maxMultiplicity, store.outputWrites)
				}
				store.assertReleased()
				if ok, err := stream.Next(ctx, memory.NewBatch()); ok || err != nil {
					t.Fatalf("repeated EOF: ok=%t err=%v", ok, err)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				store.assertReleased()
				assertByteSpillClean(t, owner, query, directory)
			})
		}
	}
}

func TestJoinInputRetirementPartialDeleteFailure(t *testing.T) {
	rows := []skewBudgetRow{{key: 7}, {key: 7}, {key: 7}}
	for _, representation := range []string{"numeric", "dictionary", "bytes"} {
		for _, side := range []string{"build", "probe"} {
			for _, end := range []string{"next", "close"} {
				t.Run(representation+"/"+side+"/"+end, func(t *testing.T) {
					ctx, owner, stream, query, directory, store := openJoinRetirement(t, rows, rows, representation, join.Full)
					stateBefore := stream.state.Used()
					var dictionaryBytes int64
					if stream.owner.shape == joinDictionary {
						dictionaryBytes = int64(stream.owner.leftDict.Len()+stream.owner.rightDict.Len())*40 + int64(len(stream.owner.leftDict.Data)+len(stream.owner.rightDict.Data))
						if stream.dictionaryReservation == nil || stream.dictionaryReservation.Bytes() != dictionaryBytes {
							t.Fatal("fixture dictionary canonicalization reservation does not match its data")
						}
					}
					if stateBefore != dictionaryBytes {
						t.Fatalf("initial JoinBuild state=%d, want only dictionary canonicalization=%d", stateBefore, dictionaryBytes)
					}
					partition := -1
					for i := range stream.rightRuns {
						if len(stream.rightRuns[i].Values) == len(rows) {
							partition = i
						}
					}
					if partition < 0 || len(stream.leftRuns[partition].Values) != len(rows) {
						t.Fatal("fixture must preserve three input runs on each side")
					}
					refs := stream.rightRuns[partition].Values
					if side == "probe" {
						refs = stream.leftRuns[partition].Values
					}
					firstID := refs[0].handle.ID()
					store.failID = refs[1].handle.ID()
					store.failErr = errors.New("injected second " + side + " input retirement failure")
					var err error
					if end == "next" {
						dst := memory.NewBatch()
						var ok bool
						ok, err = stream.Next(ctx, dst)
						if ok || dst.Rows != 0 {
							t.Fatal("retirement failure returned partial output")
						}
					} else {
						err = stream.processRefs(ctx, &stream.leftRuns[partition], &stream.rightRuns[partition], 1, partition)
					}
					if !errors.Is(err, store.failErr) || !store.failed || store.inputs[firstID].deletes != 1 {
						t.Fatalf("did not fail after a successful retirement: err=%v first=%+v", err, store.inputs[firstID])
					}
					// Direct processing leaves stream-level canonical dictionaries
					// alive. Next aborts the stream and must release those as well.
					var wantState int64
					if end == "close" {
						wantState = stateBefore
					}
					if stream.restorer.Used() != 0 || stream.state.Used() != wantState {
						t.Fatalf("failed processing retained build/probe: restore=%d state=%d wantState=%d", stream.restorer.Used(), stream.state.Used(), wantState)
					}
					if stream.leftRuns[partition].Values != nil || stream.rightRuns[partition].Values != nil {
						t.Fatal("failed processing retained caller input reference owners")
					}
					if side == "probe" && (stream.outputCount == 0 || store.outputWrites == 0) {
						t.Fatal("probe deletion failed before exercising partial output cleanup")
					}
					if side == "build" && (stream.outputCount != 0 || store.outputWrites != 0) {
						t.Fatal("build deletion failure must precede probing and output")
					}
					if end == "next" {
						writes, output := store.outputWrites, stream.outputCount
						for retry := 0; retry < 2; retry++ {
							if ok, retryErr := stream.Next(context.Background(), memory.NewBatch()); ok || retryErr != err {
								t.Fatalf("terminal failure changed: first=%v retry=%v", err, retryErr)
							}
						}
						if store.outputWrites != writes || stream.outputCount != output {
							t.Fatal("terminal Next retried partial output")
						}
						store.assertReleased()
					} else {
						// processRefs relinquishes its arrays on error, but failed
						// files remain in Store's private ownership until Close.
						failed := store.inputs[store.failID]
						if failed.attempts != 1 || failed.deletes != 0 || query.Used() == 0 || store.fileCount() == 0 {
							t.Fatal("failed input was lost before its owner could clean up")
						}
						before := query.Used()
						restored, restoreErr := store.SpillManager.Restore(ctx, store.failedHandle, memory.SchemaFingerprint(failed.schema), stream.restorer)
						if restoreErr != nil {
							t.Fatalf("failed deletion lost the readable source file: %v", restoreErr)
						}
						_ = restored.Close()
						if query.Used() != before {
							t.Fatal("checking the still-owned failed file leaked a reservation")
						}
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					for id, input := range store.inputs {
						wantAttempts := 1
						if id == store.failID {
							wantAttempts++
						}
						if input.deletes != 1 || input.attempts != wantAttempts || input.restores > 1 {
							t.Fatalf("cleanup/retry ownership for %s: %+v", id, input)
						}
					}
					store.assertReleased()
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					assertByteSpillClean(t, owner, query, directory)
				})
			}
		}
	}
}

func TestJoinInputRetirementFailureAfterBorrowedOutput(t *testing.T) {
	// Ensure one partition has already delivered output before retirement of
	// a later partition fails. Do not depend on a particular hash assignment.
	first, later := int32(1), int32(2)
	partition := func(key int32) uint64 {
		return hashagg.Hash128(hashagg.Key128{A: uint64(key)}) & (joinRadixPartitions - 1)
	}
	for partition(first) == partition(later) {
		later++
	}
	if partition(first) > partition(later) {
		first, later = later, first
	}
	rows := []skewBudgetRow{{key: first}, {key: later}, {key: later}, {key: later}}
	ctx, owner, stream, query, directory, store := openJoinRetirement(t, rows, rows, "numeric", join.Inner)
	store.failID = stream.leftRuns[int(partition(later))].Values[1].handle.ID()
	store.failErr = errors.New("injected retirement failure after borrowed output")
	borrowed, dst := memory.NewBatch(), memory.NewBatch()
	if ok, err := stream.Next(ctx, borrowed); !ok || err != nil || borrowed.Rows != 1 {
		t.Fatalf("first partition: ok=%t rows=%d err=%v", ok, borrowed.Rows, err)
	}
	if borrowed.Int64s["row"].Values[0] != 0 || borrowed.Int64s["row_right"].Values[0] != 0 {
		t.Fatal("first partition returned the wrong row identities")
	}
	before := stream.outputCount
	ok, err := stream.Next(ctx, dst)
	if ok || !errors.Is(err, store.failErr) || borrowed.Rows != 0 || dst.Rows != 0 {
		t.Fatalf("late failure retained borrowed/partial output: ok=%t borrowed=%d dst=%d err=%v", ok, borrowed.Rows, dst.Rows, err)
	}
	if stream.outputCount <= before || stream.maxBuildRows != 3 || stream.maxDepth != 1 {
		t.Fatal("late failure did not follow loaded-build partial output")
	}
	writes, output := store.outputWrites, stream.outputCount
	if ok, retry := stream.Next(context.Background(), dst); ok || retry != err || store.outputWrites != writes || stream.outputCount != output {
		t.Fatalf("late failure retried or changed: first=%v retry=%v", err, retry)
	}
	store.assertReleased()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	assertByteSpillClean(t, owner, query, directory)
}
