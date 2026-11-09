package tuplesort

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"gocolumn/memory"
	"sort"
	"testing"
)

type lifetimeStore struct {
	*memory.QuerySpillStore
	live, peak       int
	writes, deletes  int
	failAfterDeletes int
	failDelete       bool
}

var errLifetimeDelete = errors.New("injected segment deletion failure")

func (s *lifetimeStore) SpillWithMetadata(ctx context.Context, obj memory.SpillObject, manager memory.Manager) (memory.SpillHandle, error) {
	h, err := s.QuerySpillStore.SpillWithMetadata(ctx, obj, manager)
	if err == nil {
		s.live++
		s.writes++
		s.peak = max(s.peak, s.live)
	}
	return h, err
}

func (s *lifetimeStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	if s.failDelete || s.failAfterDeletes > 0 && s.deletes >= s.failAfterDeletes {
		return errLifetimeDelete
	}
	if err := s.QuerySpillStore.Delete(ctx, h); err != nil {
		return err
	}
	s.live--
	s.deletes++
	if s.live < 0 {
		panic("segment deleted twice")
	}
	return nil
}

func TestMergeReclaimsInputSegments(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(map[bool]string{false: "deduplicate", true: "keep"}[keep], func(t *testing.T) {
			ctx := context.Background()
			manager := memory.NewBudgetManager(128 << 10)
			raw, err := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			store := &lifetimeStore{QuerySpillStore: raw}
			s, err := New(Config{Manager: manager, Spill: store, BlockBytes: 4096, KeepDuplicates: keep})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var want [][]byte
			// Match the encoded widths and record count of the quantile quota
			// regression, without relying on an engine or pipeline fixture.
			for i := 1535; i >= 0; i-- {
				for _, width := range []int{30, 41} {
					key := make([]byte, width)
					key[0] = byte(width)
					binary.BigEndian.PutUint64(key[1:9], uint64(i))
					want = append(want, key)
					if err = s.Add(ctx, key); err != nil {
						t.Fatal(err)
					}
				}
			}
			sort.Slice(want, func(i, j int) bool { return bytes.Compare(want[i], want[j]) < 0 })
			if err = s.Finish(ctx); err != nil {
				t.Fatal(err)
			}
			t.Logf("peak live segments: %d", store.peak)
			if store.peak > 40 {
				t.Fatalf("merge retained %d simultaneous segments", store.peak)
			}
			before := store.live
			diskBefore := store.Used()
			for i, expected := range want {
				key, ok, err := s.Next(ctx)
				if err != nil || !ok || !bytes.Equal(key, expected) {
					t.Fatalf("row %d: key=%x ok=%v err=%v", i, key, ok, err)
				}
				if i == 0 && (store.live != before-1 || store.Used() >= diskBefore) {
					t.Fatalf("restored segment remains live: handles %d -> %d, disk %d -> %d", before, store.live, diskBefore, store.Used())
				}
			}
			if _, ok, err := s.Next(ctx); err != nil || ok {
				t.Fatal(ok, err)
			}
			if manager.Used() != 0 || store.Used() != 0 || store.live != 0 {
				t.Fatal(manager.Used(), store.Used(), store.live)
			}
		})
	}
}

func TestConsumedSegmentDeleteFailureRetainsOwnership(t *testing.T) {
	for _, duringMerge := range []bool{true, false} {
		t.Run(map[bool]string{true: "merge", false: "drain"}[duringMerge], func(t *testing.T) {
			ctx := context.Background()
			manager := memory.NewBudgetManager(1 << 20)
			raw, err := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			store := &lifetimeStore{QuerySpillStore: raw, failDelete: duringMerge}
			s, err := New(Config{Manager: manager, Spill: store, BlockBytes: 32})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for i := 0; i < 20; i++ {
				err = s.Add(ctx, []byte{byte(i)})
				if err != nil {
					break
				}
			}
			if !duringMerge {
				if err != nil {
					t.Fatal(err)
				}
				if err = s.Finish(ctx); err != nil {
					t.Fatal(err)
				}
				store.failDelete = true
				_, _, err = s.Next(ctx)
			}
			if !errors.Is(err, errLifetimeDelete) {
				t.Fatal(err)
			}
			// Read failures are terminal. Only cleanup may be retried.
			if _, _, err = s.Next(ctx); !errors.Is(err, errLifetimeDelete) {
				t.Fatal("read resumed after deletion failure", err)
			}
			if store.live == 0 || store.Used() == 0 || manager.Used() == 0 {
				t.Fatal("failed deletion lost ownership/accounting")
			}
			store.failDelete = false
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 || store.Used() != 0 || store.live != 0 {
				t.Fatal(manager.Used(), store.Used(), store.live)
			}
		})
	}
}

func TestPartialMergeDeleteFailureRetainsInputAndOutput(t *testing.T) {
	ctx := context.Background()
	manager := memory.NewBudgetManager(128 << 10)
	raw, err := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	store := &lifetimeStore{QuerySpillStore: raw}
	s, err := New(Config{Manager: manager, Spill: store, BlockBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.init(ctx); err != nil {
		t.Fatal(err)
	}
	// Each input segment holds one nine-byte key. Interleaving the two runs
	// makes the merge write output before the injected later deletion fails.
	makeRun := func(parity byte) *run {
		r := &run{}
		for i := byte(0); i < 8; i++ {
			lease, err := manager.Reserve(ctx, header+8+9)
			if err != nil {
				t.Fatal(err)
			}
			payload := make([]byte, header+8+9)
			binary.LittleEndian.PutUint64(payload[header:header+8], 9)
			payload[header+8] = i*2 + parity
			err = s.write(ctx, r, payload, 1)
			lease.Release()
			if err != nil {
				t.Fatal(err)
			}
		}
		return r
	}
	a, b := makeRun(0), makeRun(1)
	initialWrites := store.writes
	store.failAfterDeletes = 4
	_, err = s.merge(ctx, a, b)
	if !errors.Is(err, errLifetimeDelete) {
		t.Fatal(err)
	}
	if store.deletes != 4 || store.writes <= initialWrites {
		t.Fatalf("failure did not follow partial input consumption and output: deletes=%d writes=%d initial=%d", store.deletes, store.writes, initialWrites)
	}
	owned := 0
	for x := s.owned; x != nil; x = x.ownerNext {
		owned++
		if x.handle == nil || x.mem == nil || x.mem.Bytes() == 0 {
			t.Fatal("live segment lost metadata accounting")
		}
	}
	if owned != store.live || owned == 0 || manager.Used() == 0 || store.Used() == 0 {
		t.Fatal(owned, store.live, manager.Used(), store.Used())
	}
	if err = s.Close(); !errors.Is(err, errLifetimeDelete) {
		t.Fatal(err)
	}
	if store.live != owned || manager.Used() == 0 || store.Used() == 0 {
		t.Fatal("failed Close released live files or accounting")
	}
	store.failAfterDeletes = 0
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 || store.Used() != 0 || store.live != 0 {
		t.Fatal(manager.Used(), store.Used(), store.live)
	}
}

func TestLastSegmentPayloadRemainsAdmitted(t *testing.T) {
	for _, earlyClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "eof", true: "close"}[earlyClose], func(t *testing.T) {
			ctx := context.Background()
			manager := memory.NewBudgetManager(128 << 10)
			raw, err := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			store := &lifetimeStore{QuerySpillStore: raw}
			s, err := New(Config{Manager: manager, Spill: store, BlockBytes: 32})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			want := bytes.Repeat([]byte{0x73}, 40)
			if err = s.Add(ctx, want); err != nil {
				t.Fatal(err)
			}
			if err = s.Finish(ctx); err != nil {
				t.Fatal(err)
			}
			got, ok, err := s.Next(ctx)
			if err != nil || !ok || !bytes.Equal(got, want) {
				t.Fatal(got, ok, err)
			}
			if store.live != 0 || store.Used() != 0 {
				t.Fatal("last restored file was not deleted")
			}
			if s.cursor.restored == nil {
				t.Fatal("borrowed payload lost its reservation")
			}
			payloadCharge := int64(header + 8 + len(want) + len(codec))
			if got := manager.Used(); got != s.base.Bytes()+payloadCharge {
				t.Fatalf("restored payload charge=%d, want base %d + payload %d", got, s.base.Bytes(), payloadCharge)
			}
			if earlyClose {
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, ok, err = s.Next(ctx); err != nil || ok {
					t.Fatal(ok, err)
				}
			}
			if manager.Used() != 0 || store.Used() != 0 {
				t.Fatal(manager.Used(), store.Used())
			}
		})
	}
}
