package aggregate

import (
	"bytes"
	"context"
	"errors"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestBytesEndpointsOwnershipOrderingAndNull(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeString, memory.TypeBinary} {
		budget := memory.NewBudgetManager(1024)
		s, err := NewBytesEndpointState(typ, func(n int64) (memory.Reservation, error) { return budget.Reserve(context.Background(), n) })
		if err != nil {
			t.Fatal(err)
		}
		input := []byte{0, 255, 'a'}
		if err = s.AddBytes(input, true, SourceOrdinal{2, 8}); err != nil {
			t.Fatal(err)
		}
		input[0] = 5
		r := s.Snapshot()
		if !bytes.Equal(r.First, []byte{0, 255, 'a'}) || !bytes.Equal(r.Last, r.First) || budget.Used() != 6 {
			t.Fatal(r, budget.Used())
		}
		if err = s.AddBytes([]byte("older"), true, SourceOrdinal{1, 99}); err != nil {
			t.Fatal(err)
		}
		if err = s.AddBytes(nil, false, SourceOrdinal{3, 0}); err != nil {
			t.Fatal(err)
		}
		r = s.Snapshot()
		if string(r.First) != "older" || r.LastValid || r.LastOrdinal != (SourceOrdinal{3, 0}) || budget.Used() != 5 {
			t.Fatal(r, budget.Used())
		}
		before := r
		if err = s.AddBytes([]byte("different"), true, r.FirstOrdinal); err == nil || !reflect.DeepEqual(before, s.Snapshot()) {
			t.Fatal("conflict was not atomic")
		}
		s.Reset()
		if budget.Used() != 0 || s.Snapshot().Seen {
			t.Fatal("reset leak")
		}
		if err = s.AddBytes(nil, true, SourceOrdinal{}); err != nil {
			t.Fatal(err)
		}
		r = s.Snapshot()
		if !r.FirstValid || !r.LastValid || budget.Used() != 0 {
			t.Fatal("empty bytes confused with null")
		}
		s.Close()
		s.Close()
		if err = s.AddBytes(nil, true, SourceOrdinal{}); err == nil {
			t.Fatal("closed accepted update")
		}
	}
}

func TestBytesEndpointsTransactionalAdmission(t *testing.T) {
	budget := memory.NewBudgetManager(1024)
	calls := 0
	failAt := 0
	sentinel := errors.New("admission rejected")
	reserve := func(n int64) (memory.Reservation, error) {
		calls++
		if calls == failAt {
			return nil, sentinel
		}
		return budget.Reserve(context.Background(), n)
	}
	s, _ := NewBytesEndpointState(memory.TypeBinary, reserve)
	if err := s.AddBytes([]byte("old"), true, SourceOrdinal{1, 5}); err != nil {
		t.Fatal(err)
	}
	before := s.Snapshot()
	failAt = calls + 2
	incoming := BytesEndpointResult{Type: memory.TypeBinary, Seen: true, FirstOrdinal: SourceOrdinal{0, 0}, LastOrdinal: SourceOrdinal{2, 0}, First: []byte("new-first"), Last: []byte("new-last"), FirstValid: true, LastValid: true}
	if err := s.Merge(incoming); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if budget.Used() != 6 || !reflect.DeepEqual(before, s.Snapshot()) {
		t.Fatal("second admission leaked or mutated")
	}
	failAt = 0
	if err := s.Merge(incoming); err != nil {
		t.Fatal(err)
	}
	incoming.First[0] = 'X'
	incoming.Last[0] = 'X'
	r := s.Snapshot()
	if string(r.First) != "new-first" || string(r.Last) != "new-last" || budget.Used() != 17 {
		t.Fatal(r, budget.Used())
	}
	s.Close()
	if budget.Used() != 0 {
		t.Fatal("close leaked")
	}
}

func TestBytesEndpointsReplacementOverlap(t *testing.T) {
	budget := memory.NewBudgetManager(9)
	s, _ := NewBytesEndpointState(memory.TypeString, func(n int64) (memory.Reservation, error) { return budget.Reserve(context.Background(), n) })
	if err := s.AddBytes([]byte("old"), true, SourceOrdinal{}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddBytes([]byte("four"), true, SourceOrdinal{0, 1}); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal("old3+old3+new4 must require10", err)
	}
	if budget.Used() != 6 || string(s.Snapshot().Last) != "old" {
		t.Fatal("failed replacement mutated")
	}
	s.Close()
}

func TestBytesEndpointsMalformedAndMergePermutations(t *testing.T) {
	bad := []BytesEndpointResult{
		{Type: memory.TypeInt64},
		{Type: memory.TypeString, FirstValid: true},
		{Type: memory.TypeBinary, Seen: true, First: []byte{1}},
		{Type: memory.TypeString, Seen: true, FirstOrdinal: SourceOrdinal{1, 0}},
		{Type: memory.TypeString, Seen: true, FirstValid: true, LastValid: true, First: []byte("x"), Last: []byte("y")},
	}
	for _, r := range bad {
		if ValidateBytesEndpoints(r) == nil {
			t.Fatal("accepted malformed", r)
		}
	}
	for _, order := range [][]int{{0, 1, 2}, {2, 0, 1}, {1, 2, 0}} {
		budget := memory.NewBudgetManager(1024)
		reserve := func(n int64) (memory.Reservation, error) { return budget.Reserve(context.Background(), n) }
		s, _ := NewBytesEndpointState(memory.TypeString, reserve)
		for _, i := range order {
			part, _ := NewBytesEndpointState(memory.TypeString, reserve)
			if err := part.AddBytes([]byte{byte('a' + i)}, i != 0, SourceOrdinal{uint64(i), 3}); err != nil {
				t.Fatal(err)
			}
			if err := s.Merge(part.Snapshot()); err != nil {
				t.Fatal(err)
			}
			part.Close()
		}
		r := s.Snapshot()
		if r.FirstValid || string(r.Last) != "c" || r.FirstOrdinal != (SourceOrdinal{0, 3}) {
			t.Fatal(r)
		}
		s.Close()
		if budget.Used() != 0 {
			t.Fatal("merge leak")
		}
	}
}

func TestBytesEndpointsCancellationAndOversizedPayload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	budget := memory.NewBudgetManager(64)
	s, _ := NewBytesEndpointState(memory.TypeBinary, func(n int64) (memory.Reservation, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return budget.Reserve(ctx, n)
	})
	if err := s.AddBytes([]byte("old"), true, SourceOrdinal{0, 1}); err != nil {
		t.Fatal(err)
	}
	before := s.Snapshot()
	if err := s.AddBytes(make([]byte, 65), true, SourceOrdinal{0, 2}); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if budget.Used() != 6 || !reflect.DeepEqual(before, s.Snapshot()) {
		t.Fatal("oversize mutated")
	}
	cancel()
	if err := s.AddBytes([]byte("later"), true, SourceOrdinal{0, 2}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if budget.Used() != 6 || !reflect.DeepEqual(before, s.Snapshot()) {
		t.Fatal("cancellation mutated")
	}
	s.Close()
	if budget.Used() != 0 {
		t.Fatal("cancellation cleanup leaked")
	}
}

func TestBytesEndpointsNoChangeAdmissionAndTypeMismatch(t *testing.T) {
	calls := 0
	budget := memory.NewBudgetManager(128)
	s, _ := NewBytesEndpointState(memory.TypeString, func(n int64) (memory.Reservation, error) { calls++; return budget.Reserve(context.Background(), n) })
	for _, ordinal := range []uint64{1, 2, 3} {
		if err := s.AddBytes([]byte("same"), true, SourceOrdinal{0, ordinal}); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 || budget.Used() != 8 {
		t.Fatal("equal bytes must reuse allocation", calls, budget.Used())
	}
	r := s.Snapshot()
	if r.LastOrdinal.Row != 3 {
		t.Fatal("reused bytes did not advance ordinal")
	}
	other := r
	other.Type = memory.TypeBinary
	if err := s.Merge(other); err == nil {
		t.Fatal("type mismatch accepted")
	}
	if !reflect.DeepEqual(r, s.Snapshot()) {
		t.Fatal("type mismatch mutated")
	}
	// A conflicting last ordinal must roll back a candidate earlier first.
	other = r
	other.FirstOrdinal = SourceOrdinal{}
	other.First = []byte("earlier")
	other.Last = []byte("conflict")
	if err := s.Merge(other); err == nil {
		t.Fatal("last conflict accepted")
	}
	if !reflect.DeepEqual(r, s.Snapshot()) || budget.Used() != 8 {
		t.Fatal("late merge conflict mutated")
	}
	s.Close()
}

func TestBytesEndpointsInvalidConstructionAndReservation(t *testing.T) {
	if _, err := NewBytesEndpointState(memory.TypeString, nil); err == nil {
		t.Fatal("nil reservation accepted")
	}
	if _, err := NewBytesEndpointState(memory.TypeInt64, func(int64) (memory.Reservation, error) { return nil, nil }); err == nil {
		t.Fatal("integer type accepted")
	}
	s, _ := NewBytesEndpointState(memory.TypeString, func(int64) (memory.Reservation, error) { return nil, nil })
	if err := s.AddBytes([]byte("x"), true, SourceOrdinal{}); err == nil {
		t.Fatal("nil token accepted")
	}
	if s.Snapshot().Seen || s.RetainedBytes() != 0 {
		t.Fatal("nil token mutated")
	}
	var zero BytesEndpointState
	if err := zero.AddBytes(nil, true, SourceOrdinal{}); err == nil {
		t.Fatal("zero state accepted")
	}
	zero.Close()
}
