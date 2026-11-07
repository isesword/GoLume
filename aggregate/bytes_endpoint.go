package aggregate

import (
	"bytes"
	"fmt"
	"gocolumn/memory"
)

// BytesEndpointResult is borrowed raw string/binary endpoint state. Its payloads
// remain valid only until the owning state is mutated, reset or closed. NULL is
// distinct from valid empty bytes. Source ordinals are logical input positions.
type BytesEndpointResult struct {
	Type                      memory.DataType
	Seen                      bool
	FirstOrdinal, LastOrdinal SourceOrdinal
	First, Last               []byte
	FirstValid, LastValid     bool
}

// ValidateBytesEndpoints checks an untrusted raw endpoint payload without allocation.
func ValidateBytesEndpoints(r BytesEndpointResult) error {
	if r.Type != memory.TypeString && r.Type != memory.TypeBinary {
		return fmt.Errorf("aggregate: byte endpoints require string or binary")
	}
	if ordinalLess(r.LastOrdinal, r.FirstOrdinal) || (!r.FirstValid && len(r.First) != 0) || (!r.LastValid && len(r.Last) != 0) {
		return fmt.Errorf("aggregate: invalid byte endpoint state")
	}
	if !r.Seen && (r.FirstValid || r.LastValid || r.FirstOrdinal != (SourceOrdinal{}) || r.LastOrdinal != (SourceOrdinal{})) {
		return fmt.Errorf("aggregate: nonempty unseen byte endpoint")
	}
	if r.Seen && r.FirstOrdinal == r.LastOrdinal && (r.FirstValid != r.LastValid || !bytes.Equal(r.First, r.Last)) {
		return fmt.Errorf("aggregate: conflicting byte endpoint ordinal")
	}
	return nil
}

// BytesEndpointState owns exact endpoint payloads. Its containing struct must be
// admitted by its owner; payload allocations are admitted through reserve before
// allocation, including full replacement overlap. It is not safe for concurrent use.
type BytesEndpointState struct {
	result                BytesEndpointResult
	reserve               BundleReserve
	firstLease, lastLease memory.Reservation
	closed                bool
}

// NewBytesEndpointState creates empty owned endpoint state without allocating payloads.
func NewBytesEndpointState(typ memory.DataType, reserve BundleReserve) (BytesEndpointState, error) {
	r := BytesEndpointResult{Type: typ}
	if err := ValidateBytesEndpoints(r); err != nil {
		return BytesEndpointState{}, err
	}
	if reserve == nil {
		return BytesEndpointState{}, fmt.Errorf("aggregate: byte endpoints require reservation callback")
	}
	return BytesEndpointState{result: r, reserve: reserve}, nil
}

// Snapshot borrows payloads; callers must not mutate or retain them beyond mutation.
func (s *BytesEndpointState) Snapshot() BytesEndpointResult { return s.result }

// RetainedBytes reports exact live payload backing bytes, excluding the containing struct.
func (s *BytesEndpointState) RetainedBytes() int64 {
	return int64(len(s.result.First)) + int64(len(s.result.Last))
}

func addBytesEndpoint(r *BytesEndpointResult, payload []byte, valid bool, o SourceOrdinal) error {
	if !valid {
		payload = nil
	}
	if r.Seen && ((o == r.FirstOrdinal && (valid != r.FirstValid || !bytes.Equal(payload, r.First))) || (o == r.LastOrdinal && (valid != r.LastValid || !bytes.Equal(payload, r.Last)))) {
		return fmt.Errorf("aggregate: conflicting byte endpoint ordinal")
	}
	if !r.Seen || ordinalLess(o, r.FirstOrdinal) {
		r.FirstOrdinal = o
		r.First = payload
		r.FirstValid = valid
	}
	if !r.Seen || ordinalLess(r.LastOrdinal, o) {
		r.LastOrdinal = o
		r.Last = payload
		r.LastValid = valid
	}
	r.Seen = true
	return nil
}

// BytesEndpointMutation owns a prepared update. Its containing storage must be
// admitted by its caller. Do not copy a live mutation. Resolve it with Commit or
// Abort before mutating, moving, resetting or closing the target state. Preparing
// more than one mutation for the same state at a time is unsupported.
type BytesEndpointMutation struct {
	target                *BytesEndpointState
	next                  BytesEndpointResult
	first, last           bool
	firstLease, lastLease memory.Reservation
}

// Abort releases prepared payloads without changing the target. It is idempotent.
func (p *BytesEndpointMutation) Abort() {
	if p.firstLease != nil {
		p.firstLease.Release()
	}
	if p.lastLease != nil {
		p.lastLease.Release()
	}
	*p = BytesEndpointMutation{}
}

// Commit installs an admitted update without allocation or failure. It is idempotent.
func (p *BytesEndpointMutation) Commit() {
	if p.target == nil {
		return
	}
	s := p.target
	if p.first {
		if s.firstLease != nil {
			s.firstLease.Release()
		}
		s.firstLease = p.firstLease
	}
	if p.last {
		if s.lastLease != nil {
			s.lastLease.Release()
		}
		s.lastLease = p.lastLease
	}
	s.result = p.next
	*p = BytesEndpointMutation{}
}

// PrepareBytes admits and copies selected payloads without changing the state.
func (s *BytesEndpointState) PrepareBytes(payload []byte, valid bool, o SourceOrdinal) (BytesEndpointMutation, error) {
	if s.closed || s.reserve == nil {
		return BytesEndpointMutation{}, fmt.Errorf("aggregate: byte endpoint state is closed or uninitialized")
	}
	next := s.result
	if err := addBytesEndpoint(&next, payload, valid, o); err != nil {
		return BytesEndpointMutation{}, err
	}
	return s.prepare(next)
}

// PrepareMergeBytes validates and prepares borrowed raw intermediate state.
func (s *BytesEndpointState) PrepareMergeBytes(other BytesEndpointResult) (BytesEndpointMutation, error) {
	if s.closed || s.reserve == nil {
		return BytesEndpointMutation{}, fmt.Errorf("aggregate: byte endpoint state is closed or uninitialized")
	}
	if err := ValidateBytesEndpoints(other); err != nil {
		return BytesEndpointMutation{}, err
	}
	if other.Type != s.result.Type {
		return BytesEndpointMutation{}, fmt.Errorf("aggregate: incompatible byte endpoint types")
	}
	next := s.result
	if other.Seen {
		if err := addBytesEndpoint(&next, other.First, other.FirstValid, other.FirstOrdinal); err != nil {
			return BytesEndpointMutation{}, err
		}
		if err := addBytesEndpoint(&next, other.Last, other.LastValid, other.LastOrdinal); err != nil {
			return BytesEndpointMutation{}, err
		}
	}
	return s.prepare(next)
}

// AddBytes atomically considers a row, including boundary NULLs.
func (s *BytesEndpointState) AddBytes(payload []byte, valid bool, o SourceOrdinal) error {
	p, err := s.PrepareBytes(payload, valid, o)
	if err != nil {
		return err
	}
	p.Commit()
	return nil
}

// Merge validates and atomically owns selected borrowed intermediate payloads.
func (s *BytesEndpointState) Merge(other BytesEndpointResult) error {
	p, err := s.PrepareMergeBytes(other)
	if err != nil {
		return err
	}
	p.Commit()
	return nil
}

func (s *BytesEndpointState) prepare(next BytesEndpointResult) (BytesEndpointMutation, error) {
	p := BytesEndpointMutation{target: s, next: next,
		first: next.FirstValid != s.result.FirstValid || !bytes.Equal(next.First, s.result.First),
		last:  next.LastValid != s.result.LastValid || !bytes.Equal(next.Last, s.result.Last)}
	var err error
	if p.first && len(next.First) > 0 {
		p.firstLease, err = reserveBundle(s.reserve, int64(len(next.First)))
		if err != nil {
			return BytesEndpointMutation{}, err
		}
	}
	if p.last && len(next.Last) > 0 {
		p.lastLease, err = reserveBundle(s.reserve, int64(len(next.Last)))
		if err != nil {
			p.Abort()
			return BytesEndpointMutation{}, err
		}
	}
	// Copy only after both admissions succeed, retaining the full old/new overlap.
	if p.first {
		p.next.First = make([]byte, len(next.First))
		copy(p.next.First, next.First)
	} else {
		p.next.First = s.result.First
	}
	if p.last {
		p.next.Last = make([]byte, len(next.Last))
		copy(p.next.Last, next.Last)
	} else {
		p.next.Last = s.result.Last
	}
	return p, nil
}

// Reset releases all endpoint payloads, retaining the type and reservation callback.
func (s *BytesEndpointState) Reset() {
	if s.firstLease != nil {
		s.firstLease.Release()
		s.firstLease = nil
	}
	if s.lastLease != nil {
		s.lastLease.Release()
		s.lastLease = nil
	}
	s.result = BytesEndpointResult{Type: s.result.Type}
}

// Close releases payloads idempotently and rejects subsequent updates.
func (s *BytesEndpointState) Close() { s.Reset(); s.closed = true }
