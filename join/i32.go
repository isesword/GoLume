package join

import (
	"gocolumn/hashagg"
	"gocolumn/memory"
)

// Type identifies the relational join semantics applied during probe.
type Type uint8

const (
	// Inner emits only matching probe/build row pairs.
	Inner Type = iota
	// Left emits matching pairs and unmatched probe rows with BuildRow set to -1.
	Left
	// Semi emits each probe row at most once when at least one build match exists.
	Semi
	// Anti emits probe rows that have no build-side match.
	Anti
)

// String returns the stable textual name of the join type.
func (t Type) String() string {
	switch t {
	case Inner:
		return "inner"
	case Left:
		return "left"
	case Semi:
		return "semi"
	case Anti:
		return "anti"
	default:
		return "unknown"
	}
}

// Pair identifies one emitted probe/build row association.
// BuildRow is -1 for unmatched rows emitted by left and anti joins.
type Pair struct {
	ProbeRow int
	BuildRow int
}

// Summary reports row counts observed and emitted by a join probe.
type Summary struct {
	ProbeRows        int
	MatchedProbeRows int
	OutputRows       int
}

// EmitFunc consumes one logical join result pair. Returning an error stops probing.
type EmitFunc func(Pair) error

// I32Table is a reusable build-side hash table. Duplicate build keys are kept
// in per-key chains, so the operator has correct one-to-many join semantics.
// Build keys are copied into retained workspace; callers may reuse input batches
// after Build returns.
type I32Table struct {
	slotKeys   []int32
	head       []int32 // build row+1, 0 means none
	epoch      []uint32
	gen        uint32
	mask       uint64
	buildKeys  []int32
	next       []int32 // next build row+1
	buildValid memory.Bitmap
}

// NewI32Table allocates a reusable int32 build-side hash table.
// cardinalityHint is a sizing hint only; Build grows the slot arrays when needed.
func NewI32Table(cardinalityHint int) *I32Table {
	if cardinalityHint < 8 {
		cardinalityHint = 8
	}
	n := 1
	for n < cardinalityHint*2 {
		n <<= 1
	}
	return &I32Table{slotKeys: make([]int32, n), head: make([]int32, n), epoch: make([]uint32, n), mask: uint64(n - 1)}
}

// Reset clears the logical contents while retaining allocated workspace.
func (t *I32Table) Reset() {
	t.gen++
	if t.gen == 0 {
		clear(t.epoch)
		t.gen = 1
	}
	t.buildKeys = t.buildKeys[:0]
	t.next = t.next[:0]
	t.buildValid = nil
}
func (t *I32Table) ensureSlots(cardinality int) {
	need := 8
	for need < cardinality*2 {
		need <<= 1
	}
	if need <= len(t.slotKeys) {
		return
	}
	t.slotKeys = make([]int32, need)
	t.head = make([]int32, need)
	t.epoch = make([]uint32, need)
	t.mask = uint64(need - 1)
	// Recreated slot arrays have no active epochs. Reset generation so the next
	// build can use the arrays without clearing them.
	t.gen = 1
}

// Build replaces the build side with keys and optional validity. NULL build keys are skipped.
// Duplicate non-NULL keys are retained so one-to-many join semantics remain correct.
func (t *I32Table) Build(keys []int32, validity memory.Bitmap) {
	t.Reset()
	t.ensureSlots(len(keys))
	if cap(t.buildKeys) < len(keys) {
		t.buildKeys = make([]int32, len(keys))
	} else {
		t.buildKeys = t.buildKeys[:len(keys)]
	}
	copy(t.buildKeys, keys)
	if cap(t.next) < len(keys) {
		t.next = make([]int32, len(keys))
	} else {
		t.next = t.next[:len(keys)]
	}
	clear(t.next)
	if validity != nil {
		t.buildValid = memory.EnsureBitmap(t.buildValid, len(keys))
		copy(t.buildValid, validity)
	}
	for row, k := range keys {
		if validity != nil && !memory.IsSet(validity, row) {
			continue
		}
		p := hashagg.Hash64(uint64(uint32(k))) & t.mask
		for {
			if t.epoch[p] != t.gen {
				t.epoch[p] = t.gen
				t.slotKeys[p] = k
				t.head[p] = int32(row + 1)
				break
			}
			if t.slotKeys[p] == k {
				t.next[row] = t.head[p]
				t.head[p] = int32(row + 1)
				break
			}
			p = (p + 1) & t.mask
		}
	}
}
func (t *I32Table) find(k int32) int32 {
	p := hashagg.Hash64(uint64(uint32(k))) & t.mask
	for probes := 0; probes < len(t.epoch); probes++ {
		if t.epoch[p] != t.gen {
			return 0
		}
		if t.slotKeys[p] == k {
			return t.head[p]
		}
		p = (p + 1) & t.mask
	}
	return 0
}

// Probe applies typ to probe-side keys and invokes emit for each logical output row.
// NULL probe keys never match build keys.
func (t *I32Table) Probe(keys []int32, validity memory.Bitmap, typ Type, emit EmitFunc) (Summary, error) {
	var s Summary
	s.ProbeRows = len(keys)
	for row, k := range keys {
		valid := validity == nil || memory.IsSet(validity, row)
		head := int32(0)
		if valid {
			head = t.find(k)
		}
		matched := head != 0
		if matched {
			s.MatchedProbeRows++
		}
		switch typ {
		case Inner, Left:
			if matched {
				for n := head; n != 0; n = t.next[n-1] {
					s.OutputRows++
					if emit != nil {
						if err := emit(Pair{ProbeRow: row, BuildRow: int(n - 1)}); err != nil {
							return s, err
						}
					}
				}
			} else if typ == Left {
				s.OutputRows++
				if emit != nil {
					if err := emit(Pair{ProbeRow: row, BuildRow: -1}); err != nil {
						return s, err
					}
				}
			}
		case Semi:
			if matched {
				s.OutputRows++
				if emit != nil {
					if err := emit(Pair{ProbeRow: row, BuildRow: int(head - 1)}); err != nil {
						return s, err
					}
				}
			}
		case Anti:
			if !matched {
				s.OutputRows++
				if emit != nil {
					if err := emit(Pair{ProbeRow: row, BuildRow: -1}); err != nil {
						return s, err
					}
				}
			}
		}
	}
	return s, nil
}

// EstimateI32TableBytes estimates retained bytes for an int32 join table and build rows.
func EstimateI32TableBytes(cardinalityHint, buildRows int) int64 {
	if cardinalityHint < 8 {
		cardinalityHint = 8
	}
	n := 1
	for n < cardinalityHint*2 {
		n <<= 1
	}
	return int64(n)*(4+4+4) + int64(buildRows)*(4+4) + int64(memory.WordsFor(buildRows))*8
}
