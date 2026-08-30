package join

import (
	"fmt"
	"gocolumn/hashagg"
)

// PairTable is a reusable build-side hash table for fully valid fixed-width
// 128-bit composite equality keys. Rows with any NULL key component are not
// inserted because SQL equi-join predicates evaluate to UNKNOWN for them.
//
// Duplicate build keys are retained in per-key row chains so inner and left
// joins preserve one-to-many semantics.
type PairTable struct {
	a               []uint64
	b               []uint64
	head            []int32 // build row+1; zero means empty/no match
	count           []uint32
	epoch           []uint32
	gen             uint32
	mask            uint64
	next            []int32 // duplicate build row chain, row+1
	maxMultiplicity int
}

// NewPairTable creates a reusable fixed-width composite join table. The
// cardinality hint affects initial capacity only; Build grows to fit the actual
// build row count before inserting keys.
func NewPairTable(cardinalityHint int) *PairTable {
	if cardinalityHint < 8 {
		cardinalityHint = 8
	}
	n := 1
	for n < cardinalityHint*2 {
		n <<= 1
	}
	return &PairTable{
		a:     make([]uint64, n),
		b:     make([]uint64, n),
		head:  make([]int32, n),
		count: make([]uint32, n),
		epoch: make([]uint32, n),
		mask:  uint64(n - 1),
	}
}

// Reset clears the logical build side while retaining allocated workspace.
func (t *PairTable) Reset() {
	t.gen++
	if t.gen == 0 {
		clear(t.epoch)
		t.gen = 1
	}
	t.next = t.next[:0]
	t.maxMultiplicity = 0
}

func (t *PairTable) ensureSlots(buildRows int) {
	need := 8
	for need < buildRows*2 {
		need <<= 1
	}
	if need <= len(t.a) {
		return
	}
	t.a = make([]uint64, need)
	t.b = make([]uint64, need)
	t.head = make([]int32, need)
	t.count = make([]uint32, need)
	t.epoch = make([]uint32, need)
	t.mask = uint64(need - 1)
	// Recreated slot arrays contain no active epochs.
	t.gen = 1
}

// Build replaces the build side with keys. validMasks uses bit 0 for Key.A
// and bit 1 for Key.B; nil means both components are valid for every row.
// Rows whose mask is not 0b11 are skipped according to SQL equality semantics.
func (t *PairTable) Build(keys []hashagg.Key128, validMasks []uint8) error {
	if validMasks != nil && len(validMasks) != len(keys) {
		return fmt.Errorf("pair join: key/validity length mismatch")
	}
	t.Reset()
	t.ensureSlots(len(keys))
	if cap(t.next) < len(keys) {
		t.next = make([]int32, len(keys))
	} else {
		t.next = t.next[:len(keys)]
		clear(t.next)
	}
	for row, key := range keys {
		if validMasks != nil && validMasks[row]&3 != 3 {
			continue
		}
		p := hashagg.Hash128(key) & t.mask
		for {
			if t.epoch[p] != t.gen {
				t.epoch[p] = t.gen
				t.a[p], t.b[p] = key.A, key.B
				t.head[p] = int32(row + 1)
				t.count[p] = 1
				if t.maxMultiplicity < 1 {
					t.maxMultiplicity = 1
				}
				break
			}
			if t.a[p] == key.A && t.b[p] == key.B {
				t.next[row] = t.head[p]
				t.head[p] = int32(row + 1)
				t.count[p]++
				if int(t.count[p]) > t.maxMultiplicity {
					t.maxMultiplicity = int(t.count[p])
				}
				break
			}
			p = (p + 1) & t.mask
		}
	}
	return nil
}

func (t *PairTable) find(key hashagg.Key128) int32 {
	p := hashagg.Hash128(key) & t.mask
	for probes := 0; probes < len(t.epoch); probes++ {
		if t.epoch[p] != t.gen {
			return 0
		}
		if t.a[p] == key.A && t.b[p] == key.B {
			return t.head[p]
		}
		p = (p + 1) & t.mask
	}
	return 0
}

// Probe applies typ to fixed-width composite probe keys. validMasks follows
// the same two-bit convention as Build; a probe row with any NULL component
// never matches. The emit callback may be nil when only Summary is needed.
func (t *PairTable) Probe(keys []hashagg.Key128, validMasks []uint8, typ Type, emit EmitFunc) (Summary, error) {
	if validMasks != nil && len(validMasks) != len(keys) {
		return Summary{}, fmt.Errorf("pair join: key/validity length mismatch")
	}
	var s Summary
	s.ProbeRows = len(keys)
	for row, key := range keys {
		valid := validMasks == nil || validMasks[row]&3 == 3
		head := int32(0)
		if valid {
			head = t.find(key)
		}
		matched := head != 0
		if matched {
			s.MatchedProbeRows++
		}
		switch typ {
		case Inner, Left, Right, Full:
			if matched {
				for n := head; n != 0; n = t.next[n-1] {
					s.OutputRows++
					if emit != nil {
						if err := emit(Pair{ProbeRow: row, BuildRow: int(n - 1)}); err != nil {
							return s, err
						}
					}
				}
			} else if typ == Left || typ == Full {
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
		default:
			return s, fmt.Errorf("unsupported join type %d", typ)
		}
	}
	return s, nil
}

// MaxMultiplicity returns the largest number of build rows sharing one non-NULL key.
// It is used by streaming join sources to derive safe probe-output row bounds
// without pre-reading the probe side.
func (t *PairTable) MaxMultiplicity() int { return t.maxMultiplicity }

// EstimatePairTableBytes estimates retained bytes for a fixed-width composite
// join directory plus duplicate-row chains.
func EstimatePairTableBytes(cardinalityHint, buildRows int) int64 {
	if cardinalityHint < 8 {
		cardinalityHint = 8
	}
	n := 1
	// Build grows based on rows, so estimate with the larger of the hint and
	// actual build-side row count when that information is available.
	need := cardinalityHint
	if buildRows > need {
		need = buildRows
	}
	for n < need*2 {
		n <<= 1
	}
	return int64(n)*(8+8+4+4+4) + int64(buildRows)*4
}
