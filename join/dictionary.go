package join

import (
	"fmt"
	"gocolumn/hashagg"
	"gocolumn/memory"
)

// Dictionary describes one immutable dictionary domain used by encoded string
// join keys. Offsets contains one more entry than the number of dictionary
// values.
type Dictionary struct {
	Domain  string
	Offsets []uint32
	Data    []byte
}

// Len returns the number of dictionary values.
func (d Dictionary) Len() int {
	if len(d.Offsets) == 0 {
		return 0
	}
	return len(d.Offsets) - 1
}

// Value returns one dictionary value as an immutable byte view.
func (d Dictionary) Value(code uint32) []byte {
	i := int(code)
	if i < 0 || i+1 >= len(d.Offsets) {
		return nil
	}
	lo, hi := d.Offsets[i], d.Offsets[i+1]
	if lo > hi || int(hi) > len(d.Data) {
		return nil
	}
	return d.Data[lo:hi]
}

// Validate checks offset ordering and the terminal data boundary.
func (d Dictionary) Validate() error {
	if len(d.Offsets) == 0 {
		return fmt.Errorf("dictionary requires at least a terminal offset")
	}
	prev := uint32(0)
	for i, off := range d.Offsets {
		if i == 0 && off != 0 {
			return fmt.Errorf("dictionary first offset=%d want=0", off)
		}
		if off < prev {
			return fmt.Errorf("dictionary offset %d is not monotonic", i)
		}
		prev = off
	}
	if int(d.Offsets[len(d.Offsets)-1]) > len(d.Data) {
		return fmt.Errorf("dictionary terminal offset exceeds data")
	}
	return nil
}

// DictionaryTable joins dictionary-encoded string keys without decoding every
// row. Equal stable domains compare integer codes directly. Different domains
// are normalized once per dictionary value and then probe rows use O(1) code
// translation.
type DictionaryTable struct {
	slotCodes       []uint32
	head            []int32
	count           []uint32
	epoch           []uint32
	gen             uint32
	mask            uint64
	next            []int32
	maxMultiplicity int

	buildDomain     string
	buildDictionary Dictionary
	buildCanonical  []uint32
	valueToCode     map[string]uint32
}

// NewDictionaryTable creates a reusable dictionary join table.
func NewDictionaryTable(cardinalityHint int) *DictionaryTable {
	if cardinalityHint < 8 {
		cardinalityHint = 8
	}
	n := 1
	for n < cardinalityHint*2 {
		n <<= 1
	}
	return &DictionaryTable{slotCodes: make([]uint32, n), head: make([]int32, n), count: make([]uint32, n), epoch: make([]uint32, n), mask: uint64(n - 1)}
}

// Reset clears build-side state while retaining table capacity.
func (t *DictionaryTable) Reset() {
	t.gen++
	if t.gen == 0 {
		clear(t.epoch)
		t.gen = 1
	}
	t.next = t.next[:0]
	t.maxMultiplicity = 0
	t.buildDomain = ""
	t.buildDictionary = Dictionary{}
	t.buildCanonical = t.buildCanonical[:0]
	clear(t.valueToCode)
}

func (t *DictionaryTable) ensureSlots(cardinality int) {
	need := 8
	for need < cardinality*2 {
		need <<= 1
	}
	if need <= len(t.slotCodes) {
		return
	}
	t.slotCodes = make([]uint32, need)
	t.head = make([]int32, need)
	t.count = make([]uint32, need)
	t.epoch = make([]uint32, need)
	t.mask = uint64(need - 1)
	// Slot arrays were recreated, so generation can restart at one.
	t.gen = 1
}

func (t *DictionaryTable) canonicalizeBuild(d Dictionary) error {
	if err := d.Validate(); err != nil {
		return err
	}
	t.ensureSlots(d.Len())
	if cap(t.buildCanonical) < d.Len() {
		t.buildCanonical = make([]uint32, d.Len())
	} else {
		t.buildCanonical = t.buildCanonical[:d.Len()]
	}
	if t.valueToCode == nil {
		t.valueToCode = make(map[string]uint32, d.Len())
	} else {
		clear(t.valueToCode)
	}
	for i := 0; i < d.Len(); i++ {
		value := d.Value(uint32(i))
		key := string(value) // dictionary-level allocation, never per input row
		canonical, ok := t.valueToCode[key]
		if !ok {
			canonical = uint32(i)
			t.valueToCode[key] = canonical
		}
		t.buildCanonical[i] = canonical
	}
	t.buildDomain = d.Domain
	t.buildDictionary = d
	return nil
}

// Build constructs the build-side table. NULL keys are intentionally omitted
// because SQL equi-join NULL never matches NULL.
func (t *DictionaryTable) Build(indices []uint32, validity memory.Bitmap, dictionary Dictionary) error {
	t.Reset()
	if err := t.canonicalizeBuild(dictionary); err != nil {
		return err
	}
	if cap(t.next) < len(indices) {
		t.next = make([]int32, len(indices))
	} else {
		t.next = t.next[:len(indices)]
		clear(t.next)
	}
	for row, code := range indices {
		if validity != nil && !memory.IsSet(validity, row) {
			continue
		}
		if int(code) >= len(t.buildCanonical) {
			return fmt.Errorf("build dictionary code %d out of range", code)
		}
		canonical := t.buildCanonical[code]
		p := hashagg.Hash64(uint64(canonical)) & t.mask
		for {
			if t.epoch[p] != t.gen {
				t.epoch[p] = t.gen
				t.slotCodes[p] = canonical
				t.head[p] = int32(row + 1)
				t.count[p] = 1
				if t.maxMultiplicity < 1 {
					t.maxMultiplicity = 1
				}
				break
			}
			if t.slotCodes[p] == canonical {
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

func (t *DictionaryTable) find(code uint32) int32 {
	p := hashagg.Hash64(uint64(code)) & t.mask
	for probes := 0; probes < len(t.epoch); probes++ {
		if t.epoch[p] != t.gen {
			return 0
		}
		if t.slotCodes[p] == code {
			return t.head[p]
		}
		p = (p + 1) & t.mask
	}
	return 0
}

func (t *DictionaryTable) translation(probe Dictionary) ([]int64, error) {
	if err := probe.Validate(); err != nil {
		return nil, err
	}
	out := make([]int64, probe.Len())
	for i := range out {
		out[i] = -1
	}
	if probe.Domain != "" && probe.Domain == t.buildDomain {
		for i := range out {
			if i < len(t.buildCanonical) {
				out[i] = int64(t.buildCanonical[i])
			}
		}
		return out, nil
	}
	for i := 0; i < probe.Len(); i++ {
		if code, ok := t.valueToCode[string(probe.Value(uint32(i)))]; ok {
			out[i] = int64(code)
		}
	}
	return out, nil
}

// MaxMultiplicity returns the largest number of build rows sharing one non-NULL
// canonical dictionary value.
func (t *DictionaryTable) MaxMultiplicity() int { return t.maxMultiplicity }

// Probe probes dictionary-encoded rows using the requested join semantics.
// Domain normalization is performed once per probe dictionary value.
func (t *DictionaryTable) Probe(indices []uint32, validity memory.Bitmap, dictionary Dictionary, typ Type, emit EmitFunc) (Summary, error) {
	translation, err := t.translation(dictionary)
	if err != nil {
		return Summary{}, err
	}
	var s Summary
	s.ProbeRows = len(indices)
	for row, code := range indices {
		valid := validity == nil || memory.IsSet(validity, row)
		head := int32(0)
		if valid {
			if int(code) >= len(translation) {
				return s, fmt.Errorf("probe dictionary code %d out of range", code)
			}
			canonical := translation[code]
			if canonical >= 0 {
				head = t.find(uint32(canonical))
			}
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
		default:
			return s, fmt.Errorf("unsupported join type %d", typ)
		}
	}
	return s, nil
}
