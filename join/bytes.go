package join

import (
	"fmt"
	"gocolumn/hashagg"
	"math"
)

// BytesKeyFunc returns a canonical key and whether every component is non-NULL.
// Returned bytes may be reused on the next call. Build copies retained keys;
// Probe never retains the returned bytes.
type BytesKeyFunc func(row int) ([]byte, bool, error)

// BytesTable joins exact, variable-width canonical keys. Hashes choose slots
// only: every candidate is checked using all key bytes, including collisions.
// Duplicate chains preserve the fixed-key table's reverse build-row order.
type BytesTable struct {
	keys            []string
	head            []int32
	count           []int32
	next            []int32
	mask            uint64
	maxMultiplicity int
}

func bytesTableSlots(rows int) (int, error) {
	if rows < 0 || int64(rows) > math.MaxInt32 {
		return 0, fmt.Errorf("bytes join build row count %d exceeds supported range [0,%d]", rows, int64(math.MaxInt32))
	}
	n := 16
	for int64(n) < int64(rows)*2 {
		if n > int(^uint(0)>>1)/2 {
			return 0, fmt.Errorf("bytes join directory exceeds platform capacity")
		}
		n *= 2
	}
	return n, nil
}

// EstimateBytesTableBytes bounds the directory, duplicate chains, and owned
// key copies. keyBytes is the sum of encoded build key lengths (duplicates may
// be included). Padding bounds allocator size-class rounding for key strings.
// The caller separately accounts for key-encoding scratch and input payloads.
func EstimateBytesTableBytes(rows int, keyBytes int64) (int64, error) {
	n, err := bytesTableSlots(rows)
	if err != nil {
		return 0, err
	}
	if keyBytes < 0 || keyBytes > (math.MaxInt64-128-int64(n)*24-int64(rows)*20)/2 {
		return 0, fmt.Errorf("bytes join byte estimate overflows int64")
	}
	return 128 + int64(n)*24 + int64(rows)*20 + keyBytes*2, nil
}

// BuildBytesTable builds a table of rows, omitting any NULL keys. Callers using
// a memory budget must reserve EstimateBytesTableBytes before invoking it.
func BuildBytesTable(rows int, key BytesKeyFunc) (*BytesTable, error) {
	n, err := bytesTableSlots(rows)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, fmt.Errorf("bytes join requires key reader")
	}
	t := &BytesTable{keys: make([]string, n), head: make([]int32, n), count: make([]int32, n), next: make([]int32, rows), mask: uint64(n - 1)}
	for row := 0; row < rows; row++ {
		value, valid, err := key(row)
		if err != nil {
			return nil, err
		}
		if !valid {
			continue
		}
		p := hashBytes(value) & t.mask
		for t.head[p] != 0 && t.keys[p] != string(value) {
			p = (p + 1) & t.mask
		}
		if t.head[p] == 0 {
			t.keys[p] = string(value)
		}
		t.next[row] = t.head[p]
		t.head[p] = int32(row + 1)
		t.count[p]++
		if int(t.count[p]) > t.maxMultiplicity {
			t.maxMultiplicity = int(t.count[p])
		}
	}
	return t, nil
}

func hashBytes(key []byte) uint64 {
	// FNV-1a plus the fixed-width table's avalanche keeps short and structured
	// keys spread over the power-of-two directory. Equality never uses this hash.
	h := uint64(14695981039346656037)
	for _, b := range key {
		h = (h ^ uint64(b)) * 1099511628211
	}
	return hashagg.Hash64(h)
}

func (t *BytesTable) find(key []byte) int32 {
	p := hashBytes(key) & t.mask
	for probes := 0; probes < len(t.head); probes++ {
		if t.head[p] == 0 {
			return 0
		}
		if t.keys[p] == string(key) {
			return t.head[p]
		}
		p = (p + 1) & t.mask
	}
	return 0
}

// MaxMultiplicity returns the maximum count of a fully valid build key.
func (t *BytesTable) MaxMultiplicity() int { return t.maxMultiplicity }

// Probe applies the existing six SQL equality join modes. Right/Full unmatched
// build rows are emitted by the query-wide owner after all probe batches.
func (t *BytesTable) Probe(rows int, key BytesKeyFunc, typ Type, emit EmitFunc) (Summary, error) {
	if rows < 0 || key == nil {
		return Summary{}, fmt.Errorf("bytes join requires non-negative rows and key reader")
	}
	if typ < Inner || typ > Full {
		return Summary{}, fmt.Errorf("unsupported join type %d", typ)
	}
	s := Summary{ProbeRows: rows}
	for row := 0; row < rows; row++ {
		value, valid, err := key(row)
		if err != nil {
			return s, err
		}
		head := int32(0)
		if valid {
			head = t.find(value)
		}
		if head != 0 {
			s.MatchedProbeRows++
		}
		output := func(build int) error {
			s.OutputRows++
			if emit != nil {
				return emit(Pair{ProbeRow: row, BuildRow: build})
			}
			return nil
		}
		switch typ {
		case Inner, Left, Right, Full:
			if head != 0 {
				for n := head; n != 0; n = t.next[n-1] {
					if err := output(int(n - 1)); err != nil {
						return s, err
					}
				}
			} else if typ == Left || typ == Full {
				if err := output(-1); err != nil {
					return s, err
				}
			}
		case Semi:
			if head != 0 {
				if err := output(int(head - 1)); err != nil {
					return s, err
				}
			}
		case Anti:
			if head == 0 {
				if err := output(-1); err != nil {
					return s, err
				}
			}
		}
	}
	return s, nil
}
