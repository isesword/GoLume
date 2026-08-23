package join

import (
	"fmt"
	"gocolumn/hashagg"
)

// RadixPairTable partitions a fixed-width build side by hash prefix before
// constructing independent PairTable instances. Smaller bucket-local tables
// improve cache locality for large joins while preserving one-to-many row
// identity through local-to-global build-row maps.
type RadixPairTable struct {
	bits            uint8
	mask            uint64
	buckets         []*PairTable
	buildRows       [][]int
	maxMultiplicity int
}

// NewRadixPairTable constructs a radix-partitioned fixed-key join table. bits
// is clamped to [1,8] to keep partition metadata bounded in Phase 2.6.
func NewRadixPairTable(bits uint8) *RadixPairTable {
	if bits < 1 {
		bits = 1
	}
	if bits > 8 {
		bits = 8
	}
	n := 1 << bits
	return &RadixPairTable{bits: bits, mask: uint64(n - 1), buckets: make([]*PairTable, n), buildRows: make([][]int, n)}
}

// RadixBits returns the hash-prefix width used by the table.
func (t *RadixPairTable) RadixBits() uint8 { return t.bits }

// MaxMultiplicity returns the largest duplicate build-key count across all buckets.
func (t *RadixPairTable) MaxMultiplicity() int { return t.maxMultiplicity }

// Build partitions and builds the fixed-width keys. NULL keys are omitted by
// the underlying PairTable according to SQL equality semantics.
func (t *RadixPairTable) Build(keys []hashagg.Key128, validMasks []uint8) error {
	if validMasks != nil && len(validMasks) != len(keys) {
		return fmt.Errorf("radix pair join: key/validity length mismatch")
	}
	n := len(t.buckets)
	counts := make([]int, n)
	for i, key := range keys {
		if validMasks != nil && validMasks[i]&3 != 3 {
			continue
		}
		counts[t.bucket(key)]++
	}
	bucketKeys := make([][]hashagg.Key128, n)
	bucketMasks := make([][]uint8, n)
	for i := 0; i < n; i++ {
		bucketKeys[i] = make([]hashagg.Key128, 0, counts[i])
		bucketMasks[i] = make([]uint8, 0, counts[i])
		t.buildRows[i] = t.buildRows[i][:0]
	}
	for row, key := range keys {
		if validMasks != nil && validMasks[row]&3 != 3 {
			continue
		}
		b := t.bucket(key)
		bucketKeys[b] = append(bucketKeys[b], key)
		bucketMasks[b] = append(bucketMasks[b], 3)
		t.buildRows[b] = append(t.buildRows[b], row)
	}
	t.maxMultiplicity = 0
	for i := 0; i < n; i++ {
		if len(bucketKeys[i]) == 0 {
			t.buckets[i] = nil
			continue
		}
		if t.buckets[i] == nil {
			t.buckets[i] = NewPairTable(len(bucketKeys[i]))
		}
		if err := t.buckets[i].Build(bucketKeys[i], bucketMasks[i]); err != nil {
			return err
		}
		if m := t.buckets[i].MaxMultiplicity(); m > t.maxMultiplicity {
			t.maxMultiplicity = m
		}
	}
	return nil
}

// RadixProbeWorkspace retains probe-side partitioning buffers across repeated
// radix join probes. A workspace belongs to one reader/execution and must not be
// shared concurrently.
type RadixProbeWorkspace struct {
	counts  []int
	starts  []int
	cursors []int
	rows    []int
	keys    []hashagg.Key128
	matched []bool
}

func (w *RadixProbeWorkspace) ensure(parts, rows int) {
	if cap(w.counts) < parts {
		w.counts = make([]int, parts)
		w.starts = make([]int, parts+1)
		w.cursors = make([]int, parts)
	} else {
		w.counts = w.counts[:parts]
		w.starts = w.starts[:parts+1]
		w.cursors = w.cursors[:parts]
		clear(w.counts)
	}
	if cap(w.rows) < rows {
		w.rows = make([]int, rows)
		w.keys = make([]hashagg.Key128, rows)
		w.matched = make([]bool, rows)
	} else {
		w.rows = w.rows[:rows]
		w.keys = w.keys[:rows]
		w.matched = w.matched[:rows]
		clear(w.matched)
	}
}

// Probe applies the radix join using a temporary workspace. Repeated physical
// executions should prefer ProbeWithWorkspace so partitioning scratch is reused.
func (t *RadixPairTable) Probe(keys []hashagg.Key128, validMasks []uint8, typ Type, emit EmitFunc) (Summary, error) {
	var w RadixProbeWorkspace
	return t.ProbeWithWorkspace(keys, validMasks, typ, emit, &w)
}

// ProbeWithWorkspace probes fixed-width rows while reusing caller-owned radix
// scratch. Inner joins physically partition the probe batch by hash prefix;
// left/semi/anti preserve input-row semantics with direct bucket-local probes.
func (t *RadixPairTable) ProbeWithWorkspace(keys []hashagg.Key128, validMasks []uint8, typ Type, emit EmitFunc, w *RadixProbeWorkspace) (Summary, error) {
	if validMasks != nil && len(validMasks) != len(keys) {
		return Summary{}, fmt.Errorf("radix pair join: key/validity length mismatch")
	}
	if w == nil {
		w = &RadixProbeWorkspace{}
	}
	var total Summary
	total.ProbeRows = len(keys)

	if typ == Inner {
		parts := len(t.buckets)
		w.ensure(parts, len(keys))
		validRows := 0
		for row, key := range keys {
			if validMasks != nil && validMasks[row]&3 != 3 {
				continue
			}
			w.counts[t.bucket(key)]++
			validRows++
		}
		w.starts[0] = 0
		for i := 0; i < parts; i++ {
			w.starts[i+1] = w.starts[i] + w.counts[i]
			w.cursors[i] = w.starts[i]
		}
		for row, key := range keys {
			if validMasks != nil && validMasks[row]&3 != 3 {
				continue
			}
			bi := t.bucket(key)
			pos := w.cursors[bi]
			w.cursors[bi]++
			w.rows[pos] = row
			w.keys[pos] = key
		}
		for bi := 0; bi < parts; bi++ {
			lo, hi := w.starts[bi], w.starts[bi+1]
			if lo == hi || t.buckets[bi] == nil {
				continue
			}
			_, err := t.buckets[bi].Probe(w.keys[lo:hi], nil, Inner, func(p Pair) error {
				probeRow := w.rows[lo+p.ProbeRow]
				w.matched[probeRow] = true
				total.OutputRows++
				if emit != nil {
					return emit(Pair{ProbeRow: probeRow, BuildRow: t.buildRows[bi][p.BuildRow]})
				}
				return nil
			})
			if err != nil {
				return Summary{}, err
			}
		}
		_ = validRows
		for _, ok := range w.matched {
			if ok {
				total.MatchedProbeRows++
			}
		}
		return total, nil
	}

	if typ != Left && typ != Semi && typ != Anti {
		return total, fmt.Errorf("unsupported join type %d", typ)
	}
	for row, key := range keys {
		valid := validMasks == nil || validMasks[row]&3 == 3
		matched := false
		if valid {
			bi := t.bucket(key)
			if table := t.buckets[bi]; table != nil {
				emitted := false
				s, err := table.Probe(keys[row:row+1], nil, Inner, func(p Pair) error {
					matched = true
					if typ == Anti {
						return nil
					}
					if typ == Semi && emitted {
						return nil
					}
					emitted = true
					total.OutputRows++
					if emit != nil {
						return emit(Pair{ProbeRow: row, BuildRow: t.buildRows[bi][p.BuildRow]})
					}
					return nil
				})
				if err != nil {
					return Summary{}, err
				}
				matched = matched || s.MatchedProbeRows != 0
			}
		}
		if matched {
			total.MatchedProbeRows++
		}
		switch typ {
		case Left:
			if !matched {
				total.OutputRows++
				if emit != nil {
					if err := emit(Pair{ProbeRow: row, BuildRow: -1}); err != nil {
						return total, err
					}
				}
			}
		case Semi:
			// The first matching build row was emitted by the callback above.
		case Anti:
			if !matched {
				total.OutputRows++
				if emit != nil {
					if err := emit(Pair{ProbeRow: row, BuildRow: -1}); err != nil {
						return total, err
					}
				}
			}
		}
	}
	return total, nil
}

func (t *RadixPairTable) bucket(key hashagg.Key128) int {
	return int(hashagg.Hash128(key) & t.mask)
}

// EstimateRadixPairTableBytes estimates retained bucket tables and global-row
// maps. The estimate intentionally reserves roughly twice the equivalent
// single-table directory to cover per-bucket power-of-two rounding and skew;
// it is an admission bound, not a heap profiler measurement.
func EstimateRadixPairTableBytes(buildRows int, bits uint8) int64 {
	if bits < 1 {
		bits = 1
	}
	if bits > 8 {
		bits = 8
	}
	parts := int64(1 << bits)
	base := EstimatePairTableBytes(buildRows, buildRows)
	return base*2 + int64(buildRows)*8 + parts*64
}
