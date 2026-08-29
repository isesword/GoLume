package radix

import (
	"context"
	"gocolumn/hashagg"
	"gocolumn/kernel"
	"gocolumn/scheduler"
	"sync/atomic"
)

// PairGroup is one materialized fixed-width 128-bit radix-aggregation result.
// ValidMask uses bit 0 for Key.A and bit 1 for Key.B; a cleared bit represents
// the SQL NULL value for that key component.
type PairGroup struct {
	Key       hashagg.Key128
	ValidMask uint8
	Sum       int64
	Count     uint64
}

// PairWorkspace owns reusable buffers for partitioning and fixed-width 128-bit
// key aggregation. A PairWorkspace is not safe for concurrent Aggregate calls.
type PairWorkspace struct {
	bits                 uint
	parts                int
	shift                uint
	localCap             int
	localMask            uint64
	counts, starts       []int
	hashes               []uint64
	hashA, hashB         []uint64
	pkeys                []hashagg.Key128
	pvalid               []uint8
	pvals                []int64
	phashes              []uint64
	ta, tb               []uint64
	tvalid               []uint8
	tableSums            []int64
	tableCounts          []uint64
	epoch                []uint32
	gen                  uint32
	localCounts, offsets []int
	partial              []Summary
	kernels              kernel.Set
}

// NewPairWorkspace allocates reusable buffers for pair-key radix aggregation.
func NewPairWorkspace(maxRows, cardinalityHint int, radixBits uint, kernels kernel.Set) *PairWorkspace {
	if !kernels.Valid() {
		panic("radix: NewPairWorkspace requires a valid kernel set")
	}
	if radixBits < 1 {
		radixBits = 1
	}
	if radixBits > 12 {
		radixBits = 12
	}
	parts := 1 << radixBits
	lc := nextPow2(((cardinalityHint + parts - 1) / parts) * 2)
	if lc < 16 {
		lc = 16
	}
	slots := parts * lc
	return &PairWorkspace{
		bits: radixBits, parts: parts, shift: 64 - radixBits, localCap: lc, localMask: uint64(lc - 1),
		counts: make([]int, parts), starts: make([]int, parts+1), hashes: make([]uint64, maxRows), hashA: make([]uint64, maxRows), hashB: make([]uint64, maxRows),
		pkeys: make([]hashagg.Key128, maxRows), pvalid: make([]uint8, maxRows), pvals: make([]int64, maxRows), phashes: make([]uint64, maxRows),
		ta: make([]uint64, slots), tb: make([]uint64, slots), tvalid: make([]uint8, slots),
		tableSums: make([]int64, slots), tableCounts: make([]uint64, slots), epoch: make([]uint32, slots), kernels: kernels,
	}
}

func (p *PairWorkspace) nextGen() uint32 {
	p.gen++
	if p.gen == 0 {
		clear(p.epoch)
		p.gen = 1
	}
	return p.gen
}

func (p *PairWorkspace) ensureScratch(workers int) {
	n := workers * p.parts
	if cap(p.localCounts) < n {
		p.localCounts = make([]int, n)
	} else {
		p.localCounts = p.localCounts[:n]
	}
	if cap(p.offsets) < n {
		p.offsets = make([]int, n)
	} else {
		p.offsets = p.offsets[:n]
	}
	if cap(p.partial) < workers {
		p.partial = make([]Summary, workers)
	} else {
		p.partial = p.partial[:workers]
	}
	clear(p.localCounts)
	clear(p.partial)
}

func normalizePairKey(k hashagg.Key128, validMask uint8) hashagg.Key128 {
	if validMask&1 == 0 {
		k.A = 0
	}
	if validMask&2 == 0 {
		k.B = 0
	}
	return k
}

// Aggregate radix-partitions fully valid fixed-width pair keys and performs
// partition-owned SUM/COUNT aggregation. It returns ErrLocalTableFull when a
// partition exceeds the estimated local cardinality.
func (p *PairWorkspace) Aggregate(ctx context.Context, sched scheduler.Scheduler, keys []hashagg.Key128, vals []int64, workers int) (Summary, error) {
	return p.AggregateNullable(ctx, sched, keys, nil, vals, workers)
}

// AggregateNullable radix-partitions fixed-width pair keys while preserving
// SQL GROUP BY NULL semantics. validMasks uses bit 0/1 for the validity of
// Key.A/Key.B. A nil validMasks slice means every key component is valid.
func (p *PairWorkspace) AggregateNullable(ctx context.Context, sched scheduler.Scheduler, keys []hashagg.Key128, validMasks []uint8, vals []int64, workers int) (Summary, error) {
	if len(keys) != len(vals) {
		panic("radix: pair key/value length mismatch")
	}
	if validMasks != nil && len(validMasks) != len(keys) {
		panic("radix: pair key/validity length mismatch")
	}
	if len(keys) > len(p.hashes) {
		panic("radix: pair workspace maxRows exceeded")
	}
	if workers < 1 {
		workers = 1
	}
	if workers > sched.MaxWorkers() {
		workers = sched.MaxWorkers()
	}
	p.ensureScratch(workers)
	local := p.localCounts
	if err := sched.ParallelFor(ctx, workers, workers, func(w int) error {
		lo, hi := shard(len(keys), workers, w)
		cnt := local[w*p.parts : (w+1)*p.parts]
		for i := lo; i < hi; i++ {
			p.hashA[i], p.hashB[i] = keys[i].A, keys[i].B
		}
		var masks []uint8
		if validMasks != nil {
			masks = validMasks[lo:hi]
		}
		p.kernels.Hash128Batch(p.hashA[lo:hi], p.hashB[lo:hi], masks, p.hashes[lo:hi])
		for i := lo; i < hi; i++ {
			h := p.hashes[i]
			cnt[int(h>>p.shift)]++
		}
		return nil
	}); err != nil {
		return Summary{}, err
	}

	total := 0
	for part := 0; part < p.parts; part++ {
		p.starts[part] = total
		pos := total
		for w := 0; w < workers; w++ {
			idx := w*p.parts + part
			p.offsets[idx] = pos
			pos += local[idx]
		}
		total = pos
	}
	p.starts[p.parts] = total

	if err := sched.ParallelFor(ctx, workers, workers, func(w int) error {
		lo, hi := shard(len(keys), workers, w)
		cur := p.offsets[w*p.parts : (w+1)*p.parts]
		for i := lo; i < hi; i++ {
			vm := uint8(3)
			if validMasks != nil {
				vm = validMasks[i] & 3
			}
			k := normalizePairKey(keys[i], vm)
			h := p.hashes[i]
			part := int(h >> p.shift)
			pos := cur[part]
			cur[part]++
			p.pkeys[pos] = k
			p.pvalid[pos] = vm
			p.pvals[pos] = vals[i]
			p.phashes[pos] = h
		}
		return nil
	}); err != nil {
		return Summary{}, err
	}

	gen := p.nextGen()
	var next atomic.Uint32
	partial := p.partial
	if err := sched.ParallelFor(ctx, workers, workers, func(w int) error {
		var s Summary
		for {
			part := int(next.Add(1) - 1)
			if part >= p.parts {
				break
			}
			base := part * p.localCap
			for i := p.starts[part]; i < p.starts[part+1]; i++ {
				k := p.pkeys[i]
				vm := p.pvalid[i]
				pos := uint64(base) + (p.phashes[i] & p.localMask)
				for probes := 0; ; probes++ {
					if p.epoch[pos] != gen {
						p.epoch[pos] = gen
						p.ta[pos], p.tb[pos], p.tvalid[pos] = k.A, k.B, vm
						p.tableSums[pos] = p.pvals[i]
						p.tableCounts[pos] = 1
						s.Groups++
						s.Rows++
						s.Checksum += p.pvals[i]
						break
					}
					if p.tvalid[pos] == vm && p.ta[pos] == k.A && p.tb[pos] == k.B {
						p.tableSums[pos] += p.pvals[i]
						p.tableCounts[pos]++
						s.Rows++
						s.Checksum += p.pvals[i]
						break
					}
					if probes+1 >= p.localCap {
						return ErrLocalTableFull
					}
					pos = uint64(base) + ((pos - uint64(base) + 1) & p.localMask)
				}
			}
		}
		partial[w] = s
		return nil
	}); err != nil {
		return Summary{}, err
	}

	var out Summary
	for _, s := range partial {
		out.Groups += s.Groups
		out.Rows += s.Rows
		out.Checksum += s.Checksum
	}
	return out, nil
}

// Materialize appends the current generation of pair-key groups into reusable
// dst storage, including validity masks for nullable key components.
func (p *PairWorkspace) Materialize(dst []PairGroup) []PairGroup {
	dst = dst[:0]
	for i := range p.epoch {
		if p.epoch[i] == p.gen {
			dst = append(dst, PairGroup{Key: hashagg.Key128{A: p.ta[i], B: p.tb[i]}, ValidMask: p.tvalid[i], Sum: p.tableSums[i], Count: p.tableCounts[i]})
		}
	}
	return dst
}

// EstimatePairWorkspaceBytes estimates retained bytes required by a pair-key
// radix workspace, including nullable-key validity buffers.
func EstimatePairWorkspaceBytes(maxRows, cardinalityHint int, radixBits uint, workers int) int64 {
	if radixBits < 1 {
		radixBits = 1
	}
	if radixBits > 12 {
		radixBits = 12
	}
	parts := 1 << radixBits
	lc := nextPow2(((cardinalityHint + parts - 1) / parts) * 2)
	if lc < 16 {
		lc = 16
	}
	slots := parts * lc
	return int64(maxRows)*(8+8+8+16+1+8+8) + int64(slots)*(8+8+1+8+8+4) + int64(parts+1)*8 + int64(workers*parts)*16 + int64(workers)*24
}
