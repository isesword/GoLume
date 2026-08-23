package radix

import (
	"context"
	"errors"
	"gocolumn/hashagg"
	"gocolumn/scheduler"
	"math/bits"
	"sync/atomic"
)

// Group is one materialized uint64 radix-aggregation result.
type Group struct {
	Key   uint64
	Sum   int64
	Count uint64
}

// ErrLocalTableFull indicates that the per-partition cardinality exceeded the
// radix workspace estimate. Callers may retry with an adaptive global table.
var ErrLocalTableFull = errors.New("radix local aggregation table full")

// Summary reports aggregate cardinality, checksum, and consumed rows without materializing groups.
type Summary struct {
	Groups   int
	Checksum int64
	Rows     int
}

// Workspace owns reusable buffers for partitioning and uint64 radix aggregation.
// A Workspace is not safe for concurrent Aggregate calls; callers should lease one per execution.
type Workspace struct {
	bits      uint
	parts     int
	shift     uint
	localCap  int
	localMask uint64

	counts  []int
	starts  []int
	hashes  []uint64
	pkeys   []uint64
	pvals   []int64
	phashes []uint64

	tableKeys   []uint64
	tableSums   []int64
	tableCounts []uint64
	epoch       []uint32
	gen         uint32

	localCounts []int
	offsets     []int
	partial     []Summary
}

func nextPow2(v int) int {
	if v <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(v-1))
}

// NewWorkspace allocates reusable buffers sized for maxRows and the estimated group cardinality.
// cardinalityHint affects local-table sizing only; callers may fall back when ErrLocalTableFull is returned.
func NewWorkspace(maxRows, cardinalityHint int, radixBits uint) *Workspace {
	if radixBits < 1 {
		radixBits = 1
	}
	if radixBits > 12 {
		radixBits = 12
	}
	parts := 1 << radixBits
	localCap := nextPow2(((cardinalityHint + parts - 1) / parts) * 2)
	if localCap < 16 {
		localCap = 16
	}
	slots := parts * localCap
	return &Workspace{
		bits: radixBits, parts: parts, shift: 64 - radixBits, localCap: localCap, localMask: uint64(localCap - 1),
		counts: make([]int, parts), starts: make([]int, parts+1), hashes: make([]uint64, maxRows),
		pkeys: make([]uint64, maxRows), pvals: make([]int64, maxRows), phashes: make([]uint64, maxRows),
		tableKeys: make([]uint64, slots), tableSums: make([]int64, slots), tableCounts: make([]uint64, slots), epoch: make([]uint32, slots),
	}
}

func (w *Workspace) nextGen() uint32 {
	w.gen++
	if w.gen == 0 {
		clear(w.epoch)
		w.gen = 1
	}
	return w.gen
}

// Aggregate radix-partitions keys and performs partition-owned SUM/COUNT aggregation.
// It returns ErrLocalTableFull when a partition exceeds the pre-sized local table.
func (w *Workspace) Aggregate(ctx context.Context, sched scheduler.Scheduler, keys []uint64, vals []int64, workers int) (Summary, error) {
	if len(keys) != len(vals) {
		panic("radix: key/value length mismatch")
	}
	if len(keys) > len(w.hashes) {
		panic("radix: workspace maxRows exceeded")
	}
	if workers < 1 {
		workers = 1
	}
	if workers > sched.MaxWorkers() {
		workers = sched.MaxWorkers()
	}
	clear(w.counts)

	// Hash + histogram. Each task gets a private histogram to avoid cache-line contention.
	w.ensureWorkerScratch(workers)
	localCounts := w.localCounts[:workers*w.parts]
	clear(localCounts)
	if err := sched.ParallelFor(ctx, workers, workers, func(worker int) error {
		lo, hi := shard(len(keys), workers, worker)
		cnt := localCounts[worker*w.parts : (worker+1)*w.parts]
		for i := lo; i < hi; i++ {
			h := hashagg.Hash64(keys[i])
			w.hashes[i] = h
			cnt[int(h>>w.shift)]++
		}
		return nil
	}); err != nil {
		return Summary{}, err
	}

	total := 0
	offsets := w.offsets[:len(localCounts)]
	for p := 0; p < w.parts; p++ {
		w.starts[p] = total
		pos := total
		for worker := 0; worker < workers; worker++ {
			idx := worker*w.parts + p
			offsets[idx] = pos
			pos += localCounts[idx]
		}
		total = pos
	}
	w.starts[w.parts] = total

	if err := sched.ParallelFor(ctx, workers, workers, func(worker int) error {
		lo, hi := shard(len(keys), workers, worker)
		cur := offsets[worker*w.parts : (worker+1)*w.parts]
		for i := lo; i < hi; i++ {
			h := w.hashes[i]
			p := int(h >> w.shift)
			pos := cur[p]
			cur[p]++
			w.pkeys[pos] = keys[i]
			w.pvals[pos] = vals[i]
			w.phashes[pos] = h
		}
		return nil
	}); err != nil {
		return Summary{}, err
	}

	gen := w.nextGen()
	var next atomic.Uint32
	partial := w.partial[:workers]
	clear(partial)
	if err := sched.ParallelFor(ctx, workers, workers, func(worker int) error {
		var s Summary
		for {
			part := int(next.Add(1) - 1)
			if part >= w.parts {
				break
			}
			base := part * w.localCap
			for i := w.starts[part]; i < w.starts[part+1]; i++ {
				k := w.pkeys[i]
				pos := uint64(base) + (w.phashes[i] & w.localMask)
				for probes := 0; ; probes++ {
					if w.epoch[pos] != gen {
						w.epoch[pos] = gen
						w.tableKeys[pos] = k
						w.tableSums[pos] = w.pvals[i]
						w.tableCounts[pos] = 1
						s.Groups++
						s.Checksum += w.pvals[i]
						s.Rows++
						break
					}
					if w.tableKeys[pos] == k {
						w.tableSums[pos] += w.pvals[i]
						w.tableCounts[pos]++
						s.Checksum += w.pvals[i]
						s.Rows++
						break
					}
					if probes+1 >= w.localCap {
						return ErrLocalTableFull
					}
					pos = uint64(base) + ((pos - uint64(base) + 1) & w.localMask)
				}
			}
		}
		partial[worker] = s
		return nil
	}); err != nil {
		return Summary{}, err
	}
	var out Summary
	for _, s := range partial {
		out.Groups += s.Groups
		out.Checksum += s.Checksum
		out.Rows += s.Rows
	}
	return out, nil
}

// Materialize appends the current generation of groups into reusable dst storage.
func (w *Workspace) Materialize(dst []Group) []Group {
	dst = dst[:0]
	for i := range w.epoch {
		if w.epoch[i] == w.gen {
			dst = append(dst, Group{Key: w.tableKeys[i], Sum: w.tableSums[i], Count: w.tableCounts[i]})
		}
	}
	return dst
}

func (w *Workspace) ensureWorkerScratch(workers int) {
	n := workers * w.parts
	if cap(w.localCounts) < n {
		w.localCounts = make([]int, n)
	} else {
		w.localCounts = w.localCounts[:n]
	}
	if cap(w.offsets) < n {
		w.offsets = make([]int, n)
	} else {
		w.offsets = w.offsets[:n]
	}
	if cap(w.partial) < workers {
		w.partial = make([]Summary, workers)
	} else {
		w.partial = w.partial[:workers]
	}
}

// EstimateWorkspaceBytes estimates retained bytes required by a uint64 radix workspace.
func EstimateWorkspaceBytes(maxRows, cardinalityHint int, radixBits uint, workers int) int64 {
	if radixBits < 1 {
		radixBits = 1
	}
	if radixBits > 12 {
		radixBits = 12
	}
	parts := 1 << radixBits
	localCap := nextPow2(((cardinalityHint + parts - 1) / parts) * 2)
	if localCap < 16 {
		localCap = 16
	}
	slots := parts * localCap
	// hashes/pkeys/pvals/phashes + table key/sum/count/epoch + histograms/offsets/partials.
	return int64(maxRows)*(8+8+8+8) + int64(slots)*(8+8+8+4) + int64(parts+1)*8 + int64(workers*parts)*16 + int64(workers)*24
}

func shard(n, workers, worker int) (int, int) {
	chunk := (n + workers - 1) / workers
	lo := worker * chunk
	hi := lo + chunk
	if lo > n {
		lo = n
	}
	if hi > n {
		hi = n
	}
	return lo, hi
}
