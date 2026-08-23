package join

import (
	"context"
	"fmt"
	"gocolumn/hashagg"
)

// ParallelExecutor is the minimal scheduler contract required by parallel radix
// build/probe. Implementations must execute each task index at most once.
type ParallelExecutor interface {
	MaxWorkers() int
	ParallelFor(ctx context.Context, tasks, parallelism int, fn func(task int) error) error
}

// ParallelRadixProbeWorkspace retains partitioning and per-bucket result buffers
// across repeated probes. One workspace belongs to one reader and must not be
// shared by concurrent Execute calls.
type ParallelRadixProbeWorkspace struct {
	Base      RadixProbeWorkspace
	Pairs     [][]Pair
	Summaries []Summary
	NullPairs []Pair
}

func (w *ParallelRadixProbeWorkspace) ensure(parts int) {
	if cap(w.Pairs) < parts {
		w.Pairs = make([][]Pair, parts)
		w.Summaries = make([]Summary, parts)
	} else {
		w.Pairs = w.Pairs[:parts]
		w.Summaries = w.Summaries[:parts]
		clear(w.Summaries)
	}
	for i := range w.Pairs {
		w.Pairs[i] = w.Pairs[i][:0]
	}
	w.NullPairs = w.NullPairs[:0]
}

// BuildParallel partitions build keys once and constructs independent radix
// buckets concurrently. The resulting table is immutable during probe and safe
// for concurrent readers.
func (t *RadixPairTable) BuildParallel(ctx context.Context, keys []hashagg.Key128, validMasks []uint8, workers int, exec ParallelExecutor) error {
	if exec == nil || workers <= 1 {
		return t.Build(keys, validMasks)
	}
	if validMasks != nil && len(validMasks) != len(keys) {
		return fmt.Errorf("radix pair join: key/validity length mismatch")
	}
	parts := len(t.buckets)
	counts := make([]int, parts)
	for i, key := range keys {
		if validMasks != nil && validMasks[i]&3 != 3 {
			continue
		}
		counts[t.bucket(key)]++
	}
	bucketKeys := make([][]hashagg.Key128, parts)
	bucketMasks := make([][]uint8, parts)
	for i := 0; i < parts; i++ {
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
	multiplicity := make([]int, parts)
	if workers > exec.MaxWorkers() {
		workers = exec.MaxWorkers()
	}
	if err := exec.ParallelFor(ctx, parts, workers, func(i int) error {
		if len(bucketKeys[i]) == 0 {
			t.buckets[i] = nil
			return nil
		}
		if t.buckets[i] == nil {
			t.buckets[i] = NewPairTable(len(bucketKeys[i]))
		}
		if err := t.buckets[i].Build(bucketKeys[i], bucketMasks[i]); err != nil {
			return err
		}
		multiplicity[i] = t.buckets[i].MaxMultiplicity()
		return nil
	}); err != nil {
		return err
	}
	t.maxMultiplicity = 0
	for _, m := range multiplicity {
		if m > t.maxMultiplicity {
			t.maxMultiplicity = m
		}
	}
	return nil
}

// ProbeParallelWithWorkspace partitions one probe batch by radix owner and
// probes independent bucket-local hash tables concurrently. Result callbacks are
// replayed serially after workers finish, so callers never need a thread-safe
// emit function. Output row ordering is unspecified unless a downstream Sort is
// present, matching relational join semantics.
func (t *RadixPairTable) ProbeParallelWithWorkspace(ctx context.Context, keys []hashagg.Key128, validMasks []uint8, typ Type, emit EmitFunc, workers int, exec ParallelExecutor, w *ParallelRadixProbeWorkspace) (Summary, error) {
	if exec == nil || workers <= 1 {
		if w == nil {
			w = &ParallelRadixProbeWorkspace{}
		}
		return t.ProbeWithWorkspace(keys, validMasks, typ, emit, &w.Base)
	}
	if validMasks != nil && len(validMasks) != len(keys) {
		return Summary{}, fmt.Errorf("radix pair join: key/validity length mismatch")
	}
	if typ != Inner && typ != Left && typ != Semi && typ != Anti {
		return Summary{}, fmt.Errorf("unsupported join type %d", typ)
	}
	if w == nil {
		w = &ParallelRadixProbeWorkspace{}
	}
	parts := len(t.buckets)
	w.ensure(parts)
	w.Base.ensure(parts, len(keys))

	for row, key := range keys {
		if validMasks != nil && validMasks[row]&3 != 3 {
			if typ == Left || typ == Anti {
				w.NullPairs = append(w.NullPairs, Pair{ProbeRow: row, BuildRow: -1})
			}
			continue
		}
		w.Base.counts[t.bucket(key)]++
	}
	w.Base.starts[0] = 0
	for i := 0; i < parts; i++ {
		w.Base.starts[i+1] = w.Base.starts[i] + w.Base.counts[i]
		w.Base.cursors[i] = w.Base.starts[i]
	}
	for row, key := range keys {
		if validMasks != nil && validMasks[row]&3 != 3 {
			continue
		}
		bi := t.bucket(key)
		pos := w.Base.cursors[bi]
		w.Base.cursors[bi]++
		w.Base.rows[pos] = row
		w.Base.keys[pos] = key
	}

	if workers > exec.MaxWorkers() {
		workers = exec.MaxWorkers()
	}
	if err := exec.ParallelFor(ctx, parts, workers, func(bi int) error {
		lo, hi := w.Base.starts[bi], w.Base.starts[bi+1]
		if lo == hi {
			return nil
		}
		pairs := w.Pairs[bi][:0]
		table := t.buckets[bi]
		if table == nil {
			if typ == Left || typ == Anti {
				for pos := lo; pos < hi; pos++ {
					pairs = append(pairs, Pair{ProbeRow: w.Base.rows[pos], BuildRow: -1})
				}
				w.Summaries[bi] = Summary{ProbeRows: hi - lo, OutputRows: hi - lo}
			} else {
				w.Summaries[bi] = Summary{ProbeRows: hi - lo}
			}
			w.Pairs[bi] = pairs
			return nil
		}
		if emit == nil {
			summary, err := table.Probe(w.Base.keys[lo:hi], nil, typ, nil)
			w.Summaries[bi] = summary
			w.Pairs[bi] = pairs
			return err
		}
		summary, err := table.Probe(w.Base.keys[lo:hi], nil, typ, func(p Pair) error {
			p.ProbeRow = w.Base.rows[lo+p.ProbeRow]
			if p.BuildRow >= 0 {
				p.BuildRow = t.buildRows[bi][p.BuildRow]
			}
			pairs = append(pairs, p)
			return nil
		})
		w.Pairs[bi] = pairs
		w.Summaries[bi] = summary
		return err
	}); err != nil {
		return Summary{}, err
	}

	total := Summary{ProbeRows: len(keys)}
	for bi := 0; bi < parts; bi++ {
		total.MatchedProbeRows += w.Summaries[bi].MatchedProbeRows
		total.OutputRows += w.Summaries[bi].OutputRows
		if emit != nil {
			for _, p := range w.Pairs[bi] {
				if err := emit(p); err != nil {
					return total, err
				}
			}
		}
	}
	total.OutputRows += len(w.NullPairs)
	if emit != nil {
		for _, p := range w.NullPairs {
			if err := emit(p); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}
