package physical

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"sort"
	"unsafe"
)

type topKReader struct {
	query      *memory.QueryBudget
	closed     bool
	exhausted  bool
	owner      *TopKSource
	first      source.BatchStream
	scratch    *memory.Batch
	done       bool
	readOpts   source.ReadOptions
	workspace  *sortWorkspace
	output     *residentSortAccumulator
	borrowed   *memory.Batch
	emitted    int
	order      []int
	orderLease memory.Reservation
}

func (r *topKReader) clearBorrow() {
	if r.output != nil {
		r.output.clearOutput()
	}
	if r.borrowed != nil {
		r.borrowed.ClearColumns()
		r.borrowed = nil
	}
}
func (r *topKReader) release() {
	r.clearBorrow()
	if r.output != nil {
		r.output.close()
		r.output = nil
	}
	r.order = nil
	if r.orderLease != nil {
		r.orderLease.Release()
		r.orderLease = nil
	}
	if r.workspace != nil {
		r.workspace.close()
		r.workspace = nil
	}
}

// Close ends output borrows and releases retained candidates and upstream.
func (r *topKReader) Close() error {
	r.exhausted = false
	r.closed = true
	r.release()
	var err error
	if r.first != nil {
		first := r.first
		r.first = nil
		err = first.Close()
	}
	if r.scratch != nil {
		r.scratch.ClearColumns()
	}
	return err
}

// Next selects winners once, then emits owned results as revocable borrows.
func (r *topKReader) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	r.clearBorrow()
	if dst == nil {
		return false, errors.Join(fmt.Errorf("top-k destination is nil"), r.Close())
	}
	if r.closed {
		dst.ClearColumns()
		if !r.exhausted {
			return false, source.ErrBatchStreamClosed
		}
		return false, nil
	}
	if ctx == nil {
		return false, errors.Join(fmt.Errorf("top-k context is nil"), r.Close())
	}
	q := memory.QueryBudgetFromContext(ctx)
	if q != nil && q != r.query {
		return false, errors.Join(fmt.Errorf("top-k query budget changed"), r.Close())
	}
	if q == nil && r.query != nil {
		ctx = memory.WithQueryBudget(ctx, r.query)
	}
	if err := ctx.Err(); err != nil {
		return false, errors.Join(err, r.Close())
	}
	if !r.done {
		r.done = true
		if r.owner.k > 0 {
			if err := r.collect(ctx); err != nil {
				dst.ClearColumns()
				return false, errors.Join(err, r.Close())
			}
		}
	}
	if r.output == nil || r.emitted >= r.output.batch.Rows {
		dst.ClearColumns()
		err := r.Close()
		r.exhausted = true
		return false, err
	}
	row := -1
	if r.output.hasOpaque() {
		row = r.emitted
		if r.order != nil {
			row = r.order[r.emitted]
		}
	}
	if err := r.output.bindOutput(ctx, dst, row); err != nil {
		return false, errors.Join(err, r.Close())
	}
	r.borrowed = dst
	if row < 0 {
		r.emitted = r.output.batch.Rows
	} else {
		r.emitted++
	}
	return true, nil
}

// All retained capacities are admitted before allocation. In particular K and
// source statistics are bounds, not allocation sizes: unknown/small inputs pay
// for only the candidates actually seen.
func (r *topKReader) collect(ctx context.Context) (resultErr error) {
	manager := r.owner.manager
	if manager == nil {
		manager = r.readOpts.MemoryManager
	}
	if q := memory.QueryBudgetFromContext(ctx); q != nil {
		manager = q.Operator("TopKResident", memory.MemoryWorkspace)
	}
	if manager == nil {
		manager = memory.NewBudgetManager(0)
	}
	r.workspace = &sortWorkspace{parent: manager, bulkCredits: true}
	reservation, err := r.workspace.Reserve(ctx, 1024)
	if err != nil {
		return err
	}
	state := reservation.(*sortWorkspaceReservation)
	defer state.Release()
	admit := func(n int64) error {
		if n <= state.Bytes() {
			return ctx.Err()
		}
		return state.grow(ctx, n-state.Bytes())
	}
	h := &candidateHeap{keys: r.owner.keys}
	var candidateBytes, ordinal int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := r.first.Next(ctx, r.scratch)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		for row := 0; row < r.scratch.Rows; row++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if ordinal == math.MaxInt64 {
				return fmt.Errorf("top-k input row ordinal overflows int64")
			}
			insert := h.Len() < r.owner.k
			better := insert
			if !insert {
				better, err = batchRowLessCandidate(r.scratch, r.owner.keyInput.Schema(), r.owner.keys, row, ordinal, h.items[0])
				if err != nil {
					return err
				}
			}
			if better {
				// Snapshot into new exact-capacity cells instead of append's opaque growth.
				// The old key remains charged until its replacement has been copied.
				textBytes, err := sortKeyVariableBytes(r.scratch, r.owner.keyInput.Schema(), r.owner.keys, row)
				if err != nil {
					return err
				}
				cellsBytes := int64(len(r.owner.keys)) * int64(unsafe.Sizeof(sortCell{}))
				newBytes, err := sortWorkspaceSize(cellsBytes, textBytes)
				if err != nil {
					return err
				}
				oldHeap := int64(cap(h.items)) * int64(unsafe.Sizeof(rowCandidate{}))
				newCap := cap(h.items)
				if insert && len(h.items) == newCap {
					if newCap == 0 {
						newCap = 1
					} else if newCap > math.MaxInt/2 {
						return memory.ErrBudgetExceeded
					} else {
						newCap *= 2
					}
					newCap = min(newCap, r.owner.k)
				}
				heapBytes := int64(newCap) * int64(unsafe.Sizeof(rowCandidate{}))
				peak, err := sortWorkspaceSize(1024, candidateBytes, newBytes, oldHeap)
				if err != nil {
					return err
				}
				if newCap != cap(h.items) {
					peak, err = sortWorkspaceSize(peak, heapBytes)
					if err != nil {
						return err
					}
				}
				if err := admit(peak); err != nil {
					return fmt.Errorf("top-k candidate memory: %w", err)
				}
				if newCap != cap(h.items) {
					items := make([]rowCandidate, len(h.items), newCap)
					copy(items, h.items)
					h.items = items
				}
				cand := rowCandidate{ordinal: ordinal, cells: make([]sortCell, len(r.owner.keys))}
				for i, key := range r.owner.keys {
					cell, err := snapshotCell(r.scratch, r.owner.keyInput.Schema(), key.Column, row)
					if err != nil {
						return err
					}
					if cell.text != nil {
						b := make([]byte, len(cell.text))
						copy(b, cell.text)
						cell.text = b
					}
					cand.cells[i] = cell
				}
				if insert {
					h.items = append(h.items, cand)
					heap.Fix(h, len(h.items)-1)
					candidateBytes += newBytes
				} else {
					candidateBytes -= int64(cap(h.items[0].cells))*int64(unsafe.Sizeof(sortCell{})) + candidateVariableBytes(h.items[0])
					h.items[0] = cand
					candidateBytes += newBytes
					heap.Fix(h, 0)
				}
			}
			ordinal++
		}
	}
	first := r.first
	r.first = nil
	if err := first.Close(); err != nil {
		return err
	}
	if len(h.items) == 0 {
		return nil
	}
	// Sort in place and use compact arrays rather than maps or per-batch copies.
	sort.Slice(h.items, func(i, j int) bool { return candidateLess(h.items[i], h.items[j], r.owner.keys) })
	n := len(h.items)
	indexesBytes := int64(n)*int64(unsafe.Sizeof(topKOrdinal{})) + int64(n)*int64(unsafe.Sizeof(int(0)))
	indexLease, err := r.workspace.Reserve(ctx, indexesBytes+256)
	if err != nil {
		return err
	}
	defer func() {
		if indexLease != nil {
			indexLease.Release()
		}
	}()
	ordinals := make([]topKOrdinal, n)
	ranks := make([]int, n)
	for i, w := range h.items {
		ordinals[i] = topKOrdinal{ordinal: w.ordinal, rank: i}
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i].ordinal < ordinals[j].ordinal })
	for i, w := range ordinals {
		ranks[w.rank] = i
	}
	h.items = nil
	state.Release()
	r.workspace.trim(0)
	second, err := source.OpenBatchStream(ctx, r.owner.input, r.readOpts)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, second.Close()) }()
	var selected *residentSortAccumulator
	defer func() {
		if selected != nil {
			selected.close()
		}
	}()
	var replayOrdinal int64
	nextWinner := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := second.Next(ctx, r.scratch)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if int64(r.scratch.Rows) > math.MaxInt64-replayOrdinal {
			return fmt.Errorf("top-k replay row ordinal overflows int64")
		}
		end := replayOrdinal + int64(r.scratch.Rows)
		for nextWinner < n && ordinals[nextWinner].ordinal < end {
			winner := ordinals[nextWinner].ordinal
			if winner < replayOrdinal {
				return fmt.Errorf("top-k replay passed selected row ordinal %d", winner)
			}
			if selected == nil {
				selected, err = newResidentSortAccumulator(ctx, r.owner.input.Schema(), r.workspace, r.scratch)
				if err != nil {
					return err
				}
			}
			if err := selected.appendRow(ctx, r.scratch, int(winner-replayOrdinal)); err != nil {
				return err
			}
			nextWinner++
		}
		replayOrdinal = end
	}
	if replayOrdinal != ordinal {
		return fmt.Errorf("top-k replay changed row count: keys=%d payload=%d", ordinal, replayOrdinal)
	}
	if nextWinner != n {
		return fmt.Errorf("top-k replay selected %d of %d winner ordinals", nextWinner, n)
	}
	if err := selected.finish(ctx); err != nil {
		return err
	}
	if selected.hasOpaque() {
		r.output = selected
		selected = nil
		r.order = ranks
		r.orderLease = indexLease
		indexLease = nil
		return nil
	}
	r.output, err = newResidentSortedAccumulator(ctx, selected, ranks)
	return err
}

type topKOrdinal struct {
	ordinal int64
	rank    int
}

func sortKeyVariableBytes(batch *memory.Batch, schema memory.Schema, keys []SortKey, row int) (int64, error) {
	var bytes int64
	for _, key := range keys {
		cell, err := snapshotCell(batch, schema, key.Column, row)
		if err != nil {
			return 0, err
		}
		if int64(len(cell.text)) > math.MaxInt64-bytes {
			return 0, fmt.Errorf("top-k candidate string bytes overflow int64")
		}
		bytes += int64(len(cell.text))
	}
	return bytes, nil
}
func candidateVariableBytes(candidate rowCandidate) int64 {
	var bytes int64
	for _, cell := range candidate.cells {
		bytes += int64(cap(cell.text))
	}
	return bytes
}
