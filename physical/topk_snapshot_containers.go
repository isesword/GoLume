package physical

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"
	"unsafe"

	"gocolumn/memory"
	"gocolumn/source"
)

// topKContainerSnapshotStream never relies on Stats or on replaying a source. Every
// admitted row owns its typed payload and dictionaries and retains opaque
// Arrow backing buffers. Those buffers are charged in full, not by the size
// of the logical Arrow slice. Retained bound is K; atomic replacement holds K+1.
// This is not K fixed-size
// rows; variable-width values or a large opaque backing may exhaust a budget.
// Typed winners are emitted in owned pages of at most 64 rows; opaque payloads
// retain one-row output without arbitrary Arrow concatenation.
type topKContainerSnapshotStream struct {
	state         unaryStreamState
	owner         *TopKSource
	manager       memory.Manager
	query         *memory.QueryBudget
	input         *memory.Batch
	borrowed      *memory.Batch
	heap          topKContainerSnapshotHeap
	indexMem      memory.Reservation
	baseMem       memory.Reservation
	page          *residentSortAccumulator
	pageWorkspace *sortWorkspace
	pageRows      int
	prepared      bool
	next          int
	rows          int64
}

type topKContainerSnapshot struct {
	key       rowCandidate
	batch     *memory.Batch
	lease     memory.Reservation
	owner     *residentSortAccumulator
	workspace *sortWorkspace
}

func (c *topKContainerSnapshot) release() {
	c.key = rowCandidate{}
	if c.owner != nil {
		c.owner.close()
	}
	if c.workspace != nil {
		c.workspace.close()
	}
	if c.lease != nil {
		c.lease.Release()
	}
	*c = topKContainerSnapshot{}
}

type topKContainerSnapshotHeap struct {
	items []topKContainerSnapshot
	keys  []SortKey
}

// Len returns the number of retained snapshots.
func (h topKContainerSnapshotHeap) Len() int { return len(h.items) }

// Less puts the worst candidate at the root, with input ordinals breaking ties.
func (h topKContainerSnapshotHeap) Less(i, j int) bool {
	return candidateLess(h.items[j].key, h.items[i].key, h.keys)
}

// Swap exchanges two snapshot slots.
func (h topKContainerSnapshotHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

// Push appends an already admitted snapshot.
func (h *topKContainerSnapshotHeap) Push(v any) { h.items = append(h.items, v.(topKContainerSnapshot)) }

// Pop removes the final snapshot without retaining its buffers in the heap.
func (h *topKContainerSnapshotHeap) Pop() any {
	n := len(h.items) - 1
	v := h.items[n]
	h.items[n] = topKContainerSnapshot{}
	h.items = h.items[:n]
	return v
}

func newTopKContainerSnapshotStream(ctx context.Context, owner *TopKSource, opts source.ReadOptions) (*topKContainerSnapshotStream, error) {
	manager := owner.manager
	query := memory.QueryBudgetFromContext(ctx)
	if query != nil {
		manager = query.Operator("TopKSnapshot", memory.MemoryRetainedState)
		opts.MemoryManager = query.Operator("SourceScan", memory.MemoryWorkspace)
	} else if manager == nil {
		manager = opts.MemoryManager
	}
	if manager == nil {
		manager = memory.NewBudgetManager(0)
	}
	// Covers stream/input-map headers and transient validation workspace objects.
	base, err := manager.Reserve(ctx, 4096)
	if err != nil {
		return nil, err
	}
	s := &topKContainerSnapshotStream{owner: owner, manager: manager, query: query, input: memory.NewBatch(), baseMem: base}
	s.pageRows = 64
	if opts.BatchRows > 0 && opts.BatchRows < s.pageRows {
		s.pageRows = opts.BatchRows
	}
	s.heap.keys = owner.keys
	s.state.closeOwn = s.cleanup
	// K=0 has no input dependency and must not open a one-shot source.
	if owner.k > 0 {
		child, err := source.OpenBatchStream(ctx, owner.input, opts)
		if err != nil {
			s.cleanup()
			return nil, err
		}
		s.state.child = child
	}
	return s, nil
}

func (s *topKContainerSnapshotStream) reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	if s.manager == nil {
		return nil, ctx.Err()
	}
	return s.manager.Reserve(ctx, bytes)
}

func (s *topKContainerSnapshotStream) growIndex(ctx context.Context) error {
	if len(s.heap.items) < cap(s.heap.items) {
		return nil
	}
	capacity := cap(s.heap.items)
	if capacity == 0 {
		capacity = 1
	} else if capacity > s.owner.k/2 {
		capacity = s.owner.k
	} else {
		capacity *= 2
	}
	width := int64(unsafe.Sizeof(topKContainerSnapshot{}))
	if int64(capacity) > math.MaxInt64/width {
		return fmt.Errorf("top-k snapshot index overflows int64")
	}
	// Charge both old and new backing arrays until the copy is complete.
	lease, err := s.reserve(ctx, int64(capacity)*width)
	if err != nil {
		return fmt.Errorf("top-k snapshot index: %w", err)
	}
	items := make([]topKContainerSnapshot, len(s.heap.items), capacity)
	copy(items, s.heap.items)
	s.heap.items = items
	if s.indexMem != nil {
		s.indexMem.Release()
	}
	s.indexMem = lease
	return nil
}

func (s *topKContainerSnapshotStream) prepare(ctx context.Context) error {
	for {
		ok, err := s.state.child.Next(ctx, s.input)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !ok {
			break
		}
		validation := &sortWorkspace{parent: s.manager}
		err = validateResidentExtendedInput(ctx, s.owner.input.Schema(), s.input, validation)
		validation.close()
		if err != nil {
			return err
		}
		for row := 0; row < s.input.Rows; row++ {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if s.rows == math.MaxInt64 {
				return fmt.Errorf("top-k input row ordinal overflows int64")
			}
			admit := s.heap.Len() < s.owner.k
			if !admit {
				admit, err = batchRowLessCandidate(s.input, s.owner.Schema(), s.owner.keys, row, s.rows, s.heap.items[0].key)
				if err != nil {
					return err
				}
			}
			if admit {
				replace := s.heap.Len() == s.owner.k
				if !replace {
					if err := s.growIndex(ctx); err != nil {
						return err
					}
				}

				candidate, err := s.snapshot(ctx, row)
				if err != nil {
					return err
				}
				if replace {
					old := s.heap.items[0]
					s.heap.items[0] = candidate
					heap.Fix(&s.heap, 0)
					old.release()
				} else {
					heap.Push(&s.heap, candidate)
				}
			}
			s.rows++
		}
	}
	// Close upstream before exposing results; close failures must not become a
	// successful partial result. Snapshots are independent of borrowed inputs.
	child := s.state.child
	s.state.child = nil
	if err := child.Close(); err != nil {
		return err
	}
	s.input.ClearColumns()
	sort.Slice(s.heap.items, func(i, j int) bool {
		return candidateLess(s.heap.items[i].key, s.heap.items[j].key, s.owner.keys)
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	s.prepared = true
	return nil
}

func (s *topKContainerSnapshotStream) clearOutput() {
	if s.page != nil {
		s.page.close()
		s.page = nil
		s.pageWorkspace.close()
		s.pageWorkspace = nil
		s.borrowed = nil
		return
	}
	if s.borrowed != nil {
		s.borrowed.ClearColumns()
		s.borrowed = nil
		s.heap.items[s.next-1].release()
	}
}

// bindPage materializes at most 64 typed winners, never all K. Each copied
// snapshot stays charged until its row append succeeds; all still-live winners
// overlap the page owner. Opaque backing has no generic concatenation contract.
func (s *topKContainerSnapshotStream) bindPage(ctx context.Context, dst *memory.Batch) error {
	c := &s.heap.items[s.next]
	s.pageWorkspace = &sortWorkspace{parent: s.manager}
	page, err := newResidentSortAccumulator(ctx, c.owner.schema, s.pageWorkspace, c.owner.batch)
	if err != nil {
		s.pageWorkspace.close()
		s.pageWorkspace = nil
		return err
	}
	s.page = page
	limit := s.pageRows
	if limit <= 0 || limit > 64 {
		limit = 64
	}
	for count := 0; count < limit && s.next < len(s.heap.items); count++ {
		c = &s.heap.items[s.next]
		if c.owner.hasOpaque() {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := page.appendFrom(ctx, c.owner, 0); err != nil {
			return err
		}
		c.release()
		s.next++
	}
	if err := page.finish(ctx); err != nil {
		return err
	}
	if err := page.bindOutput(ctx, dst, -1); err != nil {
		return err
	}
	s.borrowed = dst
	return nil
}

// Next scans once, then emits bounded typed pages or one-row opaque views.
func (s *topKContainerSnapshotStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.clearOutput()
	if stop, err := s.state.before(ctx, dst); stop {
		if err != nil && !s.state.closed && s.state.terminalErr == nil {
			err = s.state.fail(err)
		}
		return false, err
	}
	dst.ClearColumns()
	if query := memory.QueryBudgetFromContext(ctx); query != nil && query != s.query {
		return false, s.state.fail(fmt.Errorf("top-k stream cannot change query ownership"))
	} else if query == nil && s.query != nil {
		ctx = memory.WithQueryBudget(ctx, s.query)
	}
	if s.owner.k == 0 {
		return false, s.state.finish()
	}
	if !s.prepared {
		if err := s.prepare(ctx); err != nil {
			return false, s.state.fail(err)
		}
	}
	if s.next == len(s.heap.items) {
		return false, s.state.finish()
	}
	candidate := &s.heap.items[s.next]
	if !candidate.owner.hasOpaque() {
		if err := s.bindPage(ctx, dst); err != nil {
			return false, s.state.fail(err)
		}
		return true, nil
	}
	row := -1
	if candidate.owner.hasOpaque() {
		row = 0
	}
	if err := candidate.owner.bindOutput(ctx, dst, row); err != nil {
		return false, s.state.fail(err)
	}
	s.borrowed = dst
	s.next++
	return true, nil
}

func (s *topKContainerSnapshotStream) cleanup() error {
	s.clearOutput()
	for i := range s.heap.items {
		s.heap.items[i].release()
	}
	s.heap.items = nil
	if s.indexMem != nil {
		s.indexMem.Release()
		s.indexMem = nil
	}
	var err error
	if s.state.child != nil {
		err = s.state.child.Close()
		s.state.child = nil
	}
	s.input.ClearColumns()
	if s.baseMem != nil {
		s.baseMem.Release()
		s.baseMem = nil
	}
	return err
}

// Close releases snapshots, borrowed output, reservations, and upstream once.
func (s *topKContainerSnapshotStream) Close() error { return s.state.Close() }

// StreamDiagnostics reports the selected one-pass strategy and rows scanned.
func (s *topKContainerSnapshotStream) StreamDiagnostics() map[string]string {
	return map[string]string{"strategy": "one-pass-snapshots", "input_rows": fmt.Sprint(s.rows), "output_rows": fmt.Sprint(s.next)}
}

var _ source.BatchStream = (*topKContainerSnapshotStream)(nil)
var _ source.BatchStreamDiagnostics = (*topKContainerSnapshotStream)(nil)

// The old worst candidate remains owned and charged until this entire new row
// and its keys are admitted. Per-candidate workspaces end on eviction, avoiding
// a historical winner-credit high-water mark growing with input length.
func (s *topKContainerSnapshotStream) snapshot(ctx context.Context, row int) (_ topKContainerSnapshot, err error) {
	bytes, err := sortWorkspaceSize(2048, int64(len(s.owner.keys))*int64(unsafe.Sizeof(sortCell{})))
	if err != nil {
		return topKContainerSnapshot{}, err
	}
	lease, err := s.reserve(ctx, bytes)
	if err != nil {
		return topKContainerSnapshot{}, err
	}
	c := topKContainerSnapshot{lease: lease}
	defer func() {
		if err != nil {
			c.release()
		}
	}()
	c.workspace = &sortWorkspace{parent: s.manager}
	c.owner, err = newResidentSortAccumulator(ctx, s.owner.input.Schema(), c.workspace, s.input)
	if err != nil {
		return topKContainerSnapshot{}, err
	}
	if err = c.owner.appendRow(ctx, s.input, row); err != nil {
		return topKContainerSnapshot{}, err
	}
	if err = c.owner.finish(ctx); err != nil {
		return topKContainerSnapshot{}, err
	}
	// Scalar ordering keys borrow the owned typed row directly. Opaque-backed
	// keys were never supported; do not retain K redundant output view graphs.
	// Opaque payload remains owned by the sidecar until actual output binding.
	c.batch = c.owner.batch
	c.key = rowCandidate{ordinal: s.rows, cells: make([]sortCell, len(s.owner.keys))}
	for i, key := range s.owner.keys {
		if err = ctx.Err(); err != nil {
			return topKContainerSnapshot{}, err
		}
		c.key.cells[i], err = snapshotCell(c.batch, s.owner.Schema(), key.Column, 0)
		if err != nil {
			return topKContainerSnapshot{}, err
		}
	}
	if err = ctx.Err(); err != nil {
		return topKContainerSnapshot{}, err
	}
	return c, nil
}
