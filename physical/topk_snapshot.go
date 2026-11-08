package physical

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
)

// topKSnapshotStream never relies on Stats or on replaying a source. Every
// admitted row owns its typed payload and dictionaries and retains opaque
// Arrow backing buffers. Those buffers are charged in full, not by the size
// of the logical Arrow slice. Thus the bound is K snapshots, not K fixed-size
// rows; variable-width values or a large opaque backing may exhaust a budget.
// Winners are emitted individually to avoid an additional materialization and
// to preserve batch-local dictionary domains and opaque types unchanged.
type topKSnapshotStream struct {
	state    unaryStreamState
	owner    *TopKSource
	manager  memory.Manager
	query    *memory.QueryBudget
	input    *memory.Batch
	borrowed *memory.Batch
	heap     topKSnapshotHeap
	indexMem memory.Reservation
	prepared bool
	next     int
	rows     int64
}

type topKSnapshot struct {
	key   rowCandidate
	batch *memory.Batch
	lease memory.Reservation
}

func (c *topKSnapshot) release() {
	if c.batch != nil {
		c.batch.ClearColumns()
	}
	if c.lease != nil {
		c.lease.Release()
	}
	*c = topKSnapshot{}
}

type topKSnapshotHeap struct {
	items []topKSnapshot
	keys  []SortKey
}

// Len returns the number of retained snapshots.
func (h topKSnapshotHeap) Len() int { return len(h.items) }

// Less puts the worst candidate at the root, with input ordinals breaking ties.
func (h topKSnapshotHeap) Less(i, j int) bool {
	return candidateLess(h.items[j].key, h.items[i].key, h.keys)
}

// Swap exchanges two snapshot slots.
func (h topKSnapshotHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

// Push appends an already admitted snapshot.
func (h *topKSnapshotHeap) Push(v any) { h.items = append(h.items, v.(topKSnapshot)) }

// Pop removes the final snapshot without retaining its buffers in the heap.
func (h *topKSnapshotHeap) Pop() any {
	n := len(h.items) - 1
	v := h.items[n]
	h.items[n] = topKSnapshot{}
	h.items = h.items[:n]
	return v
}

func newTopKSnapshotStream(ctx context.Context, owner *TopKSource, opts source.ReadOptions) (*topKSnapshotStream, error) {
	s := &topKSnapshotStream{owner: owner, manager: owner.manager, input: memory.NewBatch()}
	s.query = memory.QueryBudgetFromContext(ctx)
	if s.query != nil {
		s.manager = s.query.Operator("TopKSnapshot", memory.MemoryRetainedState)
		opts.MemoryManager = s.query.Operator("SourceScan", memory.MemoryWorkspace)
	} else if s.manager == nil {
		s.manager = opts.MemoryManager
	}
	s.heap.keys = owner.keys
	s.state.closeOwn = s.cleanup
	// K=0 has no input dependency and must not open a one-shot source.
	if owner.k > 0 {
		child, err := source.OpenBatchStream(ctx, owner.input, opts)
		if err != nil {
			return nil, err
		}
		s.state.child = child
	}
	return s, nil
}

func (s *topKSnapshotStream) reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	if s.manager == nil {
		return nil, ctx.Err()
	}
	return s.manager.Reserve(ctx, bytes)
}

func (s *topKSnapshotStream) growIndex(ctx context.Context) error {
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
	width := int64(unsafe.Sizeof(topKSnapshot{}))
	if int64(capacity) > math.MaxInt64/width {
		return fmt.Errorf("top-k snapshot index overflows int64")
	}
	// Charge both old and new backing arrays until the copy is complete.
	lease, err := s.reserve(ctx, int64(capacity)*width)
	if err != nil {
		return fmt.Errorf("top-k snapshot index: %w", err)
	}
	items := make([]topKSnapshot, len(s.heap.items), capacity)
	copy(items, s.heap.items)
	s.heap.items = items
	if s.indexMem != nil {
		s.indexMem.Release()
	}
	s.indexMem = lease
	return nil
}

func (s *topKSnapshotStream) snapshot(ctx context.Context, row int) (topKSnapshot, error) {
	bytes, err := memory.EstimateBatchRangeBytes(s.input, row, 1)
	if err != nil {
		return topKSnapshot{}, err
	}
	extra, err := topKArrowBackingExtra(s.input)
	if err != nil {
		return topKSnapshot{}, err
	}
	if extra > math.MaxInt64-bytes {
		return topKSnapshot{}, fmt.Errorf("top-k snapshot backing bytes overflow int64")
	}
	bytes += extra
	if columns, fixed := topKFixedColumns(s.input); fixed {
		// A reusable fixed-width snapshot may acquire NULLs in a later batch.
		// Admit their maximum validity storage now, before any reuse allocation.
		if int64(columns) > (math.MaxInt64-bytes)/8 {
			return topKSnapshot{}, fmt.Errorf("top-k snapshot validity bytes overflow int64")
		}
		bytes += int64(columns) * 8
	}
	// Include candidate cells and conservative batch/map/vector header space.
	// CloneBatchRange also owns dictionary data and accounts retained opaque
	// buffers, including any backing outside this logical row.
	headers, err := topKSnapshotHeaders(s.input)
	if err != nil {
		return topKSnapshot{}, err
	}
	keyBytes := int64(len(s.owner.keys)) * int64(unsafe.Sizeof(sortCell{}))
	if keyBytes > math.MaxInt64-headers || bytes > (math.MaxInt64-headers-keyBytes)/2 {
		return topKSnapshot{}, fmt.Errorf("top-k snapshot bytes overflow int64")
	}
	// Clone helpers append into fresh slices. A second payload share covers
	// their allocator rounding/capacity and small struct builder workspaces;
	// never admit only logical lengths and grow the reservation after copying.
	lease, err := s.reserve(ctx, bytes*2+headers+keyBytes)
	if err != nil {
		return topKSnapshot{}, fmt.Errorf("top-k winner snapshot: %w", err)
	}
	c := topKSnapshot{lease: lease}
	c.batch, err = memory.CloneBatchRange(s.input, row, 1)
	if err != nil {
		c.release()
		return topKSnapshot{}, err
	}
	c.key = rowCandidate{ordinal: s.rows, cells: make([]sortCell, len(s.owner.keys))}
	for i, key := range s.owner.keys {
		c.key.cells[i], err = snapshotCell(c.batch, s.owner.Schema(), key.Column, 0)
		if err != nil {
			c.release()
			return topKSnapshot{}, err
		}
	}
	// Key bytes borrow the owned snapshot, so no second string copy is needed.
	return c, nil
}

func topKSnapshotHeaders(batch *memory.Batch) (int64, error) {
	columns := len(batch.Int8s) + len(batch.Int16s) + len(batch.Int32s) + len(batch.Int64s) +
		len(batch.Uint8s) + len(batch.Uint16s) + len(batch.Uint32s) + len(batch.Uint64s) +
		len(batch.Float32s) + len(batch.Float64s) + len(batch.Strings) + len(batch.DictStrings) +
		len(batch.StringLists) + len(batch.Structs) + len(batch.Opaque)
	if int64(columns) > (math.MaxInt64-2048)/512 {
		return 0, fmt.Errorf("top-k snapshot metadata overflows int64")
	}
	// Includes both snapshot headers and temporary non-owning output headers.
	bytes := int64(2048) + int64(columns)*512
	for _, vector := range batch.Structs {
		children, err := topKSnapshotHeaders(vector.Values)
		if err != nil {
			return 0, err
		}
		if children > math.MaxInt64-bytes {
			return 0, fmt.Errorf("top-k snapshot metadata overflows int64")
		}
		bytes += children
	}
	return bytes, nil
}

// EstimateBatchRangeBytes includes Arrow logical buffer lengths. Retaining an
// Arrow slice can pin more: resizable spare capacity, sliced-buffer parents,
// children, dictionary arrays and their metadata. Add the difference without
// allocating a visited map. Shared buffers are conservatively charged again.
func topKArrowBackingExtra(batch *memory.Batch) (int64, error) {
	var total int64
	add := func(n int64) error {
		if n < 0 || n > math.MaxInt64-total {
			return fmt.Errorf("top-k opaque backing bytes overflow int64")
		}
		total += n
		return nil
	}
	for _, vector := range batch.Opaque {
		values, ok := vector.Value.(arrow.Array)
		if !ok {
			return 0, fmt.Errorf("top-k opaque payload has no Arrow backing contract")
		}
		retained, err := topKArrowDataBytes(values.Data())
		if err != nil {
			return 0, err
		}
		logical := values.Data().SizeInBytes()
		if logical > uint64(retained) {
			return 0, fmt.Errorf("top-k opaque backing is smaller than reported buffers")
		}
		if err := add(retained - int64(logical)); err != nil {
			return 0, err
		}
	}
	for _, vector := range batch.Structs {
		bytes, err := topKArrowBackingExtra(vector.Values)
		if err != nil {
			return 0, err
		}
		if err := add(bytes); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func topKArrowDataBytes(data arrow.ArrayData) (int64, error) {
	// Arrow's Dictionary returns an ArrayData interface containing (*Data)(nil).
	if data == nil || (reflect.ValueOf(data).Kind() == reflect.Ptr && reflect.ValueOf(data).IsNil()) {
		return 0, nil
	}
	total := int64(256)
	add := func(n int64) error {
		if n < 0 || n > math.MaxInt64-total {
			return fmt.Errorf("top-k opaque backing bytes overflow int64")
		}
		total += n
		return nil
	}
	buffers, children := data.Buffers(), data.Children()
	bufferSlot, childSlot := int64(unsafe.Sizeof((*arrowmem.Buffer)(nil))), int64(unsafe.Sizeof(arrow.ArrayData(nil)))
	if int64(cap(buffers)) > math.MaxInt64/bufferSlot || int64(cap(children)) > math.MaxInt64/childSlot {
		return 0, fmt.Errorf("top-k opaque metadata capacity overflows int64")
	}
	if err := add(int64(cap(buffers)) * bufferSlot); err != nil {
		return 0, err
	}
	if err := add(int64(cap(children)) * childSlot); err != nil {
		return 0, err
	}
	for _, buffer := range buffers {
		for buffer != nil {
			if err := add(int64(unsafe.Sizeof(arrowmem.Buffer{}))); err != nil {
				return 0, err
			}
			parent := buffer.Parent()
			if parent == nil {
				if err := add(int64(cap(buffer.Buf()))); err != nil {
					return 0, err
				}
			}
			buffer = parent
		}
	}
	for _, child := range children {
		bytes, err := topKArrowDataBytes(child)
		if err != nil {
			return 0, err
		}
		if err := add(bytes); err != nil {
			return 0, err
		}
	}
	dictionary, err := topKArrowDataBytes(data.Dictionary())
	if err != nil {
		return 0, err
	}
	if err := add(dictionary); err != nil {
		return 0, err
	}
	return total, nil
}

func (s *topKSnapshotStream) prepare(ctx context.Context) error {
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
				if replace {
					reused, err := s.reuseFixed(&s.heap.items[0], row)
					if err != nil {
						return err
					}
					if reused {
						heap.Fix(&s.heap, 0)
						s.rows++
						continue
					}
					// The evicted row can never be returned. Release it before
					// admitting the replacement, avoiding a K+1 payload peak.
					s.heap.items[0].release()
				} else if err := s.growIndex(ctx); err != nil {
					return err
				}
				candidate, err := s.snapshot(ctx, row)
				if err != nil {
					return err
				}
				if replace {
					s.heap.items[0] = candidate
					heap.Fix(&s.heap, 0)
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

func (s *topKSnapshotStream) clearOutput() {
	if s.borrowed != nil {
		s.borrowed.ClearColumns()
		s.borrowed = nil
		s.heap.items[s.next-1].release()
	}
}

// Next scans once, then emits owned winner snapshots as borrowed one-row views.
func (s *topKSnapshotStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
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
	bindAll(dst, s.heap.items[s.next].batch)
	borrowFilterOwners(dst)
	dst.Reset(1)
	s.borrowed = dst
	s.next++
	return true, nil
}

func (s *topKSnapshotStream) cleanup() error {
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
	return err
}

// Close releases snapshots, borrowed output, reservations, and upstream once.
func (s *topKSnapshotStream) Close() error { return s.state.Close() }

// StreamDiagnostics reports the selected one-pass strategy and rows scanned.
func (s *topKSnapshotStream) StreamDiagnostics() map[string]string {
	return map[string]string{"strategy": "one-pass-snapshots", "input_rows": fmt.Sprint(s.rows), "output_rows": fmt.Sprint(s.next)}
}

var _ source.BatchStream = (*topKSnapshotStream)(nil)
var _ source.BatchStreamDiagnostics = (*topKSnapshotStream)(nil)
