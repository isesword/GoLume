package lazy

import (
	"fmt"

	"gocolumn/memory"
)

// Schema returns an immutable copy of the DataFrame schema. A closed or zero
// DataFrame returns an empty schema.
func (f DataFrame) Schema() memory.Schema {
	if f.state == nil {
		return memory.Schema{}
	}
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	if f.state.closed {
		return memory.Schema{}
	}
	return memory.NewSchema(f.state.schema.Fields...)
}

// Shape returns the materialized row and column counts.
func (f DataFrame) Shape() (rows int64, columns int) {
	if f.state == nil {
		return 0, 0
	}
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	if f.state.closed {
		return 0, 0
	}
	return f.state.rows, len(f.state.schema.Fields)
}

// Height returns the materialized row count.
func (f DataFrame) Height() int64 { rows, _ := f.Shape(); return rows }

// Width returns the number of columns.
func (f DataFrame) Width() int { _, columns := f.Shape(); return columns }

// Columns returns column names in schema order.
func (f DataFrame) Columns() []string {
	schema := f.Schema()
	out := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		out[i] = field.Name
	}
	return out
}

// Column returns a borrowed, chunked, read-only Series.
func (f DataFrame) Column(name string) (Series, error) {
	if f.state == nil {
		return Series{}, ErrDataFrameClosed
	}
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	if f.state.closed {
		return Series{}, ErrDataFrameClosed
	}
	field, ok := f.state.schema.Field(name)
	if !ok {
		return Series{}, fmt.Errorf("data frame column %q not found", name)
	}
	return Series{state: f.state, field: field, offset: 0, length: f.state.rows}, nil
}

// FrameView is a borrowed zero-copy row range of a DataFrame.
type FrameView struct {
	state          *dataFrameState
	offset, length int64
}

// Head returns a borrowed view of at most the first n rows. Negative n is
// clamped to zero.
func (f DataFrame) Head(n int64) FrameView {
	rows := f.Height()
	if n < 0 {
		n = 0
	}
	if n > rows {
		n = rows
	}
	return FrameView{state: f.state, length: n}
}

// Tail returns a borrowed view of at most the last n rows. Negative n is
// clamped to zero.
func (f DataFrame) Tail(n int64) FrameView {
	rows := f.Height()
	if n < 0 {
		n = 0
	}
	if n > rows {
		n = rows
	}
	return FrameView{state: f.state, offset: rows - n, length: n}
}

// Slice returns a borrowed zero-copy view. Offset and length must be
// non-negative; a range extending past the end is truncated.
func (f DataFrame) Slice(offset, length int64) (FrameView, error) {
	if offset < 0 || length < 0 {
		return FrameView{}, fmt.Errorf("data frame slice requires non-negative offset and length")
	}
	if f.state == nil {
		return FrameView{}, ErrDataFrameClosed
	}
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	if f.state.closed {
		return FrameView{}, ErrDataFrameClosed
	}
	if offset > f.state.rows {
		offset = f.state.rows
	}
	if length > f.state.rows-offset {
		length = f.state.rows - offset
	}
	return FrameView{state: f.state, offset: offset, length: length}, nil
}

// Shape returns the view row count and owning DataFrame column count.
func (v FrameView) Shape() (int64, int) {
	if v.state == nil {
		return 0, 0
	}
	v.state.mu.RLock()
	defer v.state.mu.RUnlock()
	if v.state.closed {
		return 0, 0
	}
	return v.length, len(v.state.schema.Fields)
}

// Height returns the view row count.
func (v FrameView) Height() int64 { rows, _ := v.Shape(); return rows }

// Width returns the owning DataFrame column count.
func (v FrameView) Width() int { _, columns := v.Shape(); return columns }

// Columns returns column names in schema order.
func (v FrameView) Columns() []string {
	if v.state == nil {
		return nil
	}
	v.state.mu.RLock()
	defer v.state.mu.RUnlock()
	if v.state.closed {
		return nil
	}
	out := make([]string, len(v.state.schema.Fields))
	for i, field := range v.state.schema.Fields {
		out[i] = field.Name
	}
	return out
}

// Column returns a Series restricted to the view row range.
func (v FrameView) Column(name string) (Series, error) {
	if v.state == nil {
		return Series{}, ErrDataFrameClosed
	}
	v.state.mu.RLock()
	defer v.state.mu.RUnlock()
	if v.state.closed {
		return Series{}, ErrDataFrameClosed
	}
	field, ok := v.state.schema.Field(name)
	if !ok {
		return Series{}, fmt.Errorf("frame view column %q not found", name)
	}
	return Series{state: v.state, field: field, offset: v.offset, length: v.length}, nil
}

type batchSpan struct {
	batch      *memory.Batch
	start, end int
}

func spansForRange(state *dataFrameState, offset, length int64) []batchSpan {
	if state == nil || length == 0 {
		return nil
	}
	wantStart, wantEnd := offset, offset+length
	position := int64(0)
	out := make([]batchSpan, 0, len(state.batches))
	for _, batch := range state.batches {
		batchEnd := position + int64(batch.Rows)
		if wantStart < batchEnd && wantEnd > position {
			start := max64(wantStart-position, 0)
			end := min64(wantEnd-position, int64(batch.Rows))
			out = append(out, batchSpan{batch: batch, start: int(start), end: int(end)})
		}
		position = batchEnd
		if position >= wantEnd {
			break
		}
	}
	return out
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
