package lazy

import "gocolumn/memory"

// Int64ListChunk is a borrowed List<Int64> chunk.
type Int64ListChunk struct {
	chunkBase
	vector *memory.Int64ListVector
}

// Len returns the chunk row count.
func (c Int64ListChunk) Len() int { return c.chunkBase.length() }

// Int64ListValue is a borrowed view of one valid list row.
type Int64ListValue struct {
	state      *dataFrameState
	values     *memory.Int64Vector
	start, end int
}

// NullableInt64 represents one copied List<Int64> child value.
type NullableInt64 struct {
	Value int64
	Valid bool
}

// Int64List is one copied List<Int64> row.
type Int64List []NullableInt64

// Len returns the child element count.
func (v Int64ListValue) Len() int { return v.end - v.start }

// Element returns one copied int64 value and its validity.
func (v Int64ListValue) Element(i int) (int64, bool, error) {
	base := chunkBase{state: v.state, start: v.start, end: v.end}
	if err := base.check(i); err != nil {
		return 0, false, err
	}
	row := v.start + i
	return v.values.Values[row], v.values.IsValid(row), nil
}

// Value returns a borrowed list value and parent validity.
func (c Int64ListChunk) Value(i int) (Int64ListValue, bool, error) {
	if err := c.check(i); err != nil {
		return Int64ListValue{}, false, err
	}
	row := c.start + i
	start, end, ok := c.vector.ValueRange(row)
	if !ok {
		return Int64ListValue{}, false, nil
	}
	return Int64ListValue{state: c.state, values: c.vector.Values, start: start, end: end}, true, nil
}

// Int64ListChunks returns zero-copy chunks for List<Int64> columns.
func (s Series) Int64ListChunks() ([]Int64ListChunk, error) {
	if s.state == nil {
		return nil, ErrDataFrameClosed
	}
	if s.field.Type != memory.TypeList {
		return nil, seriesTypeError(s, "list<int64>")
	}
	if err := s.field.ValidateListInt64(); err != nil {
		return nil, err
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	spans, err := s.lockedSpans()
	if err != nil {
		return nil, err
	}
	out := make([]Int64ListChunk, 0, len(spans))
	for _, span := range spans {
		v, e := span.batch.Int64List(s.field.Name)
		if e != nil {
			return nil, e
		}
		out = append(out, Int64ListChunk{chunkBase: chunkBase{s.state, span.start, span.end}, vector: v})
	}
	return out, nil
}

// CopyInt64Lists flattens a List<Int64> Series. Parent validity is returned
// separately while each child preserves its own validity flag.
func (s Series) CopyInt64Lists() ([]Int64List, memory.Bitmap, error) {
	chunks, err := s.Int64ListChunks()
	if err != nil {
		return nil, nil, err
	}
	values := make([]Int64List, 0, s.length)
	validity := memory.EnsureBitmap(nil, int(s.length))
	row := 0
	for _, chunk := range chunks {
		for i := 0; i < chunk.Len(); i++ {
			value, valid, e := chunk.Value(i)
			if e != nil {
				return nil, nil, e
			}
			if !valid {
				values = append(values, nil)
				row++
				continue
			}
			items := make(Int64List, value.Len())
			for j := range items {
				x, ok, e := value.Element(j)
				if e != nil {
					return nil, nil, e
				}
				items[j] = NullableInt64{Value: x, Valid: ok}
			}
			values = append(values, items)
			memory.Set(validity, row)
			row++
		}
	}
	if !memory.HasNulls(validity, row) {
		validity = nil
	}
	return values, validity, nil
}
