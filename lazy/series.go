package lazy

import (
	"fmt"

	"gocolumn/memory"
)

// Series is a borrowed, typed, chunked column view.
type Series struct {
	state          *dataFrameState
	field          memory.Field
	offset, length int64
}

// Name returns the column name.
func (s Series) Name() string { return s.field.Name }

// Type returns the logical column type.
func (s Series) Type() memory.DataType { return s.field.Type }

// Len returns the logical row count, or zero after the owner is closed.
func (s Series) Len() int64 {
	if s.state == nil {
		return 0
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	if s.state.closed {
		return 0
	}
	return s.length
}

// NullCount returns the number of NULL rows, or zero after owner closure.
func (s Series) NullCount() int64 {
	if s.state == nil {
		return 0
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	if s.state.closed {
		return 0
	}
	var nulls int64
	for _, span := range spansForRange(s.state, s.offset, s.length) {
		for row := span.start; row < span.end; row++ {
			if !seriesRowValid(span.batch, s.field, row) {
				nulls++
			}
		}
	}
	return nulls
}

type chunkBase struct {
	state      *dataFrameState
	start, end int
}

func (c chunkBase) length() int { return c.end - c.start }
func (c chunkBase) check(i int) error {
	if c.state == nil {
		return ErrDataFrameClosed
	}
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	if c.state.closed {
		return ErrDataFrameClosed
	}
	if i < 0 || i >= c.length() {
		return fmt.Errorf("series chunk index %d outside [0,%d)", i, c.length())
	}
	return nil
}

// Int32Chunk is a borrowed chunk for int8/int16/int32/date32/time32 storage.
type Int32Chunk struct {
	chunkBase
	vector *memory.Int32Vector
}

// Len returns the chunk row count.
func (c Int32Chunk) Len() int { return c.chunkBase.length() }

// Value returns a value, its validity, and any lifecycle or bounds error.
func (c Int32Chunk) Value(i int) (int32, bool, error) {
	if err := c.check(i); err != nil {
		return 0, false, err
	}
	row := c.start + i
	return c.vector.Values[row], c.vector.IsValid(row), nil
}

// Int64Chunk is a borrowed chunk for int64/date64/timestamp/time64/duration storage.
type Int64Chunk struct {
	chunkBase
	vector *memory.Int64Vector
}

// Len returns the chunk row count.
func (c Int64Chunk) Len() int { return c.chunkBase.length() }

// Value returns a value, its validity, and any lifecycle or bounds error.
func (c Int64Chunk) Value(i int) (int64, bool, error) {
	if err := c.check(i); err != nil {
		return 0, false, err
	}
	row := c.start + i
	return c.vector.Values[row], c.vector.IsValid(row), nil
}

// Uint32Chunk is a borrowed chunk for uint8/uint16/uint32 storage.
type Uint32Chunk struct {
	chunkBase
	vector *memory.Uint32Vector
}

// Len returns the chunk row count.
func (c Uint32Chunk) Len() int { return c.chunkBase.length() }

// Value returns a value, its validity, and any lifecycle or bounds error.
func (c Uint32Chunk) Value(i int) (uint32, bool, error) {
	if err := c.check(i); err != nil {
		return 0, false, err
	}
	row := c.start + i
	return c.vector.Values[row], c.vector.IsValid(row), nil
}

// Uint64Chunk is a borrowed chunk for uint64 storage.
type Uint64Chunk struct {
	chunkBase
	vector *memory.Uint64Vector
}

// Len returns the chunk row count.
func (c Uint64Chunk) Len() int { return c.chunkBase.length() }

// Value returns a value, its validity, and any lifecycle or bounds error.
func (c Uint64Chunk) Value(i int) (uint64, bool, error) {
	if err := c.check(i); err != nil {
		return 0, false, err
	}
	row := c.start + i
	return c.vector.Values[row], c.vector.IsValid(row), nil
}

// Float64Chunk is a borrowed chunk for float32/float64 storage.
type Float64Chunk struct {
	chunkBase
	vector *memory.Float64Vector
}

// Len returns the chunk row count.
func (c Float64Chunk) Len() int { return c.chunkBase.length() }

// Value returns a value, its validity, and any lifecycle or bounds error.
func (c Float64Chunk) Value(i int) (float64, bool, error) {
	if err := c.check(i); err != nil {
		return 0, false, err
	}
	row := c.start + i
	return c.vector.Values[row], c.vector.IsValid(row), nil
}

// StringChunk is a borrowed plain or dictionary string chunk.
type StringChunk struct {
	chunkBase
	view       *memory.StringViewVector
	dictionary *memory.DictionaryStringVector
}

// Len returns the chunk row count.
func (c StringChunk) Len() int { return c.chunkBase.length() }

// Bytes returns a borrowed read-only byte view and validity.
func (c StringChunk) Bytes(i int) ([]byte, bool, error) {
	if err := c.check(i); err != nil {
		return nil, false, err
	}
	row := c.start + i
	if c.view != nil {
		return c.view.ValueBytes(row), c.view.IsValid(row), nil
	}
	if !c.dictionary.IsValid(row) {
		return nil, false, nil
	}
	return c.dictionary.DictionaryValue(c.dictionary.Indices[row]), true, nil
}

// Value returns a copied Go string and validity.
func (c StringChunk) Value(i int) (string, bool, error) {
	value, valid, err := c.Bytes(i)
	return string(value), valid, err
}

// StringListChunk is a borrowed List<String> chunk.
type StringListChunk struct {
	chunkBase
	vector *memory.StringListVector
}

// Len returns the chunk row count.
func (c StringListChunk) Len() int { return c.chunkBase.length() }

// StringListValue is a borrowed view of one valid list row.
type StringListValue struct {
	state      *dataFrameState
	values     *memory.StringViewVector
	start, end int
}

// NullableString represents one copied List<String> child value.
type NullableString struct {
	Value string
	Valid bool
}

// StringList is one copied List<String> row.
type StringList []NullableString

// Len returns the child element count.
func (v StringListValue) Len() int { return v.end - v.start }

// Element returns one copied string value and its validity.
func (v StringListValue) Element(i int) (string, bool, error) {
	base := chunkBase{state: v.state, start: v.start, end: v.end}
	if err := base.check(i); err != nil {
		return "", false, err
	}
	row := v.start + i
	return string(v.values.ValueBytes(row)), v.values.IsValid(row), nil
}

// Value returns a borrowed list value and parent validity.
func (c StringListChunk) Value(i int) (StringListValue, bool, error) {
	if err := c.check(i); err != nil {
		return StringListValue{}, false, err
	}
	row := c.start + i
	start, end, ok := c.vector.ValueRange(row)
	if !ok {
		return StringListValue{}, false, nil
	}
	return StringListValue{state: c.state, values: c.vector.Values, start: start, end: end}, true, nil
}

func (s Series) lockedSpans() ([]batchSpan, error) {
	if s.state == nil {
		return nil, ErrDataFrameClosed
	}
	if s.state.closed {
		return nil, ErrDataFrameClosed
	}
	return spansForRange(s.state, s.offset, s.length), nil
}

// Int32Chunks returns zero-copy chunks for the int32 physical family.
func (s Series) Int32Chunks() ([]Int32Chunk, error) {
	if !isI32Type(s.field.Type) {
		return nil, seriesTypeError(s, "int32")
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	spans, err := s.lockedSpans()
	if err != nil {
		return nil, err
	}
	out := make([]Int32Chunk, 0, len(spans))
	for _, span := range spans {
		v, e := span.batch.Int32(s.field.Name)
		if e != nil {
			return nil, e
		}
		out = append(out, Int32Chunk{chunkBase: chunkBase{s.state, span.start, span.end}, vector: v})
	}
	return out, nil
}

// Int64Chunks returns zero-copy chunks for the int64 physical family.
func (s Series) Int64Chunks() ([]Int64Chunk, error) {
	if !isI64Type(s.field.Type) {
		return nil, seriesTypeError(s, "int64")
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	spans, err := s.lockedSpans()
	if err != nil {
		return nil, err
	}
	out := make([]Int64Chunk, 0, len(spans))
	for _, span := range spans {
		v, e := span.batch.Int64(s.field.Name)
		if e != nil {
			return nil, e
		}
		out = append(out, Int64Chunk{chunkBase: chunkBase{s.state, span.start, span.end}, vector: v})
	}
	return out, nil
}

// Date32Chunks returns zero-copy physical chunks for a date32 Series.
func (s Series) Date32Chunks() ([]Int32Chunk, error) {
	if s.field.Type != memory.TypeDate32 {
		return nil, seriesTypeError(s, "date32")
	}
	return s.Int32Chunks()
}

// Date64Chunks returns zero-copy physical chunks for a date64 Series.
func (s Series) Date64Chunks() ([]Int64Chunk, error) {
	if s.field.Type != memory.TypeDate64 {
		return nil, seriesTypeError(s, "date64")
	}
	return s.Int64Chunks()
}

// TimestampChunks returns zero-copy UTC nanosecond chunks for a timestamp Series.
func (s Series) TimestampChunks() ([]Int64Chunk, error) {
	if s.field.Type != memory.TypeTimestamp {
		return nil, seriesTypeError(s, "timestamp")
	}
	return s.Int64Chunks()
}

// Time32Chunks returns zero-copy physical chunks for a time32 Series.
func (s Series) Time32Chunks() ([]Int32Chunk, error) {
	if s.field.Type != memory.TypeTime32 {
		return nil, seriesTypeError(s, "time32")
	}
	return s.Int32Chunks()
}

// Time64Chunks returns zero-copy physical chunks for a time64 Series.
func (s Series) Time64Chunks() ([]Int64Chunk, error) {
	if s.field.Type != memory.TypeTime64 {
		return nil, seriesTypeError(s, "time64")
	}
	return s.Int64Chunks()
}

// DurationChunks returns zero-copy nanosecond chunks for a duration Series.
func (s Series) DurationChunks() ([]Int64Chunk, error) {
	if s.field.Type != memory.TypeDuration {
		return nil, seriesTypeError(s, "duration")
	}
	return s.Int64Chunks()
}

// Uint32Chunks returns zero-copy chunks for the uint32 physical family.
func (s Series) Uint32Chunks() ([]Uint32Chunk, error) {
	if !isU32Type(s.field.Type) {
		return nil, seriesTypeError(s, "uint32")
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	spans, err := s.lockedSpans()
	if err != nil {
		return nil, err
	}
	out := make([]Uint32Chunk, 0, len(spans))
	for _, span := range spans {
		v, e := span.batch.Uint32(s.field.Name)
		if e != nil {
			return nil, e
		}
		out = append(out, Uint32Chunk{chunkBase: chunkBase{s.state, span.start, span.end}, vector: v})
	}
	return out, nil
}

// Uint64Chunks returns zero-copy chunks for uint64 columns.
func (s Series) Uint64Chunks() ([]Uint64Chunk, error) {
	if s.field.Type != memory.TypeUint64 {
		return nil, seriesTypeError(s, "uint64")
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	spans, err := s.lockedSpans()
	if err != nil {
		return nil, err
	}
	out := make([]Uint64Chunk, 0, len(spans))
	for _, span := range spans {
		v, e := span.batch.Uint64(s.field.Name)
		if e != nil {
			return nil, e
		}
		out = append(out, Uint64Chunk{chunkBase: chunkBase{s.state, span.start, span.end}, vector: v})
	}
	return out, nil
}

// Float64Chunks returns zero-copy chunks for float32/float64 columns.
func (s Series) Float64Chunks() ([]Float64Chunk, error) {
	if s.field.Type != memory.TypeFloat32 && s.field.Type != memory.TypeFloat64 {
		return nil, seriesTypeError(s, "float64")
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	spans, err := s.lockedSpans()
	if err != nil {
		return nil, err
	}
	out := make([]Float64Chunk, 0, len(spans))
	for _, span := range spans {
		v, e := span.batch.Float64(s.field.Name)
		if e != nil {
			return nil, e
		}
		out = append(out, Float64Chunk{chunkBase: chunkBase{s.state, span.start, span.end}, vector: v})
	}
	return out, nil
}

// StringChunks returns zero-copy chunks for UTF-8 string columns.
func (s Series) StringChunks() ([]StringChunk, error) {
	if s.field.Type != memory.TypeString {
		return nil, seriesTypeError(s, "string")
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	spans, err := s.lockedSpans()
	if err != nil {
		return nil, err
	}
	out := make([]StringChunk, 0, len(spans))
	for _, span := range spans {
		c := StringChunk{chunkBase: chunkBase{s.state, span.start, span.end}}
		if v, ok := span.batch.Strings[s.field.Name]; ok {
			c.view = v
		} else if v, ok := span.batch.DictStrings[s.field.Name]; ok {
			c.dictionary = v
		} else {
			return nil, fmt.Errorf("string column %q not present in batch", s.field.Name)
		}
		out = append(out, c)
	}
	return out, nil
}

// StringListChunks returns zero-copy chunks for List<String> columns.
func (s Series) StringListChunks() ([]StringListChunk, error) {
	if s.field.Type != memory.TypeList {
		return nil, seriesTypeError(s, "list<string>")
	}
	if err := s.field.ValidateListString(); err != nil {
		return nil, err
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	spans, err := s.lockedSpans()
	if err != nil {
		return nil, err
	}
	out := make([]StringListChunk, 0, len(spans))
	for _, span := range spans {
		v, e := span.batch.StringList(s.field.Name)
		if e != nil {
			return nil, e
		}
		out = append(out, StringListChunk{chunkBase: chunkBase{s.state, span.start, span.end}, vector: v})
	}
	return out, nil
}

// CopyFloat64 flattens a float Series and returns copied values plus validity.
func (s Series) CopyFloat64() ([]float64, memory.Bitmap, error) {
	chunks, err := s.Float64Chunks()
	if err != nil {
		return nil, nil, err
	}
	values := make([]float64, 0, s.length)
	validity := memory.EnsureBitmap(nil, int(s.length))
	row := 0
	for _, chunk := range chunks {
		for i := 0; i < chunk.Len(); i++ {
			value, valid, e := chunk.Value(i)
			if e != nil {
				return nil, nil, e
			}
			values = append(values, value)
			if valid {
				memory.Set(validity, row)
			}
			row++
		}
	}
	if !memory.HasNulls(validity, row) {
		validity = nil
	}
	return values, validity, nil
}

// CopyInt64 flattens an int64 physical-family Series and returns copied values
// plus validity.
func (s Series) CopyInt64() ([]int64, memory.Bitmap, error) {
	chunks, err := s.Int64Chunks()
	if err != nil {
		return nil, nil, err
	}
	values := make([]int64, 0, s.length)
	validity := memory.EnsureBitmap(nil, int(s.length))
	row := 0
	for _, chunk := range chunks {
		for i := 0; i < chunk.Len(); i++ {
			value, valid, e := chunk.Value(i)
			if e != nil {
				return nil, nil, e
			}
			values = append(values, value)
			if valid {
				memory.Set(validity, row)
			}
			row++
		}
	}
	if !memory.HasNulls(validity, row) {
		validity = nil
	}
	return values, validity, nil
}

// CopyStrings flattens a string Series and returns copied values plus validity.
func (s Series) CopyStrings() ([]string, memory.Bitmap, error) {
	chunks, err := s.StringChunks()
	if err != nil {
		return nil, nil, err
	}
	values := make([]string, 0, s.length)
	validity := memory.EnsureBitmap(nil, int(s.length))
	row := 0
	for _, chunk := range chunks {
		for i := 0; i < chunk.Len(); i++ {
			value, valid, e := chunk.Value(i)
			if e != nil {
				return nil, nil, e
			}
			values = append(values, value)
			if valid {
				memory.Set(validity, row)
			}
			row++
		}
	}
	if !memory.HasNulls(validity, row) {
		validity = nil
	}
	return values, validity, nil
}

// CopyStringLists flattens a List<String> Series. Parent validity is returned
// separately while each child preserves its own validity flag.
func (s Series) CopyStringLists() ([]StringList, memory.Bitmap, error) {
	chunks, err := s.StringListChunks()
	if err != nil {
		return nil, nil, err
	}
	values := make([]StringList, 0, s.length)
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
			items := make(StringList, value.Len())
			for j := range items {
				x, ok, e := value.Element(j)
				if e != nil {
					return nil, nil, e
				}
				items[j] = NullableString{Value: x, Valid: ok}
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

// ToAnySlice materializes a low-performance []any representation. NULL values
// and NULL list elements are represented by nil.
func (s Series) ToAnySlice() ([]any, error) {
	if s.state == nil {
		return nil, ErrDataFrameClosed
	}
	out := make([]any, 0, s.length)
	switch {
	case isI32Type(s.field.Type):
		chunks, e := s.Int32Chunks()
		if e != nil {
			return nil, e
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, ok, e := c.Value(i)
				if e != nil {
					return nil, e
				}
				if ok {
					out = append(out, v)
				} else {
					out = append(out, nil)
				}
			}
		}
	case isI64Type(s.field.Type):
		chunks, e := s.Int64Chunks()
		if e != nil {
			return nil, e
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, ok, e := c.Value(i)
				if e != nil {
					return nil, e
				}
				if ok {
					out = append(out, v)
				} else {
					out = append(out, nil)
				}
			}
		}
	case s.field.Type == memory.TypeUint64:
		chunks, e := s.Uint64Chunks()
		if e != nil {
			return nil, e
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, ok, e := c.Value(i)
				if e != nil {
					return nil, e
				}
				if ok {
					out = append(out, v)
				} else {
					out = append(out, nil)
				}
			}
		}
	case s.field.Type == memory.TypeFloat32 || s.field.Type == memory.TypeFloat64:
		chunks, e := s.Float64Chunks()
		if e != nil {
			return nil, e
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, ok, e := c.Value(i)
				if e != nil {
					return nil, e
				}
				if ok {
					out = append(out, v)
				} else {
					out = append(out, nil)
				}
			}
		}
	case s.field.Type == memory.TypeString:
		chunks, e := s.StringChunks()
		if e != nil {
			return nil, e
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, ok, e := c.Value(i)
				if e != nil {
					return nil, e
				}
				if ok {
					out = append(out, v)
				} else {
					out = append(out, nil)
				}
			}
		}
	case s.field.Type == memory.TypeList:
		chunks, e := s.StringListChunks()
		if e != nil {
			return nil, e
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, ok, e := c.Value(i)
				if e != nil {
					return nil, e
				}
				if !ok {
					out = append(out, nil)
					continue
				}
				items := make([]any, v.Len())
				for j := range items {
					x, valid, e := v.Element(j)
					if e != nil {
						return nil, e
					}
					if valid {
						items[j] = x
					}
				}
				out = append(out, items)
			}
		}
	default:
		return nil, fmt.Errorf("series type %s does not support ToAnySlice", s.field.Type)
	}
	return out, nil
}

func seriesTypeError(s Series, want string) error {
	return fmt.Errorf("series %q has type %s, not %s", s.field.Name, s.field.Type, want)
}
func isI32Type(t memory.DataType) bool {
	switch t {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		return true
	}
	return false
}
func isI64Type(t memory.DataType) bool {
	switch t {
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		return true
	}
	return false
}
func isU32Type(t memory.DataType) bool {
	switch t {
	case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32:
		return true
	}
	return false
}

func seriesRowValid(batch *memory.Batch, field memory.Field, row int) bool {
	switch {
	case isI32Type(field.Type):
		return batch.Int32s[field.Name].IsValid(row)
	case isI64Type(field.Type):
		return batch.Int64s[field.Name].IsValid(row)
	case isU32Type(field.Type):
		return batch.Uint32s[field.Name].IsValid(row)
	case field.Type == memory.TypeUint64:
		return batch.Uint64s[field.Name].IsValid(row)
	case field.Type == memory.TypeFloat32 || field.Type == memory.TypeFloat64:
		return batch.Float64s[field.Name].IsValid(row)
	case field.Type == memory.TypeString:
		if v := batch.Strings[field.Name]; v != nil {
			return v.IsValid(row)
		}
		return batch.DictStrings[field.Name].IsValid(row)
	case field.Type == memory.TypeList:
		return batch.StringLists[field.Name].IsValid(row)
	}
	return false
}
