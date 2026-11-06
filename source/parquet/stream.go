package parquet

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"strconv"
	"unicode/utf8"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
)

// NewBatchStream decodes projected row groups incrementally. BatchRows and
// BatchBytes bound GoLume's live decoded vector buffers and decode scratch,
// including allocation overlap. O(columns) stream metadata (including inline
// single-value lookahead), Go object overhead, and Arrow page, dictionary,
// codec and metadata allocations are excluded; this is not an RSS limit. Input
// chunks use buffered I/O rather than whole-column compressed buffering.
//
// A row that cannot fit by itself fails with memory.ErrBudgetExceeded. Strings
// are inspected through borrowed Arrow page views before any GoLume payload
// allocation. A byte boundary may produce fewer rows than BatchRows. If a
// large row reduces fixed-vector capacity, that smaller capacity is retained
// for the rest of its row group and reconsidered at the next row group.
func (s *Source) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if s == nil {
		return nil, fmt.Errorf("nil parquet source")
	}
	return s.NewBatchStreamWithPartitions(ctx, opts, s.parts)
}

// NewBatchStreamWithPartitions borrows the immutable prepared partition slice
// and preserves its exact order and subset. It does not re-list partitions.
func (s *Source) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if s == nil || ctx == nil {
		return nil, fmt.Errorf("parquet stream requires a source and context")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts = opts.Normalize()
	if opts.MemoryManager == nil {
		if query := memory.QueryBudgetFromContext(ctx); query != nil {
			opts.MemoryManager = query.Operator("SourceScan", memory.MemoryWorkspace)
		}
	}
	props := arrowparquet.NewReaderProperties(nil)
	props.BufferedStreamEnabled = true
	props.PageStreamingEnabled = true
	r, err := file.OpenParquetFile(s.path, false, file.WithReadProps(props))
	if err != nil {
		return nil, err
	}
	out := &batchStream{
		ctx: ctx, r: r, parts: parts, bindings: s.bindings, opts: opts,
		decode: reader{kernels: opts.Kernels},
	}
	for i, b := range s.bindings {
		if b.Type == memory.TypeString {
			out.strings = append(out.strings, streamString{binding: b, column: i})
		}
	}
	return out, nil
}

type streamString struct {
	binding columnBinding
	column  int
	// One borrowed value per column is the only lookahead. Never advance its
	// column reader until this value has been copied into an owned vector.
	value  [1]arrowparquet.ByteArray
	defs   [1]int16
	valid  bool
	charge memory.Reservation
}

type batchStream struct {
	ctx         context.Context
	r           *file.Reader
	parts       []source.Partition
	bindings    []columnBinding
	opts        source.ReadOptions
	decode      reader
	strings     []streamString
	chunks      []file.ColumnChunkReader
	part        int
	remaining   int64
	groupOpen   bool
	pending     bool
	batch       *memory.Batch
	capacity    int
	fixedCharge memory.Reservation
	managed     int64
	peak        int64
	batches     int64
	rows        int64
	exhausted   bool
	closed      bool
	terminalErr error
	closeErr    error
}

func (s *batchStream) checkContext(ctx context.Context) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	return ctx.Err()
}

// Next fills dst with the next borrowed, bounded batch. Errors are terminal
// and release every owned buffer and reader; successful calls do not retain
// the call-specific context.
func (s *batchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("batch stream destination is nil")
	}
	dst.ClearColumns()
	if ctx == nil {
		return false, s.fail(fmt.Errorf("batch stream context is nil"))
	}
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminalErr != nil {
		return false, s.terminalErr
	}
	if s.exhausted {
		return false, nil
	}
	if err := s.checkContext(ctx); err != nil {
		return false, s.fail(err)
	}
	for !s.groupOpen || s.remaining == 0 {
		if err := s.closeChunks(); err != nil {
			return false, s.fail(err)
		}
		s.releaseBuffers()
		if s.part == len(s.parts) {
			s.exhausted = true
			if err := s.release(); err != nil {
				s.terminalErr = err
				return false, err
			}
			return false, nil
		}
		if err := s.openGroup(ctx); err != nil {
			return false, s.fail(err)
		}
		// Empty groups do not create data batches; schema is source metadata.
	}
	want := s.opts.BatchRows
	if int64(want) > s.remaining {
		want = int(s.remaining)
	}
	if s.batch == nil {
		if err := s.allocateFixed(ctx, want, 0); err != nil {
			return false, s.fail(err)
		}
	}
	if want > s.capacity {
		want = s.capacity
	}
	s.resetStrings()
	rows := want
	if len(s.strings) != 0 {
		var err error
		rows, err = s.readStrings(ctx, want)
		if err != nil {
			return false, s.fail(err)
		}
	}
	for i, b := range s.bindings {
		if err := s.checkContext(ctx); err != nil {
			return false, s.fail(err)
		}
		var err error
		switch b.Type {
		case memory.TypeInt64, memory.TypeTimestamp:
			err = s.decode.readInt64(s.chunks[i], b, rows, s.batch)
		case memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16, memory.TypeInt32, memory.TypeDate32:
			err = s.decode.readInt32(s.chunks[i], b, rows, s.batch)
		case memory.TypeFloat32:
			err = s.decode.readFloat32(s.chunks[i], b, rows, s.batch)
		case memory.TypeFloat64:
			err = s.decode.readFloat64(s.chunks[i], b, rows, s.batch)
		case memory.TypeBool:
			err = s.decode.readBool(s.chunks[i], b, rows, s.batch)
		case memory.TypeString:
			v := s.batch.Strings[b.Name]
			v.Offsets = v.Offsets[:rows+1]
			if b.Nullable {
				v.Validity = v.Validity[:memory.WordsFor(rows)]
			}
		default:
			err = fmt.Errorf("unsupported projected type %s for %q", b.Type, b.Name)
		}
		if err != nil {
			return false, s.fail(err)
		}
	}
	if err := s.checkContext(ctx); err != nil {
		return false, s.fail(err)
	}
	s.remaining -= int64(rows)
	s.batch.Reset(rows)
	bindStreamBatch(dst, s.batch)
	s.batches++
	s.rows += int64(rows)
	return true, nil
}

func (s *batchStream) openGroup(ctx context.Context) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	p := s.parts[s.part]
	s.part++
	if p.ID < 0 || p.ID >= s.r.NumRowGroups() {
		return fmt.Errorf("row group %d out of range", p.ID)
	}
	rg := s.r.RowGroup(p.ID)
	s.remaining = rg.MetaData().NumRows()
	if s.remaining < 0 {
		return fmt.Errorf("row group %d has negative row count", p.ID)
	}
	s.chunks = make([]file.ColumnChunkReader, len(s.bindings))
	for i, b := range s.bindings {
		if err := s.checkContext(ctx); err != nil {
			return err
		}
		c, err := rg.Column(b.Index)
		if err != nil {
			return err
		}
		s.chunks[i] = c
	}
	s.groupOpen = true
	return nil
}

// fixedBytes includes every typed scratch buffer the legacy typed decoders
// may retain simultaneously, not merely the largest scratch buffer.
func (s *batchStream) fixedBytes(rows int) int64 {
	var bytes int64
	var i64, i32, f32, f64, boolean, defs bool
	for _, b := range s.bindings {
		width := int64(0)
		switch b.Type {
		case memory.TypeInt8, memory.TypeUint8:
			width = 1
			i32 = true
			defs = true
		case memory.TypeInt16, memory.TypeUint16:
			width = 2
			i32 = true
			defs = true
		case memory.TypeInt32, memory.TypeDate32:
			width = 4
			i32 = i32 || b.Nullable
		case memory.TypeInt64, memory.TypeTimestamp:
			width = 8
			i64 = i64 || b.Nullable
		case memory.TypeFloat32:
			width = 4
			f32 = f32 || b.Nullable
		case memory.TypeFloat64:
			width = 8
			f64 = f64 || b.Nullable
		case memory.TypeBool:
			width = 4
			boolean = true
		case memory.TypeString:
			width = 4
			bytes += 4
		}
		bytes += int64(rows) * width
		if b.Nullable {
			bytes += int64(memory.WordsFor(rows)) * 8
			defs = defs || b.Type != memory.TypeString
		}
	}
	if i64 {
		bytes += int64(rows) * 8
	}
	if i32 {
		bytes += int64(rows) * 4
	}
	if f32 {
		bytes += int64(rows) * 4
	}
	if f64 {
		bytes += int64(rows) * 8
	}
	if boolean {
		bytes += int64(rows)
	}
	if defs {
		bytes += int64(rows) * 2
	}
	return bytes
}

func (s *batchStream) available() int64 {
	available := s.opts.BatchBytes - s.managed
	if m := s.opts.MemoryManager; m != nil && m.Limit() > 0 {
		if free := m.Limit() - m.Used(); free < available {
			available = free
		}
	}
	return max(available, 0)
}

func (s *batchStream) reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	if n < 0 || n > s.opts.BatchBytes-s.managed {
		return nil, s.budgetError(n)
	}
	var charge memory.Reservation
	if s.opts.MemoryManager != nil {
		var err error
		charge, err = s.opts.MemoryManager.Reserve(ctx, n)
		if err != nil {
			return nil, err
		}
	}
	s.managed += n
	s.peak = max(s.peak, s.managed)
	return charge, nil
}

func (s *batchStream) budgetError(n int64) error {
	return fmt.Errorf("parquet decoded batch cannot fit row/buffer: requested=%d live=%d limit=%d: %w", n, s.managed, s.opts.BatchBytes, memory.ErrBudgetExceeded)
}

func (s *batchStream) allocateFixed(ctx context.Context, want int, stringBytes int64) error {
	limit := s.available()
	// Every supported projected vector needs at least one byte per row.
	// Restrict candidate lengths before arithmetic, including WordsFor's +63.
	// The divisor also makes the full per-column sum provably int64-safe.
	if len(s.bindings) > 0 {
		safeRows := int64(math.MaxInt64) / (32*int64(len(s.bindings)) + 64)
		want = min(want, int(min(safeRows, int64(math.MaxInt-64))), int(min(limit, int64(math.MaxInt-64))))
	}
	if want < 1 {
		return s.budgetError(s.fixedBytes(1) + stringBytes)
	}
	if stringBytes > limit || s.fixedBytes(1) > limit-stringBytes {
		return s.budgetError(s.fixedBytes(1) + stringBytes)
	}
	fixedLimit := limit - stringBytes
	// Leave room for variable data on the first batch. If a single row needs
	// more, readStrings retries with smaller fixed capacity before rejecting it.
	if len(s.strings) != 0 && stringBytes == 0 {
		fixedLimit = max(s.fixedBytes(1), limit/2)
	}
	lo, hi := 1, want
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if s.fixedBytes(mid) <= fixedLimit {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	n := lo
	charge, err := s.reserve(ctx, s.fixedBytes(n))
	if err != nil {
		return err
	}
	s.fixedCharge = charge
	s.capacity = n
	s.batch = memory.NewBatch()
	for _, b := range s.bindings {
		var valid memory.Bitmap
		if b.Nullable {
			valid = make(memory.Bitmap, memory.WordsFor(n))
		}
		switch b.Type {
		case memory.TypeInt8:
			s.batch.Int8s[b.Name] = &memory.Int8Vector{Values: make([]int8, n), Validity: valid}
		case memory.TypeUint8:
			s.batch.Uint8s[b.Name] = &memory.Uint8Vector{Values: make([]uint8, n), Validity: valid}
		case memory.TypeInt16:
			s.batch.Int16s[b.Name] = &memory.Int16Vector{Values: make([]int16, n), Validity: valid}
		case memory.TypeUint16:
			s.batch.Uint16s[b.Name] = &memory.Uint16Vector{Values: make([]uint16, n), Validity: valid}
		case memory.TypeInt32, memory.TypeDate32:
			s.batch.Int32s[b.Name] = &memory.Int32Vector{Values: make([]int32, n), Validity: valid}
		case memory.TypeInt64, memory.TypeTimestamp:
			s.batch.Int64s[b.Name] = &memory.Int64Vector{Values: make([]int64, n), Validity: valid}
		case memory.TypeFloat32:
			s.batch.Float32s[b.Name] = &memory.Float32Vector{Values: make([]float32, n), Validity: valid}
		case memory.TypeFloat64:
			s.batch.Float64s[b.Name] = &memory.Float64Vector{Values: make([]float64, n), Validity: valid}
		case memory.TypeBool:
			s.batch.Uint32s[b.Name] = &memory.Uint32Vector{Values: make([]uint32, n), Validity: valid}
		case memory.TypeString:
			s.batch.Strings[b.Name] = &memory.StringViewVector{Offsets: make([]uint32, n+1), Validity: valid, Encoding: memory.EncodingView}
		}
		if b.Nullable && b.Type != memory.TypeString || isNarrowBinding(b) {
			s.decode.defs = resize(s.decode.defs, n)
		}
		switch b.Type {
		case memory.TypeInt64, memory.TypeTimestamp:
			if b.Nullable {
				s.decode.dense64 = resize(s.decode.dense64, n)
			}
		case memory.TypeInt32, memory.TypeDate32:
			if b.Nullable {
				s.decode.dense32 = resize(s.decode.dense32, n)
			}
		case memory.TypeInt8, memory.TypeUint8, memory.TypeInt16, memory.TypeUint16:
			s.decode.dense32 = resize(s.decode.dense32, n)
		case memory.TypeFloat32:
			if b.Nullable {
				s.decode.denseF32 = resize(s.decode.denseF32, n)
			}
		case memory.TypeFloat64:
			if b.Nullable {
				s.decode.denseF64 = resize(s.decode.denseF64, n)
			}
		case memory.TypeBool:
			s.decode.denseBool = resize(s.decode.denseBool, n)
		}
	}
	return nil
}

func isNarrowBinding(b columnBinding) bool {
	return b.Type == memory.TypeInt8 || b.Type == memory.TypeInt16 || b.Type == memory.TypeUint8 || b.Type == memory.TypeUint16
}

func (s *batchStream) resetStrings() {
	for i := range s.strings {
		c := &s.strings[i]
		v := s.batch.Strings[c.binding.Name]
		v.Offsets = v.Offsets[:s.capacity+1]
		v.Offsets[0] = 0
		v.Data = v.Data[:0]
		if c.binding.Nullable {
			v.Validity = memory.EnsureBitmap(v.Validity, s.capacity)
		}
	}
}

func (s *batchStream) readPending(ctx context.Context) error {
	for i := range s.strings {
		if err := s.checkContext(ctx); err != nil {
			return err
		}
		c := &s.strings[i]
		chunk, ok := s.chunks[c.column].(*file.ByteArrayColumnChunkReader)
		if !ok {
			return fmt.Errorf("column %q is not a byte-array chunk reader", c.binding.Name)
		}
		c.value[0] = nil
		var defs []int16
		if c.binding.Nullable {
			defs = c.defs[:]
		}
		total, values, err := chunk.ReadBatchInPage(1, c.value[:], defs, nil)
		if err != nil {
			return err
		}
		if total != 1 || values < 0 || values > 1 || (!c.binding.Nullable && values != 1) {
			return fmt.Errorf("column %q row count mismatch total=%d values=%d want=1", c.binding.Name, total, values)
		}
		c.valid = !c.binding.Nullable || c.defs[0] != 0
		if c.valid != (values == 1) {
			return fmt.Errorf("column %q definition/value count mismatch", c.binding.Name)
		}
		if !c.valid {
			c.value[0] = nil
		}
		if !utf8.Valid(c.value[0]) {
			return fmt.Errorf("column %q contains invalid UTF-8", c.binding.Name)
		}
	}
	s.pending = true
	return nil
}

func (s *batchStream) pendingBytes() (int64, error) {
	var n int64
	for i := range s.strings {
		length := int64(len(s.strings[i].value[0]))
		if length > math.MaxUint32 || length > math.MaxInt64-n {
			return 0, s.budgetError(length)
		}
		n += length
	}
	return n, nil
}

// growString reserves the complete replacement allocation while the old one
// remains live. No append is allowed to choose an unaccounted Go capacity.
func (s *batchStream) growString(ctx context.Context, c *streamString, need int) error {
	v := s.batch.Strings[c.binding.Name]
	maxData := min(int64(math.MaxUint32), int64(math.MaxInt))
	if need < 0 || int64(need) > maxData {
		return s.budgetError(int64(need))
	}
	if need <= cap(v.Data) {
		return nil
	}
	free := s.available()
	if int64(need) > free {
		return s.budgetError(int64(need))
	}
	target := int64(need)
	// Geometric growth is only an optimization, not an additional reason to
	// reject a row. Exact required capacity is used near either limit.
	if doubled := min(int64(cap(v.Data))*2, maxData); doubled > target && doubled <= free {
		target = doubled
	}
	charge, err := s.reserve(ctx, target)
	if err != nil {
		return err
	}
	data := make([]byte, len(v.Data), int(target))
	copy(data, v.Data)
	old := cap(v.Data)
	v.Data = data
	if c.charge != nil {
		c.charge.Release()
	}
	c.charge = charge
	s.managed -= int64(old)
	return nil
}

func (s *batchStream) fitPending(ctx context.Context) error {
	for i := range s.strings {
		c := &s.strings[i]
		v := s.batch.Strings[c.binding.Name]
		if len(c.value[0]) > math.MaxInt-len(v.Data) {
			return s.budgetError(math.MaxInt64)
		}
		if err := s.growString(ctx, c, len(v.Data)+len(c.value[0])); err != nil {
			return err
		}
	}
	return nil
}

func (s *batchStream) readStrings(ctx context.Context, want int) (int, error) {
	for row := 0; row < want; row++ {
		if err := s.checkContext(ctx); err != nil {
			return 0, err
		}
		if !s.pending {
			if err := s.readPending(ctx); err != nil {
				return 0, err
			}
		}
		if err := s.fitPending(ctx); err != nil {
			if !errors.Is(err, memory.ErrBudgetExceeded) {
				return 0, err
			}
			if row != 0 {
				return row, nil
			}
			// Previous buffers may have capacity skewed toward another string
			// column, or fixed capacity may crowd out this larger single row.
			// Drop them before retrying so an individually admissible row works.
			bytes, sizeErr := s.pendingBytes()
			if sizeErr != nil {
				return 0, sizeErr
			}
			s.releaseBuffers()
			if err := s.allocateFixed(ctx, want, bytes); err != nil {
				return 0, err
			}
			want = min(want, s.capacity)
			s.resetStrings()
			if err := s.fitPending(ctx); err != nil {
				return 0, err
			}
		}
		for i := range s.strings {
			c := &s.strings[i]
			v := s.batch.Strings[c.binding.Name]
			start := len(v.Data)
			v.Data = v.Data[:start+len(c.value[0])]
			copy(v.Data[start:], c.value[0])
			v.Offsets[row+1] = uint32(len(v.Data))
			if c.binding.Nullable && c.valid {
				memory.Set(v.Validity, row)
			}
			c.value[0] = nil
		}
		s.pending = false
	}
	return want, nil
}

func (s *batchStream) closeChunks() error {
	var err error
	for _, c := range s.chunks {
		if c != nil {
			err = errors.Join(err, c.Close())
		}
	}
	s.chunks = nil
	s.groupOpen = false
	return err
}

func (s *batchStream) releaseBuffers() {
	s.batch = nil
	s.decode.dense64 = nil
	s.decode.dense32 = nil
	s.decode.denseF32 = nil
	s.decode.denseF64 = nil
	s.decode.denseBool = nil
	s.decode.defs = nil
	if s.fixedCharge != nil {
		s.fixedCharge.Release()
		s.fixedCharge = nil
	}
	for i := range s.strings {
		if c := s.strings[i].charge; c != nil {
			c.Release()
			s.strings[i].charge = nil
		}
	}
	s.managed = 0
	s.capacity = 0
}

func (s *batchStream) release() error {
	err := s.closeChunks()
	s.releaseBuffers()
	for i := range s.strings {
		s.strings[i].value[0] = nil
	}
	s.pending = false
	if s.r != nil {
		err = errors.Join(err, s.r.Close())
		s.r = nil
	}
	s.closeErr = errors.Join(s.closeErr, err)
	return s.closeErr
}

func (s *batchStream) fail(err error) error {
	s.terminalErr = errors.Join(err, s.release())
	return s.terminalErr
}

// Close releases owned buffers and column/file readers exactly once.
func (s *batchStream) Close() error {
	if s == nil {
		return nil
	}
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	return s.release()
}

// StreamDiagnostics describes only managed decode buffers, not process RSS.
func (s *batchStream) StreamDiagnostics() map[string]string {
	return map[string]string{
		"parquet_batches":            strconv.FormatInt(s.batches, 10),
		"parquet_rows":               strconv.FormatInt(s.rows, 10),
		"parquet_peak_managed_bytes": strconv.FormatInt(s.peak, 10),
		"parquet_memory_scope":       "decoded vector capacities and scratch; excludes Arrow pages, dictionaries, codecs, stream metadata and Go object overhead",
	}
}

func bindStreamBatch(dst, src *memory.Batch) {
	if dst.Int8s == nil || dst.Int16s == nil || dst.Int32s == nil || dst.Int64s == nil || dst.Uint8s == nil || dst.Uint16s == nil || dst.Uint32s == nil || dst.Float32s == nil || dst.Float64s == nil || dst.Strings == nil {
		*dst = *memory.NewBatch()
	}
	for k, v := range src.Int8s {
		dst.Int8s[k] = v
	}
	for k, v := range src.Int16s {
		dst.Int16s[k] = v
	}
	for k, v := range src.Int32s {
		dst.Int32s[k] = v
	}
	for k, v := range src.Int64s {
		dst.Int64s[k] = v
	}
	for k, v := range src.Uint8s {
		dst.Uint8s[k] = v
	}
	for k, v := range src.Uint16s {
		dst.Uint16s[k] = v
	}
	for k, v := range src.Uint32s {
		dst.Uint32s[k] = v
	}
	for k, v := range src.Float32s {
		dst.Float32s[k] = v
	}
	for k, v := range src.Float64s {
		dst.Float64s[k] = v
	}
	for k, v := range src.Strings {
		dst.Strings[k] = v
	}
	dst.Rows = src.Rows
}

var _ source.BatchStreamProvider = (*Source)(nil)
var _ source.PreparedBatchStreamProvider = (*Source)(nil)
var _ source.BatchStreamDiagnostics = (*batchStream)(nil)
