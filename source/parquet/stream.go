package parquet

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"strconv"
	"unicode/utf8"
	"unsafe"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
)

// NewBatchStream decodes projected row groups incrementally. BatchRows and
// BatchBytes bound GoLume's live decoded vector buffers and decode scratch,
// including allocation overlap and borrowed string-view header arrays.
// Stream metadata (including inline single-value lookahead, optional page
// indexes and selected row ranges), Go
// object overhead, and Arrow page, dictionary,
// codec and metadata allocations are excluded; this is not an RSS limit. Input
// chunks use buffered I/O rather than whole-column compressed buffering.
// Separate compressed/uncompressed page caps reject oversized declared page
// bodies before Arrow allocates them; these caps are not aggregate quotas.
//
// A row that cannot fit by itself fails with memory.ErrBudgetExceeded. Strings
// are inspected through borrowed Arrow page views before any GoLume payload
// allocation. A byte boundary may produce fewer rows than BatchRows. A
// large row can temporarily reduce fixed-vector capacity; later batches
// reconsider capacity, so one outlier does not throttle the rest of its group.
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
	props.MaxCompressedPageSize = opts.MaxCompressedPageBytes
	props.MaxUncompressedPageSize = opts.MaxUncompressedPageBytes
	r, err := file.OpenParquetFile(s.path, false, file.WithReadProps(props))
	if err != nil {
		return nil, err
	}
	return s.batchStreamFromReader(ctx, opts, parts, r), nil
}

// batchStreamFromReader takes ownership of an already opened reader. Options
// must be normalized and validated by the caller.
func (s *Source) batchStreamFromReader(ctx context.Context, opts source.ReadOptions, parts []source.Partition, r *file.Reader) *batchStream {
	out := &batchStream{
		ctx: ctx, r: r, parts: parts, bindings: s.bindings, opts: opts,
		decode: typedDecoder{kernels: opts.Kernels}, pagePredicate: s.pagePredicate,
	}
	for i, b := range s.bindings {
		switch b.Type {
		case memory.TypeInt8, memory.TypeUint8:
			out.payloadRowBytes++
		case memory.TypeInt16, memory.TypeUint16:
			out.payloadRowBytes += 2
		case memory.TypeInt32, memory.TypeUint32, memory.TypeDate32, memory.TypeFloat32, memory.TypeBool:
			out.payloadRowBytes += 4
		case memory.TypeInt64, memory.TypeUint64, memory.TypeTimestamp, memory.TypeFloat64:
			out.payloadRowBytes += 8
		case memory.TypeDecimal:
			out.payloadRowBytes += 16
		case memory.TypeString:
			out.payloadRowBytes += 4
			out.payloadOffsetBytes += 4
		}
		if b.Nullable {
			out.payloadValidityColumns++
		}
		if b.Type == memory.TypeString {
			out.strings = append(out.strings, streamString{binding: b, column: i})
		}
	}
	return out
}

// streamString owns only bounded headers. Payloads borrow exactly one Arrow
// page per column and are never carried across a reader advance. Inline slots
// preserve single-row admission when even bulk headers would crowd out a row.
type streamString struct {
	binding     columnBinding
	column      int
	inlineValue [1]arrowparquet.ByteArray
	inlineDef   [1]int16
	values      []arrowparquet.ByteArray
	defs        []int16
	pos, rows   int
	target      int
	vector      *memory.StringViewVector
	charge      memory.Reservation
}

type batchStream struct {
	ctx                    context.Context
	r                      *file.Reader
	parts                  []source.Partition
	bindings               []columnBinding
	opts                   source.ReadOptions
	decode                 typedDecoder
	strings                []streamString
	chunks                 []file.ColumnChunkReader
	part                   int
	remaining              int64
	groupOpen              bool
	groupID                int
	groupRows              int64
	pagePredicate          expr.Expr
	pageRanges             []pageRowRange
	pageRange              int
	pageEnd                int64
	pagePrunedRows         int64
	anchorPagesPruned      int64
	payloadRowBytes        int64
	payloadOffsetBytes     int64
	payloadValidityColumns int64
	headerCapacity         int
	headerBytes            int64
	headerCharge           memory.Reservation
	restoreCapacity        bool
	stringReadCalls        int64
	stringReplays          int64
	batch                  *memory.Batch
	capacity               int
	fixedCharge            memory.Reservation
	managed                int64
	peak                   int64
	batches                int64
	rows                   int64
	exhausted              bool
	closed                 bool
	terminalErr            error
	closeErr               error
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
	groupChanged := false
	for {
		if s.groupOpen && s.remaining > 0 {
			if err := s.advancePageRange(ctx); err != nil {
				return false, s.fail(err)
			}
			if s.remaining > 0 {
				break
			}
		}
		if err := s.closeChunks(); err != nil {
			return false, s.fail(err)
		}
		// Owned vectors/scratch can serve the next group unchanged. Drop only
		// borrowed page references and cursor positions at a group boundary.
		// Releasing every vector here turns a bounded scan into one large
		// allocation per group when consumers request large batches.
		for i := range s.strings {
			c := &s.strings[i]
			clear(c.values)
			c.pos, c.rows = 0, 0
		}
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
		groupChanged = true
		// Empty groups do not create data batches; schema is source metadata.
	}
	want := s.opts.BatchRows
	if int64(want) > s.remaining {
		want = int(s.remaining)
	}
	if s.pageRanges != nil && int64(want) > s.pageEnd-(s.groupRows-s.remaining) {
		want = int(s.pageEnd - (s.groupRows - s.remaining))
	}
	var err error
	want, err = s.limitPayloadRows(want)
	if err != nil {
		return false, s.fail(err)
	}
	if groupChanged && s.batch != nil && want > s.capacity {
		// A preceding small group must not constrain a larger one. Release
		// before rebuilding so the replacement never overlaps uncharged.
		s.releaseBuffers()
		s.releaseHeaders()
	}
	if s.restoreCapacity {
		s.releaseBuffers()
		s.restoreCapacity = false
		// An inline outlier cursor is exhausted here. Restore batched page
		// reads along with normal output capacity on the very next batch.
		if s.headerCapacity == 1 {
			exhausted := true
			for i := range s.strings {
				exhausted = exhausted && s.strings[i].pos == s.strings[i].rows
			}
			if exhausted {
				s.releaseHeaders()
			}
		}
	}
	if s.headerCapacity == 0 {
		if err := s.allocateHeaders(ctx, want); err != nil {
			return false, s.fail(err)
		}
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
		case memory.TypeUint32:
			err = s.decode.readUint32(s.chunks[i], b, rows, s.batch)
		case memory.TypeUint64:
			err = s.decode.readUint64(s.chunks[i], b, rows, s.batch)
		case memory.TypeFloat32:
			err = s.decode.readFloat32(s.chunks[i], b, rows, s.batch)
		case memory.TypeDecimal:
			err = s.decode.readDecimal(s.chunks[i], b, rows, s.batch)
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
	s.groupID, s.groupRows = p.ID, s.remaining
	if s.remaining < 0 {
		return fmt.Errorf("row group %d has negative row count", p.ID)
	}
	s.pageRanges, s.pageRange, s.pageEnd = nil, 0, 0
	if plan := planPagePruning(ctx, s.r, p.ID, s.bindings, s.pagePredicate); plan != nil {
		s.pagePrunedRows += plan.prunedRows
		s.anchorPagesPruned += plan.prunedPages
		s.pageRanges = plan.ranges
		if len(plan.ranges) == 0 {
			s.remaining = 0
			s.groupOpen = true
			return nil
		}
	}
	s.chunks = make([]file.ColumnChunkReader, len(s.bindings))
	for i, b := range s.bindings {
		if err := s.checkContext(ctx); err != nil {
			return err
		}
		if b.Type == memory.TypeDecimal {
			physical := s.r.MetaData().Schema.Column(b.Index).PhysicalType()
			if err := validateDecimalPages(ctx, rg, b, physical); err != nil {
				return err
			}
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

// outputFixedBytes counts output lengths only, independently of owned decode
// scratch, header capacities, and replacement overlap. Saturation also keeps
// extreme row options safe before allocateFixed applies its tighter bound.
func (s *batchStream) outputFixedBytes(rows int) int64 {
	n := s.payloadOffsetBytes
	if rows < 0 {
		return math.MaxInt64
	}
	if s.payloadRowBytes > 0 && int64(rows) > (math.MaxInt64-n)/s.payloadRowBytes {
		return math.MaxInt64
	}
	n += int64(rows) * s.payloadRowBytes
	var words int64
	if rows > 0 {
		words = int64((rows-1)/64) + 1
	}
	if s.payloadValidityColumns > 0 && words > (math.MaxInt64-n)/s.payloadValidityColumns/8 {
		return math.MaxInt64
	}
	return n + words*s.payloadValidityColumns*8
}

func (s *batchStream) payloadLimit(rows int) int64 {
	limit := s.opts.BatchPayloadBytes
	if target := s.opts.BatchPayloadTargetBytes; rows > 1 && target > 0 && (limit == 0 || target < limit) {
		limit = target
	}
	return limit
}

func (s *batchStream) payloadError(n int64) error {
	return fmt.Errorf("parquet row output payload %d exceeds hard limit %d: %w", n, s.opts.BatchPayloadBytes, memory.ErrBudgetExceeded)
}

func (s *batchStream) limitPayloadRows(want int) (int, error) {
	hard, target := s.opts.BatchPayloadBytes, s.opts.BatchPayloadTargetBytes
	if hard == 0 && target == 0 {
		return want, nil
	}
	one := s.outputFixedBytes(1)
	if hard > 0 && one > hard {
		return 0, s.payloadError(one)
	}
	limit := hard
	if target > 0 && (limit == 0 || target < limit) {
		limit = target
	}
	if one > limit {
		return 1, nil
	} // A legal single row may exceed the soft target.
	lo, hi := 1, want
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if s.outputFixedBytes(mid) <= limit {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo, nil
}

// fixedBytes includes every typed scratch buffer the native typed decoders
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
		case memory.TypeUint32:
			width = 4
			i32 = true
		case memory.TypeUint64:
			width = 8
			i64 = true
		case memory.TypeFloat32:
			width = 4
			f32 = f32 || b.Nullable
		case memory.TypeDecimal:
			width = 16
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
			defs = defs || (b.Type != memory.TypeString && b.Type != memory.TypeDecimal)
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
		case memory.TypeDecimal:
			s.batch.Decimals[b.Name] = &memory.Decimal128Vector{Values: make([]memory.Decimal128, n), Validity: valid, Precision: b.DecimalPrecision, Scale: b.DecimalScale}
		case memory.TypeFloat64:
			s.batch.Float64s[b.Name] = &memory.Float64Vector{Values: make([]float64, n), Validity: valid}
		case memory.TypeUint64:
			s.batch.Uint64s[b.Name] = &memory.Uint64Vector{Values: make([]uint64, n), Validity: valid}
		case memory.TypeUint32, memory.TypeBool:
			s.batch.Uint32s[b.Name] = &memory.Uint32Vector{Values: make([]uint32, n), Validity: valid}
		case memory.TypeString:
			s.batch.Strings[b.Name] = &memory.StringViewVector{Offsets: make([]uint32, n+1), Validity: valid, Encoding: memory.EncodingView}
		}
		if b.Nullable && b.Type != memory.TypeString && b.Type != memory.TypeDecimal || isNarrowBinding(b) {
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
		case memory.TypeUint32:
			s.decode.dense32 = resize(s.decode.dense32, n)
		case memory.TypeUint64:
			s.decode.dense64 = resize(s.decode.dense64, n)
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

func resize[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}
	return s[:n]
}

func isNarrowBinding(b columnBinding) bool {
	return b.Type == memory.TypeInt8 || b.Type == memory.TypeInt16 || b.Type == memory.TypeUint8 || b.Type == memory.TypeUint16
}

// allocateHeaders reserves all slice backing arrays before reading any borrowed
// values. Header storage uses at most one eighth of the available budget and
// is independent of row-group size. A one-row cursor lives in stream metadata.
func (s *batchStream) allocateHeaders(ctx context.Context, want int) error {
	if len(s.strings) == 0 {
		s.headerCapacity = 1
		return nil
	}
	width := int64(len(s.strings)) * int64(unsafe.Sizeof(arrowparquet.ByteArray{}))
	for i := range s.strings {
		if s.strings[i].binding.Nullable {
			width += 2
		}
	}
	n := int(min(int64(want), 1024, s.available()/8/width))
	if n < 2 {
		n = 1
	}
	if n > 1 {
		bytes := int64(n) * width
		charge, err := s.reserve(ctx, bytes)
		if err != nil {
			return err
		}
		s.headerCharge, s.headerBytes = charge, bytes
	}
	s.headerCapacity = n
	for i := range s.strings {
		c := &s.strings[i]
		if n == 1 {
			c.values, c.defs = c.inlineValue[:], c.inlineDef[:]
		} else {
			c.values = make([]arrowparquet.ByteArray, n)
			if c.binding.Nullable {
				c.defs = make([]int16, n)
			}
		}
	}
	return nil
}

func (s *batchStream) releaseHeaders() {
	for i := range s.strings {
		c := &s.strings[i]
		c.values, c.defs = nil, nil
		c.inlineValue[0] = nil
		c.pos, c.rows = 0, 0
	}
	if s.headerCharge != nil {
		s.headerCharge.Release()
		s.headerCharge = nil
	}
	s.managed -= s.headerBytes
	s.headerBytes, s.headerCapacity = 0, 0
}

func (s *batchStream) resetStrings() {
	for i := range s.strings {
		c := &s.strings[i]
		v := s.batch.Strings[c.binding.Name]
		c.vector = v
		v.Offsets = v.Offsets[:s.capacity+1]
		v.Offsets[0] = 0
		v.Data = v.Data[:0]
		if c.binding.Nullable {
			v.Validity = v.Validity[:memory.WordsFor(s.capacity)]
			clear(v.Validity)
		}
	}
}

// refillStrings returns an aligned span available in every string cursor.
// Different columns may end their pages on different logical rows. A reader
// advances only after every borrowed value from its preceding read was copied.
func (s *batchStream) refillStrings(ctx context.Context, want int) (int, error) {
	span := want
	for i := range s.strings {
		c := &s.strings[i]
		if c.pos == c.rows {
			if err := s.checkContext(ctx); err != nil {
				return 0, err
			}
			chunk, ok := s.chunks[c.column].(*file.ByteArrayColumnChunkReader)
			if !ok {
				return 0, fmt.Errorf("column %q is not a byte-array chunk reader", c.binding.Name)
			}
			clear(c.values)
			var defs []int16
			if c.binding.Nullable {
				defs = c.defs
			}
			// Never look past the output row limit, even when a previous call retained
			// a larger header buffer. Invalid later rows are reported by their batch.
			n := min(want, len(c.values))
			total, values, err := chunk.ReadBatchInPage(int64(n), c.values, defs, nil)
			s.stringReadCalls++
			if err != nil {
				return 0, err
			}
			if total <= 0 || total > int64(n) || values < 0 || int64(values) > total || (!c.binding.Nullable && int64(values) != total) {
				return 0, fmt.Errorf("column %q row count mismatch total=%d values=%d want<=%d", c.binding.Name, total, values, n)
			}
			c.pos, c.rows = 0, int(total)
			if c.binding.Nullable {
				// Scatter dense page views backwards into row order without another
				// header allocation. Definitions distinguish empty strings from NULL.
				dense := values
				for row := c.rows - 1; row >= 0; row-- {
					if c.defs[row] == 0 {
						c.values[row] = nil
						continue
					}
					if c.defs[row] != 1 || dense == 0 {
						return 0, fmt.Errorf("column %q definition/value count mismatch", c.binding.Name)
					}
					dense--
					c.values[row] = c.values[dense]
				}
				if dense != 0 {
					return 0, fmt.Errorf("column %q definition/value count mismatch", c.binding.Name)
				}
			}
		}
		span = min(span, c.rows-c.pos)
	}
	return span, nil
}

// admissibleSpan checks payload lengths in batches, with no per-row allocator,
// context, or Arrow calls. All replacement buffers must fit together while the
// preceding allocations remain live. No unaccounted append growth is possible.
func (s *batchStream) admissibleSpan(baseRows, span int) (int, error) {
	for i := range s.strings {
		c := &s.strings[i]
		c.target = len(c.vector.Data)
	}
	free := s.available()
	for row := 0; row < span; row++ {
		payloadLimit := s.payloadLimit(baseRows + row + 1)
		var payload int64
		var replacements int64
		fits := true
		for i := range s.strings {
			c := &s.strings[i]
			value := c.values[c.pos+row]
			if !utf8.Valid(value) {
				return 0, fmt.Errorf("column %q contains invalid UTF-8", c.binding.Name)
			}
			if len(value) > math.MaxInt-c.target || int64(len(value))+int64(c.target) > math.MaxUint32 {
				fits = false
				continue
			}
			c.target += len(value)
			if payloadLimit > 0 {
				if int64(c.target) > math.MaxInt64-payload {
					fits = false
				} else {
					payload += int64(c.target)
				}
			}
			if c.target > cap(c.vector.Data) {
				// Each target is at most MaxUint32. Saturate against the actual free
				// capacity to avoid overflow even with an extreme column count.
				if int64(c.target) > free-replacements {
					fits = false
				} else {
					replacements += int64(c.target)
				}
			}
		}
		if payloadLimit > 0 {
			fixed := s.outputFixedBytes(baseRows + row + 1)
			if fixed > payloadLimit || payload > payloadLimit-fixed {
				fits = false
			}
		}
		if !fits {
			for i := range s.strings {
				c := &s.strings[i]
				c.target = len(c.vector.Data)
				for prior := 0; prior < row; prior++ {
					c.target += len(c.values[c.pos+prior])
				}
			}
			return row, nil
		}
	}
	return span, nil
}

func (s *batchStream) growStrings(ctx context.Context) error {
	// Admission checked aggregate replacement capacity. Reserve and allocate
	// exact targets, releasing each old allocation only after copying it.
	for i := range s.strings {
		c := &s.strings[i]
		v := c.vector
		if c.target <= cap(v.Data) {
			continue
		}
		charge, err := s.reserve(ctx, int64(c.target))
		if err != nil {
			return err
		}
		data := make([]byte, len(v.Data), c.target)
		copy(data, v.Data)
		old := cap(v.Data)
		v.Data = data
		if c.charge != nil {
			c.charge.Release()
		}
		c.charge = charge
		s.managed -= int64(old)
	}
	return nil
}

func (s *batchStream) copyStrings(row, count int) {
	for i := range s.strings {
		c := &s.strings[i]
		v := c.vector
		for j := 0; j < count; j++ {
			at := c.pos + j
			value := c.values[at]
			start := len(v.Data)
			v.Data = v.Data[:start+len(value)]
			copy(v.Data[start:], value)
			v.Offsets[row+j+1] = uint32(len(v.Data))
			if c.binding.Nullable && c.defs[at] != 0 {
				memory.Set(v.Validity, row+j)
			}
			c.values[at] = nil
		}
		c.pos += count
	}
}

func (s *batchStream) pendingBytes() (int64, error) {
	var n int64
	for i := range s.strings {
		c := &s.strings[i]
		length := int64(len(c.values[c.pos]))
		if length > math.MaxUint32 || length > math.MaxInt64-n {
			return 0, s.budgetError(length)
		}
		n += length
	}
	return n, nil
}

// replayInline is an exceptional outlier path, not a second reader API. If
// charged lookahead headers prevent a row from fitting by itself, discard the
// views and reopen only string chunks at the committed row. This preserves
// exact admission without uncharged clones or dropping prefetched rows.
// Replaying can reread preceding pages, so an adversarial outlier distribution
// trades throughput for exact bounded admission; no whole group is buffered.
func (s *batchStream) replayInline(ctx context.Context) error {
	s.releaseHeaders()
	s.headerCapacity = 1
	for i := range s.strings {
		if err := s.checkContext(ctx); err != nil {
			return err
		}
		c := &s.strings[i]
		if err := s.chunks[c.column].Close(); err != nil {
			return err
		}
		s.chunks[c.column] = nil
		chunk, err := s.r.RowGroup(s.groupID).Column(c.binding.Index)
		if err != nil {
			return err
		}
		s.chunks[c.column] = chunk
		// Skip on a fresh reader handles files with or without an offset index.
		skip := s.groupRows - s.remaining
		for skip > 0 {
			if err := s.checkContext(ctx); err != nil {
				return err
			}
			// Arrow may allocate a definition-level scratch slice for a
			// partial-page skip. Bound each request instead of letting a
			// row-group-sized skip create row-group-sized decode scratch.
			step := min(skip, 1024)
			n, err := chunk.(*file.ByteArrayColumnChunkReader).Skip(step)
			if err != nil {
				return err
			}
			if n != step {
				return fmt.Errorf("column %q replay skipped %d rows, want %d", c.binding.Name, n, step)
			}
			skip -= step
		}
		c.values, c.defs = c.inlineValue[:], c.inlineDef[:]
	}
	s.stringReplays++
	_, err := s.refillStrings(ctx, 1)
	return err
}

func (s *batchStream) readStrings(ctx context.Context, want int) (int, error) {
	row := 0
	for row < want {
		span, err := s.refillStrings(ctx, want-row)
		if err != nil {
			return 0, err
		}
		count, err := s.admissibleSpan(row, span)
		if err != nil {
			return 0, err
		}
		if count == 0 {
			if row != 0 {
				return row, nil
			}
			bytes, err := s.pendingBytes()
			if err != nil {
				return 0, err
			}
			if hard := s.opts.BatchPayloadBytes; hard > 0 {
				fixed := s.outputFixedBytes(1)
				if fixed > hard || bytes > hard-fixed {
					return 0, s.payloadError(min(math.MaxInt64-bytes, fixed) + bytes)
				}
			}
			// Release old output before resizing, never alongside a live replacement.
			// Header arrays and borrowed pages remain intact unless a replay is needed.
			s.releaseBuffers()
			err = s.allocateFixed(ctx, want, bytes)
			if errors.Is(err, memory.ErrBudgetExceeded) && s.headerBytes != 0 {
				if err := s.replayInline(ctx); err != nil {
					return 0, err
				}
				err = s.allocateFixed(ctx, want, bytes)
			}
			if err != nil {
				return 0, err
			}
			s.restoreCapacity = true
			want = min(want, s.capacity)
			s.resetStrings()
			count, err = s.admissibleSpan(0, 1)
			if err != nil {
				return 0, err
			}
			if count == 0 {
				return 0, s.budgetError(bytes)
			}
		}
		if err := s.growStrings(ctx); err != nil {
			return 0, err
		}
		s.copyStrings(row, count)
		row += count
	}
	return row, nil
}

func (s *batchStream) closeChunks() error {
	var err error
	for _, c := range s.chunks {
		if c != nil {
			err = errors.Join(err, c.Close())
		}
	}
	s.chunks = nil
	s.pageRanges = nil
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
	s.managed = s.headerBytes
	s.capacity = 0
	for i := range s.strings {
		s.strings[i].vector = nil
	}
}

func (s *batchStream) release() error {
	err := s.closeChunks()
	s.releaseBuffers()
	s.releaseHeaders()
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
		"parquet_batches":                     strconv.FormatInt(s.batches, 10),
		"parquet_rows":                        strconv.FormatInt(s.rows, 10),
		"parquet_page_pruned_rows":            strconv.FormatInt(s.pagePrunedRows, 10),
		"parquet_anchor_pages_pruned":         strconv.FormatInt(s.anchorPagesPruned, 10),
		"parquet_peak_managed_bytes":          strconv.FormatInt(s.peak, 10),
		"parquet_max_compressed_page_bytes":   strconv.FormatInt(s.opts.MaxCompressedPageBytes, 10),
		"parquet_max_uncompressed_page_bytes": strconv.FormatInt(s.opts.MaxUncompressedPageBytes, 10),
		"parquet_string_page_reads":           strconv.FormatInt(s.stringReadCalls, 10),
		"parquet_string_replays":              strconv.FormatInt(s.stringReplays, 10),
		"parquet_memory_scope":                "decoded vector capacities, typed scratch and borrowed-view headers; excludes Arrow pages, dictionaries, codecs, stream metadata and Go object overhead",
	}
}

func bindStreamBatch(dst, src *memory.Batch) {
	if dst.Int8s == nil || dst.Int16s == nil || dst.Int32s == nil || dst.Int64s == nil || dst.Uint8s == nil || dst.Uint16s == nil || dst.Uint32s == nil || dst.Uint64s == nil || dst.Float32s == nil || dst.Float64s == nil || dst.Decimals == nil || dst.Strings == nil {
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
	for k, v := range src.Uint64s {
		dst.Uint64s[k] = v
	}
	for k, v := range src.Float32s {
		dst.Float32s[k] = v
	}
	for k, v := range src.Decimals {
		dst.Decimals[k] = v
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
