package parquet

import (
	"context"
	"fmt"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
)

type fixedStream struct {
	metadataBytes     int64
	ctx               context.Context
	opts              source.ReadOptions
	reader            *file.Reader
	fields            []memory.Field
	leaves            []fixedLeaf
	parts             []source.Partition
	chunks            []file.ColumnChunkReader
	part              int
	remaining         int64
	batch             *memory.Batch
	lease             memory.Reservation
	scratchLease      memory.Reservation
	closed, exhausted bool
	terminal          error
	scratch           fixedScratch
}

// Scratch is shared across leaves: one logical row never grows with file size.
type fixedScratch struct {
	defs         [1]int16
	i32          [1]int32
	i64          [1]int64
	f32          [1]float32
	f64          [1]float64
	boolean      [1]bool
	decimal      memory.Decimal128
	decimalBytes [1]arrowparquet.ByteArray
	decimalFixed [1]arrowparquet.FixedLenByteArray
}

func (s *Source) newFixedStream(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("native fixed stream requires context")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts = opts.Normalize()
	if opts.MemoryManager == nil {
		if q := memory.QueryBudgetFromContext(ctx); q != nil {
			opts.MemoryManager = q.Operator("SourceScan", memory.MemoryWorkspace)
		}
	}
	count := 0
	for _, l := range s.fixed.leaves {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := l.parent
		if name == "" {
			name = l.binding.Name
		}
		if _, ok := s.schema.Field(name); ok {
			count++
		}
	}
	if int64(count) > (math.MaxInt64-2048)/2048 {
		return nil, fmt.Errorf("native fixed metadata size overflow")
	}
	scratchBytes := int64(2048) + int64(count)*2048
	extra, e := s.recursiveForestMetadata(ctx)
	if e != nil {
		return nil, e
	}
	if extra > math.MaxInt64-scratchBytes {
		return nil, fmt.Errorf("recursive Struct metadata overflow")
	}
	scratchBytes += extra
	groupExtra, e := s.structListGroupMetadata(ctx)
	if e != nil {
		return nil, e
	}
	if groupExtra > math.MaxInt64-scratchBytes {
		return nil, fmt.Errorf("StructList metadata overflow")
	}
	scratchBytes += groupExtra
	arrayExtra, e := s.arrayMetadata(ctx)
	if e != nil {
		return nil, e
	}
	if arrayExtra > math.MaxInt64-scratchBytes {
		return nil, fmt.Errorf("array metadata overflow")
	}
	scratchBytes += arrayExtra
	if scratchBytes >= opts.BatchBytes {
		return nil, fmt.Errorf("native fixed scratch: %w", memory.ErrBudgetExceeded)
	}
	var lease memory.Reservation
	var err error
	if opts.MemoryManager != nil {
		lease, err = opts.MemoryManager.Reserve(ctx, scratchBytes)
		if err != nil {
			return nil, err
		}
	}
	leaves := make([]fixedLeaf, 0, count)
	for _, l := range s.fixed.leaves {
		if err := ctx.Err(); err != nil {
			if lease != nil {
				lease.Release()
			}
			return nil, err
		}
		name := l.parent
		if name == "" {
			name = l.binding.Name
		}
		if _, ok := s.schema.Field(name); ok {
			leaves = append(leaves, l)
		}
	}
	props := arrowparquet.NewReaderProperties(nil)
	props.BufferedStreamEnabled = true
	props.PageStreamingEnabled = true
	props.MaxCompressedPageSize = opts.MaxCompressedPageBytes
	props.MaxUncompressedPageSize = opts.MaxUncompressedPageBytes
	r, err := file.OpenParquetFile(s.path, false, file.WithReadProps(props))
	if err != nil {
		if lease != nil {
			lease.Release()
		}
		return nil, err
	}
	return &fixedStream{metadataBytes: scratchBytes, ctx: ctx, opts: opts, reader: r, fields: memory.NewSchema(s.schema.Fields...).Fields, leaves: leaves, parts: parts, scratchLease: lease}, nil
}

func fixedWidth(t memory.DataType) int64 {
	switch t {
	case memory.TypeDecimal:
		return 16
	case memory.TypeInt8, memory.TypeUint8:
		return 1
	case memory.TypeInt16, memory.TypeUint16:
		return 2
	case memory.TypeString, memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32, memory.TypeBool, memory.TypeDate32:
		return 4
	default:
		return 8
	}
}
func (s *fixedStream) payload(rows int) int64 {
	if rows < 0 {
		return -1
	}
	r := int64(rows)
	words := (r / 64) * 8
	if r%64 != 0 {
		words += 8
	}
	total := int64(0)
	add := func(width int64) bool {
		if r > (math.MaxInt64-words)/width {
			return false
		}
		n := r*width + words
		if total > math.MaxInt64-n {
			return false
		}
		total += n
		return true
	}
	for _, f := range s.fields {
		if f.Type == memory.TypeStruct {
			if total > math.MaxInt64-words {
				return -1
			}
			total += words
			for _, c := range f.Children {
				if !add(fixedWidth(c.Type)) {
					return -1
				}
			}
		} else {
			if !add(fixedWidth(f.Type)) {
				return -1
			}
		}
	}
	return total
}
func (s *fixedStream) clearBatch() {
	if s.batch != nil {
		s.batch.ClearColumns()
		s.batch = nil
	}
	if s.lease != nil {
		s.lease.Release()
		s.lease = nil
	}
}
func (s *fixedStream) closeChunks() error {
	var first error
	for _, c := range s.chunks {
		if c != nil {
			if e := c.Close(); e != nil && first == nil {
				first = e
			}
		}
	}
	s.chunks = nil
	return first
}
func (s *fixedStream) release() error {
	s.scratch.decimalBytes[0] = nil
	s.scratch.decimalFixed[0] = nil
	s.scratch.decimal = memory.Decimal128{}
	s.clearBatch()
	err := s.closeChunks()
	if s.reader != nil {
		if e := s.reader.Close(); err == nil {
			err = e
		}
		s.reader = nil
	}
	if s.scratchLease != nil {
		s.scratchLease.Release()
		s.scratchLease = nil
	}
	return err
}

// Close releases native buffers, reservations and readers. Repeated calls are safe.
func (s *fixedStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.release()
}
func (s *fixedStream) fail(err error) (bool, error) { s.terminal = err; s.release(); return false, err }

// Next fills dst with a borrowed batch valid until the next call or Close.
// Decode, budget and cancellation errors are terminal and release stream resources.
// Both the construction context and this call's context govern decoding.
func (s *fixedStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("nil batch destination")
	}
	dst.ClearColumns()
	if ctx == nil {
		return s.fail(fmt.Errorf("nil stream context"))
	}
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminal != nil {
		return false, s.terminal
	}
	if s.exhausted {
		return false, nil
	}
	if err := fixedContext(s.ctx, ctx); err != nil {
		return s.fail(err)
	}
	s.clearBatch()
	for s.remaining == 0 {
		if err := s.closeChunks(); err != nil {
			return s.fail(err)
		}
		if s.part == len(s.parts) {
			s.exhausted = true
			if err := s.release(); err != nil {
				return s.fail(err)
			}
			return false, nil
		}
		p := s.parts[s.part]
		s.part++
		if p.ID < 0 || p.ID >= s.reader.NumRowGroups() {
			return s.fail(fmt.Errorf("row group %d out of range", p.ID))
		}
		rg := s.reader.RowGroup(p.ID)
		s.remaining = rg.MetaData().NumRows()
		if s.remaining < 0 {
			return s.fail(fmt.Errorf("negative row group count"))
		}
		s.chunks = make([]file.ColumnChunkReader, len(s.leaves))
		for i, l := range s.leaves {
			if err := fixedContext(s.ctx, ctx); err != nil {
				return s.fail(err)
			}
			metadata, err := rg.MetaData().ColumnChunk(l.binding.Index)
			if err != nil {
				return s.fail(err)
			}
			if metadata.NumValues() != s.remaining {
				return s.fail(fmt.Errorf("native fixed leaf %q value count %d differs from row group rows %d", l.binding.Name, metadata.NumValues(), s.remaining))
			}
			if err := s.validateRootDecimalPages(ctx, rg, l); err != nil {
				return s.fail(err)
			}
			c, err := rg.Column(l.binding.Index)
			if err != nil {
				return s.fail(err)
			}
			s.chunks[i] = c
		}
	}
	want := s.opts.BatchRows
	if int64(want) > s.remaining {
		want = int(s.remaining)
	}
	scratchBytes := int64(2048) + int64(len(s.leaves))*2048
	limit := s.opts.BatchBytes - scratchBytes
	fits := func(n int) bool {
		p := s.payload(n)
		return p >= 0 && p <= limit && (s.opts.BatchPayloadBytes == 0 || p <= s.opts.BatchPayloadBytes) && (n == 1 || s.opts.BatchPayloadTargetBytes == 0 || p <= s.opts.BatchPayloadTargetBytes)
	}
	if !fits(1) {
		return s.fail(fmt.Errorf("native fixed row: %w", memory.ErrBudgetExceeded))
	}
	lo, hi := 1, want
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if fits(mid) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	rows := lo
	if s.opts.MemoryManager != nil {
		lease, err := s.opts.MemoryManager.Reserve(ctx, s.payload(rows))
		if err != nil {
			return s.fail(err)
		}
		s.lease = lease
	}
	s.batch = memory.NewBatch()
	s.batch.Reset(rows)
	for _, f := range s.fields {
		if f.Type == memory.TypeStruct {
			b := memory.NewBatch()
			b.Reset(rows)
			for _, c := range f.Children {
				allocateFixedColumn(b, c, rows)
			}
			s.batch.Structs[f.Name] = &memory.StructVector{Rows: rows, Fields: append([]memory.Field(nil), f.Children...), Values: b, Validity: make(memory.Bitmap, memory.WordsFor(rows))}
		} else {
			allocateFixedColumn(s.batch, f, rows)
		}
	}
	for row := 0; row < rows; row++ {
		for i, l := range s.leaves {
			if err := fixedContext(s.ctx, ctx); err != nil {
				return s.fail(err)
			}
			def, count, err := s.readOneContext(ctx, s.chunks[i], l)
			if err != nil {
				return s.fail(err)
			}
			if def < 0 || def > l.maxDef {
				return s.fail(fmt.Errorf("invalid definition level %d for %q", def, l.binding.Name))
			}
			valid := def == l.maxDef
			if (valid && count != 1) || (!valid && count != 0) {
				return s.fail(fmt.Errorf("dense count disagrees with definition level for %q", l.binding.Name))
			}
			dest := s.batch
			if l.parent != "" {
				v := s.batch.Structs[l.parent]
				parentValid := !l.parentOptional || def >= 1
				// All siblings must describe the same parent presence.
				first := i == 0 || s.leaves[i-1].parent != l.parent
				if first {
					if parentValid {
						memory.Set(v.Validity, row)
					}
				} else if memory.IsSet(v.Validity, row) != parentValid {
					return s.fail(fmt.Errorf("inconsistent Struct parent validity for %q", l.parent))
				}
				dest = v.Values
			}
			if err := s.storeOne(dest, l.binding, row, valid); err != nil {
				return s.fail(err)
			}
		}
	}
	if err := fixedContext(s.ctx, ctx); err != nil {
		return s.fail(err)
	}
	s.remaining -= int64(rows)
	bindStreamBatch(dst, s.batch)
	if dst.Structs == nil {
		dst.Structs = make(map[string]*memory.StructVector)
	}
	for name, value := range s.batch.Structs {
		dst.Structs[name] = value
	}
	return true, nil
}

func (s *fixedStream) readOne(c file.ColumnChunkReader, l fixedLeaf) (int16, int, error) {
	return s.readOneContext(s.ctx, c, l)
}
func (s *fixedStream) readOneContext(ctx context.Context, c file.ColumnChunkReader, l fixedLeaf) (int16, int, error) {
	if l.binding.Type == memory.TypeDecimal {
		return s.readRootDecimal(ctx, c, l)
	}
	s.scratch.defs[0] = l.maxDef
	var defs []int16
	if l.maxDef > 0 {
		defs = s.scratch.defs[:]
	}
	var total int64
	var count int
	var err error
	switch r := c.(type) {
	case *file.Int32ColumnChunkReader:
		total, count, err = r.ReadBatch(1, s.scratch.i32[:], defs, nil)
	case *file.Int64ColumnChunkReader:
		total, count, err = r.ReadBatch(1, s.scratch.i64[:], defs, nil)
	case *file.Float32ColumnChunkReader:
		total, count, err = r.ReadBatch(1, s.scratch.f32[:], defs, nil)
	case *file.Float64ColumnChunkReader:
		total, count, err = r.ReadBatch(1, s.scratch.f64[:], defs, nil)
	case *file.BooleanColumnChunkReader:
		total, count, err = r.ReadBatch(1, s.scratch.boolean[:], defs, nil)
	default:
		return 0, 0, fmt.Errorf("unsupported native fixed chunk")
	}
	if err != nil {
		return 0, 0, err
	}
	if total != 1 {
		return 0, 0, fmt.Errorf("native fixed leaf row count mismatch: %d", total)
	}
	return s.scratch.defs[0], count, nil
}

func (s *fixedStream) storeOne(dst *memory.Batch, b columnBinding, row int, valid bool) error {
	if b.Type == memory.TypeDecimal {
		v := dst.Decimals[b.Name]
		if valid {
			v.Values[row] = s.scratch.decimal
			memory.Set(v.Validity, row)
		} else {
			v.Values[row] = memory.Decimal128{}
		}
		s.scratch.decimal = memory.Decimal128{}
		return nil
	}
	if !valid {
		return nil
	}
	switch b.Type {
	case memory.TypeInt8:
		if s.scratch.i32[0] < math.MinInt8 || s.scratch.i32[0] > math.MaxInt8 {
			return fmt.Errorf("int8 overflow")
		}
	case memory.TypeInt16:
		if s.scratch.i32[0] < math.MinInt16 || s.scratch.i32[0] > math.MaxInt16 {
			return fmt.Errorf("int16 overflow")
		}
	case memory.TypeUint8:
		if s.scratch.i32[0] < 0 || s.scratch.i32[0] > math.MaxUint8 {
			return fmt.Errorf("uint8 overflow")
		}
	case memory.TypeUint16:
		if s.scratch.i32[0] < 0 || s.scratch.i32[0] > math.MaxUint16 {
			return fmt.Errorf("uint16 overflow")
		}
	case memory.TypeTimestamp:
		if b.TimestampScale != 1 {
			v := s.scratch.i64[0]
			if v > math.MaxInt64/b.TimestampScale || v < math.MinInt64/b.TimestampScale {
				return fmt.Errorf("timestamp overflows nanoseconds")
			}
			s.scratch.i64[0] = v * b.TimestampScale
		}
	}
	switch b.Type {
	case memory.TypeInt8:
		v := dst.Int8s[b.Name]
		v.Values[row] = int8(s.scratch.i32[0])
		memory.Set(v.Validity, row)
	case memory.TypeInt16:
		v := dst.Int16s[b.Name]
		v.Values[row] = int16(s.scratch.i32[0])
		memory.Set(v.Validity, row)
	case memory.TypeInt32, memory.TypeDate32:
		v := dst.Int32s[b.Name]
		v.Values[row] = int32(s.scratch.i32[0])
		memory.Set(v.Validity, row)
	case memory.TypeInt64, memory.TypeTimestamp:
		v := dst.Int64s[b.Name]
		v.Values[row] = int64(s.scratch.i64[0])
		memory.Set(v.Validity, row)
	case memory.TypeUint8:
		v := dst.Uint8s[b.Name]
		v.Values[row] = uint8(s.scratch.i32[0])
		memory.Set(v.Validity, row)
	case memory.TypeUint16:
		v := dst.Uint16s[b.Name]
		v.Values[row] = uint16(s.scratch.i32[0])
		memory.Set(v.Validity, row)
	case memory.TypeUint32:
		v := dst.Uint32s[b.Name]
		v.Values[row] = uint32(s.scratch.i32[0])
		memory.Set(v.Validity, row)
	case memory.TypeUint64:
		v := dst.Uint64s[b.Name]
		v.Values[row] = uint64(s.scratch.i64[0])
		memory.Set(v.Validity, row)
	case memory.TypeFloat32:
		v := dst.Float32s[b.Name]
		v.Values[row] = float32(s.scratch.f32[0])
		memory.Set(v.Validity, row)
	case memory.TypeFloat64:
		v := dst.Float64s[b.Name]
		v.Values[row] = float64(s.scratch.f64[0])
		memory.Set(v.Validity, row)
	case memory.TypeBool:
		v := dst.Uint32s[b.Name]
		if s.scratch.boolean[0] {
			v.Values[row] = 1
		}
		memory.Set(v.Validity, row)
	}
	return nil
}
func allocateFixedColumn(dst *memory.Batch, f memory.Field, rows int) {
	valid := make(memory.Bitmap, memory.WordsFor(rows))
	switch f.Type {
	case memory.TypeDecimal:
		dst.Decimals[f.Name] = &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Validity: valid, Precision: f.DecimalPrecision, Scale: f.DecimalScale}
	case memory.TypeInt8:
		dst.Int8s[f.Name] = &memory.Int8Vector{Values: make([]int8, rows), Validity: valid, Encoding: memory.EncodingPlain}
	case memory.TypeInt16:
		dst.Int16s[f.Name] = &memory.Int16Vector{Values: make([]int16, rows), Validity: valid, Encoding: memory.EncodingPlain}
	case memory.TypeInt32, memory.TypeDate32:
		dst.Int32s[f.Name] = &memory.Int32Vector{Values: make([]int32, rows), Validity: valid, Encoding: memory.EncodingPlain}
	case memory.TypeInt64, memory.TypeTimestamp:
		dst.Int64s[f.Name] = &memory.Int64Vector{Values: make([]int64, rows), Validity: valid, Encoding: memory.EncodingPlain}
	case memory.TypeUint8:
		dst.Uint8s[f.Name] = &memory.Uint8Vector{Values: make([]uint8, rows), Validity: valid, Encoding: memory.EncodingPlain}
	case memory.TypeUint16:
		dst.Uint16s[f.Name] = &memory.Uint16Vector{Values: make([]uint16, rows), Validity: valid, Encoding: memory.EncodingPlain}
	case memory.TypeUint32:
		dst.Uint32s[f.Name] = &memory.Uint32Vector{Values: make([]uint32, rows), Validity: valid, Encoding: memory.EncodingPlain}
	case memory.TypeUint64:
		dst.Uint64s[f.Name] = &memory.Uint64Vector{Values: make([]uint64, rows), Validity: valid, Encoding: memory.EncodingPlain}
	case memory.TypeFloat32:
		dst.Float32s[f.Name] = &memory.Float32Vector{Values: make([]float32, rows), Validity: valid, Encoding: memory.EncodingPlain}
	case memory.TypeFloat64:
		dst.Float64s[f.Name] = &memory.Float64Vector{Values: make([]float64, rows), Validity: valid, Encoding: memory.EncodingPlain}
	case memory.TypeBool:
		dst.Uint32s[f.Name] = &memory.Uint32Vector{Values: make([]uint32, rows), Validity: valid, Encoding: memory.EncodingPlain}
	}
}
