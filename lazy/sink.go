package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ErrSinkTargetExists reports a destination that already exists while
// Overwrite is false.
var ErrSinkTargetExists = errors.New("sink target already exists")

// SinkOptions controls atomic file output and optional directory sharding.
// With no shard limits, Path names one file. With either limit set, Path names
// a directory containing part files. TargetBytesPerFile is an uncompressed
// planning target; MaxRowsPerFile is a strict maximum.
type SinkOptions struct {
	Batch              BatchOptions
	MaxRowsPerFile     int64
	TargetBytesPerFile int64
	Overwrite          bool
}

func (o SinkOptions) validate() error {
	if o.MaxRowsPerFile < 0 || o.TargetBytesPerFile < 0 {
		return fmt.Errorf("sink shard limits must be non-negative")
	}
	return nil
}

func (o SinkOptions) sharded() bool {
	return o.MaxRowsPerFile > 0 || o.TargetBytesPerFile > 0
}

// ParquetCompression selects a portable Parquet compression codec.
type ParquetCompression string

const (
	// ParquetCompressionZstd is the default balanced codec.
	ParquetCompressionZstd ParquetCompression = "zstd"
	// ParquetCompressionSnappy favors decode and encode speed.
	ParquetCompressionSnappy ParquetCompression = "snappy"
	// ParquetCompressionGzip favors compatibility.
	ParquetCompressionGzip ParquetCompression = "gzip"
	// ParquetCompressionNone disables compression.
	ParquetCompressionNone ParquetCompression = "none"
)

// ParquetSinkOptions controls native Parquet output.
type ParquetSinkOptions struct {
	SinkOptions
	Compression  ParquetCompression
	RowGroupRows int64
}

func (o ParquetSinkOptions) normalize() (ParquetSinkOptions, error) {
	if err := o.SinkOptions.validate(); err != nil {
		return o, err
	}
	if o.RowGroupRows < 0 {
		return o, fmt.Errorf("Parquet row group rows must be non-negative")
	}
	if o.Compression == "" {
		o.Compression = ParquetCompressionZstd
	}
	if o.RowGroupRows == 0 {
		o.RowGroupRows = 64 << 10
	}
	if _, err := parquetCodec(o.Compression); err != nil {
		return o, err
	}
	return o, nil
}

func parquetCodec(value ParquetCompression) (compress.Compression, error) {
	switch value {
	case ParquetCompressionZstd:
		return compress.Codecs.Zstd, nil
	case ParquetCompressionSnappy:
		return compress.Codecs.Snappy, nil
	case ParquetCompressionGzip:
		return compress.Codecs.Gzip, nil
	case ParquetCompressionNone:
		return compress.Codecs.Uncompressed, nil
	default:
		return 0, fmt.Errorf("unsupported Parquet compression %q", value)
	}
}

// IPCCompression selects Arrow IPC buffer compression.
type IPCCompression string

const (
	// IPCCompressionNone is the default uncompressed Arrow IPC format.
	IPCCompressionNone IPCCompression = "none"
	// IPCCompressionZstd enables Zstandard IPC buffers.
	IPCCompressionZstd IPCCompression = "zstd"
	// IPCCompressionLZ4 enables LZ4-frame IPC buffers.
	IPCCompressionLZ4 IPCCompression = "lz4"
)

// IPCSinkOptions controls native Arrow IPC file output.
type IPCSinkOptions struct {
	SinkOptions
	Compression IPCCompression
}

func (o IPCSinkOptions) normalize() (IPCSinkOptions, error) {
	if err := o.SinkOptions.validate(); err != nil {
		return o, err
	}
	if o.Compression == "" {
		o.Compression = IPCCompressionNone
	}
	switch o.Compression {
	case IPCCompressionNone, IPCCompressionZstd, IPCCompressionLZ4:
		return o, nil
	default:
		return o, fmt.Errorf("unsupported IPC compression %q", o.Compression)
	}
}

// SinkResult summarizes committed native files. Files contains final paths,
// never temporary staging names.
type SinkResult struct {
	Rows  int64
	Bytes int64
	Files []string
}

type recordSinkWriter interface {
	Write(arrow.RecordBatch) error
	Close() error
}

type sinkWriterFactory func(io.Writer, *arrow.Schema, arrowmem.Allocator) (recordSinkWriter, error)

// sinkWriteOnly deliberately hides Close from native encoders. The sink owns
// fsync and close ordering so a format writer cannot close the staging file
// before it has been durably flushed.
type sinkWriteOnly struct{ io.Writer }

// SinkParquet streams the optimized physical plan into an atomically committed
// Parquet file or sharded directory.
func (f LazyFrame) SinkParquet(ctx context.Context, path string, opts ParquetSinkOptions) (SinkResult, error) {
	normalized, err := opts.normalize()
	if err != nil {
		return SinkResult{}, err
	}
	codec, _ := parquetCodec(normalized.Compression)
	factory := func(output io.Writer, schema *arrow.Schema, allocator arrowmem.Allocator) (recordSinkWriter, error) {
		props := parquet.NewWriterProperties(parquet.WithAllocator(allocator), parquet.WithCompression(codec), parquet.WithMaxRowGroupLength(normalized.RowGroupRows))
		return pqarrow.NewFileWriter(schema, output, props, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(allocator)))
	}
	return f.sinkFiles(ctx, path, ".parquet", normalized.SinkOptions, factory, "ParquetSinkEncode")
}

// SinkIPC streams the optimized physical plan into an atomically committed
// Arrow IPC file or sharded directory.
func (f LazyFrame) SinkIPC(ctx context.Context, path string, opts IPCSinkOptions) (SinkResult, error) {
	normalized, err := opts.normalize()
	if err != nil {
		return SinkResult{}, err
	}
	factory := func(output io.Writer, schema *arrow.Schema, allocator arrowmem.Allocator) (recordSinkWriter, error) {
		ipcOptions := []ipc.Option{ipc.WithSchema(schema), ipc.WithAllocator(allocator)}
		switch normalized.Compression {
		case IPCCompressionZstd:
			ipcOptions = append(ipcOptions, ipc.WithZstd())
		case IPCCompressionLZ4:
			ipcOptions = append(ipcOptions, ipc.WithLZ4())
		}
		return ipc.NewFileWriter(output, ipcOptions...)
	}
	return f.sinkFiles(ctx, path, ".arrow", normalized.SinkOptions, factory, "IPCSinkEncode")
}

type atomicSinkTarget struct {
	final     string
	stage     string
	sharded   bool
	overwrite bool
	committed bool
}

func prepareAtomicSinkTarget(path string, options SinkOptions) (*atomicSinkTarget, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("sink path is empty")
	}
	final, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	info, statErr := os.Lstat(final)
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}
	if statErr == nil {
		if !options.Overwrite {
			return nil, fmt.Errorf("%w: %s", ErrSinkTargetExists, final)
		}
		if options.sharded() {
			return nil, fmt.Errorf("atomic overwrite of an existing sharded directory is not supported; choose a new target")
		}
		if info.IsDir() {
			return nil, fmt.Errorf("sink file target %s is a directory", final)
		}
	}
	parent := filepath.Dir(final)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, err
	}
	target := &atomicSinkTarget{final: final, sharded: options.sharded(), overwrite: options.Overwrite}
	if target.sharded {
		target.stage, err = os.MkdirTemp(parent, "."+filepath.Base(final)+".tmp-")
	} else {
		file, createErr := os.CreateTemp(parent, "."+filepath.Base(final)+".tmp-")
		if createErr == nil {
			target.stage = file.Name()
			createErr = file.Close()
		}
		err = createErr
	}
	if err != nil {
		return nil, err
	}
	return target, nil
}

func (t *atomicSinkTarget) abort() error {
	if t == nil || t.committed || t.stage == "" {
		return nil
	}
	if t.sharded {
		return os.RemoveAll(t.stage)
	}
	return os.Remove(t.stage)
}

func (t *atomicSinkTarget) commit() error {
	if t == nil || t.committed {
		return nil
	}
	if t.sharded {
		directory, err := os.Open(t.stage)
		if err != nil {
			return err
		}
		syncErr := directory.Sync()
		closeErr := directory.Close()
		if err := errors.Join(syncErr, closeErr); err != nil {
			return err
		}
	}
	if !t.overwrite {
		if _, err := os.Lstat(t.final); err == nil {
			return fmt.Errorf("%w: %s", ErrSinkTargetExists, t.final)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.Rename(t.stage, t.final); err != nil {
		return err
	}
	t.committed = true
	parent, err := os.Open(filepath.Dir(t.final))
	if err != nil {
		return err
	}
	return errors.Join(parent.Sync(), parent.Close())
}

type sinkPart struct {
	file      *os.File
	writer    recordSinkWriter
	stagePath string
	finalPath string
	rows      int64
	estimated int64
}

type sinkAllocationPanic struct{ err error }

// sinkBudgetAllocator adapts Arrow's infallible Allocator interface to the
// fallible GoLume query budget. Budget failures cross the Arrow boundary as a
// private panic and are recovered at every native encoder entry point.
type sinkBudgetAllocator struct {
	ctx          context.Context
	operator     *memory.OperatorBudget
	base         arrowmem.Allocator
	mu           sync.Mutex
	reservations map[*byte]memory.Reservation
	closed       bool
}

func newSinkBudgetAllocator(ctx context.Context, operator *memory.OperatorBudget) *sinkBudgetAllocator {
	return &sinkBudgetAllocator{ctx: ctx, operator: operator, base: arrowmem.NewGoAllocator(), reservations: make(map[*byte]memory.Reservation)}
}

// Allocate reserves query memory before allocating an aligned Arrow buffer.
func (a *sinkBudgetAllocator) Allocate(size int) []byte {
	if size <= 0 {
		return nil
	}
	// Arrow's Go allocator retains up to 64 extra bytes for alignment.
	reservation, err := a.operator.Reserve(a.ctx, int64(size)+64)
	if err != nil {
		panic(sinkAllocationPanic{err: err})
	}
	var buffer []byte
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				reservation.Release()
				panic(recovered)
			}
		}()
		buffer = a.base.Allocate(size)
	}()
	key := sinkAllocationKey(buffer)
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		a.base.Free(buffer)
		reservation.Release()
		panic(sinkAllocationPanic{err: fmt.Errorf("sink Arrow allocator is closed")})
	}
	a.reservations[key] = reservation
	a.mu.Unlock()
	return buffer
}

// Reallocate grows an Arrow buffer under the same query budget.
func (a *sinkBudgetAllocator) Reallocate(size int, buffer []byte) []byte {
	if size <= cap(buffer) {
		return buffer[:size]
	}
	replacement := a.Allocate(size)
	copy(replacement, buffer)
	a.Free(buffer)
	return replacement
}

// Free releases an Arrow buffer and its matching query reservation.
func (a *sinkBudgetAllocator) Free(buffer []byte) {
	key := sinkAllocationKey(buffer)
	if key == nil {
		return
	}
	a.mu.Lock()
	reservation := a.reservations[key]
	delete(a.reservations, key)
	a.mu.Unlock()
	a.base.Free(buffer)
	if reservation != nil {
		reservation.Release()
	}
}

func sinkAllocationKey(buffer []byte) *byte {
	if cap(buffer) == 0 {
		return nil
	}
	return &buffer[:1][0]
}

// Close releases any reservations retained by a failed native encoder.
func (a *sinkBudgetAllocator) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	reservations := a.reservations
	a.reservations = nil
	a.mu.Unlock()
	for _, reservation := range reservations {
		reservation.Release()
	}
	return nil
}

func callNativeSink(fn func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if allocation, ok := recovered.(sinkAllocationPanic); ok {
				err = fmt.Errorf("native sink allocation: %w", allocation.err)
				return
			}
			panic(recovered)
		}
	}()
	return fn()
}

func (p *sinkPart) close() error {
	if p == nil {
		return nil
	}
	var out error
	if p.writer != nil {
		out = errors.Join(out, callNativeSink(p.writer.Close))
		p.writer = nil
	}
	if p.file != nil {
		if out == nil {
			out = errors.Join(out, p.file.Sync())
		}
		out = errors.Join(out, p.file.Close())
		p.file = nil
	}
	return out
}

func (f LazyFrame) sinkFiles(ctx context.Context, path, extension string, options SinkOptions, factory sinkWriterFactory, budgetName string) (result SinkResult, resultErr error) {
	if ctx == nil {
		return SinkResult{}, fmt.Errorf("sink context is nil")
	}
	if err := options.validate(); err != nil {
		return SinkResult{}, err
	}
	target, err := prepareAtomicSinkTarget(path, options)
	if err != nil {
		return SinkResult{}, err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, target.abort())
		}
	}()
	reader, err := f.CollectBatches(ctx, options.Batch)
	if err != nil {
		return SinkResult{}, err
	}
	reader.holdBudgetUntilClose = true
	defer func() { resultErr = errors.Join(resultErr, reader.Close()) }()
	allocator := newSinkBudgetAllocator(ctx, reader.budget.Operator(budgetName, memory.MemoryWorkspace))
	defer func() { resultErr = errors.Join(resultErr, allocator.Close()) }()
	schema := reader.Schema()
	arrowSchema, err := memory.ArrowSchema(schema)
	if err != nil {
		return SinkResult{}, err
	}
	var current *sinkPart
	parts := make([]*sinkPart, 0, 1)
	openPart := func() error {
		index := len(parts)
		stagePath, finalPath := target.stage, target.final
		if target.sharded {
			name := fmt.Sprintf("part-%05d%s", index, extension)
			stagePath = filepath.Join(target.stage, name)
			finalPath = filepath.Join(target.final, name)
		}
		file, openErr := os.OpenFile(stagePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if openErr != nil {
			return openErr
		}
		var writer recordSinkWriter
		writerErr := callNativeSink(func() error {
			var factoryErr error
			writer, factoryErr = factory(sinkWriteOnly{Writer: file}, arrowSchema, allocator)
			return factoryErr
		})
		if writerErr != nil {
			_ = file.Close()
			return writerErr
		}
		current = &sinkPart{file: file, writer: writer, stagePath: stagePath, finalPath: finalPath}
		parts = append(parts, current)
		return nil
	}
	if err := openPart(); err != nil {
		return SinkResult{}, err
	}
	closeParts := func() error {
		var out error
		for _, part := range parts {
			out = errors.Join(out, part.close())
		}
		return out
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, closeParts())
		}
	}()
	for reader.Next(ctx) {
		batch := reader.batch
		for offset := 0; offset < batch.Rows; {
			if target.sharded && shouldRotateSinkPart(current, options) {
				if err := current.close(); err != nil {
					return SinkResult{}, err
				}
				if err := openPart(); err != nil {
					return SinkResult{}, err
				}
			}
			rows, estimate, err := chooseSinkRange(batch, offset, current, options)
			if err != nil {
				return SinkResult{}, err
			}
			if rows == 0 {
				if err := current.close(); err != nil {
					return SinkResult{}, err
				}
				if err := openPart(); err != nil {
					return SinkResult{}, err
				}
				continue
			}
			if err := writeSinkRange(allocator, current, schema, batch, offset, rows); err != nil {
				return SinkResult{}, err
			}
			offset += rows
			current.rows += int64(rows)
			current.estimated += estimate
			result.Rows += int64(rows)
		}
	}
	if err := reader.Err(); err != nil {
		return SinkResult{}, err
	}
	if err := closeParts(); err != nil {
		return SinkResult{}, err
	}
	if err := allocator.Close(); err != nil {
		return SinkResult{}, err
	}
	for _, part := range parts {
		info, err := os.Stat(part.stagePath)
		if err != nil {
			return SinkResult{}, err
		}
		result.Files = append(result.Files, part.finalPath)
		result.Bytes += info.Size()
	}
	if err := target.commit(); err != nil {
		return SinkResult{}, err
	}
	return result, nil
}

func shouldRotateSinkPart(part *sinkPart, options SinkOptions) bool {
	if part == nil || part.rows == 0 {
		return false
	}
	return options.MaxRowsPerFile > 0 && part.rows >= options.MaxRowsPerFile ||
		options.TargetBytesPerFile > 0 && part.estimated >= options.TargetBytesPerFile
}

func chooseSinkRange(batch *memory.Batch, offset int, part *sinkPart, options SinkOptions) (int, int64, error) {
	rows := batch.Rows - offset
	if options.MaxRowsPerFile > 0 {
		remaining := options.MaxRowsPerFile - part.rows
		if remaining <= 0 {
			return 0, 0, nil
		}
		if int64(rows) > remaining {
			rows = int(remaining)
		}
	}
	if options.TargetBytesPerFile <= 0 {
		estimate, err := memory.EstimateBatchRangeBytes(batch, offset, rows)
		return rows, estimate, err
	}
	remainingBytes := options.TargetBytesPerFile - part.estimated
	if remainingBytes <= 0 && part.rows > 0 {
		return 0, 0, nil
	}
	lo, hi := 1, rows
	bestRows, bestBytes := 0, int64(0)
	for lo <= hi {
		mid := lo + (hi-lo)/2
		estimate, err := memory.EstimateBatchRangeBytes(batch, offset, mid)
		if err != nil {
			return 0, 0, err
		}
		if estimate <= remainingBytes || part.rows == 0 && mid == 1 {
			bestRows, bestBytes = mid, estimate
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if bestRows == 0 {
		return 0, 0, nil
	}
	return bestRows, bestBytes, nil
}

func writeSinkRange(allocator arrowmem.Allocator, part *sinkPart, schema memory.Schema, batch *memory.Batch, offset, rows int) error {
	return callNativeSink(func() error {
		record, err := memory.ArrowRecordRange(allocator, schema, batch, offset, rows)
		if err != nil {
			return err
		}
		defer record.Release()
		return part.writer.Write(record)
	})
}
