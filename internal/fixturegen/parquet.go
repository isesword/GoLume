package fixturegen

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gocolumn/memory"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ErrTargetExists reports a Parquet output path that already exists while
// overwrite is disabled.
var ErrTargetExists = errors.New("fixture target already exists")

// ParquetCompression selects a portable Parquet compression codec.
type ParquetCompression string

const (
	// CompressionZstd is the default balanced codec.
	CompressionZstd ParquetCompression = "zstd"
	// CompressionSnappy favors decode and encode speed.
	CompressionSnappy ParquetCompression = "snappy"
	// CompressionGzip favors compatibility.
	CompressionGzip ParquetCompression = "gzip"
	// CompressionNone disables compression.
	CompressionNone ParquetCompression = "none"
)

// ParquetOptions controls fixture Parquet output. The zero value selects
// zstd compression and 64k-row row groups.
type ParquetOptions struct {
	// Compression selects the page codec; empty means zstd.
	Compression ParquetCompression
	// RowGroupRows caps rows per row group; zero means 65536.
	RowGroupRows int64
}

func (o ParquetOptions) normalize() (ParquetOptions, error) {
	if o.Compression == "" {
		o.Compression = CompressionZstd
	}
	switch o.Compression {
	case CompressionZstd, CompressionSnappy, CompressionGzip, CompressionNone:
	default:
		return o, fmt.Errorf("unsupported Parquet compression %q", o.Compression)
	}
	if o.RowGroupRows < 0 {
		return o, fmt.Errorf("Parquet row group rows must be non-negative")
	}
	if o.RowGroupRows == 0 {
		o.RowGroupRows = 64 << 10
	}
	return o, nil
}

func (o ParquetOptions) codec() compress.Compression {
	switch o.Compression {
	case CompressionSnappy:
		return compress.Codecs.Snappy
	case CompressionGzip:
		return compress.Codecs.Gzip
	case CompressionNone:
		return compress.Codecs.Uncompressed
	default:
		return compress.Codecs.Zstd
	}
}

// writeOnly hides Close from the Parquet encoder so WriteParquet never closes
// a caller-owned writer.
type writeOnly struct{ io.Writer }

// WriteParquet encodes the full table as one Parquet file into w. The writer
// stays open; the caller flushes and closes it. Output is byte-identical for
// identical tables and arrow-go versions.
func WriteParquet(w io.Writer, table *Table, opts ParquetOptions) error {
	if w == nil {
		return fmt.Errorf("parquet writer is nil")
	}
	if table == nil {
		return fmt.Errorf("fixture table is nil")
	}
	normalized, err := opts.normalize()
	if err != nil {
		return err
	}
	arrowSchema, err := memory.ArrowSchema(table.schema)
	if err != nil {
		return err
	}
	allocator := arrowmem.NewGoAllocator()
	props := parquet.NewWriterProperties(
		parquet.WithAllocator(allocator),
		parquet.WithCompression(normalized.codec()),
		parquet.WithMaxRowGroupLength(normalized.RowGroupRows),
	)
	writer, err := pqarrow.NewFileWriter(arrowSchema, writeOnly{Writer: w}, props, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(allocator)))
	if err != nil {
		return err
	}
	rows := table.batch.Rows
	for start := 0; start < rows; start += int(normalized.RowGroupRows) {
		end := start + int(normalized.RowGroupRows)
		if end > rows {
			end = rows
		}
		record, err := buildRecord(allocator, arrowSchema, table, start, end)
		if err != nil {
			_ = writer.Close()
			return err
		}
		err = writer.Write(record)
		record.Release()
		if err != nil {
			_ = writer.Close()
			return err
		}
	}
	return writer.Close()
}

// WriteParquetFile atomically writes the table to path and returns the file
// size in bytes. An existing target is refused unless overwrite is true.
func WriteParquetFile(path string, table *Table, opts ParquetOptions, overwrite bool) (int64, error) {
	if path == "" {
		return 0, fmt.Errorf("parquet output path is empty")
	}
	if _, err := opts.normalize(); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if !overwrite {
		flags = os.O_CREATE | os.O_WRONLY | os.O_EXCL
	}
	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		if !overwrite && errors.Is(err, os.ErrExist) {
			return 0, fmt.Errorf("%w: %s", ErrTargetExists, path)
		}
		return 0, err
	}
	writeErr := WriteParquet(file, table, opts)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// buildRecord copies the row range [start, end) into an owned Arrow record
// using column-wise typed builders. The caller must Release the record.
func buildRecord(allocator arrowmem.Allocator, schema *arrow.Schema, table *Table, start, end int) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()
	for index, field := range table.schema.Fields {
		if err := appendColumn(builder.Field(index), field, table.batch, start, end); err != nil {
			return nil, fmt.Errorf("column %q: %w", field.Name, err)
		}
	}
	return builder.NewRecordBatch(), nil
}

func appendColumn(builder array.Builder, field memory.Field, batch *memory.Batch, start, end int) error {
	switch field.Type {
	case memory.TypeInt64:
		vector := batch.Int64s[field.Name]
		builder.(*array.Int64Builder).AppendValues(vector.Values[start:end], validMask(vector.Validity, start, end))
	case memory.TypeFloat64:
		vector := batch.Float64s[field.Name]
		builder.(*array.Float64Builder).AppendValues(vector.Values[start:end], validMask(vector.Validity, start, end))
	case memory.TypeBool:
		vector := batch.Uint32s[field.Name]
		typed, ok := builder.(*array.BooleanBuilder)
		if !ok {
			return fmt.Errorf("arrow builder is %T, want *array.BooleanBuilder", builder)
		}
		values := make([]bool, end-start)
		for row := start; row < end; row++ {
			values[row-start] = vector.Values[row] != 0
		}
		typed.AppendValues(values, validMask(vector.Validity, start, end))
	case memory.TypeString:
		vector := batch.Strings[field.Name]
		typed, ok := builder.(*array.StringBuilder)
		if !ok {
			return fmt.Errorf("arrow builder is %T, want *array.StringBuilder", builder)
		}
		typed.Reserve(end - start)
		for row := start; row < end; row++ {
			if vector.IsValid(row) {
				typed.Append(string(vector.ValueBytes(row)))
			} else {
				typed.AppendNull()
			}
		}
	default:
		return fmt.Errorf("logical type %s is not supported by fixture Parquet output", field.Type)
	}
	return nil
}

// validMask converts the packed validity bitmap for [start, end) into the
// per-row bool slice Arrow bulk appenders expect. A nil bitmap means all rows
// are valid and yields a nil mask.
func validMask(validity memory.Bitmap, start, end int) []bool {
	if validity == nil {
		return nil
	}
	mask := make([]bool, end-start)
	for row := start; row < end; row++ {
		mask[row-start] = memory.IsSet(validity, row)
	}
	return mask
}
