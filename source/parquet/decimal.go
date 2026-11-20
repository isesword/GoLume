package parquet

import (
	"context"
	"fmt"
	"gocolumn/memory"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
)

// readDecimal consumes one page-borrowed coefficient at a time. This first
// correctness slice deliberately avoids row-sized byte headers or retained
// arbitrary-length coefficients: all output is already reserved fixed-width
// decimal128, and a borrowed value is converted before its page advances.
func (r *typedDecoder) readDecimal(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	v := dst.Decimals[b.Name]
	v.Values = v.Values[:rows]
	if b.Nullable {
		v.Validity = v.Validity[:memory.WordsFor(rows)]
		clear(v.Validity)
	} else {
		v.Validity = nil
	}
	var defs []int16
	if b.Nullable {
		defs = r.decimalDef[:]
	}
	for row := 0; row < rows; row++ {
		var total int64
		var count int
		var err error
		var value memory.Decimal128
		var bytes []byte
		r.decimalDef[0] = 0
		switch c := chunk.(type) {
		case *file.Int32ColumnChunkReader:
			total, count, err = c.ReadBatchInPage(1, r.decimalI32[:], defs, nil)
			value = decimal128.FromI64(int64(r.decimalI32[0]))
		case *file.Int64ColumnChunkReader:
			total, count, err = c.ReadBatchInPage(1, r.decimalI64[:], defs, nil)
			value = decimal128.FromI64(r.decimalI64[0])
		case *file.ByteArrayColumnChunkReader:
			total, count, err = c.ReadBatchInPage(1, r.decimalBytes[:], defs, nil)
			bytes = r.decimalBytes[0]
			r.decimalBytes[0] = nil
		case *file.FixedLenByteArrayColumnChunkReader:
			total, count, err = c.ReadBatchInPage(1, r.decimalFixed[:], defs, nil)
			bytes = r.decimalFixed[0]
			r.decimalFixed[0] = nil
		default:
			return fmt.Errorf("decimal column %q unsupported physical reader %T", b.Name, chunk)
		}
		if err != nil {
			return fmt.Errorf("decimal column %q row %d: %w", b.Name, row, err)
		}
		if total != 1 || count < 0 || count > 1 {
			return fmt.Errorf("decimal column %q row count mismatch total=%d values=%d", b.Name, total, count)
		}
		valid := !b.Nullable || r.decimalDef[0] == 1
		if b.Nullable && r.decimalDef[0] != 0 && r.decimalDef[0] != 1 || (count == 1) != valid {
			return fmt.Errorf("decimal column %q invalid definition/value count", b.Name)
		}
		if !valid {
			v.Values[row] = memory.Decimal128{}
			continue
		}
		switch chunk.(type) {
		case *file.ByteArrayColumnChunkReader, *file.FixedLenByteArrayColumnChunkReader:
			value, err = memory.Decimal128FromBigEndian(bytes, b.DecimalPrecision)
			if err != nil {
				return fmt.Errorf("decimal column %q row %d: %w", b.Name, row, err)
			}
		}
		if !memory.Decimal128FitsPrecision(value, b.DecimalPrecision) {
			return fmt.Errorf("decimal column %q row %d coefficient exceeds precision %d", b.Name, row, b.DecimalPrecision)
		}
		v.Values[row] = value
		if b.Nullable {
			memory.Set(v.Validity, row)
		}
	}
	return nil
}

// Decimal page encodings are checked before Arrow constructs a decoder. Arrow
// v18.7.0 lacks fixed-length DELTA_BYTE_ARRAY and its scalar INT32/INT64
// BYTE_STREAM_SPLIT decoder panics on zero values (including a NULL row).
// Reject these combinations on every CPU, not only when a fallback is active.
func validateDecimalPageEncodings(meta *metadata.FileMetaData, index int) error {
	return validateFixedScalarPageEncodings(meta, index, "decimal", decimalValueEncodingSupported)
}

func validateFixedScalarPageEncodings(meta *metadata.FileMetaData, index int, label string, valueEncodingSupported func(arrowparquet.Type, arrowparquet.Encoding) bool) error {
	descriptor := meta.Schema.Column(index)
	for group := 0; group < meta.NumRowGroups(); group++ {
		chunk, err := meta.RowGroup(group).ColumnChunk(index)
		if err != nil {
			return err
		}
		for _, encoding := range chunk.Encodings() {
			// Footer lists also contain definition/repetition-level encodings;
			// those encodings are not valid coefficient page encodings.
			if encoding == arrowparquet.Encodings.RLE || encoding == arrowparquet.Encodings.BitPacked {
				continue
			}
			if !valueEncodingSupported(descriptor.PhysicalType(), encoding) {
				return fmt.Errorf("parquet %s column %q row group %d encoding %s for %s is not supported", label, descriptor.Path(), group, encoding, descriptor.PhysicalType())
			}
		}
	}
	return nil
}

// validateDecimalPages checks actual page headers as well as the footer's
// advisory encoding list. It visits one bounded Arrow page at a time, retains
// no coefficient values and closes the reader before the normal decoder opens.
// This extra pass is restricted to projected decimal columns and uses the same
// page-size caps and Arrow page-buffer accounting as the ordinary scan.
func validateDecimalPages(ctx context.Context, rg *file.RowGroupReader, b columnBinding, physical arrowparquet.Type) error {
	return validateFixedScalarPages(ctx.Err, rg, b, physical, "decimal", decimalValueEncodingSupported)
}

func validateFixedScalarPages(check func() error, rg *file.RowGroupReader, b columnBinding, physical arrowparquet.Type, label string, valueEncodingSupported func(arrowparquet.Type, arrowparquet.Encoding) bool) error {
	return validateParquetDataPages(check, rg, b, physical, label, valueEncodingSupported, -1)
}

func validateParquetDataPages(check func() error, rg *file.RowGroupReader, b columnBinding, physical arrowparquet.Type, label string, valueEncodingSupported func(arrowparquet.Type, arrowparquet.Encoding) bool, expectedLevels int64) (err error) {
	pages, err := rg.GetColumnPageReader(b.Index)
	if err != nil {
		// Arrow can return an allocated reader together with an init error,
		// for example when the column names an unavailable compression codec.
		if pages != nil {
			_ = pages.Close()
		}
		return err
	}
	defer func() {
		if closeErr := pages.Close(); err == nil {
			err = closeErr
		}
	}()
	// Arrow's V2 page iterator advances its termination counter by rows,
	// not level tuples. A repeated column's footer counts tuples; stop as
	// soon as actual data pages account for that many tuples, or fail if
	// the iterator stops first. Scalar callers leave termination unchanged.
	levels := int64(0)
	for pageIndex := 0; expectedLevels < 0 || levels < expectedLevels; pageIndex++ {
		if err := check(); err != nil {
			return err
		}
		if !pages.Next() {
			if e := pages.Err(); e != nil {
				return e
			}
			if expectedLevels >= 0 {
				return fmt.Errorf("%s column %q page level count %d; want %d", label, b.Name, levels, expectedLevels)
			}
			return nil
		}
		page := pages.Page()
		if page == nil {
			return fmt.Errorf("%s column %q page %d is missing", label, b.Name, pageIndex)
		}
		encoding := arrowparquet.Encoding(page.Encoding())
		supported := false
		switch page.Type() {
		case file.PageTypeDictionaryPage:
			supported = encoding == arrowparquet.Encodings.Plain || encoding == arrowparquet.Encodings.PlainDict
		case file.PageTypeDataPage, file.PageTypeDataPageV2:
			supported = valueEncodingSupported(physical, encoding)
			if expectedLevels >= 0 {
				n := int64(page.NumValues())
				if n < 0 || n > expectedLevels-levels {
					return fmt.Errorf("%s column %q page %d has invalid level count %d", label, b.Name, pageIndex, n)
				}
				levels += n
			}
		}
		if !supported {
			return fmt.Errorf("%s column %q page %d encoding %s for %s is not supported", label, b.Name, pageIndex, encoding, physical)
		}
	}
	return nil
}

func decimalValueEncodingSupported(physical arrowparquet.Type, encoding arrowparquet.Encoding) bool {
	switch encoding {
	case arrowparquet.Encodings.Plain, arrowparquet.Encodings.RLEDict, arrowparquet.Encodings.PlainDict:
		return true
	case arrowparquet.Encodings.DeltaBinaryPacked:
		return physical == arrowparquet.Types.Int32 || physical == arrowparquet.Types.Int64
	case arrowparquet.Encodings.DeltaByteArray, arrowparquet.Encodings.DeltaLengthByteArray:
		return physical == arrowparquet.Types.ByteArray
	case arrowparquet.Encodings.ByteStreamSplit:
		return physical == arrowparquet.Types.FixedLenByteArray
	default:
		return false
	}
}
