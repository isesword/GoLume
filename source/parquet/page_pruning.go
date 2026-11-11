package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"math"

	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/thrift/lib/go/thrift"
)

// These are optional optimization limits, not limits on readable files. Arrow
// coalesces index reads across all columns, including gaps, so bound the read
// spans rather than just the sum of the individual index lengths.
const maxPruningIndexBytes int64 = 1 << 20
const maxPruningPages = 65536

type pageRowRange struct{ start, end int64 }
type pagePruningPlan struct {
	ranges                  []pageRowRange
	prunedRows, prunedPages int64
}
type pruningPageStatistics struct {
	rows   int64
	column int
	stats  metadata.TypedStatistics
}

// planPagePruning proves that whole logical row ranges cannot satisfy the
// predicate. Only one predicate column supplies page statistics; all other
// columns are unknown. This preserves AND/OR/NOT and SQL NULL semantics without
// assuming that different columns have matching page boundaries. The residual
// predicate is still required for every returned row.
//
// All optional metadata is validated before any data reader is repositioned.
// Arrow's index constructors panic on malformed thrift and encoded bounds;
// contain that failure here and keep the ordinary sequential scan instead.
func planPagePruning(ctx context.Context, r *file.Reader, group int, bindings []columnBinding, predicate expr.Expr) (plan *pagePruningPlan) {
	defer func() {
		if recover() != nil {
			plan = nil
		}
	}()
	if predicate == nil || len(bindings) == 0 {
		return nil
	}
	meta := r.MetaData()
	rg := meta.RowGroup(group)
	if rg.NumRows() <= 0 || meta.FileDecryptor != nil || !pruningIndexRangesValid(meta, rg) {
		return nil
	}
	byName := make(map[string]columnBinding, len(bindings))
	for _, b := range bindings {
		// Skipping rows must not hide demanded logical-conversion errors in a
		// projected column. Narrow integers, UTF-8, decimals and timestamps
		// can fail conversion, so they keep the sequential reader.
		switch b.Type {
		case memory.TypeInt32, memory.TypeInt64, memory.TypeUint32, memory.TypeUint64,
			memory.TypeDate32, memory.TypeBool, memory.TypeFloat32, memory.TypeFloat64:
		default:
			return nil
		}
		byName[b.Name] = b
	}
	var anchor columnBinding
	found := false
	for _, name := range expr.Columns(predicate) {
		b, ok := byName[name]
		if ok && (b.Type == memory.TypeInt32 || b.Type == memory.TypeInt64 || b.Type == memory.TypeDate32) {
			anchor, found = b, true
			break
		}
	}
	if !found {
		return nil
	}
	// If trusted footer statistics prove that every row satisfies the
	// predicate, there is nothing for page indexes to exclude. Avoid their
	// I/O and decoding on this common full-match control.
	cache := pruningStatisticsCache{}
	whole := &pruningRowGroup{RowGroupMetaData: rg, file: meta, index: group, statistics: &cache}
	if truth, err := predicateTruth(whole, predicate, byName); err == nil && truth == truthTrue {
		return nil
	}
	var offsets metadata.OffsetIndex
	for _, b := range bindings {
		if ctx.Err() != nil {
			return nil
		}
		descr := meta.Schema.Column(b.Index)
		if descr.MaxRepetitionLevel() != 0 || descr.MaxDefinitionLevel() > 1 {
			return nil
		}
		location, present := rg.OffsetIndexLocation(b.Index)
		if !present {
			return nil
		}
		encoded := readPruningIndex(ctx, r, location, false)
		if encoded == nil {
			return nil
		}
		idx := metadata.NewOffsetIndex(encoded, r.GetPageIndexReader().Props, nil)
		if !pruningOffsetsValid(meta, rg, b.Index, idx) {
			return nil
		}
		if b.Index == anchor.Index {
			offsets = idx
		}
	}
	location, present := rg.ColumnIndexLocation(anchor.Index)
	if !present {
		return nil
	}
	encoded := readPruningIndex(ctx, r, location, true)
	if encoded == nil {
		return nil
	}
	column := metadata.NewColumnIndex(meta.Schema.Column(anchor.Index), encoded, r.GetPageIndexReader().Props, nil)
	if column == nil || offsets == nil {
		return nil
	}
	locations := offsets.GetPageLocations()
	count := len(locations)
	if len(column.GetNullPages()) != count || len(column.GetMinValues()) != count || len(column.GetMaxValues()) != count ||
		(column.IsSetNullCounts() && len(column.GetNullCounts()) != count) {
		return nil
	}
	plan = &pagePruningPlan{}
	descr := meta.Schema.Column(anchor.Index)
	for i, location := range locations {
		if i%128 == 0 && ctx.Err() != nil {
			return nil
		}
		end := rg.NumRows()
		if i+1 < count {
			end = locations[i+1].FirstRowIndex
		}
		rows := end - location.FirstRowIndex
		stats, ok := pruningPageStats(meta, descr, column, i, rows, anchor.Nullable)
		if !ok {
			return nil
		}
		domain := &pruningRowGroup{RowGroupMetaData: rg, file: meta, index: group,
			page: &pruningPageStatistics{rows: rows, column: anchor.Index, stats: stats}}
		truth, err := predicateTruth(domain, predicate, byName)
		if err != nil {
			return nil
		}
		if truth&truthTrue == 0 {
			plan.prunedPages++
			plan.prunedRows += rows
			continue
		}
		if n := len(plan.ranges); n > 0 && plan.ranges[n-1].end == location.FirstRowIndex {
			plan.ranges[n-1].end = end
		} else {
			plan.ranges = append(plan.ranges, pageRowRange{location.FirstRowIndex, end})
		}
	}
	if plan.prunedPages == 0 {
		return nil
	}
	return plan
}

func pruningIndexRangesValid(meta *metadata.FileMetaData, rg *metadata.RowGroupMetaData) bool {
	footer := meta.GetSourceFileSize() - 8 - int64(meta.Size())
	if footer < 4 {
		return false
	}
	starts, ends := [2]int64{math.MaxInt64, math.MaxInt64}, [2]int64{}
	for i := 0; i < rg.NumColumns(); i++ {
		ci, haveCI := rg.ColumnIndexLocation(i)
		oi, haveOI := rg.OffsetIndexLocation(i)
		for kind, location := range []metadata.IndexLocation{ci, oi} {
			if (kind == 0 && !haveCI) || (kind == 1 && !haveOI) {
				continue
			}
			if location.Offset < 4 || location.Length <= 0 || location.Offset > footer || int64(location.Length) > footer-location.Offset {
				return false
			}
			starts[kind] = min(starts[kind], location.Offset)
			ends[kind] = max(ends[kind], location.Offset+int64(location.Length))
		}
	}
	if ends[0] == 0 || ends[1] == 0 {
		return false
	}
	var bytes int64
	for i := range starts {
		if ends[i] != 0 {
			span := ends[i] - starts[i]
			if span > maxPruningIndexBytes-bytes {
				return false
			}
			bytes += span
		}
	}
	return true
}

func pruningOffsetsValid(meta *metadata.FileMetaData, rg *metadata.RowGroupMetaData, column int, idx metadata.OffsetIndex) bool {
	if idx == nil {
		return false
	}
	locations := idx.GetPageLocations()
	if len(locations) == 0 || len(locations) > maxPruningPages {
		return false
	}
	chunk, err := rg.ColumnChunk(column)
	if err != nil || chunk.FilePath() != "" || chunk.CryptoMetadata() != nil {
		return false
	}
	start := chunk.DataPageOffset()
	if chunk.HasDictionaryPage() {
		start = chunk.DictionaryPageOffset()
		if start < 4 || start >= chunk.DataPageOffset() {
			return false
		}
	}
	footer := meta.GetSourceFileSize() - 8 - int64(meta.Size())
	if start < 4 || chunk.TotalCompressedSize() <= 0 || start > footer || chunk.TotalCompressedSize() > footer-start {
		return false
	}
	end := start + chunk.TotalCompressedSize()
	// Page locations must stay in the data chunk, not its dictionary, optional
	// indexes or footer. The first indexed page is the first data page.
	for i := 0; i < rg.NumColumns(); i++ {
		for _, get := range []func(int) (metadata.IndexLocation, bool){rg.ColumnIndexLocation, rg.OffsetIndexLocation} {
			if loc, ok := get(i); ok && loc.Offset < end && loc.Offset+int64(loc.Length) > start {
				return false
			}
		}
	}
	previousRow, previousEnd := int64(-1), chunk.DataPageOffset()
	for i, location := range locations {
		if location == nil || location.FirstRowIndex <= previousRow || location.FirstRowIndex >= rg.NumRows() ||
			location.Offset != previousEnd || location.CompressedPageSize <= 0 || location.Offset > end || int64(location.CompressedPageSize) > end-location.Offset {
			return false
		}
		if i == 0 && (location.FirstRowIndex != 0 || location.Offset != chunk.DataPageOffset()) {
			return false
		}
		previousRow, previousEnd = location.FirstRowIndex, location.Offset+int64(location.CompressedPageSize)
	}
	// A gap or suffix could hide an unindexed data page whose rows would
	// otherwise inherit the wrong statistics. Unusual chunk layouts fall back.
	return previousEnd == end
}

func pruningPageStats(meta *metadata.FileMetaData, descr *schema.Column, index metadata.ColumnIndex, page int, rows int64, nullable bool) (metadata.TypedStatistics, bool) {
	if descr.ColumnOrder() != arrowparquet.ColumnOrders.TypeDefinedOrder || descr.SortOrder() != schema.SortSIGNED || (descr.PhysicalType() != arrowparquet.Types.Int32 && descr.PhysicalType() != arrowparquet.Types.Int64) {
		return nil, false
	}
	allNull := index.GetNullPages()[page]
	nulls := int64(0)
	if index.IsSetNullCounts() {
		nulls = index.GetNullCounts()[page]
	} else if allNull {
		nulls = rows
	} else if nullable {
		return nil, false // absent is unknown, never zero
	}
	if nulls < 0 || nulls > rows || allNull != (nulls == rows) || (!nullable && nulls != 0) {
		return nil, false
	}
	encoded := metadata.EncodedStatistics{Signed: true, HasNullCount: true, NullCount: nulls}
	if !allNull {
		lo, hi := index.GetMinValues()[page], index.GetMaxValues()[page]
		width := 4
		if descr.PhysicalType() == arrowparquet.Types.Int64 {
			width = 8
		}
		if len(lo) != width || len(hi) != width {
			return nil, false
		}
		if (width == 4 && int32(binary.LittleEndian.Uint32(lo)) > int32(binary.LittleEndian.Uint32(hi))) ||
			(width == 8 && int64(binary.LittleEndian.Uint64(lo)) > int64(binary.LittleEndian.Uint64(hi))) {
			return nil, false
		}
		encoded.SetMin(lo).SetMax(hi)
	}
	if !meta.WriterVersion().HasCorrectStatistics(descr.PhysicalType(), descr.LogicalType(), encoded, descr.SortOrder()) {
		return nil, false
	}
	if descr.PhysicalType() == arrowparquet.Types.Int32 {
		return metadata.NewInt32StatisticsFromEncoded(descr, arrowmemory.DefaultAllocator, rows-nulls, encoded.ToThrift()), true
	}
	return metadata.NewInt64StatisticsFromEncoded(descr, arrowmemory.DefaultAllocator, rows-nulls, encoded.ToThrift()), true
}

func (s *batchStream) advancePageRange(ctx context.Context) error {
	if s.pageRanges == nil || s.groupRows-s.remaining < s.pageEnd {
		return nil
	}
	if s.pageRange == len(s.pageRanges) {
		s.remaining = 0
		return nil
	}
	span := s.pageRanges[s.pageRange]
	s.pageRange++
	if span.start != s.groupRows-s.remaining {
		for i, chunk := range s.chunks {
			if err := s.checkContext(ctx); err != nil {
				return err
			}
			if err := chunk.SeekToRow(span.start); err != nil {
				// A reader may already have moved. Never fall back using a mix
				// of old and new column positions; Next makes this terminal.
				return fmt.Errorf("parquet page seek column %q row %d: %w", s.bindings[i].Name, span.start, err)
			}
		}
	}
	s.remaining = s.groupRows - span.start
	s.pageEnd = span.end
	return nil
}

// readPruningIndex preflights compact Thrift without allocating containers from
// their advertised lengths. Arrow's generated readers allocate such containers
// before they discover truncated input, so recover alone cannot bound that risk.
// The same immutable bytes are decoded only after this complete validation.
func readPruningIndex(ctx context.Context, r *file.Reader, location metadata.IndexLocation, column bool) []byte {
	if location.Length <= 0 || int64(location.Length) > maxPruningIndexBytes || ctx.Err() != nil {
		return nil
	}
	data := make([]byte, int(location.Length))
	if _, err := r.GetPageIndexReader().Input.ReadAt(data, location.Offset); err != nil {
		return nil
	}
	if !pruningIndexEncodingValid(ctx, data, column) {
		return nil
	}
	return data
}

func pruningIndexEncodingValid(ctx context.Context, data []byte, column bool) bool {
	if len(data) == 0 || int64(len(data)) > maxPruningIndexBytes || ctx.Err() != nil {
		return false
	}
	transport := &thrift.TMemoryBuffer{Buffer: bytes.NewBuffer(data)}
	protocol := pruningIndexProtocol{TProtocol: thrift.NewTCompactProtocolConf(transport,
		&thrift.TConfiguration{MaxMessageSize: int32(len(data))})}
	return pruningIndexFields(ctx, protocol, column, false) == nil && transport.RemainingBytes() == 0 && ctx.Err() == nil
}

// Apply the container limit to every field, including unknown extension fields.
// Compact-Thrift binary/string lengths are separately capped by MaxMessageSize.
// The generic skipper iterates elements and never preallocates their containers.
type pruningIndexProtocol struct{ thrift.TProtocol }

// ReadListBegin bounds lists before any generated reader can allocate them.
func (p pruningIndexProtocol) ReadListBegin(ctx context.Context) (thrift.TType, int, error) {
	kind, size, err := p.TProtocol.ReadListBegin(ctx)
	return kind, size, pruningContainerError(size, err)
}

// ReadSetBegin applies the same bound to unknown set extension fields.
func (p pruningIndexProtocol) ReadSetBegin(ctx context.Context) (thrift.TType, int, error) {
	kind, size, err := p.TProtocol.ReadSetBegin(ctx)
	return kind, size, pruningContainerError(size, err)
}

// ReadMapBegin applies the same bound to unknown map extension fields.
func (p pruningIndexProtocol) ReadMapBegin(ctx context.Context) (thrift.TType, thrift.TType, int, error) {
	key, value, size, err := p.TProtocol.ReadMapBegin(ctx)
	return key, value, size, pruningContainerError(size, err)
}

func pruningContainerError(size int, err error) error {
	if err != nil {
		return err
	}
	if size < 0 || size > maxPruningPages {
		return fmt.Errorf("parquet optional index container exceeds page-pruning limit")
	}
	return nil
}

// The generated Arrow readers ignore list element type tags on known fields.
// Validate those against the Parquet schema, not merely the advertised type:
// otherwise an integer could pass a generic Skip yet become a binary allocation
// length when Arrow subsequently reads a minima/maxima list.
func pruningIndexFields(ctx context.Context, p pruningIndexProtocol, column, pageLocation bool) error {
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return err
	}
	for {
		_, kind, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return err
		}
		if kind == thrift.STOP {
			return p.ReadStructEnd(ctx)
		}
		expected, element := thrift.TType(thrift.STOP), thrift.TType(thrift.STOP)
		if pageLocation {
			switch id {
			case 1, 3:
				expected = thrift.I64
			case 2:
				expected = thrift.I32
			}
		} else if column {
			switch id {
			case 1:
				expected, element = thrift.LIST, thrift.BOOL
			case 2, 3:
				expected, element = thrift.LIST, thrift.STRING
			case 4:
				expected = thrift.I32
			case 5, 6, 7:
				expected, element = thrift.LIST, thrift.I64
			}
		} else {
			switch id {
			case 1:
				expected, element = thrift.LIST, thrift.STRUCT
			case 2:
				expected, element = thrift.LIST, thrift.I64
			}
		}
		if expected != thrift.STOP && kind != expected {
			return fmt.Errorf("parquet optional index field has inconsistent type")
		}
		if expected == thrift.LIST {
			actual, size, err := p.ReadListBegin(ctx)
			if err != nil {
				return err
			}
			if actual != element {
				return fmt.Errorf("parquet optional index list has inconsistent element type")
			}
			for i := 0; i < size; i++ {
				if i%128 == 0 && ctx.Err() != nil {
					return ctx.Err()
				}
				if element == thrift.STRUCT {
					err = pruningIndexFields(ctx, p, false, true)
				} else {
					err = thrift.Skip(ctx, p, element, 8)
				}
				if err != nil {
					return err
				}
			}
			if err := p.ReadListEnd(ctx); err != nil {
				return err
			}
		} else if err := thrift.Skip(ctx, p, kind, 8); err != nil {
			return err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return err
		}
	}
}
