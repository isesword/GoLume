package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/physical"
	"gocolumn/source"
	"math"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/thrift/lib/go/thrift"
)

// pageTestIndex deliberately exposes raw, possibly inconsistent optional
// statistics without depending on Arrow's normalization of malformed input.
type pageTestIndex struct {
	metadata.ColumnIndex
	nulls      []bool
	counts     []int64
	mins, maxs [][]byte
}

func (i pageTestIndex) GetNullPages() []bool   { return i.nulls }
func (i pageTestIndex) IsSetNullCounts() bool  { return i.counts != nil }
func (i pageTestIndex) GetNullCounts() []int64 { return i.counts }
func (i pageTestIndex) GetMinValues() [][]byte { return i.mins }
func (i pageTestIndex) GetMaxValues() [][]byte { return i.maxs }

func pageTestInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func TestPagePruningStatisticsPresenceAndBounds(t *testing.T) {
	s := metadataCacheFixture(t, 1, 1, true)
	for _, tc := range []struct {
		name            string
		allNull         bool
		counts          []int64
		lo, hi          []byte
		nullable, valid bool
	}{
		{"normal", false, []int64{1}, pageTestInt64(10), pageTestInt64(20), true, true},
		{"extremes", false, []int64{0}, pageTestInt64(math.MinInt64), pageTestInt64(math.MaxInt64), true, true},
		{"absent optional count", false, nil, pageTestInt64(10), pageTestInt64(20), true, false},
		{"absent required count", false, nil, pageTestInt64(10), pageTestInt64(20), false, true},
		{"all null absent count", true, nil, nil, nil, true, true},
		{"all null empty bounds", true, []int64{4}, []byte{}, []byte{}, true, true},
		{"all null ignored bounds", true, []int64{4}, []byte{1}, []byte{2}, true, true},
		{"all null inconsistent count", true, []int64{3}, nil, nil, true, false},
		{"not all null inconsistent count", false, []int64{4}, pageTestInt64(10), pageTestInt64(20), true, false},
		{"required has null", false, []int64{1}, pageTestInt64(10), pageTestInt64(20), false, false},
		{"negative count", false, []int64{-1}, pageTestInt64(10), pageTestInt64(20), true, false},
		{"oversize count", false, []int64{5}, pageTestInt64(10), pageTestInt64(20), true, false},
		{"missing minimum", false, []int64{0}, nil, pageTestInt64(20), true, false},
		{"missing maximum", false, []int64{0}, pageTestInt64(10), nil, true, false},
		{"short minimum", false, []int64{0}, []byte{10}, pageTestInt64(20), true, false},
		{"long maximum", false, []int64{0}, pageTestInt64(10), make([]byte, 9), true, false},
		{"reversed bounds", false, []int64{0}, pageTestInt64(20), pageTestInt64(10), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index := pageTestIndex{nulls: []bool{tc.allNull}, counts: tc.counts, mins: [][]byte{tc.lo}, maxs: [][]byte{tc.hi}}
			stats, ok := pruningPageStats(s.meta, s.meta.Schema.Column(0), index, 0, 4, tc.nullable)
			if ok != tc.valid {
				t.Fatalf("valid=%v want %v", ok, tc.valid)
			}
			if ok {
				want := int64(0)
				if tc.counts != nil {
					want = tc.counts[0]
				} else if tc.allNull {
					want = 4
				}
				if !stats.HasNullCount() || stats.NullCount() != want || stats.NumValues() != 4-want || stats.HasMinMax() == tc.allNull {
					t.Fatalf("incorrect public statistics: null=%d values=%d bounds=%v", stats.NullCount(), stats.NumValues(), stats.HasMinMax())
				}
			}
		})
	}
}

func TestPagePruningTruthUsesPageRowsAndUnknownOtherColumns(t *testing.T) {
	s := metadataCacheFixture(t, 2, 2, true)
	index := pageTestIndex{nulls: []bool{false}, counts: []int64{1}, mins: [][]byte{pageTestInt64(10)}, maxs: [][]byte{pageTestInt64(20)}}
	stats, ok := pruningPageStats(s.meta, s.meta.Schema.Column(0), index, 0, 3, true)
	if !ok {
		t.Fatal("valid page statistics rejected")
	}
	domain := &pruningRowGroup{RowGroupMetaData: s.meta.RowGroup(0), file: s.meta, index: 0, page: &pruningPageStatistics{rows: 3, column: 0, stats: stats}}
	bindings := map[string]columnBinding{"c0": s.allBindings[0], "c1": s.allBindings[1]}
	x, y := expr.Col("c0"), expr.Col("c1")
	for _, tc := range []struct {
		name string
		p    expr.Expr
		keep bool
	}{
		{"outside", expr.GT(x, expr.I64(30)), false},
		{"not outside", expr.Not(expr.GT(x, expr.I64(30))), true},
		{"and unknown", expr.And(expr.GT(x, expr.I64(30)), expr.EQ(y, expr.I64(10))), false},
		{"or unknown", expr.Or(expr.GT(x, expr.I64(30)), expr.EQ(y, expr.I64(10))), true},
		{"not and", expr.Not(expr.And(expr.GT(x, expr.I64(30)), expr.EQ(y, expr.I64(10)))), true},
		{"not or", expr.Not(expr.Or(expr.LE(x, expr.I64(30)), expr.EQ(y, expr.I64(10)))), false},
		{"nullable", expr.IsNull(x), true},
		{"nonnull", expr.IsNotNull(x), true},
		{"unknown nulls", expr.IsNull(y), true},
		{"in null", expr.In(x, expr.I64(30), expr.Null(memory.TypeInt64)), false},
		{"not in null", expr.Not(expr.In(x, expr.I64(30), expr.Null(memory.TypeInt64))), false},
		{"within", expr.In(x, expr.I64(15), expr.I64(30)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			truth, err := predicateTruth(domain, tc.p, bindings)
			if err != nil || (truth&truthTrue != 0) != tc.keep {
				t.Fatalf("truth=%d keep=%v err=%v", truth, tc.keep, err)
			}
		})
	}
	if domain.NumRows() != 3 {
		t.Fatal("page domain uses row-group count")
	}
	if other, err := columnStatistics(domain, bindings["c1"]); err != nil || other != nil {
		t.Fatal("other column borrowed row-group statistics")
	}
}

type pageTestOffsets struct {
	metadata.OffsetIndex
	locations []*metadata.PageLocation
}

func (i pageTestOffsets) GetPageLocations() []*metadata.PageLocation { return i.locations }

func TestPagePruningRejectsIncompleteOrInvalidOffsets(t *testing.T) {
	fixture := writePagePruningIOFixture(t, memory.TypeInt64, true, false, arrowparquet.DataPageV1)
	r, err := file.OpenParquetFile(fixture.path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	indexes, err := r.GetPageIndexReader().RowGroup(0)
	if err != nil {
		t.Fatal(err)
	}
	index, err := indexes.GetOffsetIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	original := index.GetPageLocations()
	if len(original) < 3 {
		t.Fatal("fixture needs several pages")
	}
	for _, tc := range []struct {
		name   string
		change func([]*metadata.PageLocation) []*metadata.PageLocation
	}{
		{"omitted interior", func(p []*metadata.PageLocation) []*metadata.PageLocation { return append(p[:1], p[2:]...) }},
		{"omitted suffix", func(p []*metadata.PageLocation) []*metadata.PageLocation { return p[:len(p)-1] }},
		{"omitted prefix", func(p []*metadata.PageLocation) []*metadata.PageLocation { return p[1:] }},
		{"nil page", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[1] = nil; return p }},
		{"negative row", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[0].FirstRowIndex = -1; return p }},
		{"duplicate row", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[1].FirstRowIndex = 0; return p }},
		{"row past end", func(p []*metadata.PageLocation) []*metadata.PageLocation {
			p[len(p)-1].FirstRowIndex = int64(fixture.rows)
			return p
		}},
		{"negative offset", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[0].Offset = -1; return p }},
		{"offset overflow", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[1].Offset = math.MaxInt64; return p }},
		{"overlap", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[1].Offset--; return p }},
		{"byte gap", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[1].Offset++; return p }},
		{"zero size", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[1].CompressedPageSize = 0; return p }},
		{"negative size", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[1].CompressedPageSize = -1; return p }},
		{"short suffix", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[len(p)-1].CompressedPageSize--; return p }},
		{"oversize suffix", func(p []*metadata.PageLocation) []*metadata.PageLocation { p[len(p)-1].CompressedPageSize++; return p }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			locations := make([]*metadata.PageLocation, len(original))
			for i, p := range original {
				copied := *p
				locations[i] = &copied
			}
			if !pruningOffsetsValid(r.MetaData(), r.MetaData().RowGroup(0), 0, pageTestOffsets{locations: locations}) {
				t.Fatal("valid control rejected")
			}
			if pruningOffsetsValid(r.MetaData(), r.MetaData().RowGroup(0), 0, pageTestOffsets{locations: tc.change(locations)}) {
				t.Fatal("malformed/incomplete index accepted")
			}
		})
	}
}

func TestPagePruningMalformedOptionalIndexFallsBack(t *testing.T) {
	for _, columnIndex := range []bool{true, false} {
		t.Run(fmt.Sprint("column_index=", columnIndex), func(t *testing.T) {
			fixture := writePagePruningIOFixture(t, memory.TypeInt64, true, false, arrowparquet.DataPageV1)
			base := openBatchStreamFixture(t, fixture.path)
			predicate := pagePruningIOPredicate(memory.TypeInt64)
			full := readPagePruningIO(t, base, fixture, predicate, 257)
			chunk, err := base.meta.RowGroup(0).ColumnChunk(0)
			if err != nil {
				t.Fatal(err)
			}
			location := chunk.GetOffsetIndexLocation()
			if columnIndex {
				location = chunk.GetColumnIndexLocation()
			}
			if location == nil {
				t.Fatal("fixture index missing")
			}
			f, err := os.OpenFile(fixture.path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			// Invalid compact-Thrift bytes stay inside the optional index. The
			// data pages and footer remain intact and sequentially readable.
			bad := bytes.Repeat([]byte{0xff}, int(location.Length))
			_, writeErr := f.WriteAt(bad, location.Offset)
			closeErr := f.Close()
			if writeErr != nil || closeErr != nil {
				t.Fatal(writeErr, closeErr)
			}
			pushed, _, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
			if err != nil {
				t.Fatal(err)
			}
			result := readPagePruningIO(t, pushed.(*Source), fixture, predicate, 257)
			assertPagePruningIOResults(t, fixture, full, result, false)
		})
	}
}

func TestPagePruningIndexReadBudgetAndFooterBounds(t *testing.T) {
	fixture := writePagePruningIOFixture(t, memory.TypeInt64, false, false, arrowparquet.DataPageV1)
	base := openBatchStreamFixture(t, fixture.path)
	meta, rg := base.meta, base.meta.RowGroup(0)
	if !pruningIndexRangesValid(meta, rg) {
		t.Fatal("valid index read ranges rejected")
	}
	column := meta.RowGroups[0].Columns[0]
	oldOffset, oldLength := column.ColumnIndexOffset, column.ColumnIndexLength
	defer func() { column.ColumnIndexOffset, column.ColumnIndexLength = oldOffset, oldLength }()
	for _, tc := range []struct {
		name   string
		offset int64
		length int32
	}{
		{"negative offset", -1, 1},
		{"overflow", math.MaxInt64, 100},
		{"negative length", *oldOffset, -1},
		{"zero length", *oldOffset, 0},
		{"footer overlap", meta.GetSourceFileSize() - 8 - int64(meta.Size()), 1},
		{"coalesced gap budget", 4, 1},
		{"single index budget", 4, int32(maxPruningIndexBytes + 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			offset, length := tc.offset, tc.length
			column.ColumnIndexOffset, column.ColumnIndexLength = &offset, &length
			if pruningIndexRangesValid(meta, rg) {
				t.Fatal("invalid/over-budget optional index accepted")
			}
		})
	}
}

func TestPagePruningWriterTrustAndUnknownOrder(t *testing.T) {
	s := metadataCacheFixture(t, 1, 1, true)
	createdBy := "parquet-mr version 1.7.0"
	s.meta.CreatedBy = &createdBy
	serialized, err := s.meta.Serialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metadata.NewFileMetaData(serialized, nil)
	if err != nil {
		t.Fatal(err)
	}
	index := pageTestIndex{nulls: []bool{false}, counts: []int64{0}, mins: [][]byte{pageTestInt64(10)}, maxs: [][]byte{pageTestInt64(20)}}
	// Arrow explicitly trusts signed integer statistics from this old writer.
	encoded := metadata.EncodedStatistics{Signed: true, HasNullCount: true}
	encoded.SetMin(index.mins[0]).SetMax(index.maxs[0])
	descr := meta.Schema.Column(0)
	want := meta.WriterVersion().HasCorrectStatistics(descr.PhysicalType(), descr.LogicalType(), encoded, descr.SortOrder())
	if _, ok := pruningPageStats(meta, descr, index, 0, 4, true); ok != want {
		t.Fatal("page statistics differ from Arrow writer trust")
	}
	// Reparse after dropping column orders so no cached descriptor can hide it.
	createdBy = "parquet-go-arrow version 18.7.0"
	s.meta.CreatedBy, s.meta.ColumnOrders = &createdBy, nil
	serialized, err = s.meta.Serialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	meta, err = metadata.NewFileMetaData(serialized, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := pruningPageStats(meta, meta.Schema.Column(0), index, 0, 4, true); ok {
		t.Fatal("unknown column order accepted")
	}
}

func TestPagePruningIndexPreflightRejectsInvalidLengthsAndTypes(t *testing.T) {
	ctx := context.Background()
	for _, column := range []bool{false, true} {
		for _, count := range []int{maxPruningPages + 1, 1 << 20} {
			buffer := thrift.NewTMemoryBufferLen(32)
			p := thrift.NewTCompactProtocol(buffer)
			if err := p.WriteStructBegin(ctx, "index"); err != nil {
				t.Fatal(err)
			}
			field, element := int16(1), thrift.TType(thrift.STRUCT)
			if column {
				field, element = 2, thrift.STRING
			}
			if err := p.WriteFieldBegin(ctx, "entries", thrift.LIST, field); err != nil {
				t.Fatal(err)
			}
			if err := p.WriteListBegin(ctx, element, count); err != nil {
				t.Fatal(err)
			}
			if pruningIndexEncodingValid(ctx, buffer.Bytes(), column) {
				t.Fatal("truncated oversized container accepted")
			}
		}
	}
	// Empty, wrong-typed known lists are enough to test schema validation
	// without constructing or decoding any large allocation request.
	for _, column := range []bool{false, true} {
		for _, kind := range []thrift.TType{thrift.I64, thrift.STRING, thrift.BOOL} {
			buffer := thrift.NewTMemoryBufferLen(32)
			p := thrift.NewTCompactProtocol(buffer)
			field := int16(1)
			if column {
				field = 2
				if kind == thrift.STRING {
					continue
				}
			}
			if err := p.WriteStructBegin(ctx, "index"); err != nil {
				t.Fatal(err)
			}
			if err := p.WriteFieldBegin(ctx, "entries", thrift.LIST, field); err != nil {
				t.Fatal(err)
			}
			if err := p.WriteListBegin(ctx, kind, 0); err != nil {
				t.Fatal(err)
			}
			if err := p.WriteListEnd(ctx); err != nil {
				t.Fatal(err)
			}
			if err := p.WriteFieldEnd(ctx); err != nil {
				t.Fatal(err)
			}
			if err := p.WriteFieldStop(ctx); err != nil {
				t.Fatal(err)
			}
			if err := p.WriteStructEnd(ctx); err != nil {
				t.Fatal(err)
			}
			if pruningIndexEncodingValid(ctx, buffer.Bytes(), column) {
				t.Fatal("known field list element type mismatch accepted")
			}
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if pruningIndexEncodingValid(cancelled, []byte{0}, true) {
		t.Fatal("cancelled preflight accepted")
	}
	if pruningIndexEncodingValid(ctx, []byte{0, 0}, true) {
		t.Fatal("trailing bytes accepted")
	}
}

type pageSeekTestReader struct {
	file.ColumnChunkReader
	rows      []int64
	err       error
	afterSeek func()
	closed    int
}

func (r *pageSeekTestReader) SeekToRow(row int64) error {
	r.rows = append(r.rows, row)
	if r.afterSeek != nil {
		r.afterSeek()
	}
	return r.err
}
func (r *pageSeekTestReader) Close() error { r.closed++; return nil }

func TestPagePruningPartialSeekFailureIsTerminalAndClosesAllReaders(t *testing.T) {
	for _, cancelDuringSeek := range []bool{false, true} {
		t.Run(fmt.Sprint("cancel=", cancelDuringSeek), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first, second := &pageSeekTestReader{}, &pageSeekTestReader{err: errors.New("seek failed")}
			wantErr := second.err
			if cancelDuringSeek {
				first.afterSeek = cancel
				wantErr = context.Canceled
			}
			stream := &batchStream{ctx: ctx, groupOpen: true, groupRows: 100, remaining: 100,
				bindings: []columnBinding{{Name: "a"}, {Name: "b"}},
				chunks:   []file.ColumnChunkReader{first, second}, pageRanges: []pageRowRange{{10, 20}}}
			ok, err := stream.Next(context.Background(), memory.NewBatch())
			if ok || !errors.Is(err, wantErr) {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
			if first.closed != 1 || second.closed != 1 {
				t.Fatal("partial seek failure retained readers")
			}
			if len(first.rows) != 1 || first.rows[0] != 10 {
				t.Fatal("test did not move first reader")
			}
			if cancelDuringSeek && len(second.rows) != 0 {
				t.Fatal("seek continued after cancellation")
			}
			if ok, again := stream.Next(context.Background(), memory.NewBatch()); ok || again != err {
				t.Fatal("terminal error was not retained")
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if first.closed != 1 || second.closed != 1 {
				t.Fatal("readers closed twice")
			}
		})
	}
}

func TestPagePruningFullMatchAvoidsIndexReads(t *testing.T) {
	fixture := writePagePruningIOFixture(t, memory.TypeInt64, false, false, arrowparquet.DataPageV2)
	base := openBatchStreamFixture(t, fixture.path)
	predicate := expr.GE(expr.Col("x"), expr.I64(0))
	pushed, _, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
	if err != nil {
		t.Fatal(err)
	}
	full := readPagePruningIO(t, base, fixture, predicate, 257)
	result := readPagePruningIO(t, pushed.(*Source), fixture, predicate, 257)
	if len(full.filtered) != fixture.rows || !slices.Equal(full.rows, result.rows) || !slices.Equal(full.filtered, result.filtered) {
		t.Fatal("full-match control changed rows")
	}
	if !slices.Equal(full.reads, result.reads) {
		t.Fatal("full-match plan introduced optional index I/O")
	}
}

func TestPagePruningRangeEdgesAndRepeatedPartitions(t *testing.T) {
	fixture := writePagePruningIOFixture(t, memory.TypeInt64, false, false, arrowparquet.DataPageV2)
	base := openBatchStreamFixture(t, fixture.path)
	x := expr.Col("x")
	for _, tc := range []struct {
		name      string
		predicate expr.Expr
		empty     bool
	}{
		{"prefix", expr.LT(x, expr.I64(20)), false},
		{"suffix", expr.GT(x, expr.I64(8180)), false},
		{"all pages rejected", expr.And(expr.LT(x, expr.I64(2048)), expr.GT(x, expr.I64(4096))), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pushed, report, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: tc.predicate})
			if err != nil {
				t.Fatal(err)
			}
			if report.SelectedPartitions != 1 {
				t.Fatal("row-group pruning made test vacuous")
			}
			full := readPagePruningIO(t, base, fixture, tc.predicate, 8192)
			result := readPagePruningIO(t, pushed.(*Source), fixture, tc.predicate, 8192)
			if !slices.Equal(full.filtered, result.filtered) {
				t.Fatal("range-edge residual mismatch")
			}
			if tc.empty && len(result.rows) != 0 {
				t.Fatal("fully rejected page plan emitted data")
			}
			if !tc.empty && (len(result.rows) == 0 || len(result.rows) == fixture.rows) {
				t.Fatal("range-edge fixture did not prune")
			}
			repeated := *pushed.(*Source)
			repeated.parts = append(append([]source.Partition(nil), repeated.parts...), repeated.parts...)
			stream, err := repeated.NewBatchStreamWithPartitions(context.Background(), testReadOptions(), repeated.parts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			var twice []int64
			batch := memory.NewBatch()
			for {
				ok, err := stream.Next(context.Background(), batch)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				twice = append(twice, batch.Int64s["id"].Values[:batch.Rows]...)
			}
			want := append(append([]int64(nil), result.rows...), result.rows...)
			if !slices.Equal(twice, want) {
				t.Fatal("repeated prepared partitions lost page range order")
			}
			repeated.parts = nil
			if empty := readPagePruningIO(t, &repeated, fixture, tc.predicate, 8192); len(empty.rows) != 0 {
				t.Fatal("empty prepared partition list ignored")
			}
		})
	}
}

func TestPagePruningOwnsBoundPredicateSnapshot(t *testing.T) {
	fixture := writePagePruningIOFixture(t, memory.TypeInt64, false, false, arrowparquet.DataPageV2)
	base := openBatchStreamFixture(t, fixture.path)
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprint("nested=", nested), func(t *testing.T) {
			in := expr.In(expr.Col("x"), expr.I64(2010))
			var predicate expr.Expr = in
			if nested {
				predicate = expr.And(expr.GE(expr.Col("x"), expr.I64(0)), in)
			}
			pushed, report, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
			if err != nil {
				t.Fatal(err)
			}
			if report.SelectedPartitions != 1 {
				t.Fatal("row-group pruning made test vacuous")
			}
			residual, err := physical.NewFilterSource(pushed, predicate)
			if err != nil {
				t.Fatal(err)
			}
			// Binding/compilation has finished. The caller still owns this
			// operand slice; delayed page pruning must use its owned snapshot.
			in.Values[0] = expr.I64(6000)
			stream, err := residual.NewBatchStream(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			var got, want []int64
			batch := memory.NewBatch()
			for {
				ok, err := stream.Next(context.Background(), batch)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				got = append(got, batch.Int64s["id"].Values[:batch.Rows]...)
			}
			for row := 0; row < fixture.rows; row++ {
				value, valid := pagePruningIOValue(fixture.columns[0], 0, row)
				if valid && value.(int64) == 2010 {
					want = append(want, int64(row))
				}
			}
			if len(want) == 0 || !slices.Equal(got, want) {
				t.Fatalf("caller mutation changed snapshot result: got=%v want=%v", got, want)
			}
		})
	}
}

func TestPagePruningRowsAreAnUpperBoundForDirectJoin(t *testing.T) {
	ctx := context.Background()
	fixture := writePagePruningIOFixture(t, memory.TypeInt64, false, false, arrowparquet.DataPageV2)
	base := openBatchStreamFixture(t, fixture.path)
	predicate := pagePruningIOPredicate(memory.TypeInt64)
	pushed, _, err := base.ApplyPushdown(ctx, source.PushdownRequest{Predicate: predicate})
	if err != nil {
		t.Fatal(err)
	}
	expected := readPagePruningIO(t, pushed.(*Source), fixture, predicate, 257)
	// The join's materializer supports the two demanded integer columns.
	// Exclude unrelated BOOL/DATE payloads from this integration contract.
	left, _, err := base.ApplyPushdown(ctx, source.PushdownRequest{Projection: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	build, _, err := base.ApplyPushdown(ctx, source.PushdownRequest{Projection: []string{"id"}, Predicate: predicate})
	if err != nil {
		t.Fatal(err)
	}
	opts := testReadOptions()
	manager := memory.NewBudgetManager(64 << 20)
	exact := *build.(*Source)
	exact.stats.RowsUpperBound = false
	negative, err := physical.NewJoinSource(ctx, left, &exact, join.Inner, []physical.JoinKey{{Left: "id", Right: "id"}}, "_right", manager, opts.Kernels)
	if negative != nil {
		negative.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "join build source emitted") {
		t.Fatalf("incorrect exact count did not exercise strict join validation: %v", err)
	}
	owner, err := physical.NewJoinSource(ctx, left, build, join.Inner, []physical.JoinKey{{Left: "id", Right: "id"}}, "_right", manager, opts.Kernels)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	stream, err := owner.NewBatchStream(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var got []int64
	batch := memory.NewBatch()
	for {
		ok, err := stream.Next(ctx, batch)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, batch.Int64s["id"].Values[:batch.Rows]...)
	}
	if len(expected.rows) == 0 || len(expected.rows) == fixture.rows || !slices.Equal(got, expected.rows) {
		t.Fatal("direct pushed-source join did not preserve candidate rows")
	}
	for _, tc := range []struct {
		name  string
		p     expr.Expr
		upper bool
	}{
		{"selective", predicate, true},
		{"full match", expr.GE(expr.Col("x"), expr.I64(0)), true},
		{"no predicate", nil, false},
		{"fallible", expr.GT(expr.CastTo(expr.Col("x"), memory.TypeInt8), expr.I64(0)), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, _, err := base.ApplyPushdown(ctx, source.PushdownRequest{Predicate: tc.p})
			if err != nil {
				t.Fatal(err)
			}
			stats, err := src.Stats(ctx)
			if err != nil || stats.RowsUpperBound != tc.upper || stats.Rows != int64(fixture.rows) {
				t.Fatalf("stats=%+v err=%v", stats, err)
			}
		})
	}
}
