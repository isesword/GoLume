package parquet

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
)

// These tests count the underlying file reads, not PushdownReport estimates or
// implementation counters. Both arms use the same native decoder, read caps and
// buffering; only the immutable source's pushed predicate differs. Fixtures are
// generated locally and have exactly one row group, so row-group pruning cannot
// account for either the reduced output or the avoided page reads.
func TestParquetPagePruningCountedIO(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
			for _, optional := range []bool{false, true} {
				t.Run(fmt.Sprintf("dict=%v/v=%d/optional=%v", dictionary, int(version)+1, optional), func(t *testing.T) {
					fixture := writePagePruningIOFixture(t, memory.TypeInt64, optional, dictionary, version)
					predicate := pagePruningIOPredicate(memory.TypeInt64)
					base := openBatchStreamFixture(t, fixture.path)
					pushed, report, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
					if err != nil {
						t.Fatal(err)
					}
					if report.SelectedPartitions != 1 || report.SelectedRows != int64(fixture.rows) || report.PrunedPartitions != 0 {
						t.Fatalf("fixture must retain its sole whole row group: %+v", report)
					}
					full := readPagePruningIO(t, base, fixture, predicate, 257)
					pruned := readPagePruningIO(t, pushed.(*Source), fixture, predicate, 257)
					assertPagePruningIOResults(t, fixture, full, pruned, true)
					assertPagePruningIOReadSavings(t, fixture, full, pruned)
				})
			}
		}
	}
}

func TestParquetPagePruningTypedAlignment(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeDate32} {
		t.Run(fmt.Sprint(typ), func(t *testing.T) {
			fixture := writePagePruningIOFixture(t, typ, true, false, arrowparquet.DataPageV2)
			base := openBatchStreamFixture(t, fixture.path)
			predicate := pagePruningIOPredicate(typ)
			pushed, _, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
			if err != nil {
				t.Fatal(err)
			}
			full := readPagePruningIO(t, base, fixture, predicate, 509)
			// Small batches cross bitmap words; large batches must still stop at
			// each retained range rather than reading across a rejected gap.
			for _, batchRows := range []int{63, 8192} {
				pruned := readPagePruningIO(t, pushed.(*Source), fixture, predicate, batchRows)
				assertPagePruningIOResults(t, fixture, full, pruned, true)
				assertPagePruningIOReadSavings(t, fixture, full, pruned)
			}
		})
	}
}

func TestParquetPagePruningMissingIndexReadsWholeGroup(t *testing.T) {
	for _, options := range [][]arrowparquet.WriterProperty{
		{arrowparquet.WithPageIndexEnabled(false)},
		{arrowparquet.WithPageIndexEnabledFor("payload", false)},
	} {
		fixture := writePagePruningIOFixture(t, memory.TypeInt64, true, false, arrowparquet.DataPageV1, options...)
		base := openBatchStreamFixture(t, fixture.path)
		predicate := pagePruningIOPredicate(memory.TypeInt64)
		pushed, _, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
		if err != nil {
			t.Fatal(err)
		}
		full := readPagePruningIO(t, base, fixture, predicate, 257)
		pruned := readPagePruningIO(t, pushed.(*Source), fixture, predicate, 257)
		assertPagePruningIOResults(t, fixture, full, pruned, false)
		if !slices.Equal(full.rows, pruned.rows) {
			t.Fatalf("missing demanded offset index must fall back to the whole group: %d versus %d", len(pruned.rows), len(full.rows))
		}
	}
}

// A disjunction cannot reject an anchor page just because its x statistics
// fail one branch: the other column's independently aligned pages are unknown.
func TestParquetPagePruningCrossColumnOrAndNulls(t *testing.T) {
	fixture := writePagePruningIOFixture(t, memory.TypeInt64, true, true, arrowparquet.DataPageV2)
	base := openBatchStreamFixture(t, fixture.path)
	for _, tc := range []struct {
		name      string
		predicate expr.Expr
		matches   func(int) bool
	}{
		{"other_column", expr.Or(pagePruningIOPredicate(memory.TypeInt64), expr.EQ(expr.Col("payload"), expr.I64(13*50001-19))), func(row int) bool { return pagePruningIOMatches(fixture, row) || row == 50001 }},
		{"null_branch", expr.Or(pagePruningIOPredicate(memory.TypeInt64), expr.IsNull(expr.Col("x"))), func(row int) bool {
			_, valid := pagePruningIOValue(fixture.columns[0], 0, row)
			return pagePruningIOMatches(fixture, row) || !valid
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pushed, _, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: tc.predicate})
			if err != nil {
				t.Fatal(err)
			}
			full := readPagePruningIO(t, base, fixture, tc.predicate, 257)
			pruned := readPagePruningIO(t, pushed.(*Source), fixture, tc.predicate, 257)
			var want []int64
			for row := 0; row < fixture.rows; row++ {
				if tc.matches(row) {
					want = append(want, int64(row))
				}
			}
			if !slices.Equal(full.filtered, want) || !slices.Equal(pruned.filtered, want) {
				t.Fatalf("residual/oracle mismatch: full=%d pruned=%d want=%d", len(full.filtered), len(pruned.filtered), len(want))
			}
			if len(pruned.rows) != fixture.rows {
				t.Fatalf("unknown OR/null branch discarded possible matches: candidates=%d", len(pruned.rows))
			}
		})
	}
}

func TestParquetPagePruningFloatNaNConservative(t *testing.T) {
	fixture := writePagePruningIOFixture(t, memory.TypeInt64, true, true, arrowparquet.DataPageV2)
	base := openBatchStreamFixture(t, fixture.path)
	for _, columnIndex := range []int{3, 4} {
		spec := fixture.columns[columnIndex]
		floating := expr.GT(expr.Col(spec.name), expr.F64(0))
		x := pagePruningIOPredicate(memory.TypeInt64)
		floatMatches := func(row int) bool {
			value, valid := pagePruningIOValue(spec, columnIndex, row)
			if !valid {
				return false
			}
			if v, ok := value.(float32); ok {
				return v > 0
			}
			return value.(float64) > 0
		}
		// This non-NULL NaN sits inside a retained signed-anchor range.
		// Keep the NaN regression nonvacuous if the fixture ever changes.
		value, valid := pagePruningIOValue(spec, columnIndex, 16382)
		nan := math.NaN()
		if v, ok := value.(float32); ok {
			nan = float64(v)
		} else {
			nan = value.(float64)
		}
		if !valid || !math.IsNaN(nan) || !pagePruningIOMatches(fixture, 16382) {
			t.Fatal("fixture needs a non-NULL NaN in a retained anchor range")
		}
		for _, tc := range []struct {
			name       string
			predicate  expr.Expr
			matches    func(int) bool
			wantPruned bool
		}{
			{"float_only", floating, floatMatches, false},
			{"signed_and_float", expr.And(x, floating), func(row int) bool { return pagePruningIOMatches(fixture, row) && floatMatches(row) }, true},
			{"signed_or_float", expr.Or(x, floating), func(row int) bool { return pagePruningIOMatches(fixture, row) || floatMatches(row) }, false},
		} {
			t.Run(spec.name+"/"+tc.name, func(t *testing.T) {
				pushed, _, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: tc.predicate})
				if err != nil {
					t.Fatal(err)
				}
				full := readPagePruningIO(t, base, fixture, tc.predicate, 257)
				pruned := readPagePruningIO(t, pushed.(*Source), fixture, tc.predicate, 257)
				var want []int64
				for row := 0; row < fixture.rows; row++ {
					if tc.matches(row) {
						want = append(want, int64(row))
					}
				}
				if !slices.Equal(full.filtered, want) || !slices.Equal(pruned.filtered, want) {
					t.Fatalf("float/NaN residual mismatch: full=%d pruned=%d want=%d", len(full.filtered), len(pruned.filtered), len(want))
				}
				if tc.wantPruned {
					if len(pruned.rows) >= fixture.rows {
						t.Fatal("safe signed AND did not prune")
					}
				} else if len(pruned.rows) != fixture.rows {
					t.Fatal("float-only/unknown OR unexpectedly pruned")
				}
			})
		}
	}
}

func TestParquetPagePruningFalliblePredicateKeepsErrorDomain(t *testing.T) {
	fixture := writePagePruningIOFixture(t, memory.TypeInt64, false, false, arrowparquet.DataPageV1)
	base := openBatchStreamFixture(t, fixture.path)
	fallible := expr.GT(expr.CastTo(expr.Col("x"), memory.TypeInt8), expr.I64(0))
	selective := pagePruningIOPredicate(memory.TypeInt64)
	for _, predicate := range []expr.Expr{expr.And(fallible, selective), expr.And(selective, fallible)} {
		pushed, _, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
		if err != nil {
			t.Fatal(err)
		}
		// Decode without applying the residual to observe the precise source
		// domain, rather than accepting two errors at different skipped rows.
		full := readPagePruningIO(t, base, fixture, nil, 257)
		pruned := readPagePruningIO(t, pushed.(*Source), fixture, nil, 257)
		if len(pruned.rows) != fixture.rows || !slices.Equal(full.rows, pruned.rows) {
			t.Fatal("fallible predicate removed source rows")
		}
		fullError, fullRows := pagePruningIOFailure(t, base, predicate)
		prunedError, prunedRows := pagePruningIOFailure(t, pushed.(*Source), predicate)
		if fullError != prunedError || fullRows != prunedRows {
			t.Fatalf("changed residual error domain: full=(%q,%d) pruned=(%q,%d)", fullError, fullRows, prunedError, prunedRows)
		}
	}
}

func TestParquetPagePruningProjectedConversionErrorsRemain(t *testing.T) {
	for _, kind := range []string{"narrow", "utf8"} {
		t.Run(kind, func(t *testing.T) {
			path := writePagePruningInvalidFixture(t, kind)
			base := openBatchStreamFixture(t, path)
			predicate := expr.EQ(expr.Col("x"), expr.I64(6))
			pushed, report, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
			if err != nil {
				t.Fatal(err)
			}
			if report.SelectedPartitions != 1 {
				t.Fatalf("lost sole row group: %+v", report)
			}
			fullError, fullRows := pagePruningIOFailure(t, base, nil)
			prunedError, prunedRows := pagePruningIOFailure(t, pushed.(*Source), nil)
			fragment := "int8"
			if kind == "utf8" {
				fragment = "invalid UTF-8"
			}
			if !strings.Contains(fullError, fragment) {
				t.Fatalf("unexpected failure instead of %s conversion: %s", kind, fullError)
			}
			if fullRows == 0 || fullError != prunedError || fullRows != prunedRows {
				t.Fatalf("skipped conversion error in rejected page: full=(%q,%d) pruned=(%q,%d)", fullError, fullRows, prunedError, prunedRows)
			}
		})
	}
}

func pagePruningIOFailure(t *testing.T, src *Source, residual expr.Expr) (string, int) {
	t.Helper()
	ctx := context.Background()
	manager := newBatchStreamPeakManager(1 << 20)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 63, 64<<10, manager
	stream, err := src.NewBatchStream(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	program, _, err := expr.CompilePredicate(src.Schema(), residual)
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := expr.NewWorkspace(ctx, program, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	batch := memory.NewBatch()
	rows := 0
	for {
		ok, nextErr := stream.Next(ctx, batch)
		if nextErr != nil {
			err = nextErr
			break
		}
		if !ok {
			t.Fatal("expected conversion/residual failure, scan succeeded")
		}
		if _, evalErr := workspace.EvaluateBool(ctx, batch, opts.Kernels); evalErr != nil {
			err = evalErr
			break
		}
		rows += batch.Rows
	}
	if closeErr := stream.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if manager.Used() != 0 {
		t.Fatalf("failed execution leaked %d bytes", manager.Used())
	}
	return err.Error(), rows
}

func writePagePruningInvalidFixture(t *testing.T, kind string) string {
	t.Helper()
	logical, physical := schema.LogicalType(schema.NewIntLogicalType(8, true)), arrowparquet.Types.Int32
	if kind == "utf8" {
		logical, physical = schema.StringLogicalType{}, arrowparquet.Types.ByteArray
	}
	node, err := schema.NewPrimitiveNodeLogical("bad", arrowparquet.Repetitions.Required, logical, physical, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{schema.NewInt64Node("x", arrowparquet.Repetitions.Required, -1), node}, -1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "invalid-projection.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithDictionaryDefault(false), arrowparquet.WithPageIndexEnabled(true), arrowparquet.WithDataPageSize(32<<10), arrowparquet.WithBatchSize(128))))
	defer writer.Close()
	rg := writer.AppendRowGroup()
	for index := 0; index < 2; index++ {
		column, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		for page := 0; page < 8; page++ {
			values := make([]int64, 1024)
			narrow := make([]int32, 1024)
			texts := make([]arrowparquet.ByteArray, 1024)
			for row := range values {
				values[row] = int64(page)
				narrow[row] = 1
				texts[row] = arrowparquet.ByteArray("valid")
			}
			// This value is in a page whose x is provably not 6. It must
			// nevertheless be decoded because the projection can fail.
			if page == 3 {
				narrow[7] = 128
				texts[7] = arrowparquet.ByteArray{0xff}
			}
			switch c := column.(type) {
			case *file.Int64ColumnChunkWriter:
				_, err = c.WriteBatch(values, nil, nil)
			case *file.Int32ColumnChunkWriter:
				_, err = c.WriteBatch(narrow, nil, nil)
			case *file.ByteArrayColumnChunkWriter:
				_, err = c.WriteBatch(texts, nil, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := column.(interface{ FlushCurrentPage() error }).FlushCurrentPage(); err != nil {
				t.Fatal(err)
			}
		}
		if err := column.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	// Verify the fixture really has complete indexes for both columns, so
	// missing metadata cannot accidentally explain the fallback under test.
	r, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	indexes, err := r.GetPageIndexReader().RowGroup(0)
	if err != nil || indexes == nil {
		t.Fatalf("missing fixture indexes: %v", err)
	}
	for index := 0; index < 2; index++ {
		offsets, err := indexes.GetOffsetIndex(index)
		if err != nil || offsets == nil || len(offsets.GetPageLocations()) != 8 {
			t.Fatalf("invalid fixture offset index %d: %v", index, err)
		}
	}
	return path
}

// Both benchmark arms consume the same residual predicate and checksum all
// selected IDs. The full-match case measures planning overhead when no pages
// can be skipped. Fixture generation/inspection and expression compilation are
// outside the timed loop; file open, footer/index I/O, decode and residual work
// are inside. The benchmark is not an independent byte-I/O measurement.
func BenchmarkParquetPagePruning(b *testing.B) {
	for _, dictionary := range []bool{false, true} {
		fixture := writePagePruningIOFixture(b, memory.TypeInt64, false, dictionary, arrowparquet.DataPageV2)
		base := openBatchStreamFixture(b, fixture.path)
		for _, tc := range []struct {
			name      string
			predicate expr.Expr
			fullMatch bool
		}{
			{"selective", pagePruningIOPredicate(memory.TypeInt64), false},
			{"full_match", expr.GE(expr.Col("x"), expr.I64(0)), true},
		} {
			pushed, _, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: tc.predicate})
			if err != nil {
				b.Fatal(err)
			}
			var wantRows, wantChecksum int64
			for row := 0; row < fixture.rows; row++ {
				if tc.fullMatch || pagePruningIOMatches(fixture, row) {
					wantRows++
					wantChecksum += int64(row)
				}
			}
			for _, arm := range []struct {
				name string
				src  source.Source
			}{{"full_scan", base}, {"pushed", pushed}} {
				b.Run(fmt.Sprintf("dict=%v/%s/%s", dictionary, tc.name, arm.name), func(b *testing.B) {
					ctx := context.Background()
					opts := testReadOptions()
					opts.BatchRows, opts.BatchBytes = 1024, 256<<10
					program, _, err := expr.CompilePredicate(arm.src.Schema(), tc.predicate)
					if err != nil {
						b.Fatal(err)
					}
					workspace, err := expr.NewWorkspace(ctx, program, nil)
					if err != nil {
						b.Fatal(err)
					}
					defer workspace.Close()
					batch := memory.NewBatch()
					var decoded int64
					b.ReportAllocs()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						stream, err := source.OpenBatchStream(ctx, arm.src, opts)
						if err != nil {
							b.Fatal(err)
						}
						var rows, checksum int64
						for {
							ok, err := stream.Next(ctx, batch)
							if err != nil {
								stream.Close()
								b.Fatal(err)
							}
							if !ok {
								break
							}
							decoded += int64(batch.Rows)
							selected, err := workspace.EvaluateBool(ctx, batch, opts.Kernels)
							if err != nil {
								stream.Close()
								b.Fatal(err)
							}
							for row, id := range batch.Int64s["id"].Values[:batch.Rows] {
								if memory.IsSet(selected.Truth, row) && (selected.Validity == nil || memory.IsSet(selected.Validity, row)) {
									rows++
									checksum += id
								}
							}
						}
						if err := stream.Close(); err != nil {
							b.Fatal(err)
						}
						if rows != wantRows || checksum != wantChecksum {
							b.Fatalf("residual rows/checksum=%d/%d want %d/%d", rows, checksum, wantRows, wantChecksum)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(decoded)/float64(b.N), "source-rows/op")
					b.ReportMetric(float64(wantRows), "residual-rows/op")
				})
			}
		}
	}
}

const pagePruningIORows = 65536

type pagePruningIOColumn struct {
	name     string
	typ      memory.DataType
	optional bool
	pageRows int
}

type pagePruningIOFixture struct {
	path         string
	rows         int
	columns      []pagePruningIOColumn
	pages        map[string][]pagePruningIOPage
	dictionaries []pagePruningIOInterval
}

type pagePruningIOPage struct {
	pagePruningIOInterval
	first, end int64
}

type pagePruningIOInterval struct{ first, end int64 }

type pagePruningIOResult struct {
	rows, filtered         []int64
	reads                  []pagePruningIOInterval
	readAtCalls, readCalls int
}

// The reader embeds no *os.File: every Read and ReadAt must pass through the
// recorder, including footer/index reads, dictionary loads and buffered read-
// ahead. It records bytes actually returned (not requested bytes), including
// repeated reads. Metadata inspection uses a different file descriptor.
type pagePruningIOReader struct {
	f                      *os.File
	reads                  []pagePruningIOInterval
	readAtCalls, readCalls int
}

func (r *pagePruningIOReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.f.ReadAt(p, off)
	r.readAtCalls++
	if n > 0 {
		r.reads = append(r.reads, pagePruningIOInterval{off, off + int64(n)})
	}
	return n, err
}
func (r *pagePruningIOReader) Read(p []byte) (int, error) {
	off, err := r.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	n, err := r.f.Read(p)
	r.readCalls++
	if n > 0 {
		r.reads = append(r.reads, pagePruningIOInterval{off, off + int64(n)})
	}
	return n, err
}
func (r *pagePruningIOReader) Seek(off int64, whence int) (int64, error) {
	return r.f.Seek(off, whence)
}
func (r *pagePruningIOReader) Close() error { return r.f.Close() }

func readPagePruningIO(t *testing.T, src *Source, fixture pagePruningIOFixture, residual expr.Expr, batchRows int) pagePruningIOResult {
	t.Helper()
	ctx := context.Background()
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = batchRows, 2<<20
	opts = opts.Normalize()
	manager := newBatchStreamPeakManager(4 << 20)
	opts.MemoryManager = manager
	props := arrowparquet.NewReaderProperties(nil)
	props.BufferedStreamEnabled, props.PageStreamingEnabled = true, true
	props.MaxCompressedPageSize, props.MaxUncompressedPageSize = opts.MaxCompressedPageBytes, opts.MaxUncompressedPageBytes
	f, err := os.Open(fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	counted := &pagePruningIOReader{f: f}
	r, err := file.NewParquetReader(counted, file.WithReadProps(props))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	parts, err := src.Partitions(ctx)
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	stream := src.batchStreamFromReader(ctx, opts, parts, r)
	defer stream.Close()
	program, _, err := expr.CompilePredicate(src.Schema(), residual)
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := expr.NewWorkspace(ctx, program, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	var result pagePruningIOResult
	batch := memory.NewBatch()
	for {
		ok, err := stream.Next(ctx, batch)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if batch.Rows < 1 || batch.Rows > batchRows {
			t.Fatalf("invalid batch length %d", batch.Rows)
		}
		selected, err := workspace.EvaluateBool(ctx, batch, opts.Kernels)
		if err != nil {
			t.Fatal(err)
		}
		for row := 0; row < batch.Rows; row++ {
			id := batch.Int64s["id"].Values[row]
			if id < 0 || id >= int64(fixture.rows) {
				t.Fatalf("invalid row ID %d", id)
			}
			if len(result.rows) != 0 && result.rows[len(result.rows)-1] >= id {
				t.Fatalf("duplicate or reordered row %d", id)
			}
			for index, spec := range fixture.columns {
				got, valid := pagePruningIOCell(batch, spec, row)
				want, wantValid := pagePruningIOValue(spec, index, int(id))
				if valid != wantValid || valid && !pagePruningIOValuesEqual(got, want) {
					t.Fatalf("row ID %d column %s: value=%v valid=%v, want value=%v valid=%v", id, spec.name, got, valid, want, wantValid)
				}
			}
			result.rows = append(result.rows, id)
			if memory.IsSet(selected.Truth, row) && (selected.Validity == nil || memory.IsSet(selected.Validity, row)) {
				result.filtered = append(result.filtered, id)
			}
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("stream leaked %d managed bytes", manager.Used())
	}
	result.reads, result.readAtCalls, result.readCalls = counted.reads, counted.readAtCalls, counted.readCalls
	return result
}

func pagePruningIOPredicate(typ memory.DataType) expr.Expr {
	literal := func(value int64) expr.Expr {
		if typ == memory.TypeDate32 {
			return expr.Date32Literal{Days: int32(value)}
		}
		return expr.I64(value)
	}
	x := expr.Col("x")
	return expr.Or(
		expr.And(expr.GE(x, literal(2046)), expr.LT(x, literal(2065))),
		expr.And(expr.GE(x, literal(6147)), expr.LT(x, literal(6157))),
	)
}

// An integer oracle, intentionally independent of the statistics evaluator and
// expression runtime. It also checks that NULL anchor rows do not satisfy the
// comparison, while nullable projected columns retain their original row IDs.
func pagePruningIOMatches(fixture pagePruningIOFixture, row int) bool {
	_, valid := pagePruningIOValue(fixture.columns[0], 0, row)
	x := row / 8
	return valid && (x >= 2046 && x < 2065 || x >= 6147 && x < 6157)
}

func assertPagePruningIOResults(t *testing.T, fixture pagePruningIOFixture, full, pruned pagePruningIOResult, wantPruned bool) {
	t.Helper()
	if len(full.rows) != fixture.rows {
		t.Fatalf("full scan read %d rows, want %d", len(full.rows), fixture.rows)
	}
	var want []int64
	for row := 0; row < fixture.rows; row++ {
		if pagePruningIOMatches(fixture, row) {
			want = append(want, int64(row))
		}
	}
	if !slices.Equal(full.filtered, want) || !slices.Equal(pruned.filtered, want) {
		t.Fatalf("residual differs from integer oracle: full=%v pruned=%v want=%v", full.filtered, pruned.filtered, want)
	}
	for _, row := range want {
		if _, ok := slices.BinarySearch(pruned.rows, row); !ok {
			t.Fatalf("false-negative source pruning lost row %d", row)
		}
	}
	if wantPruned && (len(pruned.rows) >= len(full.rows) || len(pruned.rows) <= len(pruned.filtered)) {
		t.Fatalf("expected a pruned conservative superset before the residual: source=%d filtered=%d full=%d", len(pruned.rows), len(pruned.filtered), len(full.rows))
	}
}

func assertPagePruningIOReadSavings(t *testing.T, fixture pagePruningIOFixture, full, pruned pagePruningIOResult) {
	t.Helper()
	var data, dictionaries []pagePruningIOInterval
	for _, pages := range fixture.pages {
		for _, page := range pages {
			data = append(data, page.pagePruningIOInterval)
		}
	}
	dictionaries = fixture.dictionaries
	count := func(reads, intervals []pagePruningIOInterval) int64 {
		var total int64
		for _, read := range reads {
			for _, interval := range intervals {
				total += max(int64(0), min(read.end, interval.end)-max(read.first, interval.first))
			}
		}
		return total
	}
	total := func(reads []pagePruningIOInterval) int64 {
		var n int64
		for _, read := range reads {
			n += read.end - read.first
		}
		return n
	}
	fullData, prunedData := count(full.reads, data), count(pruned.reads, data)
	fullDict, prunedDict := count(full.reads, dictionaries), count(pruned.reads, dictionaries)
	fullTotal, prunedTotal := total(full.reads), total(pruned.reads)
	if full.readAtCalls == 0 || pruned.readAtCalls == 0 || prunedData >= fullData || prunedTotal >= fullTotal {
		t.Fatalf("actual file reads did not fall: full total/data=%d/%d, pruned=%d/%d (ReadAt calls %d/%d)", fullTotal, fullData, prunedTotal, prunedData, full.readAtCalls, pruned.readAtCalls)
	}
	if len(dictionaries) > 0 && (fullDict == 0 || prunedDict == 0) {
		t.Fatal("dictionary reads were not included in accounting")
	}
	// Determine retained anchor ranges using fixture values, not the production
	// plan. Rejected projected pages can straddle other columns' boundaries.
	var retained []pagePruningIOInterval
	for _, page := range fixture.pages["x"] {
		for row := page.first; row < page.end; row++ {
			if pagePruningIOMatches(fixture, int(row)) {
				retained = append(retained, pagePruningIOInterval{page.first, page.end})
				break
			}
		}
	}
	untouched := 0
	for _, name := range []string{"id", "payload"} {
		columnUntouched := 0
		for _, page := range fixture.pages[name] {
			if count([]pagePruningIOInterval{{page.first, page.end}}, retained) != 0 {
				continue
			}
			// Ignore page headers and tolerate real buffered read-ahead. At
			// least one rejected page's middle must remain completely unread.
			body := page.pagePruningIOInterval
			quarter := (body.end - body.first) / 4
			interior := pagePruningIOInterval{body.first + quarter, body.end - quarter}
			if count(full.reads, []pagePruningIOInterval{interior}) < interior.end-interior.first {
				t.Fatal("full-scan control did not read the rejected page interior")
			}
			if count(pruned.reads, []pagePruningIOInterval{interior}) == 0 {
				columnUntouched++
			}
		}
		if columnUntouched == 0 {
			t.Fatalf("no rejected %s page interior was actually skipped", name)
		}
		untouched += columnUntouched
	}
	t.Logf("rows full/pruned/residual=%d/%d/%d; actual bytes full/pruned=%d/%d; data=%d/%d dictionary=%d/%d metadata/other=%d/%d; untouched rejected interiors=%d; ReadAt calls=%d/%d", len(full.rows), len(pruned.rows), len(pruned.filtered), fullTotal, prunedTotal, fullData, prunedData, fullDict, prunedDict, fullTotal-fullData-fullDict, prunedTotal-prunedData-prunedDict, untouched, full.readAtCalls, pruned.readAtCalls)
}

func writePagePruningIOFixture(t testing.TB, anchor memory.DataType, optional, dictionary bool, version arrowparquet.DataPageVersion, extra ...arrowparquet.WriterProperty) pagePruningIOFixture {
	t.Helper()
	fixture := pagePruningIOFixture{path: filepath.Join(t.TempDir(), "page-pruning.parquet"), rows: pagePruningIORows, pages: make(map[string][]pagePruningIOPage), columns: []pagePruningIOColumn{
		{"x", anchor, optional, 2048}, {"id", memory.TypeInt64, false, 1536}, {"payload", memory.TypeInt32, true, 1408},
		{"f32", memory.TypeFloat32, true, 1664}, {"f64", memory.TypeFloat64, true, 1792}, {"u32", memory.TypeUint32, true, 1280},
		{"u64", memory.TypeUint64, true, 1920}, {"flag", memory.TypeBool, true, 1152},
	}}
	var fields schema.FieldList
	for _, spec := range fixture.columns {
		rep := arrowparquet.Repetitions.Required
		if spec.optional {
			rep = arrowparquet.Repetitions.Optional
		}
		var node schema.Node
		var err error
		switch spec.typ {
		case memory.TypeInt64:
			node = schema.NewInt64Node(spec.name, rep, -1)
		case memory.TypeInt32:
			node = schema.NewInt32Node(spec.name, rep, -1)
		case memory.TypeDate32:
			node, err = schema.NewPrimitiveNodeLogical(spec.name, rep, schema.DateLogicalType{}, arrowparquet.Types.Int32, -1, -1)
		case memory.TypeUint32:
			node, err = schema.NewPrimitiveNodeLogical(spec.name, rep, schema.NewIntLogicalType(32, false), arrowparquet.Types.Int32, -1, -1)
		case memory.TypeUint64:
			node, err = schema.NewPrimitiveNodeLogical(spec.name, rep, schema.NewIntLogicalType(64, false), arrowparquet.Types.Int64, -1, -1)
		case memory.TypeFloat32:
			node = schema.NewFloat32Node(spec.name, rep, -1)
		case memory.TypeFloat64:
			node = schema.NewFloat64Node(spec.name, rep, -1)
		case memory.TypeBool:
			node = schema.NewBooleanNode(spec.name, rep, -1)
		}
		if err != nil {
			t.Fatal(err)
		}
		fields = append(fields, node)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, fields, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	props := []arrowparquet.WriterProperty{arrowparquet.WithCompression(compress.Codecs.Uncompressed), arrowparquet.WithDictionaryDefault(false), arrowparquet.WithDictionaryFor("x", dictionary), arrowparquet.WithDictionaryPageSizeLimit(1 << 20), arrowparquet.WithDataPageVersion(version), arrowparquet.WithDataPageSize(32 << 10), arrowparquet.WithBatchSize(128), arrowparquet.WithPageIndexEnabled(true)}
	props = append(props, extra...)
	writer := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(props...)))
	defer writer.Close()
	rg := writer.AppendRowGroup()
	for index, spec := range fixture.columns {
		column, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		for start := 0; start < fixture.rows; start += spec.pageRows {
			end := min(start+spec.pageRows, fixture.rows)
			// Write in small batches, then flush a complete logical page. The
			// explicit flush also creates genuine all-NULL pages. Deliberately
			// different pageRows prevent positional page-to-page alignment.
			for low := start; low < end; low += 128 {
				hi := min(low+128, end)
				var defs []int16
				if spec.optional {
					defs = make([]int16, hi-low)
				}
				var i64 []int64
				var i32 []int32
				var f32 []float32
				var f64 []float64
				var flags []bool
				for row := low; row < hi; row++ {
					value, valid := pagePruningIOValue(spec, index, row)
					if !valid {
						continue
					}
					if defs != nil {
						defs[row-low] = 1
					}
					switch value := value.(type) {
					case int64:
						i64 = append(i64, value)
					case int32:
						i32 = append(i32, value)
					case uint64:
						i64 = append(i64, int64(value))
					case uint32:
						i32 = append(i32, int32(value))
					case float32:
						f32 = append(f32, value)
					case float64:
						f64 = append(f64, value)
					case bool:
						flags = append(flags, value)
					}
				}
				switch c := column.(type) {
				case *file.Int64ColumnChunkWriter:
					_, err = c.WriteBatch(i64, defs, nil)
				case *file.Int32ColumnChunkWriter:
					_, err = c.WriteBatch(i32, defs, nil)
				case *file.Float32ColumnChunkWriter:
					_, err = c.WriteBatch(f32, defs, nil)
				case *file.Float64ColumnChunkWriter:
					_, err = c.WriteBatch(f64, defs, nil)
				case *file.BooleanColumnChunkWriter:
					_, err = c.WriteBatch(flags, defs, nil)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := column.(interface{ FlushCurrentPage() error }).FlushCurrentPage(); err != nil {
				t.Fatal(err)
			}
		}
		if err := column.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	inspectPagePruningIOFixture(t, &fixture, dictionary, version)
	return fixture
}

func pagePruningIOValue(spec pagePruningIOColumn, index, row int) (any, bool) {
	valid := !spec.optional || row%(17+index) != 0
	// One full NULL anchor page must be rejected without reading its values.
	if spec.name == "x" && spec.optional && row >= 15*2048 && row < 16*2048 {
		valid = false
	}
	value := int64(row*13 - 19)
	if spec.name == "id" {
		value = int64(row)
	}
	if spec.name == "x" {
		value = int64(row / 8)
	}
	switch spec.typ {
	case memory.TypeInt64:
		return value, valid
	case memory.TypeInt32, memory.TypeDate32:
		return int32(value), valid
	case memory.TypeUint32:
		return uint32(1<<31) + uint32(row*7), valid
	case memory.TypeUint64:
		return uint64(1<<63) + uint64(row*11), valid
	case memory.TypeFloat32:
		if row%8191 == 0 {
			return float32(math.NaN()), valid
		}
		return float32(value) / 4, valid
	case memory.TypeFloat64:
		if row%8191 == 0 {
			return math.NaN(), valid
		}
		return float64(value)/8 + 0.125, valid
	case memory.TypeBool:
		return row%3 == 1, valid
	}
	panic("unsupported fixture type")
}

func pagePruningIOValuesEqual(got, want any) bool {
	switch want := want.(type) {
	case float32:
		if math.IsNaN(float64(want)) {
			value, ok := got.(float32)
			return ok && math.IsNaN(float64(value))
		}
	case float64:
		if math.IsNaN(want) {
			value, ok := got.(float64)
			return ok && math.IsNaN(value)
		}
	}
	return reflect.DeepEqual(got, want)
}

func pagePruningIOCell(batch *memory.Batch, spec pagePruningIOColumn, row int) (any, bool) {
	valid := func(bitmap memory.Bitmap) bool { return bitmap == nil || memory.IsSet(bitmap, row) }
	switch spec.typ {
	case memory.TypeInt64:
		v := batch.Int64s[spec.name]
		return v.Values[row], valid(v.Validity)
	case memory.TypeInt32, memory.TypeDate32:
		v := batch.Int32s[spec.name]
		return v.Values[row], valid(v.Validity)
	case memory.TypeUint32:
		v := batch.Uint32s[spec.name]
		return v.Values[row], valid(v.Validity)
	case memory.TypeUint64:
		v := batch.Uint64s[spec.name]
		return v.Values[row], valid(v.Validity)
	case memory.TypeFloat32:
		v := batch.Float32s[spec.name]
		return v.Values[row], valid(v.Validity)
	case memory.TypeFloat64:
		v := batch.Float64s[spec.name]
		return v.Values[row], valid(v.Validity)
	case memory.TypeBool:
		v := batch.Uint32s[spec.name]
		return v.Values[row] != 0, valid(v.Validity)
	}
	panic("unsupported fixture type")
}

func inspectPagePruningIOFixture(t testing.TB, fixture *pagePruningIOFixture, dictionary bool, version arrowparquet.DataPageVersion) {
	t.Helper()
	r, err := file.OpenParquetFile(fixture.path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.NumRowGroups() != 1 || r.NumRows() != int64(fixture.rows) {
		t.Fatal("fixture must have one row group")
	}
	indexes, err := r.GetPageIndexReader().RowGroup(0)
	if err != nil {
		t.Fatal(err)
	}
	for index, spec := range fixture.columns {
		chunk, err := r.RowGroup(0).MetaData().ColumnChunk(index)
		if err != nil {
			t.Fatal(err)
		}
		if spec.name == "x" && chunk.HasDictionaryPage() != dictionary {
			t.Fatalf("anchor dictionary flag=%v want %v", chunk.HasDictionaryPage(), dictionary)
		}
		if chunk.HasDictionaryPage() {
			fixture.dictionaries = append(fixture.dictionaries, pagePruningIOInterval{chunk.DictionaryPageOffset(), chunk.DataPageOffset()})
		}
		if indexes != nil {
			offsets, err := indexes.GetOffsetIndex(index)
			if err != nil {
				t.Fatal(err)
			}
			if offsets != nil {
				locations := offsets.GetPageLocations()
				if len(locations) < 16 {
					t.Fatalf("column %s has only %d pages", spec.name, len(locations))
				}
				for n, location := range locations {
					end := int64(fixture.rows)
					if n+1 < len(locations) {
						end = locations[n+1].FirstRowIndex
					}
					if location.FirstRowIndex != int64(n*spec.pageRows) {
						t.Fatalf("unexpected %s page boundary %d", spec.name, location.FirstRowIndex)
					}
					fixture.pages[spec.name] = append(fixture.pages[spec.name], pagePruningIOPage{pagePruningIOInterval{location.Offset, location.Offset + int64(location.CompressedPageSize)}, location.FirstRowIndex, end})
				}
			}
		}
		pager, err := r.RowGroup(0).GetColumnPageReader(index)
		if err != nil {
			t.Fatal(err)
		}
		dataPages, dictPages := 0, 0
		for pager.Next() {
			switch page := pager.Page().(type) {
			case *file.DictionaryPage:
				dictPages++
			case *file.DataPageV1:
				if version != arrowparquet.DataPageV1 {
					t.Fatal("expected V2 page")
				}
				dataPages++
				if spec.name == "x" && dictionary && arrowparquet.Encoding(page.Encoding()) != arrowparquet.Encodings.RLEDict {
					t.Fatalf("anchor fell back from dictionary: %s", page.Encoding())
				}
			case *file.DataPageV2:
				if version != arrowparquet.DataPageV2 {
					t.Fatal("expected V1 page")
				}
				dataPages++
				if spec.name == "x" && dictionary && arrowparquet.Encoding(page.Encoding()) != arrowparquet.Encodings.RLEDict {
					t.Fatalf("anchor fell back from dictionary: %s", page.Encoding())
				}
			}
		}
		if err := pager.Err(); err != nil {
			t.Fatal(err)
		}
		if err := pager.Close(); err != nil {
			t.Fatal(err)
		}
		if dataPages != (fixture.rows+spec.pageRows-1)/spec.pageRows {
			t.Fatalf("column %s has %d data pages", spec.name, dataPages)
		}
		if spec.name == "x" && dictionary && dictPages != 1 {
			t.Fatalf("dictionary pages=%d", dictPages)
		}
	}
}
