package parquet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/thrift/lib/go/thrift"
	"gocolumn/memory"
	"gocolumn/source"
)

// This fixture's short and long optional columns end pages on different rows.
// Its explicit rows are also the oracle, independent of all decoding code.
func architectureRows(count, outlier int, outlierBytes int) [][]any {
	rows := make([][]any, count)
	for i := range rows {
		rows[i] = []any{int64(i), fmt.Sprintf("s%04d", i), fmt.Sprintf("l%04d:", i) + strings.Repeat("界", 1+i%29)}
		if i%7 == 0 {
			rows[i][1] = nil
		} else if i%11 == 0 {
			rows[i][1] = ""
		}
		if i%13 == 0 {
			rows[i][2] = nil
		} else if i%17 == 0 {
			rows[i][2] = ""
		}
	}
	if outlier >= 0 {
		rows[outlier][1] = strings.Repeat("X", outlierBytes)
		rows[outlier][2] = ""
	}
	return rows
}

func writeArchitectureFixture(t *testing.T, path string, rows [][]any, options ...arrowparquet.WriterProperty) {
	t.Helper()
	fields := schema.FieldList{schema.NewInt64Node("id", arrowparquet.Repetitions.Required, -1)}
	for _, name := range []string{"short", "long"} {
		node, err := schema.NewPrimitiveNodeLogical(name, arrowparquet.Repetitions.Optional, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
		if err != nil {
			t.Fatal(err)
		}
		fields = append(fields, node)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, fields, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	props := []arrowparquet.WriterProperty{arrowparquet.WithCompression(compress.Codecs.Uncompressed), arrowparquet.WithDictionaryDefault(false), arrowparquet.WithDataPageSize(512), arrowparquet.WithBatchSize(8)}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(append(props, options...)...)))
	defer w.Close()
	rg := w.AppendRowGroup()
	ids := make([]int64, len(rows))
	for i := range rows {
		ids[i] = rows[i][0].(int64)
	}
	writeTypedI64(t, rg, ids)
	for col := 1; col < 3; col++ {
		defs := make([]int16, len(rows))
		var values []arrowparquet.ByteArray
		for i, row := range rows {
			if row[col] != nil {
				defs[i] = 1
				values = append(values, arrowparquet.ByteArray(row[col].(string)))
			}
		}
		writeTypedBytes(t, rg, values, defs)
	}
	if err := rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParquetBulkCursorsAlignMismatchedPages(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
			t.Run(fmt.Sprintf("dictionary=%v/version=%v", dictionary, version), func(t *testing.T) {
				want := architectureRows(2049, -1, 0)
				path := filepath.Join(t.TempDir(), "pages.parquet")
				writeArchitectureFixture(t, path, want, arrowparquet.WithDictionaryDefault(dictionary), arrowparquet.WithDataPageVersion(version))
				src := openBatchStreamFixture(t, path)
				opts := testReadOptions()
				opts.BatchRows = 129
				opts.BatchBytes = 32 << 10
				stream, err := src.NewBatchStream(context.Background(), opts)
				if err != nil {
					t.Fatal(err)
				}
				native := stream.(*batchStream)
				got := collectBatchStreamRows(t, stream, src.Schema(), opts)
				if !reflect.DeepEqual(got, want) {
					t.Fatal("mismatched pages changed row/null/string alignment")
				}
				if native.stringReadCalls >= int64(len(want))/2 {
					t.Fatalf("Arrow reads=%d rows=%d; bulk cursors degenerated into row-wise reads", native.stringReadCalls, len(want))
				}
				if native.stringReplays != 0 {
					t.Fatalf("ordinary rows unexpectedly replayed %d times", native.stringReplays)
				}
			})
		}
	}
}

func TestParquetBulkHeadersReservedBeforeAllocation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "headers.parquet")
	writeArchitectureFixture(t, path, architectureRows(128, -1, 0))
	src := openBatchStreamFixture(t, path)
	opts := testReadOptions()
	opts.BatchRows = 128
	opts.BatchBytes = 32 << 10
	manager := &rejectBatchStreamManager{}
	opts.MemoryManager = manager
	stream, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	native := stream.(*batchStream)
	manager.beforeReserve = func() {
		if native.headerBytes != 0 || native.headerCapacity != 0 || native.batch != nil {
			t.Fatal("allocated state visible before header reservation")
		}
		for _, c := range native.strings {
			if c.values != nil || c.defs != nil {
				t.Fatal("header backing array allocated before reservation")
			}
		}
	}
	if ok, err := stream.Next(context.Background(), memory.NewBatch()); ok || !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if manager.requests != 1 {
		t.Fatalf("requests=%d want=1", manager.requests)
	}
}

func TestParquetOutlierReplaysAndRestoresBulkCapacity(t *testing.T) {
	const budget = 8192
	for _, dictionary := range []bool{false, true} {
		for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
			for _, offsetIndex := range []bool{false, true} {
				t.Run(fmt.Sprintf("dictionary=%v/version=%v/index=%v", dictionary, version, offsetIndex), func(t *testing.T) {
					// Row zero fits normally; row 19 cannot coexist with bulk headers. Only
					// one fixed row fits beside it, forcing a nonzero Skip and restoration.
					want := architectureRows(512, 19, budget-40)
					path := filepath.Join(t.TempDir(), "outlier.parquet")
					writeArchitectureFixture(t, path, want, arrowparquet.WithDictionaryDefault(dictionary), arrowparquet.WithDataPageVersion(version), arrowparquet.WithPageIndexEnabled(offsetIndex))
					src := openBatchStreamFixture(t, path)
					opts := testReadOptions()
					opts.BatchRows = 128
					opts.BatchBytes = budget
					manager := newBatchStreamPeakManager(budget)
					opts.MemoryManager = manager
					stream, err := src.NewBatchStream(context.Background(), opts)
					if err != nil {
						t.Fatal(err)
					}
					defer stream.Close()
					native := stream.(*batchStream)
					batch := memory.NewBatch()
					var got [][]any
					seenOutlier, restored := false, false
					for {
						ok, err := stream.Next(context.Background(), batch)
						if err != nil {
							t.Fatal(err)
						}
						if !ok {
							break
						}
						got = append(got, snapshotBatchStreamRows(t, batch, src.Schema())...)
						if batch.Int64s["id"].Values[0] == 19 {
							seenOutlier = true
							if batch.Rows != 1 || native.headerCapacity != 1 || native.headerBytes != 0 {
								t.Fatalf("outlier rows=%d headers=%d bytes=%d", batch.Rows, native.headerCapacity, native.headerBytes)
							}
						} else if seenOutlier && !restored {
							if batch.Rows <= 1 || native.capacity <= 1 || native.headerCapacity <= 1 || native.headerBytes == 0 {
								t.Fatalf("next batch did not restore bulk capacity: rows=%d capacity=%d headers=%d bytes=%d", batch.Rows, native.capacity, native.headerCapacity, native.headerBytes)
							}
							restored = true
						}
						if native.managed != manager.Used() || native.managed > budget {
							t.Fatalf("managed=%d reserved=%d", native.managed, manager.Used())
						}
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatal("outlier replay lost, duplicated or misaligned rows")
					}
					if !seenOutlier || !restored || native.stringReplays != 1 {
						t.Fatalf("seen=%v restored=%v replays=%d", seenOutlier, restored, native.stringReplays)
					}
					if manager.Used() != 0 || manager.peak.Load() > budget {
						t.Fatalf("used=%d peak=%d", manager.Used(), manager.peak.Load())
					}
				})
			}
		}
	}
}

func TestParquetBulkHeaderAndOutputRelease(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancel), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "release.parquet")
			writeArchitectureFixture(t, path, architectureRows(256, -1, 0))
			src := openBatchStreamFixture(t, path)
			opts := testReadOptions()
			opts.BatchRows = 64
			opts.BatchBytes = 32 << 10
			manager := newBatchStreamPeakManager(opts.BatchBytes)
			opts.MemoryManager = manager
			stream, err := src.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			native := stream.(*batchStream)
			batch := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), batch); !ok || err != nil {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
			width := 2 * (int64(unsafe.Sizeof(arrowparquet.ByteArray{})) + 2)
			if native.headerCapacity <= 1 || native.headerBytes != int64(native.headerCapacity)*width || manager.Used() <= native.headerBytes {
				t.Fatalf("headers=%d bytes=%d used=%d", native.headerCapacity, native.headerBytes, manager.Used())
			}
			if cancel {
				ctx, stop := context.WithCancel(context.Background())
				stop()
				if ok, err := stream.Next(ctx, batch); ok || !errors.Is(err, context.Canceled) {
					t.Fatalf("ok=%v err=%v", ok, err)
				}
			} else if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 || native.managed != 0 || native.headerBytes != 0 || native.headerCapacity != 0 || native.batch != nil {
				t.Fatalf("released stream retained headers/output: used=%d managed=%d headers=%d", manager.Used(), native.managed, native.headerBytes)
			}
			for _, c := range native.strings {
				if c.values != nil || c.defs != nil || c.vector != nil {
					t.Fatal("released stream retained a borrowed page or output vector")
				}
			}
		})
	}
}

var _ source.BatchStream = (*batchStream)(nil)

func TestParquetPageCapsRejectDataAndDictionaryPages(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		for _, codec := range []compress.Compression{compress.Codecs.Uncompressed, compress.Codecs.Gzip} {
			for _, dimension := range []string{"compressed", "uncompressed"} {
				t.Run(fmt.Sprintf("dictionary=%v/codec=%v/%s", dictionary, codec, dimension), func(t *testing.T) {
					rows := architectureRows(8, -1, 0)
					// A deterministic non-repeating ASCII payload remains larger than the cap
					// under gzip, so both independent guards are exercised on actual pages.
					payload := make([]byte, 16384)
					state := uint32(1)
					for i := range payload {
						state ^= state << 13
						state ^= state >> 17
						state ^= state << 5
						payload[i] = byte(33 + state%90)
					}
					rows[0][1] = string(payload)
					path := filepath.Join(t.TempDir(), "page-cap.parquet")
					writeArchitectureFixture(t, path, rows, arrowparquet.WithDictionaryDefault(dictionary), arrowparquet.WithCompression(codec))
					src := openBatchStreamFixture(t, path)
					md, err := src.meta.RowGroup(0).ColumnChunk(1)
					if err != nil {
						t.Fatal(err)
					}
					if md.HasDictionaryPage() != dictionary {
						t.Fatalf("fixture dictionary=%v want=%v", md.HasDictionaryPage(), dictionary)
					}
					opts := testReadOptions()
					opts.BatchRows = 8
					opts.BatchBytes = 64 << 10
					opts.MaxCompressedPageBytes = 1 << 20
					opts.MaxUncompressedPageBytes = 1 << 20
					if dimension == "compressed" {
						opts.MaxCompressedPageBytes = 512
					} else {
						opts.MaxUncompressedPageBytes = 512
					}
					manager := newBatchStreamPeakManager(opts.BatchBytes)
					opts.MemoryManager = manager
					stream, err := src.NewBatchStream(context.Background(), opts)
					if err != nil {
						t.Fatal(err)
					}
					defer stream.Close()
					batch := memory.NewBatch()
					ok, firstErr := stream.Next(context.Background(), batch)
					if ok || firstErr == nil || !strings.Contains(firstErr.Error(), "parquet: "+dimension+" page size") || !strings.Contains(firstErr.Error(), "exceeds configured limit 512") {
						t.Fatalf("guard: ok=%v err=%v", ok, firstErr)
					}
					if batch.Rows != 0 || memory.BatchBytes(batch) != 0 || manager.Used() != 0 {
						t.Fatalf("guard exposed output or leaked: rows=%d bytes=%d reserved=%d", batch.Rows, memory.BatchBytes(batch), manager.Used())
					}
					if ok, err := stream.Next(context.Background(), batch); ok || err == nil || err.Error() != firstErr.Error() {
						t.Fatalf("guard was not terminal: ok=%v err=%v first=%v", ok, err, firstErr)
					}
					// The cap applies only to projected chunks, not an unrelated giant page.
					projected, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"id"}})
					if err != nil {
						t.Fatal(err)
					}
					projectedStream, err := projected.NewBatchStream(context.Background(), opts)
					if err != nil {
						t.Fatal(err)
					}
					got := collectBatchStreamRows(t, projectedStream, projected.Schema(), opts)
					if len(got) != len(rows) {
						t.Fatalf("projection rows=%d", len(got))
					}
					for i, row := range got {
						if len(row) != 1 || row[0] != int64(i) {
							t.Fatalf("projected row %d=%v", i, row)
						}
					}
					if manager.Used() != 0 {
						t.Fatalf("projection leaked %d bytes", manager.Used())
					}
				})
			}
		}
	}
}

func TestParquetNegativePageSizesFailCleanly(t *testing.T) {
	for _, dimension := range []string{"compressed", "uncompressed"} {
		t.Run(dimension, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "negative.parquet")
			writeArchitectureFixture(t, path, architectureRows(8, -1, 0))
			src := openBatchStreamFixture(t, path)
			md, err := src.meta.RowGroup(0).ColumnChunk(1)
			if err != nil {
				t.Fatal(err)
			}
			// Serialize only the required Thrift page-header fields. A negative size
			// must be rejected before the reader inspects a data-page subheader/body.
			buffer := thrift.NewTMemoryBufferLen(32)
			protocol := thrift.NewTCompactProtocol(buffer)
			ctx := context.Background()
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			must(protocol.WriteStructBegin(ctx, "PageHeader"))
			for _, field := range []struct {
				name  string
				id    int16
				value int32
			}{{"type", 1, 0}, {"uncompressed_page_size", 2, 0}, {"compressed_page_size", 3, 0}} {
				value := field.value
				if field.name == dimension+"_page_size" {
					value = -1
				}
				must(protocol.WriteFieldBegin(ctx, field.name, thrift.I32, field.id))
				must(protocol.WriteI32(ctx, value))
				must(protocol.WriteFieldEnd(ctx))
			}
			must(protocol.WriteFieldStop(ctx))
			must(protocol.WriteStructEnd(ctx))
			must(protocol.Flush(ctx))
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.WriteAt(buffer.Bytes(), md.DataPageOffset())
			must(err)
			must(f.Close())
			opts := testReadOptions()
			opts.BatchBytes = 64 << 10
			manager := newBatchStreamPeakManager(opts.BatchBytes)
			opts.MemoryManager = manager
			stream, err := src.NewBatchStream(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			batch := memory.NewBatch()
			if ok, err := stream.Next(ctx, batch); ok || err == nil || !strings.Contains(err.Error(), "negative page size") {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
			if manager.Used() != 0 || batch.Rows != 0 || memory.BatchBytes(batch) != 0 {
				t.Fatalf("malformed page retained output/reservations: used=%d rows=%d", manager.Used(), batch.Rows)
			}
			if ok, err := stream.Next(ctx, batch); ok || err == nil {
				t.Fatalf("malformed-page error not terminal: ok=%v err=%v", ok, err)
			}
		})
	}
}

type replayCancelContext struct {
	context.Context
	cancelWhen func() bool
}

func (c replayCancelContext) Err() error {
	if c.cancelWhen() {
		return context.Canceled
	}
	return nil
}

func TestParquetCancellationBetweenReplaySkipChunks(t *testing.T) {
	const outlier = 1043
	path := filepath.Join(t.TempDir(), "cancel-replay.parquet")
	want := architectureRows(1200, outlier, 8192-40)
	writeArchitectureFixture(t, path, want)
	src := openBatchStreamFixture(t, path)
	opts := testReadOptions()
	opts.BatchRows = 128
	opts.BatchBytes = 8192
	manager := newBatchStreamPeakManager(opts.BatchBytes)
	opts.MemoryManager = manager
	stream, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	native := stream.(*batchStream)
	checks := 0
	ctx := replayCancelContext{Context: context.Background(), cancelWhen: func() bool {
		if native.headerCapacity == 1 && native.groupRows-native.remaining == outlier {
			checks++
			// Replay's first check opens the string chunk, the second precedes
			// Skip(1024), and the third precedes the next Skip(19).
			return checks >= 3
		}
		return false
	}}
	var got [][]any
	batch := memory.NewBatch()
	for {
		ok, err := stream.Next(ctx, batch)
		if err != nil {
			if ok || !errors.Is(err, context.Canceled) {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
			break
		}
		if !ok {
			t.Fatal("outlier replay never reached cancellation")
		}
		got = append(got, snapshotBatchStreamRows(t, batch, src.Schema())...)
	}
	if checks != 3 || !reflect.DeepEqual(got, want[:outlier]) {
		t.Fatalf("replay cancellation checks=%d returned rows=%d want=%d", checks, len(got), outlier)
	}
	if manager.Used() != 0 || native.managed != 0 || native.headerBytes != 0 || batch.Rows != 0 {
		t.Fatalf("canceled replay leaked: used=%d managed=%d headers=%d rows=%d", manager.Used(), native.managed, native.headerBytes, batch.Rows)
	}
	if ok, err := stream.Next(context.Background(), batch); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("replay cancellation not terminal: ok=%v err=%v", ok, err)
	}
}

// Count backing-array capacities independently of the stream's sizing formulas.
// This catches a typed converter silently growing a vector outside its charge.
func actualManagedDecodeBytes(s *batchStream) int64 {
	var bytes int64
	if s.batch != nil {
		batch := reflect.ValueOf(s.batch).Elem()
		for i := 0; i < batch.NumField(); i++ {
			columns := batch.Field(i)
			if columns.Kind() != reflect.Map {
				continue
			}
			iter := columns.MapRange()
			for iter.Next() {
				vector := iter.Value()
				if vector.Kind() != reflect.Pointer || vector.IsNil() || vector.Elem().Kind() != reflect.Struct {
					continue
				}
				vector = vector.Elem()
				for j := 0; j < vector.NumField(); j++ {
					field := vector.Field(j)
					if field.Kind() == reflect.Slice {
						bytes += int64(field.Cap()) * int64(field.Type().Elem().Size())
					}
				}
			}
		}
	}
	bytes += int64(cap(s.decode.dense64))*8 + int64(cap(s.decode.dense32))*4
	bytes += int64(cap(s.decode.denseF32))*4 + int64(cap(s.decode.denseF64))*8
	bytes += int64(cap(s.decode.denseBool)) + int64(cap(s.decode.defs))*2
	if s.headerCapacity > 1 {
		for _, c := range s.strings {
			bytes += int64(cap(c.values))*int64(unsafe.Sizeof(arrowparquet.ByteArray{})) + int64(cap(c.defs))*2
		}
	}
	return bytes
}

func TestParquetReservationsEqualLiveBackingCapacities(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exact-capacity.parquet")
	writeBatchStreamFixture(t, path, []int{1, 131, 257}, 73)
	src := openBatchStreamFixture(t, path)
	for _, limit := range []int64{1024, 8192, 32 << 10} {
		for _, rows := range []int{1, 63, 128} {
			t.Run(fmt.Sprintf("bytes=%d/rows=%d", limit, rows), func(t *testing.T) {
				opts := testReadOptions()
				opts.BatchRows, opts.BatchBytes = rows, limit
				manager := newBatchStreamPeakManager(limit)
				opts.MemoryManager = manager
				stream, err := src.NewBatchStream(context.Background(), opts)
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				native := stream.(*batchStream)
				batch := memory.NewBatch()
				for {
					ok, err := stream.Next(context.Background(), batch)
					if err != nil {
						t.Fatal(err)
					}
					actual := actualManagedDecodeBytes(native)
					if actual != native.managed || actual != manager.Used() || actual > limit {
						t.Fatalf("actual capacities=%d stream=%d manager=%d limit=%d", actual, native.managed, manager.Used(), limit)
					}
					if !ok {
						break
					}
				}
			})
		}
	}
}

func TestParquetAlternatingOutlierColumnsPreserveAllRows(t *testing.T) {
	const limit = 4096
	want := architectureRows(513, -1, 0)
	for i := 0; i < len(want); i += 47 {
		column := 1 + (i/47)%2
		want[i][column] = strings.Repeat("x", limit-40)
		want[i][3-column] = ""
	}
	path := filepath.Join(t.TempDir(), "alternating-outliers.parquet")
	writeArchitectureFixture(t, path, want)
	src := openBatchStreamFixture(t, path)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = 128, limit
	manager := newBatchStreamPeakManager(limit)
	opts.MemoryManager = manager
	stream, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	got := collectBatchStreamRows(t, stream, src.Schema(), opts)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("alternating outliers lost, duplicated, or misaligned rows")
	}
	if manager.Used() != 0 || manager.peak.Load() > limit {
		t.Fatalf("used=%d peak=%d", manager.Used(), manager.peak.Load())
	}
}

func TestParquetReusesOwnedCapacityAcrossRowGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reuse-groups.parquet")
	writeBatchStreamFixture(t, path, []int{64, 0, 64, 32}, 8)
	src := openBatchStreamFixture(t, path)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = 64, 32<<10
	manager := newBatchStreamPeakManager(opts.BatchBytes)
	opts.MemoryManager = manager
	stream, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	native := stream.(*batchStream)
	batch := memory.NewBatch()
	var first []uintptr
	var got [][]any
	for _, rows := range []int{64, 64, 32} {
		if ok, err := stream.Next(context.Background(), batch); !ok || err != nil || batch.Rows != rows {
			t.Fatalf("rows=%d: ok=%v err=%v got=%d", rows, ok, err, batch.Rows)
		}
		got = append(got, snapshotBatchStreamRows(t, batch, src.Schema())...)
		pointers := []uintptr{
			reflect.ValueOf(batch.Int64s["id"].Values).Pointer(),
			reflect.ValueOf(batch.Int64s["n"].Validity).Pointer(),
			reflect.ValueOf(batch.Strings["s"].Offsets).Pointer(),
			reflect.ValueOf(native.decode.defs).Pointer(),
			reflect.ValueOf(native.decode.dense64).Pointer(),
			reflect.ValueOf(native.decode.dense32).Pointer(),
			reflect.ValueOf(native.decode.denseF32).Pointer(),
			reflect.ValueOf(native.decode.denseF64).Pointer(),
			reflect.ValueOf(native.decode.denseBool).Pointer(),
			reflect.ValueOf(native.strings[0].values).Pointer(),
		}
		if first == nil {
			first = pointers
		} else if !reflect.DeepEqual(pointers, first) {
			t.Fatal("equal/smaller row group replaced owned fixed/scratch/header buffers")
		}
		if actual := actualManagedDecodeBytes(native); actual != manager.Used() || actual > opts.BatchBytes {
			t.Fatalf("actual=%d reserved=%d limit=%d", actual, manager.Used(), opts.BatchBytes)
		}
	}
	if !reflect.DeepEqual(got, arrowBatchStreamRows(t, src)) {
		t.Fatal("row-group reuse retained stale page values or lost rows")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := stream.Next(ctx, batch); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: ok=%v err=%v", ok, err)
	}
	if manager.Used() != 0 || actualManagedDecodeBytes(native) != 0 || batch.Rows != 0 {
		t.Fatal("cancellation retained reused row-group buffers")
	}
}

func TestNullableScatterOverwritesEveryValidityWord(t *testing.T) {
	for _, rows := range []int{0, 1, 63, 64, 65, 127, 128, 129} {
		for _, mode := range []string{"all-null", "all-valid", "alternating"} {
			t.Run(fmt.Sprintf("rows=%d/%s", rows, mode), func(t *testing.T) {
				out := make([]int64, rows)
				defs := make([]int16, rows)
				var dense []int64
				want := make([]int64, rows)
				validity := make(memory.Bitmap, memory.WordsFor(rows))
				for i := range validity {
					validity[i] = ^uint64(0)
				}
				for i := range out {
					out[i] = -999
					valid := mode == "all-valid" || mode == "alternating" && i%2 == 1
					if valid {
						defs[i] = 1
						want[i] = int64(1000 + i)
						dense = append(dense, want[i])
					}
				}
				if err := scatterNullable(out, dense, defs, validity, len(dense), "value"); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(out, want) {
					t.Fatalf("values=%v want=%v", out, want)
				}
				for i := 0; i < len(validity)*64; i++ {
					valid := i < rows && (mode == "all-valid" || mode == "alternating" && i%2 == 1)
					if memory.IsSet(validity, i) != valid {
						t.Fatalf("validity bit %d=%v want=%v", i, memory.IsSet(validity, i), valid)
					}
				}
				// Reuse the same backing storage with all NULL definitions and no
				// physical values. No stale payload or bitmap tail may survive.
				clear(defs)
				if err := scatterNullable(out, dense, defs, validity, 0, "value"); err != nil {
					t.Fatal(err)
				}
				for _, value := range out {
					if value != 0 {
						t.Fatal("NULL reuse retained payload")
					}
				}
				for _, word := range validity {
					if word != 0 {
						t.Fatal("NULL reuse retained validity")
					}
				}
			})
		}
	}
}

func TestParquetNullableBoolWordBoundariesAndReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bool-validity.parquet")
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required,
		schema.FieldList{schema.NewBooleanNode("value", arrowparquet.Repetitions.Optional, -1)}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithDataPageSize(64), arrowparquet.WithBatchSize(8))))
	var want [][]any
	for group, rows := range []int{65, 64, 63} {
		rg := writer.AppendRowGroup()
		column, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		defs := make([]int16, rows)
		var values []bool
		for i := 0; i < rows; i++ {
			row := []any{nil}
			if group == 0 || group == 1 && i%2 != 0 {
				defs[i] = 1
				value := i%3 == 0
				values = append(values, value)
				row[0] = uint32(0)
				if value {
					row[0] = uint32(1)
				}
			}
			want = append(want, row)
		}
		if _, err := column.(*file.BooleanColumnChunkWriter).WriteBatch(values, defs, nil); err != nil {
			t.Fatal(err)
		}
		if err := column.Close(); err != nil {
			t.Fatal(err)
		}
		if err := rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	src := openBatchStreamFixture(t, path)
	for _, rows := range []int{63, 64, 65} {
		opts := testReadOptions()
		opts.BatchRows, opts.BatchBytes = rows, 4096
		stream, err := src.NewBatchStream(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		if got := collectBatchStreamRows(t, stream, src.Schema(), opts); !reflect.DeepEqual(got, want) {
			t.Fatalf("rows=%d: NULL/boolean validity changed", rows)
		}
	}
}

func TestParquetPayloadLimitsDoNotChargeDecodeScratch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payload-numeric.parquet")
	writeBatchStreamFixture(t, path, []int{65}, 8)
	src := openBatchStreamFixture(t, path)
	projected, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"n"}})
	if err != nil {
		t.Fatal(err)
	}
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = 64, 4096
	opts.BatchPayloadBytes, opts.BatchPayloadTargetBytes = 16, 8
	manager := newBatchStreamPeakManager(opts.BatchBytes)
	opts.MemoryManager = manager
	stream, err := projected.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	batch := memory.NewBatch()
	var rows int
	for {
		ok, err := stream.Next(context.Background(), batch)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if batch.Rows != 1 || memory.BatchBytes(batch) != 16 {
			t.Fatalf("rows=%d payload=%d", batch.Rows, memory.BatchBytes(batch))
		}
		if manager.Used() <= opts.BatchPayloadBytes || manager.Used() > opts.BatchBytes {
			t.Fatalf("scratch was folded into payload cap: owned=%d", manager.Used())
		}
		rows += batch.Rows
	}
	if rows != 65 || manager.Used() != 0 {
		t.Fatalf("rows=%d used=%d", rows, manager.Used())
	}
}

func TestParquetPayloadSoftTargetAllowsSingleLongRow(t *testing.T) {
	const hard, target = 512, 128
	want := architectureRows(131, -1, 0)
	want[19][1], want[19][2] = strings.Repeat("x", 400), ""
	path := filepath.Join(t.TempDir(), "payload-strings.parquet")
	writeArchitectureFixture(t, path, want)
	src := openBatchStreamFixture(t, path)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = 65, 32<<10
	opts.BatchPayloadBytes, opts.BatchPayloadTargetBytes = hard, target
	stream, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	batch := memory.NewBatch()
	var got [][]any
	for {
		ok, err := stream.Next(context.Background(), batch)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		payload := memory.BatchBytes(batch)
		if payload > hard || batch.Rows > 1 && payload > target {
			t.Fatalf("rows=%d payload=%d hard=%d target=%d", batch.Rows, payload, hard, target)
		}
		if batch.Int64s["id"].Values[0] == 19 && (batch.Rows != 1 || payload <= target) {
			t.Fatal("long row was not admitted alone above target")
		}
		got = append(got, snapshotBatchStreamRows(t, batch, src.Schema())...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("payload splitting lost/null-misaligned/reordered rows")
	}
	// The same row exceeds a genuinely hard cap, so fail without output/leaks.
	opts.BatchPayloadBytes, opts.BatchPayloadTargetBytes = 128, 64
	manager := newBatchStreamPeakManager(opts.BatchBytes)
	opts.MemoryManager = manager
	tooSmall, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer tooSmall.Close()
	for {
		ok, err := tooSmall.Next(context.Background(), batch)
		if err != nil {
			if ok || !errors.Is(err, memory.ErrBudgetExceeded) || batch.Rows != 0 || manager.Used() != 0 {
				t.Fatalf("hard cap: ok=%v err=%v rows=%d used=%d", ok, err, batch.Rows, manager.Used())
			}
			break
		}
		if !ok {
			t.Fatal("hard cap silently accepted oversized row")
		}
	}
}
