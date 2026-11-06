package parquet

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
	"gocolumn/source"
)

func TestParquetBatchStreamMatchesLegacyAcrossRowGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batches.parquet")
	writeBatchStreamFixture(t, path, []int{0, 1, 0, 65, 131, 0}, 257)
	src := openBatchStreamFixture(t, path)
	if _, ok := any(src).(source.BatchStreamProvider); !ok {
		t.Fatal("Parquet must use its native batch stream, not the row-group adapter")
	}
	if _, ok := any(src).(source.PreparedBatchStreamProvider); !ok {
		t.Fatal("Parquet must honor the prepared partition list")
	}
	want := legacyBatchStreamRows(t, src)
	for _, rows := range []int{1, 2, 63, 64, 65, 128, 1024} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			opts := testReadOptions()
			opts.BatchRows, opts.BatchBytes = rows, 32<<10
			manager := newBatchStreamPeakManager(1 << 20)
			opts.MemoryManager = manager
			for execution := 0; execution < 2; execution++ {
				stream, err := source.OpenBatchStream(context.Background(), src, opts)
				if err != nil {
					t.Fatal(err)
				}
				got := collectBatchStreamRows(t, stream, src.Schema(), opts)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("execution %d differs from legacy (%d vs %d rows)", execution, len(got), len(want))
				}
				if manager.Used() != 0 {
					t.Fatalf("execution %d leaked %d managed bytes", execution, manager.Used())
				}
			}
			if manager.peak.Load() == 0 {
				t.Fatal("native output and decode workspace must be accounted")
			}
		})
	}
}

func TestParquetBatchStreamPageEncodingVariants(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
			t.Run(fmt.Sprintf("dictionary=%v/version=%v", dictionary, version), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "pages.parquet")
				writeBatchStreamFixture(t, path, []int{333, 1025}, 67, arrowparquet.WithDictionaryDefault(dictionary), arrowparquet.WithDataPageVersion(version))
				src := openBatchStreamFixture(t, path)
				opts := testReadOptions()
				opts.BatchRows, opts.BatchBytes = 37, 4096
				stream, err := source.OpenBatchStream(context.Background(), src, opts)
				if err != nil {
					t.Fatal(err)
				}
				got, want := collectBatchStreamRows(t, stream, src.Schema(), opts), legacyBatchStreamRows(t, src)
				if !reflect.DeepEqual(got, want) {
					t.Fatal("page or dictionary transition changed rows")
				}
			})
		}
	}
}

func TestParquetBatchStreamReservationPrecedesAllocation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reject-reservation.parquet")
	writeBatchStreamFixture(t, path, []int{65}, 128)
	src := openBatchStreamFixture(t, path)
	manager := &rejectBatchStreamManager{}
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 64, 32<<10, manager
	stream, err := source.OpenBatchStream(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	native, ok := stream.(*batchStream)
	if !ok {
		t.Fatalf("expected native stream, got %T", stream)
	}
	manager.beforeReserve = func() {
		if native.batch != nil || len(native.decode.dense64) != 0 || len(native.decode.dense32) != 0 || len(native.decode.defs) != 0 {
			t.Fatal("decoded buffers allocated before their reservation was accepted")
		}
	}
	batch := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), batch); ok || !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("rejected reservation: ok=%v err=%v", ok, err)
	}
	if manager.requests != 1 || batch.Rows != 0 || memory.BatchBytes(batch) != 0 {
		t.Fatalf("requests=%d rows=%d bytes=%d", manager.requests, batch.Rows, memory.BatchBytes(batch))
	}
}

type rejectBatchStreamManager struct {
	requests      int
	beforeReserve func()
}

func (m *rejectBatchStreamManager) Reserve(context.Context, int64) (memory.Reservation, error) {
	m.requests++
	if m.beforeReserve != nil {
		m.beforeReserve()
	}
	return nil, memory.ErrBudgetExceeded
}
func (*rejectBatchStreamManager) Used() int64  { return 0 }
func (*rejectBatchStreamManager) Limit() int64 { return 1 << 20 }

func TestParquetBatchStreamPreservesTypedValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "typed.parquet")
	writeTypedFixture(t, path, time.Date(2024, time.February, 29, 23, 58, 59, 123456789, time.UTC))
	src := openBatchStreamFixture(t, path)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = 1, 1024
	stream, err := source.OpenBatchStream(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := collectBatchStreamRows(t, stream, src.Schema(), opts), legacyBatchStreamRows(t, src); !reflect.DeepEqual(got, want) {
		t.Fatalf("date/timestamp/UTF-8 values differ: got %v want %v", got, want)
	}
}

func TestParquetBatchStreamByteBoundIncludesEveryColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strings.parquet")
	writeBatchStreamFixture(t, path, []int{67, 79}, 300)
	src := openBatchStreamFixture(t, path)
	want := legacyBatchStreamRows(t, src)
	for _, bytes := range []int64{1024, 2048, 4096} {
		t.Run(fmt.Sprintf("bytes=%d", bytes), func(t *testing.T) {
			opts := testReadOptions()
			opts.BatchRows, opts.BatchBytes = 4096, bytes
			stream, err := source.OpenBatchStream(context.Background(), src, opts)
			if err != nil {
				t.Fatal(err)
			}
			if got := collectBatchStreamRows(t, stream, src.Schema(), opts); !reflect.DeepEqual(got, want) {
				t.Fatal("byte-bound splitting lost, duplicated, or reordered rows")
			}
		})
	}
}

func TestParquetBatchStreamEmptyAndPreparedPartitions(t *testing.T) {
	for _, counts := range [][]int{nil, {0}, {0, 0}} {
		t.Run(fmt.Sprint(counts), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "empty.parquet")
			writeBatchStreamFixture(t, path, counts, 8)
			src := openBatchStreamFixture(t, path)
			manager := newBatchStreamPeakManager(1 << 20)
			opts := testReadOptions()
			opts.MemoryManager = manager
			stream, err := source.OpenBatchStream(context.Background(), src, opts)
			if err != nil {
				t.Fatal(err)
			}
			if got := collectBatchStreamRows(t, stream, src.Schema(), opts); len(got) != 0 {
				t.Fatalf("empty file yielded %d rows", len(got))
			}
			if manager.Used() != 0 {
				t.Fatalf("empty file leaked %d managed bytes", manager.Used())
			}
		})
	}

	path := filepath.Join(t.TempDir(), "prepared.parquet")
	writeBatchStreamFixture(t, path, []int{3, 5, 7}, 16)
	src := openBatchStreamFixture(t, path)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = 2, 1024
	parts, err := src.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, selected := range [][]source.Partition{nil, {parts[2], parts[0]}} {
		stream, err := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, selected)
		if err != nil {
			t.Fatal(err)
		}
		got := collectBatchStreamRows(t, stream, src.Schema(), opts)
		if len(selected) == 0 {
			if len(got) != 0 {
				t.Fatal("an empty prepared partition list must not reopen every row group")
			}
		} else {
			all := legacyBatchStreamRows(t, src)
			want := append(append([][]any(nil), all[8:15]...), all[:3]...)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("prepared stream ignored partition selection/order: got %v want %v", got, want)
			}
		}
	}
}

func TestParquetBatchStreamProjectionSkipsOversizedColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projection.parquet")
	writeBatchStreamFixture(t, path, []int{17, 19}, 4096)
	src := openBatchStreamFixture(t, path)
	projected, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = 7, 56
	stream, err := source.OpenBatchStream(context.Background(), projected, opts)
	if err != nil {
		t.Fatal(err)
	}
	got := collectBatchStreamRows(t, stream, projected.Schema(), opts)
	if len(got) != 36 {
		t.Fatalf("projected rows=%d want 36", len(got))
	}
	for i, row := range got {
		if len(row) != 1 || row[0] != int64(i) {
			t.Fatalf("projected row %d=%v", i, row)
		}
	}
}

func TestParquetBatchStreamRejectsOversizedRowsAndTinyBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized.parquet")
	writeBatchStreamFixture(t, path, []int{9}, 4096)
	src := openBatchStreamFixture(t, path)
	for _, tc := range []struct {
		name   string
		bytes  int64
		limit  int64
		budget bool
	}{
		{"oversized string", 1024, 1 << 20, false},
		{"fixed columns cannot fit", 1, 1 << 20, false},
		{"reservation rejected", 64 << 10, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newBatchStreamPeakManager(tc.limit)
			opts := testReadOptions()
			opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 8, tc.bytes, manager
			stream, err := source.OpenBatchStream(context.Background(), src, opts)
			if err == nil {
				defer stream.Close()
				batch := memory.NewBatch()
				for attempts := 0; attempts < 10; attempts++ {
					var ok bool
					ok, err = stream.Next(context.Background(), batch)
					if err != nil {
						if ok || batch.Rows != 0 || memory.BatchBytes(batch) != 0 {
							t.Fatal("failed decode exposed partial output")
						}
						break
					}
					if !ok {
						t.Fatal("oversized input ended silently instead of failing")
					}
					if batch.Rows == 0 || memory.BatchBytes(batch) > tc.bytes {
						t.Fatalf("invalid output rows=%d bytes=%d", batch.Rows, memory.BatchBytes(batch))
					}
				}
				if err == nil {
					t.Fatal("oversized row caused repeated output without an error")
				}
				if _, again := stream.Next(context.Background(), memory.NewBatch()); again == nil {
					t.Fatal("terminal decode error was not sticky")
				}
			}
			if !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("budget failure=%v want ErrBudgetExceeded", err)
			}
			if manager.Used() != 0 {
				t.Fatalf("error leaked %d managed bytes", manager.Used())
			}
			if tc.budget && manager.peak.Load() != 0 {
				t.Fatalf("tiny budget must reject reservation before managed allocation: requests=%d peak=%d", manager.requests.Load(), manager.peak.Load())
			}
		})
	}
}

func TestParquetBatchStreamRestoresCapacityAfterSmallRowGroup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "small-first-group.parquet")
	writeBatchStreamFixture(t, path, []int{1, 65}, 8)
	src := openBatchStreamFixture(t, path)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = 32, 32<<10
	stream, err := source.OpenBatchStream(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	batch := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), batch); !ok || err != nil || batch.Rows != 1 {
		t.Fatalf("first group: ok=%v err=%v rows=%d", ok, err, batch.Rows)
	}
	if ok, err := stream.Next(context.Background(), batch); !ok || err != nil || batch.Rows != 32 {
		t.Fatalf("larger group kept a one-row allocation: ok=%v err=%v rows=%d", ok, err, batch.Rows)
	}
}

func TestParquetBatchStreamExtremeRowLimitRespectsByteBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "extreme-rows.parquet")
	writeBatchStreamFixture(t, path, []int{129}, 16)
	src := openBatchStreamFixture(t, path)
	manager := newBatchStreamPeakManager(1024)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes, opts.MemoryManager = math.MaxInt, 1024, manager
	stream, err := source.OpenBatchStream(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := collectBatchStreamRows(t, stream, src.Schema(), opts), legacyBatchStreamRows(t, src); !reflect.DeepEqual(got, want) {
		t.Fatal("extreme row limit overflowed or changed rows")
	}
	if manager.peak.Load() > 1024 || manager.Used() != 0 {
		t.Fatalf("peak=%d remaining=%d", manager.peak.Load(), manager.Used())
	}
}

func TestParquetBatchStreamNilNextContextReleasesReservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nil-context.parquet")
	writeBatchStreamFixture(t, path, []int{65}, 16)
	src := openBatchStreamFixture(t, path)
	manager := newBatchStreamPeakManager(4096)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 7, 4096, manager
	stream, err := source.OpenBatchStream(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	batch := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), batch); !ok || err != nil {
		t.Fatalf("first: ok=%v err=%v", ok, err)
	}
	if manager.Used() == 0 {
		t.Fatal("live batch was not reserved")
	}
	if ok, err := stream.Next(nil, batch); ok || err == nil {
		t.Fatalf("nil context: ok=%v err=%v", ok, err)
	}
	if manager.Used() != 0 || batch.Rows != 0 || memory.BatchBytes(batch) != 0 {
		t.Fatalf("nil context retained output/reservations: rows=%d bytes=%d reserved=%d", batch.Rows, memory.BatchBytes(batch), manager.Used())
	}
	if ok, err := stream.Next(context.Background(), batch); ok || err == nil {
		t.Fatalf("terminal context failure restarted: ok=%v err=%v", ok, err)
	}
}

func TestParquetBatchStreamShrinksFixedBuffersForLargeAdmissibleRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admissible-large-row.parquet")
	writeBatchStreamFixture(t, path, []int{256}, 6000)
	src := openBatchStreamFixture(t, path)
	manager := newBatchStreamPeakManager(16 << 10)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 8192, 16<<10, manager
	stream, err := source.OpenBatchStream(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := collectBatchStreamRows(t, stream, src.Schema(), opts), legacyBatchStreamRows(t, src); !reflect.DeepEqual(got, want) {
		t.Fatal("individually admissible rows failed after fixed-buffer shrinking")
	}
	if manager.peak.Load() > opts.BatchBytes || manager.Used() != 0 {
		t.Fatalf("peak=%d remaining=%d", manager.peak.Load(), manager.Used())
	}
}

func TestParquetBatchStreamCancelAndCloseReleaseReservations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cancel.parquet")
	writeBatchStreamFixture(t, path, []int{129, 257}, 128)
	src := openBatchStreamFixture(t, path)
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancel), func(t *testing.T) {
			manager := newBatchStreamPeakManager(1 << 20)
			opts := testReadOptions()
			opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 7, 4096, manager
			stream, err := source.OpenBatchStream(context.Background(), src, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			batch := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), batch); !ok || err != nil {
				t.Fatalf("first batch: ok=%v err=%v", ok, err)
			}
			if manager.Used() == 0 {
				t.Fatal("live borrowed output is not reserved")
			}
			if cancel {
				ctx, stop := context.WithCancel(context.Background())
				stop()
				if ok, err := stream.Next(ctx, batch); ok || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel: ok=%v err=%v", ok, err)
				}
				if batch.Rows != 0 || memory.BatchBytes(batch) != 0 || manager.Used() != 0 {
					t.Fatalf("cancel retained output/reservations: rows=%d bytes=%d reserved=%d", batch.Rows, memory.BatchBytes(batch), manager.Used())
				}
				if _, err := stream.Next(context.Background(), batch); !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled stream restarted: %v", err)
				}
			}
			for i := 0; i < 2; i++ {
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if manager.Used() != 0 {
				t.Fatalf("close leaked %d managed bytes", manager.Used())
			}
			if ok, err := stream.Next(context.Background(), batch); ok || !errors.Is(err, source.ErrBatchStreamClosed) {
				t.Fatalf("next after close: ok=%v err=%v", ok, err)
			}
			if batch.Rows != 0 || memory.BatchBytes(batch) != 0 {
				t.Fatal("next after close retained stale output")
			}
		})
	}
}

// This deliberately measures GoLume reservations, not process RSS or Arrow's
// compressed pages/dictionaries. A fixed output/workspace budget must not grow
// with the row-group size, even though Arrow may retain a larger page.
func TestParquetBatchStreamManagedPeakIndependentOfRowGroupSize(t *testing.T) {
	const batchRows, batchBytes, budget = 64, 32 << 10, 256 << 10
	var firstPeak int64
	for _, rows := range []int{1024, 16384, 65536} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "scale.parquet")
			writeBatchStreamFixture(t, path, []int{rows}, 32)
			src := openBatchStreamFixture(t, path)
			manager := newBatchStreamPeakManager(budget)
			opts := testReadOptions()
			opts.BatchRows, opts.BatchBytes, opts.MemoryManager = batchRows, batchBytes, manager
			stream, err := source.OpenBatchStream(context.Background(), src, opts)
			if err != nil {
				t.Fatal(err)
			}
			stats, err := source.DrainBatchStream(context.Background(), stream)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Rows != int64(rows) || stats.Batches < int64(rows/batchRows) {
				t.Fatalf("drain=%+v want rows=%d", stats, rows)
			}
			peak := manager.peak.Load()
			if peak == 0 || peak > batchBytes || manager.Used() != 0 {
				t.Fatalf("managed peak=%d batch cap=%d remaining=%d", peak, batchBytes, manager.Used())
			}
			if firstPeak == 0 {
				firstPeak = peak
			} else if peak > firstPeak {
				t.Fatalf("managed peak grew with row group: %d > %d", peak, firstPeak)
			}
			t.Logf("rows=%d batches=%d peak_managed_bytes=%d (excludes Arrow page/dictionary memory and process RSS)", rows, stats.Batches, peak)
		})
	}
}

func TestParquetBatchStreamManagedPeakIndependentOfRowGroupCount(t *testing.T) {
	var firstPeak int64
	for _, groups := range []int{1, 8, 64} {
		t.Run(fmt.Sprintf("groups=%d", groups), func(t *testing.T) {
			counts := make([]int, groups)
			for i := range counts {
				counts[i] = 128
			}
			path := filepath.Join(t.TempDir(), "groups.parquet")
			writeBatchStreamFixture(t, path, counts, 32)
			src := openBatchStreamFixture(t, path)
			manager := newBatchStreamPeakManager(32 << 10)
			opts := testReadOptions()
			opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 64, 32<<10, manager
			stream, err := source.OpenBatchStream(context.Background(), src, opts)
			if err != nil {
				t.Fatal(err)
			}
			stats, err := source.DrainBatchStream(context.Background(), stream)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Rows != int64(groups*128) || stats.Batches < int64(groups*2) {
				t.Fatalf("groups=%d drain=%+v", groups, stats)
			}
			peak := manager.peak.Load()
			if peak == 0 || peak > opts.BatchBytes || manager.Used() != 0 {
				t.Fatalf("managed peak=%d cap=%d remaining=%d", peak, opts.BatchBytes, manager.Used())
			}
			if firstPeak == 0 {
				firstPeak = peak
			} else if peak > firstPeak {
				t.Fatalf("managed peak grew with row-group count: %d > %d", peak, firstPeak)
			}
			t.Logf("row_groups=%d peak_managed_bytes=%d (excludes Arrow and RSS)", groups, peak)
		})
	}
}

func TestParquetBatchStreamAdaptsToSmallerQueryBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "small-budget.parquet")
	writeBatchStreamFixture(t, path, []int{4096}, 8)
	src := openBatchStreamFixture(t, path)
	projected, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	manager := newBatchStreamPeakManager(1024)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 1024, 64<<10, manager
	stream, err := source.OpenBatchStream(context.Background(), projected, opts)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := source.DrainBatchStream(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 4096 || stats.Batches <= 4 {
		t.Fatalf("scan did not adapt batch size: %+v", stats)
	}
	if manager.peak.Load() == 0 || manager.peak.Load() > 1024 || manager.Used() != 0 {
		t.Fatalf("peak=%d remaining=%d", manager.peak.Load(), manager.Used())
	}
}

func TestParquetBatchStreamDoesNotRetainSuccessfulNextContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contexts.parquet")
	writeBatchStreamFixture(t, path, []int{129, 257}, 128)
	src := openBatchStreamFixture(t, path)
	manager := newBatchStreamPeakManager(32 << 10)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 7, 4096, manager
	stream, err := source.OpenBatchStream(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	batch := memory.NewBatch()
	var rows int
	for calls := 0; ; calls++ {
		if calls > 400 {
			t.Fatal("stream made no finite progress")
		}
		ctx, cancel := context.WithCancel(context.Background())
		ok, err := stream.Next(ctx, batch)
		cancel() // lazy.BatchReader cancels its derived context after every Next.
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		rows += batch.Rows
		batch.ClearColumns()
	}
	if rows != 386 || manager.Used() != 0 {
		t.Fatalf("rows=%d remaining=%d", rows, manager.Used())
	}
}

func TestParquetBatchStreamNativeNarrowIntegerTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "narrow-stream.parquet")
	var fields schema.FieldList
	values := [][]int32{{-128, 127, -1, 0, 1}, {-32768, 32767, -1, 0, 1}, {0, 255, 1, 0, 42}, {0, 65535, 1, 0, 42}}
	for _, spec := range []struct {
		name   string
		bits   int8
		signed bool
	}{{"i8", 8, true}, {"i16", 16, true}, {"u8", 8, false}, {"u16", 16, false}} {
		node, err := schema.NewPrimitiveNodeLogical(spec.name, arrowparquet.Repetitions.Optional, schema.NewIntLogicalType(spec.bits, spec.signed), arrowparquet.Types.Int32, -1, -1)
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
	writer := file.NewParquetWriter(f, root)
	rg := writer.AppendRowGroup()
	for _, vals := range values {
		col, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.(*file.Int32ColumnChunkWriter).WriteBatch(vals, []int16{1, 0, 1, 1, 0, 1, 1}, nil); err != nil {
			t.Fatal(err)
		}
		if err := col.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	src := openBatchStreamFixture(t, path)
	opts := testReadOptions()
	opts.BatchRows, opts.BatchBytes = 2, 1024
	stream, err := source.OpenBatchStream(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	got, want := collectBatchStreamRows(t, stream, src.Schema(), opts), legacyBatchStreamRows(t, src)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("narrow integers differ: got %v want %v", got, want)
	}

	// The native path pre-reserves these buffers once. Replacing them in the
	// typed decoder would silently allocate outside that reservation.
	reused, err := source.OpenBatchStream(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reused.Close()
	batch := memory.NewBatch()
	if ok, err := reused.Next(context.Background(), batch); !ok || err != nil {
		t.Fatalf("first narrow batch: ok=%v err=%v", ok, err)
	}
	i8, i16, u8, u16 := &batch.Int8s["i8"].Values[0], &batch.Int16s["i16"].Values[0], &batch.Uint8s["u8"].Values[0], &batch.Uint16s["u16"].Values[0]
	validity := &batch.Int8s["i8"].Validity[0]
	if ok, err := reused.Next(context.Background(), batch); !ok || err != nil {
		t.Fatalf("second narrow batch: ok=%v err=%v", ok, err)
	}
	if i8 != &batch.Int8s["i8"].Values[0] || i16 != &batch.Int16s["i16"].Values[0] || u8 != &batch.Uint8s["u8"].Values[0] || u16 != &batch.Uint16s["u16"].Values[0] || validity != &batch.Int8s["i8"].Validity[0] {
		t.Fatal("native narrow decoder replaced pre-reserved buffers between batches")
	}
}

func TestParquetBatchStreamRejectsInvalidLogicalValues(t *testing.T) {
	for _, kind := range []string{"utf8", "timestamp", "narrow"} {
		t.Run(kind, func(t *testing.T) {
			var logical schema.LogicalType
			var physical arrowparquet.Type
			switch kind {
			case "utf8":
				logical, physical = schema.StringLogicalType{}, arrowparquet.Types.ByteArray
			case "timestamp":
				logical, physical = schema.NewTimestampLogicalTypeWithOpts(schema.WithTSIsAdjustedToUTC(), schema.WithTSTimeUnitType(schema.TimeUnitMillis)), arrowparquet.Types.Int64
			case "narrow":
				logical, physical = schema.NewIntLogicalType(8, true), arrowparquet.Types.Int32
			}
			node, err := schema.NewPrimitiveNodeLogical("value", arrowparquet.Repetitions.Required, logical, physical, -1, -1)
			if err != nil {
				t.Fatal(err)
			}
			root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{node}, -1)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "invalid.parquet")
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			writer := file.NewParquetWriter(f, root)
			rg := writer.AppendRowGroup()
			col, err := rg.NextColumn()
			if err != nil {
				t.Fatal(err)
			}
			switch c := col.(type) {
			case *file.ByteArrayColumnChunkWriter:
				_, err = c.WriteBatch([]arrowparquet.ByteArray{[]byte("valid"), {0xff}}, nil, nil)
			case *file.Int64ColumnChunkWriter:
				_, err = c.WriteBatch([]int64{0, int64(^uint64(0) >> 1)}, nil, nil)
			case *file.Int32ColumnChunkWriter:
				_, err = c.WriteBatch([]int32{0, 128}, nil, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := col.Close(); err != nil {
				t.Fatal(err)
			}
			if err := rg.Close(); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			src := openBatchStreamFixture(t, path)
			manager := newBatchStreamPeakManager(4096)
			opts := testReadOptions()
			opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 1, 4096, manager
			stream, err := source.OpenBatchStream(context.Background(), src, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			batch := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), batch); !ok || err != nil {
				t.Fatalf("first valid row: ok=%v err=%v", ok, err)
			}
			if ok, err := stream.Next(context.Background(), batch); ok || err == nil {
				t.Fatalf("invalid row: ok=%v err=%v", ok, err)
			}
			if batch.Rows != 0 || memory.BatchBytes(batch) != 0 || manager.Used() != 0 {
				t.Fatalf("failed conversion retained data: rows=%d bytes=%d reserved=%d", batch.Rows, memory.BatchBytes(batch), manager.Used())
			}
		})
	}
}

func openBatchStreamFixture(t testing.TB, path string) *Source {
	t.Helper()
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func collectBatchStreamRows(t *testing.T, stream source.BatchStream, schema memory.Schema, opts source.ReadOptions) [][]any {
	t.Helper()
	defer func() {
		if err := stream.Close(); err != nil {
			t.Error(err)
		}
	}()
	var rows [][]any
	batch := memory.NewBatch()
	for calls := 0; ; calls++ {
		if calls > 100000 {
			t.Fatal("stream made no finite progress")
		}
		ok, err := stream.Next(context.Background(), batch)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			if batch.Rows != 0 || memory.BatchBytes(batch) != 0 {
				t.Fatal("EOF retained stale output")
			}
			for i := 0; i < 2; i++ {
				if again, err := stream.Next(context.Background(), batch); again || err != nil {
					t.Fatalf("repeated EOF: ok=%v err=%v", again, err)
				}
			}
			return rows
		}
		if batch.Rows <= 0 {
			t.Fatal("native stream returned a zero-row batch instead of advancing")
		}
		if opts.BatchRows > 0 && batch.Rows > opts.BatchRows {
			t.Fatalf("batch rows=%d limit=%d", batch.Rows, opts.BatchRows)
		}
		if opts.BatchBytes > 0 && memory.BatchBytes(batch) > opts.BatchBytes {
			t.Fatalf("batch bytes=%d limit=%d", memory.BatchBytes(batch), opts.BatchBytes)
		}
		rows = append(rows, snapshotBatchStreamRows(t, batch, schema)...)
		batch.ClearColumns()
	}
}

func legacyBatchStreamRows(t *testing.T, src *Source) [][]any {
	t.Helper()
	reader, err := src.NewReader(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	parts, err := src.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var rows [][]any
	batch := memory.NewBatch()
	for _, part := range parts {
		if err := reader.ReadPartition(context.Background(), part, batch); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, snapshotBatchStreamRows(t, batch, src.Schema())...)
	}
	return rows
}

func snapshotBatchStreamRows(t *testing.T, batch *memory.Batch, schema memory.Schema) [][]any {
	t.Helper()
	rows := make([][]any, batch.Rows)
	for i := range rows {
		rows[i] = make([]any, len(schema.Fields))
		for j, field := range schema.Fields {
			if field.Type == memory.TypeString {
				v, err := batch.String(field.Name)
				if err != nil {
					t.Fatal(err)
				}
				if v.IsValid(i) {
					rows[i][j] = string(v.ValueBytes(i))
				}
				continue
			}
			var v any
			switch field.Type {
			case memory.TypeInt8:
				v = batch.Int8s[field.Name]
			case memory.TypeInt16:
				v = batch.Int16s[field.Name]
			case memory.TypeUint8:
				v = batch.Uint8s[field.Name]
			case memory.TypeUint16:
				v = batch.Uint16s[field.Name]
			case memory.TypeInt32, memory.TypeDate32:
				v = batch.Int32s[field.Name]
			case memory.TypeInt64, memory.TypeTimestamp:
				v = batch.Int64s[field.Name]
			case memory.TypeFloat32:
				v = batch.Float32s[field.Name]
			case memory.TypeFloat64:
				v = batch.Float64s[field.Name]
			case memory.TypeBool:
				v = batch.Uint32s[field.Name]
			default:
				t.Fatalf("unsupported fixture type %v", field.Type)
			}
			rv := reflect.ValueOf(v)
			if rv.IsNil() {
				t.Fatalf("missing column %q", field.Name)
			}
			if v.(interface{ IsValid(int) bool }).IsValid(i) {
				rows[i][j] = rv.Elem().FieldByName("Values").Index(i).Interface()
			}
		}
	}
	return rows
}

// batchStreamPeakManager records the actual concurrent managed reservations,
// rather than summing allocations that have already been released.
type batchStreamPeakManager struct {
	*memory.BudgetManager
	peak     atomic.Int64
	requests atomic.Int64
}

func newBatchStreamPeakManager(limit int64) *batchStreamPeakManager {
	return &batchStreamPeakManager{BudgetManager: memory.NewBudgetManager(limit)}
}

func (m *batchStreamPeakManager) Reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	m.requests.Add(1)
	r, err := m.BudgetManager.Reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	used := m.Used()
	for peak := m.peak.Load(); used > peak; peak = m.peak.Load() {
		if m.peak.CompareAndSwap(peak, used) {
			break
		}
	}
	return r, nil
}

// writeBatchStreamFixture uses deliberately small pages and distinct optional
// patterns so batches cross page, bitmap-word, and row-group boundaries.
func writeBatchStreamFixture(t testing.TB, path string, groups []int, textBytes int, options ...arrowparquet.WriterProperty) {
	t.Helper()
	text := func(name string, rep arrowparquet.Repetition) *schema.PrimitiveNode {
		n, err := schema.NewPrimitiveNodeLogical(name, rep, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{
		schema.NewInt64Node("id", arrowparquet.Repetitions.Required, -1),
		schema.NewInt64Node("n", arrowparquet.Repetitions.Optional, -1),
		schema.NewInt32Node("i32", arrowparquet.Repetitions.Optional, -1),
		schema.NewFloat32Node("f32", arrowparquet.Repetitions.Optional, -1),
		schema.NewFloat64Node("f64", arrowparquet.Repetitions.Optional, -1),
		schema.NewBooleanNode("flag", arrowparquet.Repetitions.Optional, -1),
		text("s", arrowparquet.Repetitions.Optional),
		text("tail", arrowparquet.Repetitions.Required),
	}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	properties := []arrowparquet.WriterProperty{arrowparquet.WithCompression(compress.Codecs.Uncompressed), arrowparquet.WithDictionaryDefault(false), arrowparquet.WithDataPageSize(1024)}
	props := arrowparquet.NewWriterProperties(append(properties, options...)...)
	writer := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	defer writer.Close()
	base := 0
	for _, count := range groups {
		rg := writer.AppendRowGroup()
		for colIndex := 0; colIndex < 8; colIndex++ {
			col, err := rg.NextColumn()
			if err != nil {
				t.Fatal(err)
			}
			var defs []int16
			if colIndex != 0 && colIndex != 7 {
				defs = make([]int16, count)
				for i := range defs {
					if (base+i+colIndex)%7 != 0 {
						defs[i] = 1
					}
				}
			}
			var i64 []int64
			var i32 []int32
			var f32 []float32
			var f64 []float64
			var flags []bool
			var texts []arrowparquet.ByteArray
			for i := 0; i < count; i++ {
				if defs != nil && defs[i] == 0 {
					continue
				}
				n := base + i
				switch colIndex {
				case 0:
					i64 = append(i64, int64(n))
				case 1:
					i64 = append(i64, int64(n*19-2000))
				case 2:
					i32 = append(i32, int32(n*3-100))
				case 3:
					f32 = append(f32, float32(n)/4-3)
				case 4:
					f64 = append(f64, float64(n)/8+0.125)
				case 5:
					flags = append(flags, n%2 == 0)
				case 6, 7:
					var value string
					if n%11 != 0 {
						value = fmt.Sprintf("%08d:语言:", n) + strings.Repeat(string(rune('a'+n%26)), textBytes)
					}
					texts = append(texts, arrowparquet.ByteArray(value))
				}
			}
			switch c := col.(type) {
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
			case *file.ByteArrayColumnChunkWriter:
				_, err = c.WriteBatch(texts, defs, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := col.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := rg.Close(); err != nil {
			t.Fatal(err)
		}
		base += count
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}
