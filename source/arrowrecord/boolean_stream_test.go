package arrowrecord

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
)

func booleanStreamRecord(alloc arrowmem.Allocator, n int, nullable bool) arrow.RecordBatch {
	b := array.NewBooleanBuilder(alloc)
	defer b.Release()
	for i := 0; i < n; i++ {
		if nullable && i%5 == 2 {
			b.AppendNull()
		} else {
			b.Append(i%3 == 0)
		}
	}
	a := b.NewBooleanArray()
	defer a.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: nullable}}, nil), []arrow.Array{a}, int64(n))
}

func TestArrowBooleanBoundedDecodedStream(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		for _, offset := range []int{0, 1, 7, 63, 64, 65} {
			for _, limit := range []int{1, 9, 67} {
				t.Run(fmt.Sprintf("nullable%t/offset%d/rows%d", nullable, offset, limit), func(t *testing.T) {
					alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
					defer alloc.AssertSize(t, 0)
					rec := booleanStreamRecord(alloc, 150, nullable)
					sliced := rec.NewSlice(int64(offset), int64(offset+73))
					rec.Release()
					src, err := NewSource(sliced)
					sliced.Release()
					if err != nil {
						t.Fatal(err)
					}
					defer src.Close()
					// Whole-record uint32 expansion would retain O(record rows) per column.
					for _, b := range src.allBindings {
						for _, cache := range b.u32 {
							if len(cache) != 0 {
								t.Fatal("eager Boolean value cache")
							}
						}
					}
					manager := memory.NewBudgetManager(1 << 20)
					opts := testReadOptions()
					opts.BatchRows = limit
					opts.BatchBytes = 40
					opts.MemoryManager = manager
					stream, err := src.NewBatchStreamWithPartitions(context.Background(), opts, []source.Partition{{ID: 0}, {ID: 0}})
					if err != nil {
						t.Fatal(err)
					}
					defer stream.Close()
					src.Close() // Open streams retain Arrow until EOF/error/Close.
					out := memory.NewBatch()
					defer out.ClearColumns()
					seen := 0
					for {
						ok, err := stream.Next(context.Background(), out)
						if err != nil {
							t.Fatal(err)
						}
						if !ok {
							break
						}
						v := out.Uint32s["flag"]
						if v == nil || out.Opaque["flag"] != nil {
							t.Fatalf("Boolean output must use Uint32 backing, got native=%v opaque=%v", v, out.Opaque["flag"])
						}
						if out.Rows <= 0 || out.Rows > limit || cap(v.Values) != out.Rows {
							t.Fatalf("unbounded decoded output rows=%d capacity=%d", out.Rows, cap(v.Values))
						}
						wantBytes := int64(out.Rows) * 4
						if nullable && (seen%73)%64 != 0 {
							wantBytes += int64(memory.WordsFor(out.Rows)) * 8
						}
						if used := manager.Used(); used != wantBytes || used > opts.BatchBytes {
							t.Fatalf("decoded reservation=%d want=%d limit=%d", used, wantBytes, opts.BatchBytes)
						}
						for i, value := range v.Values {
							original := offset + (seen % 73) + i
							valid := !nullable || original%5 != 2
							want := uint32(0)
							if valid && original%3 == 0 {
								want = 1
							}
							if v.IsValid(i) != valid || value != want {
								t.Fatalf("row %d value=%d valid=%t want=%d/%t", original, value, v.IsValid(i), want, valid)
							}
						}
						seen += out.Rows
					}
					if seen != 146 || manager.Used() != 0 {
						t.Fatalf("rows=%d retained=%d", seen, manager.Used())
					}
					if ok, err := stream.Next(context.Background(), out); ok || err != nil {
						t.Fatalf("EOF=%t/%v", ok, err)
					}
				})
			}
		}
	}
}

func TestArrowBooleanPayloadAndFailureCleanup(t *testing.T) {
	cases := []struct {
		name                           string
		rows                           int
		bytes, payload, target, budget int64
		wantRows                       int
		fail                           bool
	}{
		{"allocation", 100, 20, 0, 0, 100, 5, false}, {"payload", 100, 100, 12, 0, 100, 3, false}, {"target", 100, 100, 20, 8, 100, 2, false}, {"soft_single_row", 100, 100, 20, 1, 100, 1, false}, {"allocation_too_small", 100, 3, 0, 0, 100, 0, true}, {"payload_too_small", 100, 100, 3, 0, 100, 0, true}, {"manager_rejected", 100, 100, 0, 0, 3, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			rec := booleanStreamRecord(alloc, 100, false)
			src, err := NewSource(rec)
			rec.Release()
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			m := memory.NewBudgetManager(tc.budget)
			opts := testReadOptions()
			opts.BatchRows = tc.rows
			opts.BatchBytes = tc.bytes
			opts.BatchPayloadBytes = tc.payload
			opts.BatchPayloadTargetBytes = tc.target
			opts.MemoryManager = m
			stream, err := src.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			src.Close()
			out := memory.NewBatch()
			defer out.ClearColumns()
			ok, err := stream.Next(context.Background(), out)
			if tc.fail {
				if ok || !errors.Is(err, memory.ErrBudgetExceeded) || out.Rows != 0 || len(out.Uint32s) != 0 || m.Used() != 0 {
					t.Fatalf("failure=%t/%v rows=%d retained=%d", ok, err, out.Rows, m.Used())
				}
				if ok, next := stream.Next(context.Background(), out); ok || next != err {
					t.Fatalf("terminal=%t/%v first=%v", ok, next, err)
				}
			} else {
				if !ok || err != nil || out.Rows != tc.wantRows {
					t.Fatalf("next=%t/%v rows=%d want=%d", ok, err, out.Rows, tc.wantRows)
				}
				stream.Close()
				if m.Used() != 0 {
					t.Fatal("reservation leaked")
				}
			}
		})
	}
}

func TestArrowBooleanIndependentStreamsAndClone(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	rec := booleanStreamRecord(alloc, 9, true)
	src, err := NewSource(rec)
	rec.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	opts := testReadOptions()
	opts.BatchRows = 3
	first, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	a, b := memory.NewBatch(), memory.NewBatch()
	defer a.ClearColumns()
	defer b.ClearColumns()
	if ok, err := first.Next(context.Background(), a); !ok || err != nil {
		t.Fatal(err)
	}
	if a.Uint32s["flag"] == nil {
		t.Fatal("missing native Boolean binding")
	}
	saved, err := memory.CloneBatch(a)
	if err != nil {
		t.Fatal(err)
	}
	defer saved.ClearColumns()
	a.Uint32s["flag"].Values[0] = 0 // Decode buffers cannot alias source or another execution.
	if ok, err := second.Next(context.Background(), b); !ok || err != nil {
		t.Fatal(err)
	}
	if b.Uint32s["flag"].Values[0] != 1 || saved.Uint32s["flag"].Values[0] != 1 {
		t.Fatal("decoded buffers alias")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := first.Next(ctx, a); ok || !errors.Is(err, context.Canceled) || a.Rows != 0 {
		t.Fatalf("cancel=%t/%v", ok, err)
	}
	src.Close()
	first.Close()
	second.Close()
	if !reflect.DeepEqual(saved.Uint32s["flag"].Values, []uint32{1, 0, 0}) {
		t.Fatal("clone changed after close")
	}
}

func TestArrowBooleanEmptyLogicalRowsAndProjection(t *testing.T) {
	for _, rows := range []int64{0, 1, 3} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			long := booleanStreamRecord(alloc, 130, true)
			defer long.Release()
			rec := array.NewRecordBatch(long.Schema(), long.Columns(), rows)
			defer rec.Release()
			src, err := NewSource(rec)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			opts := testReadOptions()
			opts.BatchRows = 9
			stream, err := src.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			defer out.ClearColumns()
			ok, err := stream.Next(context.Background(), out)
			if !ok || err != nil || int64(out.Rows) != rows || out.Uint32s["flag"] == nil {
				t.Fatalf("logical rows=%d next=%t/%v output=%d", rows, ok, err, out.Rows)
			}
			if len(out.Uint32s["flag"].Values) != int(rows) {
				t.Fatal("hidden Arrow tail exposed")
			}
			if ok, err := stream.Next(context.Background(), out); ok || err != nil {
				t.Fatalf("EOF %t/%v", ok, err)
			}
		})
	}
	t.Run("projection", func(t *testing.T) {
		alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
		defer alloc.AssertSize(t, 0)
		booleans := booleanStreamRecord(alloc, 8, false)
		defer booleans.Release()
		builder := array.NewInt64Builder(alloc)
		builder.AppendValues([]int64{0, 1, 2, 3, 4, 5, 6, 7}, nil)
		ints := builder.NewInt64Array()
		builder.Release()
		defer ints.Release()
		rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "flag", Type: arrow.FixedWidthTypes.Boolean}, {Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{booleans.Column(0), ints}, 8)
		defer rec.Release()
		src, err := NewSource(rec)
		if err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		projected, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		opts := testReadOptions()
		opts.BatchBytes = 1
		opts.MemoryManager = memory.NewBudgetManager(1)
		stream, err := projected.NewBatchStream(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		out := memory.NewBatch()
		defer out.ClearColumns()
		ok, err := stream.Next(context.Background(), out)
		if !ok || err != nil || out.Rows != 8 || len(out.Uint32s) != 0 || opts.MemoryManager.Used() != 0 {
			t.Fatalf("unused Boolean decoded: %t/%v rows=%d used=%d", ok, err, out.Rows, opts.MemoryManager.Used())
		}
	})
}

// BenchmarkArrowBooleanFirstBatch isolates open/read/close allocation for a
// fixed output range. Record construction and source-owned metadata are outside
// timing; larger retained records must not widen the first decoded value buffer.
func BenchmarkArrowBooleanFirstBatch(b *testing.B) {
	for _, rows := range []int{1024, 65536, 1048576} {
		b.Run(fmt.Sprintf("record%d", rows), func(b *testing.B) {
			rec := booleanStreamRecord(arrowmem.DefaultAllocator, rows, false)
			defer rec.Release()
			src, err := NewSource(rec)
			if err != nil {
				b.Fatal(err)
			}
			defer src.Close()
			opts := testReadOptions()
			opts.BatchRows = 1024
			opts.BatchBytes = 4096
			out := memory.NewBatch()
			defer out.ClearColumns()
			b.ReportAllocs()
			b.SetBytes(4096)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stream, err := src.NewBatchStream(context.Background(), opts)
				if err != nil {
					b.Fatal(err)
				}
				ok, err := stream.Next(context.Background(), out)
				if !ok || err != nil || len(out.Uint32s["flag"].Values) != 1024 {
					b.Fatalf("first batch=%t/%v", ok, err)
				}
				out.ClearColumns()
				if err := stream.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
