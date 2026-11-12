package source_test

import (
	"context"
	"errors"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/arrowrecord"
	"gocolumn/source/inmemory"
	"testing"
)

func TestInt64ListBorrowedSourcePayloadBudgetAndCancellation(t *testing.T) {
	schema := memory.NewSchema(memory.ListInt64Field("nums", true, true))
	b := memory.NewBatch()
	b.Rows = 70
	v := &memory.Int64ListVector{Offsets: []uint32{0}, Values: &memory.Int64Vector{Validity: memory.EnsureBitmap(nil, 140)}, Validity: memory.EnsureBitmap(nil, 70)}
	for i := 0; i < 70; i++ {
		v.Offsets = append(v.Offsets, uint32(2*i+2))
		v.Values.Values = append(v.Values.Values, int64(i), -int64(i))
		if i%3 != 1 {
			memory.Set(v.Validity, i)
		}
		memory.Set(v.Values.Validity, 2*i)
	}
	b.Int64Lists["nums"] = v
	for _, kind := range []string{"inmemory", "arrow"} {
		t.Run(kind, func(t *testing.T) {
			var src source.Source
			if kind == "inmemory" {
				s, err := inmemory.New(schema, b)
				if err != nil {
					t.Fatal(err)
				}
				src = s
			} else {
				alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
				defer alloc.AssertSize(t, 0)
				rec, err := memory.ArrowRecord(alloc, schema, b)
				if err != nil {
					t.Fatal(err)
				}
				s, err := arrowrecord.NewSource(rec)
				rec.Release()
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				src = s
			}
			manager := memory.NewBudgetManager(64)
			opts := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 70, BatchBytes: 64, BatchPayloadTargetBytes: 100, BatchPayloadBytes: 200, MemoryManager: manager}
			stream, err := source.OpenBatchStream(context.Background(), src, opts)
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			count := 0
			for {
				ok, err := stream.Next(context.Background(), out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				size, err := memory.EstimateBatchRangeBytes(out, 0, out.Rows)
				if err != nil {
					t.Fatal(err)
				}
				if size > 200 || out.Rows > 1 && size > 100 {
					t.Fatalf("payload %d", size)
				}
				list := out.Int64Lists["nums"]
				if err := list.Validate(out.Rows); err != nil {
					t.Fatal(err)
				}
				for row := 0; row < out.Rows; row++ {
					if list.IsValid(row) != (count%3 != 1) {
						t.Fatalf("parent row %d", count)
					}
					if list.IsValid(row) {
						lo, hi, _ := list.ValueRange(row)
						if hi-lo != 2 || list.Values.Values[lo] != int64(count) || !list.Values.IsValid(lo) || list.Values.IsValid(lo+1) {
							t.Fatalf("child row %d", count)
						}
					}
					count++
				}
			}
			stream.Close()
			if count != 70 || manager.Used() != 0 {
				t.Fatalf("rows=%d retained=%d", count, manager.Used())
			}
			stream, err = source.OpenBatchStream(context.Background(), src, opts)
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if ok, err := stream.Next(ctx, out); ok || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel=%v %v", ok, err)
			}
			stream.Close()
			if manager.Used() != 0 || out.Rows != 0 {
				t.Fatal("cancel leaked output/reservation")
			}
		})
	}
}
