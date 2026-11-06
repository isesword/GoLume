package source_test

import (
	"context"
	"strings"
	"testing"

	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/arrowrecord"
	"gocolumn/source/inmemory"
)

func TestNativeBorrowedSourcesBoundActualPayload(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt16}, memory.Field{Name: "text", Type: memory.TypeString, Nullable: true}, memory.ListStringField("tags", true, true))
	batches := make([]*memory.Batch, 2)
	for part := range batches {
		b := memory.NewBatch()
		b.Rows = 70
		b.Int16s["id"] = &memory.Int16Vector{Values: make([]int16, 70)}
		text := &memory.StringViewVector{Offsets: []uint32{0}, Validity: memory.EnsureBitmap(nil, 70)}
		child := &memory.StringViewVector{Offsets: []uint32{0}, Validity: memory.EnsureBitmap(nil, 70)}
		list := &memory.StringListVector{Offsets: []uint32{0}, Values: child, Validity: memory.EnsureBitmap(nil, 70)}
		for row := 0; row < 70; row++ {
			b.Int16s["id"].Values[row] = int16(part*1000 + row)
			text.Data = append(text.Data, strings.Repeat("t", 80+row)...)
			text.Offsets = append(text.Offsets, uint32(len(text.Data)))
			child.Data = append(child.Data, strings.Repeat("v", 90+row)...)
			child.Offsets = append(child.Offsets, uint32(len(child.Data)))
			list.Offsets = append(list.Offsets, uint32(row+1))
			if row%5 != 1 {
				memory.Set(text.Validity, row)
			}
			if row%7 != 1 {
				memory.Set(list.Validity, row)
			}
			if row%3 != 1 {
				memory.Set(child.Validity, row)
			}
		}
		b.Strings["text"] = text
		b.StringLists["tags"] = list
		batches[part] = b
	}
	for _, kind := range []string{"inmemory", "arrow"} {
		t.Run(kind, func(t *testing.T) {
			var src source.Source
			if kind == "inmemory" {
				value, err := inmemory.New(schema, batches...)
				if err != nil {
					t.Fatal(err)
				}
				src = value
			} else {
				alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				first, err := memory.ArrowRecord(alloc, schema, batches[0])
				if err != nil {
					t.Fatal(err)
				}
				second, err := memory.ArrowRecord(alloc, schema, batches[1])
				if err != nil {
					first.Release()
					t.Fatal(err)
				}
				value, err := arrowrecord.NewSource(first, second)
				first.Release()
				second.Release()
				if err != nil {
					t.Fatal(err)
				}
				defer value.Close()
				src = value
			}
			manager := memory.NewBudgetManager(128)
			opts := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 70, BatchBytes: 128, BatchPayloadBytes: 512, BatchPayloadTargetBytes: 256, MemoryManager: manager}
			parts := []source.Partition{{ID: 1}, {ID: 0}, {ID: 1}}
			stream, err := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, parts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
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
				bytes, err := memory.EstimateBatchRangeBytes(out, 0, out.Rows)
				if err != nil {
					t.Fatal(err)
				}
				if out.Rows > 1 && bytes > 256 {
					t.Fatalf("multi-row payload=%d exceeds soft target", bytes)
				}
				if bytes > 512 {
					t.Fatalf("payload=%d limit=512", bytes)
				}
				if manager.Used() > 128 {
					t.Fatalf("scratch=%d limit=128", manager.Used())
				}
				if err := out.StringLists["tags"].Validate(out.Rows); err != nil {
					t.Fatal(err)
				}
				for row := 0; row < out.Rows; row++ {
					originalRow := count % 70
					part := parts[count/70].ID
					if got := out.Int16s["id"].Values[row]; got != int16(part*1000+originalRow) {
						t.Fatalf("row %d id=%d", count, got)
					}
					text := out.Strings["text"]
					if text.IsValid(row) != (originalRow%5 != 1) {
						t.Fatalf("text null at row %d", count)
					}
					if text.IsValid(row) && string(text.ValueBytes(row)) != strings.Repeat("t", 80+originalRow) {
						t.Fatalf("text at row %d", count)
					}
					list := out.StringLists["tags"]
					if list.IsValid(row) != (originalRow%7 != 1) {
						t.Fatalf("parent null at row %d", count)
					}
					if list.IsValid(row) {
						lo, hi, ok := list.ValueRange(row)
						if !ok || hi-lo != 1 {
							t.Fatalf("list at row %d", count)
						}
						if list.Values.IsValid(lo) != (originalRow%3 != 1) {
							t.Fatalf("child null at row %d", count)
						}
						if list.Values.IsValid(lo) && string(list.Values.ValueBytes(lo)) != strings.Repeat("v", 90+originalRow) {
							t.Fatalf("child at row %d", count)
						}
					}
					count++
				}
			}
			if count != 210 {
				t.Fatalf("rows=%d want=210", count)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 {
				t.Fatalf("retained scratch=%d", manager.Used())
			}
		})
	}
}

func TestBatchPayloadLimitValidation(t *testing.T) {
	opts := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchPayloadBytes: -1}
	if err := opts.Validate(); err == nil {
		t.Fatal("negative payload limit accepted")
	}
	opts.BatchPayloadBytes = 0
	opts.BatchPayloadTargetBytes = -1
	if err := opts.Validate(); err == nil {
		t.Fatal("negative payload target accepted")
	}
	opts.BatchPayloadTargetBytes = 0
	if err := opts.Validate(); err != nil {
		t.Fatal(err)
	}
	if opts.Normalize().BatchPayloadBytes != 0 {
		t.Fatal("zero payload bound must remain disabled")
	}
}
