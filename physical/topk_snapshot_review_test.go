package physical

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

// These arrays have tiny logical payloads but retain either large allocations
// or many Arrow metadata nodes. Logical SizeInBytes is not an admission bound.
func TestTopKSnapshotArrowRetainedBackingReview(t *testing.T) {
	slicedParent := func(alloc arrowmem.Allocator) arrow.Array {
		parent := arrowmem.NewResizableBuffer(alloc)
		parent.Resize(1 << 20)
		// A slice at the end also hides the original size from cap(slice.Buf()).
		tiny := arrowmem.SliceBuffer(parent, (1<<20)-8, 8)
		parent.Release()
		data := array.NewData(arrow.PrimitiveTypes.Int64, 1, []*arrowmem.Buffer{nil, tiny}, nil, 0, 0)
		tiny.Release()
		values := array.NewInt64Data(data)
		data.Release()
		return values
	}
	cases := []struct {
		name    string
		build   func(arrowmem.Allocator) arrow.Array
		minimum int64
	}{
		{"sliced_parent", slicedParent, 1 << 20},
		{"resizable_capacity", func(alloc arrowmem.Allocator) arrow.Array {
			buffer := arrowmem.NewResizableBuffer(alloc)
			buffer.Reserve(1 << 20)
			buffer.ResizeNoShrink(8)
			data := array.NewData(arrow.PrimitiveTypes.Int64, 1, []*arrowmem.Buffer{nil, buffer}, nil, 0, 0)
			buffer.Release()
			values := array.NewInt64Data(data)
			data.Release()
			return values
		}, 1 << 20},
		{"buffer_slice_capacity", func(alloc arrowmem.Allocator) arrow.Array {
			buffer := arrowmem.NewResizableBuffer(alloc)
			buffer.Resize(8)
			buffers := make([]*arrowmem.Buffer, 2, 1<<17)
			buffers[1] = buffer
			data := array.NewData(arrow.PrimitiveTypes.Int64, 1, buffers, nil, 0, 0)
			buffer.Release()
			values := array.NewInt64Data(data)
			data.Release()
			return values
		}, 1 << 20},
		{"child_slice_capacity", func(alloc arrowmem.Allocator) arrow.Array {
			buffer := arrowmem.NewResizableBuffer(alloc)
			buffer.Resize(8)
			child := array.NewData(arrow.PrimitiveTypes.Int64, 1, []*arrowmem.Buffer{nil, buffer}, nil, 0, 0)
			buffer.Release()
			children := make([]arrow.ArrayData, 1, 1<<16)
			children[0] = child
			data := array.NewData(arrow.StructOf(arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64}), 1, []*arrowmem.Buffer{nil}, children, 0, 0)
			child.Release()
			values := array.NewStructData(data)
			data.Release()
			return values
		}, 1 << 20},
		{"nested_metadata", func(alloc arrowmem.Allocator) arrow.Array {
			const columns = 512
			fields := make([]arrow.Field, columns)
			children := make([]arrow.ArrayData, columns)
			for i := range fields {
				fields[i] = arrow.Field{Name: fmt.Sprintf("c%d", i), Type: arrow.PrimitiveTypes.Int64}
				buffer := arrowmem.NewResizableBuffer(alloc)
				buffer.Resize(8)
				children[i] = array.NewData(arrow.PrimitiveTypes.Int64, 1, []*arrowmem.Buffer{nil, buffer}, nil, 0, 0)
				buffer.Release()
			}
			data := array.NewData(arrow.StructOf(fields...), 1, []*arrowmem.Buffer{nil}, children, 0, 0)
			for _, child := range children {
				child.Release()
			}
			values := array.NewStructData(data)
			data.Release()
			return values
		}, 64 << 10},
		{"dictionary_parent", func(alloc arrowmem.Allocator) arrow.Array {
			dict := slicedParent(alloc)
			defer dict.Release()
			builder := array.NewInt8Builder(alloc)
			builder.Append(0)
			indices := builder.NewArray()
			builder.Release()
			defer indices.Release()
			return array.NewDictionaryArray(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.PrimitiveTypes.Int64}, indices, dict)
		}, 1 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, budget := range []int64{64 << 10, 16 << 20} {
				t.Run(fmt.Sprintf("budget%d", budget), func(t *testing.T) {
					alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
					defer alloc.AssertSize(t, 0)
					values := tc.build(alloc)
					batch := memory.NewBatch()
					batch.Rows = 1
					batch.Int64s["key"] = &memory.Int64Vector{Values: []int64{1}}
					batch.Opaque["raw"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, ArrowType: values.DataType().String(), Value: values, Owned: true}
					defer batch.ClearColumns()
					schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "raw", Type: memory.TypeArrow, ArrowType: values.DataType().String()})
					base, err := inmemory.New(schema, batch)
					if err != nil {
						t.Fatal(err)
					}
					manager := memory.NewBudgetManager(budget)
					top, err := NewTopKSourceWithManager(unstableReplaySource{inner: base}, 1, []SortKey{{Column: "key"}}, manager)
					if err != nil {
						t.Fatal(err)
					}
					stream, err := top.NewBatchStream(context.Background(), testReadOptions())
					if err != nil {
						t.Fatal(err)
					}
					defer stream.Close()
					out := memory.NewBatch()
					defer out.ClearColumns()
					ok, err := stream.Next(context.Background(), out)
					if budget == 64<<10 {
						if ok || !errors.Is(err, memory.ErrBudgetExceeded) {
							t.Fatalf("retained backing bypassed budget: ok=%v err=%v used=%d", ok, err, manager.Used())
						}
						if manager.Used() != 0 {
							t.Fatalf("rejected admission leaked %d", manager.Used())
						}
					} else {
						if !ok || err != nil {
							t.Fatalf("adequate budget: ok=%v err=%v", ok, err)
						}
						if manager.Used() < tc.minimum || manager.Used() > budget {
							t.Fatalf("retained reservation=%d, minimum=%d", manager.Used(), tc.minimum)
						}
						output := out.Opaque["raw"]
						if output.Owned || output.Value.(arrow.Array).Len() != 1 || !arrow.TypeEqual(output.Value.(arrow.Array).DataType(), values.DataType()) {
							t.Fatal("opaque output ownership/type changed")
						}
						// Caller release of borrowed output must not steal the snapshot/source.
						out.ReleaseOwned()
						if ok, err := stream.Next(context.Background(), out); ok || err != nil {
							t.Fatalf("exhaustion: %v %v", ok, err)
						}
						if manager.Used() != 0 {
							t.Fatalf("exhaustion leaked %d", manager.Used())
						}
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					if !batch.Opaque["raw"].Owned || values.Len() != 1 {
						t.Fatal("source ownership changed")
					}
				})
			}
		})
	}
}

func TestTopKFixedReuseMalformedPayloadReleasesReview(t *testing.T) {
	for _, malformed := range []string{"short_values", "short_validity"} {
		t.Run(malformed, func(t *testing.T) {
			const rows = 65
			batch := memory.NewBatch()
			batch.Rows = rows
			keys, payload := make([]int64, rows), make([]int64, rows)
			for i := range keys {
				keys[i] = int64(rows - i)
				payload[i] = int64(i)
			}
			batch.Int64s["key"] = &memory.Int64Vector{Values: keys}
			batch.Int64s["payload"] = &memory.Int64Vector{Values: payload}
			if malformed == "short_values" {
				batch.Int64s["payload"].Values = payload[:64]
			} else {
				batch.Int64s["payload"].Validity = memory.Bitmap{^uint64(0)}
			}
			input := &topKOneShotSource{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeInt64, Nullable: true}), batch: batch, count: 1, fill: func(int, *memory.Batch) {}}
			manager := memory.NewBudgetManager(16 << 10)
			top, err := NewTopKSourceWithManager(input, 1, []SortKey{{Column: "key"}}, manager)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := top.NewBatchStream(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			output := memory.NewBatch()
			ok, first := stream.Next(context.Background(), output)
			if ok || first == nil {
				t.Fatalf("malformed payload succeeded: %v %v", ok, first)
			}
			if manager.Used() != 0 || input.closes != 1 {
				t.Fatalf("error cleanup: used=%d closes=%d", manager.Used(), input.closes)
			}
			if ok, err := stream.Next(context.Background(), output); ok || !errors.Is(err, first) {
				t.Fatalf("terminal error changed: %v %v", ok, err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 || input.closes != 1 {
				t.Fatal("Close repeated resource cleanup")
			}
		})
	}
}

func TestTopKFixedReuseShapeFallbackReview(t *testing.T) {
	for _, shape := range []string{"renamed", "extra", "different_family", "variable"} {
		t.Run(shape, func(t *testing.T) {
			batch := memory.NewBatch()
			batch.Rows = 1
			batch.Int64s["key"] = &memory.Int64Vector{Values: []int64{2}}
			batch.Int64s["payload"] = &memory.Int64Vector{Values: []int64{20}}
			schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeInt64})
			base, err := inmemory.New(schema, batch)
			if err != nil {
				t.Fatal(err)
			}
			manager := memory.NewBudgetManager(16 << 10)
			top, err := NewTopKSourceWithManager(unstableReplaySource{inner: base}, 1, []SortKey{{Column: "key"}}, manager)
			if err != nil {
				t.Fatal(err)
			}
			stream := &topKSnapshotStream{owner: top.(*TopKSource), manager: manager, input: batch}
			candidate, err := stream.snapshot(context.Background(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer candidate.release()
			before := manager.Used()
			incoming := memory.NewBatch()
			incoming.Rows = 1
			incoming.Int64s["key"] = &memory.Int64Vector{Values: []int64{1}}
			incoming.Int64s["payload"] = &memory.Int64Vector{Values: []int64{10}}
			switch shape {
			case "renamed":
				incoming.Int64s["other"] = incoming.Int64s["payload"]
				delete(incoming.Int64s, "payload")
			case "extra":
				incoming.Int64s["extra"] = &memory.Int64Vector{Values: []int64{3}}
			case "different_family":
				delete(incoming.Int64s, "payload")
				incoming.Int32s["payload"] = &memory.Int32Vector{Values: []int32{10}}
			case "variable":
				incoming.Strings["extra"] = &memory.StringViewVector{Offsets: []uint32{0, 1}, Data: []byte("x")}
			}
			stream.input = incoming
			if reused, err := stream.reuseFixed(&candidate, 0); reused || err != nil {
				t.Fatalf("unsafe shape reuse: %v %v", reused, err)
			}
			if manager.Used() != before {
				t.Fatal("fallback changed reservation")
			}
			candidate.release()
			if manager.Used() != 0 {
				t.Fatal("fallback leaked reservation")
			}
		})
	}
}
