package physical

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"strings"
	"testing"
)

type residentAliasReplaySource struct{ *sortInitialRuntimeSource }

func (*residentAliasReplaySource) Replayability() source.Replayability { return source.ReplayStable }

func TestResidentNativeBindingPrecedesOpaqueExtras(t *testing.T) {
	for _, top := range []bool{false, true} {
		for _, alias := range []string{"key", "payload", "unlisted"} {
			for _, wide := range []bool{false, true} {
				t.Run(fmt.Sprintf("top=%v/alias=%s/wide=%v", top, alias, wide), func(t *testing.T) {
					allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
					defer allocator.AssertSize(t, 0)
					var extra arrow.Array
					if wide {
						builder := array.NewLargeStringBuilder(allocator)
						builder.Append(strings.Repeat("x", 1<<20))
						builder.Append("unused")
						extra = builder.NewArray()
						builder.Release()
					} else {
						builder := array.NewInt64Builder(allocator)
						builder.AppendValues([]int64{-9, -8}, nil)
						extra = builder.NewArray()
						builder.Release()
					}
					defer extra.Release()
					b := memory.NewBatch()
					b.Rows = 2
					key := &memory.Int64Vector{Values: []int64{2, 1}}
					payload := &memory.Int64Vector{Values: []int64{20, 10}}
					b.Int64s["key"], b.Int64s["payload"] = key, payload
					opaque := &memory.OpaqueVector{Rows: 2, Type: memory.TypeArrow, ArrowType: extra.DataType().String(), Value: extra}
					b.Opaque[alias] = opaque
					raw := &sortInitialRuntimeSource{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeInt64}), batches: []*memory.Batch{b}}
					input := &residentAliasReplaySource{raw}
					manager := &topKPeakManager{BudgetManager: memory.NewBudgetManager(32 << 10)}
					stream := openResidentDictionaryOrder(t, context.Background(), input, top, 2, SortKey{Column: "key"}, manager)
					defer stream.Close()
					out := memory.NewBatch()
					if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
						t.Fatal(ok, err)
					}
					if out.Rows != 2 || !reflect.DeepEqual(out.Int64s["key"].Values, []int64{1, 2}) || !reflect.DeepEqual(out.Int64s["payload"].Values, []int64{10, 20}) || len(out.Opaque) != 0 {
						t.Fatal("runtime extra displaced native output")
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					if manager.Used() != 0 || manager.peak > 32<<10 || raw.opens != raw.closes {
						t.Fatal("budget/child leak", manager.Used(), manager.peak, raw.opens, raw.closes)
					}
					if b.Int64s["key"] != key || b.Int64s["payload"] != payload || b.Opaque[alias] != opaque || !reflect.DeepEqual(key.Values, []int64{2, 1}) || !reflect.DeepEqual(payload.Values, []int64{20, 10}) || opaque.Value != extra {
						t.Fatal("input aliases changed")
					}
				})
			}
		}
	}
}

func TestResidentOpaqueFallbackIgnoresUnrelatedNativeFamily(t *testing.T) {
	for _, top := range []bool{false, true} {
		allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
		builder := array.NewLargeStringBuilder(allocator)
		builder.AppendValues([]string{"two", "one"}, nil)
		extra := builder.NewArray()
		builder.Release()
		b := memory.NewBatch()
		b.Rows = 2
		b.Int64s["key"] = &memory.Int64Vector{Values: []int64{2, 1}}
		b.Int64s["raw"] = &memory.Int64Vector{Values: []int64{-20, -10}}
		b.Opaque["raw"] = &memory.OpaqueVector{Rows: 2, Type: memory.TypeArrow, ArrowType: extra.DataType().String(), Value: extra}
		raw := &sortInitialRuntimeSource{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "raw", Type: memory.TypeArrow, ArrowType: extra.DataType().String()}), batches: []*memory.Batch{b}}
		input := &residentAliasReplaySource{raw}
		manager := memory.NewBudgetManager(64 << 10)
		stream := openResidentDictionaryOrder(t, context.Background(), input, top, 2, SortKey{Column: "key"}, manager)
		out := memory.NewBatch()
		for i, want := range []string{"one", "two"} {
			if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
				t.Fatal(top, ok, err)
			}
			if out.Rows != 1 || out.Int64s["key"].Values[0] != int64(i+1) || out.Int64s["raw"] != nil || out.Opaque["raw"].Value.(*array.LargeString).Value(0) != want {
				t.Fatal("wrong family displaced opaque fallback")
			}
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		extra.Release()
		allocator.AssertSize(t, 0)
		if manager.Used() != 0 || raw.opens != raw.closes {
			t.Fatal("ownership leak")
		}
	}
}
