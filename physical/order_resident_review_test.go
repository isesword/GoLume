package physical

import (
	"context"
	"testing"

	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestResidentDictionaryDownstreamFilterReview(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "source-table"}, memory.Field{Name: "id", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{3, 1, 2}}
	b.DictStrings["key"] = &memory.DictionaryStringVector{Indices: []uint32{1, 0, 1}, Offsets: []uint32{0, 1, 2}, Data: []byte("za"), Encoding: memory.EncodingDictionary, DictionaryDomain: "source-table"}
	input, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	for _, top := range []bool{false, true} {
		var ordered source.Source
		if top {
			ordered, err = NewTopKSourceWithManager(input, 3, []SortKey{{Column: "id"}}, nil)
		} else {
			ordered, err = NewSortSource(input, []SortKey{{Column: "id"}}, nil)
		}
		if err != nil {
			t.Fatal(err)
		}
		if f, _ := ordered.Schema().Field("key"); f.DictionaryDomain != "" {
			t.Fatal("stale source schema domain", f.DictionaryDomain)
		}
		if _, ok := ordered.(source.DictionaryProvider); ok {
			t.Fatal("remapped output forwards source dictionary")
		}
		filtered, err := NewFilterSource(ordered, expr.EQ(expr.Col("key"), expr.Str("a")))
		if err != nil {
			t.Fatal(err)
		}
		stream, err := source.OpenBatchStream(context.Background(), filtered, testReadOptions())
		if err != nil {
			t.Fatal(err)
		}
		var ids []int64
		out := memory.NewBatch()
		for {
			ok, e := stream.Next(context.Background(), out)
			if e != nil {
				t.Fatal(e)
			}
			if !ok {
				break
			}
			ids = append(ids, out.Int64s["id"].Values...)
		}
		_ = stream.Close()
		if !equalInt64s(ids, []int64{2, 3}) {
			t.Fatalf("top=%v downstream IDs=%v", top, ids)
		}
	}
}

func TestResidentOpaquePriorDescriptorStaysClearedReview(t *testing.T) {
	for _, top := range []bool{false, true} {
		input := &residentOpaqueReplaySource{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "raw", Type: memory.TypeArrow, ArrowType: "large_utf8"})}
		manager := memory.NewBudgetManager(1 << 20)
		stream := openResidentDictionaryOrder(t, context.Background(), input, top, 4, SortKey{Column: "key"}, manager)
		first, second := memory.NewBatch(), memory.NewBatch()
		if ok, err := stream.Next(context.Background(), first); !ok || err != nil {
			t.Fatal(ok, err)
		}
		previous := first.Opaque["raw"]
		if previous == nil || previous.Value == nil {
			t.Fatal("missing first opaque output")
		}
		if ok, err := stream.Next(context.Background(), second); !ok || err != nil {
			t.Fatal(ok, err)
		}
		if previous.Value != nil {
			t.Errorf("top=%v: ended opaque descriptor retains Arrow value after next output", top)
		}
		if previous == second.Opaque["raw"] {
			t.Errorf("top=%v: independent output views reuse opaque descriptor", top)
		}
		_ = stream.Close()
		if previous.Value != nil {
			t.Errorf("top=%v: closed opaque descriptor retains Arrow value", top)
		}
	}
}

func TestResidentStructBorrowDescriptorClearedReview(t *testing.T) {
	for _, top := range []bool{false, true} {
		input, unchanged := sortOutputOwnershipSource(t, 22)
		manager := memory.NewBudgetManager(1 << 20)
		stream := openResidentDictionaryOrder(t, context.Background(), input, top, 22, SortKey{Column: "key"}, manager)
		out := memory.NewBatch()
		if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
			t.Fatal(ok, err)
		}
		previous := out.Structs["record"]
		if previous == nil || previous.Values == nil {
			t.Fatal("missing struct output")
		}
		child := previous.Values.Int64s["number"]
		_ = stream.Close()
		if previous.Values != nil || previous.Fields != nil {
			t.Errorf("top=%v: ended struct descriptor retains child bindings", top)
		}
		if child.Values != nil {
			t.Errorf("top=%v: ended child descriptor retains storage", top)
		}
		unchanged()
		if manager.Used() != 0 {
			t.Fatal("leaked", manager.Used())
		}
	}
}
