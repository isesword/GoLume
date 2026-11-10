package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"reflect"
	"strings"
	"testing"
)

func TestResidentStructRuntimeDictionaryRemapping(t *testing.T) {
	child := memory.Field{Name: "text", Type: memory.TypeString, Encoding: memory.EncodingView, Nullable: true}
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.StructField("payload", true, child))
	var batches []*memory.Batch
	for part := 0; part < 2; part++ {
		b := memory.NewBatch()
		b.Rows = 3
		b.Int64s["key"] = &memory.Int64Vector{Values: []int64{int64(5 - part*3), int64(4 - part*3), int64(3 - part*3)}}
		children := memory.NewBatch()
		children.Rows = 3
		text := "ab"
		if part == 1 {
			text = "ca"
		}
		children.DictStrings["text"] = &memory.DictionaryStringVector{Indices: []uint32{0, 99, 99}, Offsets: []uint32{1, 2, 3}, Data: []byte("!" + text + "tail"), Validity: memory.Bitmap{5}, DictionaryDomain: "reused", Encoding: memory.EncodingDictionary}
		b.Structs["payload"] = &memory.StructVector{Rows: 3, Fields: []memory.Field{child}, Values: children, Validity: memory.Bitmap{3}, Encoding: memory.EncodingView}
		batches = append(batches, b)
	}
	input, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	for _, top := range []bool{false, true} {
		manager := memory.NewBudgetManager(1 << 20)
		stream := openResidentDictionaryOrder(t, context.Background(), input, top, 6, SortKey{Column: "key"}, manager)
		out := memory.NewBatch()
		if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
			t.Fatal(top, ok, err)
		}
		p := out.Structs["payload"]
		d := p.Values.DictStrings["text"]
		if d == nil || d.DictionaryLen() != 2 || len(d.DictionaryDomain) != 64 {
			t.Fatalf("top=%v dictionary=%+v", top, d)
		}
		for row, want := range []string{"", "", "c", "", "", "a"} {
			parent := row%3 != 0
			valid := row%3 == 2
			if p.IsValid(row) != parent || d.IsValid(row) != valid {
				t.Fatal(top, row, "validity")
			}
			if valid && string(d.DictionaryValue(d.Indices[row])) != want {
				t.Fatal(top, row, "value")
			}
		}
		aliases := captureSortOutputOwnershipAliases(out)
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		aliases.checkCleared(t)
		if manager.Used() != 0 {
			t.Fatal("leak", manager.Used())
		}
	}
}

func TestResidentSortNilArgumentFailureIsTerminal(t *testing.T) {
	for _, nilContext := range []bool{false, true} {
		input, _ := newResidentDictionaryInput(memory.TypeString)
		manager := memory.NewBudgetManager(1 << 20)
		stream := openResidentDictionaryOrder(t, context.Background(), input, false, 10, SortKey{Column: "id"}, manager)
		dst := memory.NewBatch()
		ctx := context.Background()
		if nilContext {
			ctx = nil
		} else {
			dst = nil
		}
		if ok, err := stream.Next(ctx, dst); ok || err == nil {
			t.Fatal(ok, err)
		}
		if ok, err := stream.Next(context.Background(), memory.NewBatch()); ok || err == nil || errors.Is(err, context.Canceled) {
			t.Fatal(ok, err)
		}
		if manager.Used() != 0 || input.opens != input.closes {
			t.Fatal("failed owner retained input", manager.Used(), input.opens, input.closes)
		}
	}
}

func TestResidentAccumulatorActualCapacityAndDeniedGrowth(t *testing.T) {
	ctx := context.Background()
	parent := &externalAccumulatorTestManager{Manager: memory.NewBudgetManager(0)}
	w := &sortWorkspace{parent: parent, bulkCredits: true}
	defer w.close()
	child := memory.Field{Name: "text", Type: memory.TypeString, Encoding: memory.EncodingView}
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "dict", Type: memory.TypeString, Encoding: memory.EncodingDictionary}, memory.StructField("payload", false, child))
	src := memory.NewBatch()
	src.Rows = 1
	src.Int64s["key"] = &memory.Int64Vector{Values: []int64{1}}
	src.DictStrings["dict"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{1, 2}, Data: []byte("_a_tail"), Encoding: memory.EncodingDictionary}
	children := memory.NewBatch()
	children.Rows = 1
	children.DictStrings["text"] = src.DictStrings["dict"]
	src.Structs["payload"] = &memory.StructVector{Rows: 1, Fields: []memory.Field{child}, Values: children}
	a, err := newResidentSortAccumulator(ctx, schema, w, src)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	if err = a.appendRow(ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	capacity := externalAccumulatorTestCapacity(reflect.ValueOf(a.batch))
	for _, index := range a.residentDictionaries {
		capacity += int64(cap(index.hashes))*8 + int64(cap(index.codes))*4
	}
	if a.capacityBytes != capacity {
		t.Fatalf("capacity=%d actual=%d", a.capacityBytes, capacity)
	}
	if err = a.finish(ctx); err != nil {
		t.Fatal(err)
	}
	if a.reservation.Bytes() < a.metadataBytes+capacity+128 {
		t.Fatal("domains or capacity not admitted")
	}
	gathered, err := newResidentSortedAccumulator(ctx, a, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	defer gathered.close()
	actual := externalAccumulatorTestCapacity(reflect.ValueOf(gathered.batch)) + 128
	if gathered.capacityBytes != actual || gathered.reservation.Bytes() < gathered.metadataBytes+actual {
		t.Fatalf("gather capacity=%d actual=%d lease=%d", gathered.capacityBytes, actual, gathered.reservation.Bytes())
	}
	a.close()
	if string(gathered.batch.DictStrings["dict"].DictionaryValue(0)) != "a" || string(gathered.batch.Structs["payload"].Values.DictStrings["text"].DictionaryValue(0)) != "a" {
		t.Fatal("source close changed gathered dictionaries")
	}
	// A refused new value cannot mutate the old data/offset/index backing.
	b, err := newResidentSortAccumulator(ctx, schema, w, src)
	if err != nil {
		t.Fatal(err)
	}
	defer b.close()
	if err = b.appendRow(ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	prior := b.batch.DictStrings["dict"]
	dataPtr := &prior.Data[0]
	offsetPtr := &prior.Offsets[0]
	bytes := parent.Used()
	src.DictStrings["dict"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1 << 20}, Data: []byte(strings.Repeat("b", 1<<20)), Encoding: memory.EncodingDictionary}
	parent.deny = true
	if err = b.appendRow(ctx, src, 0); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if &prior.Data[0] != dataPtr || &prior.Offsets[0] != offsetPtr || string(prior.Data) != "a" || len(prior.Indices) != 1 || parent.Used() != bytes {
		t.Fatal("refused growth mutated dictionary or charge")
	}
}
