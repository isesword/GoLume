package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"strings"
	"testing"
)

func TestSortDictionaryValueRemappingOwnsStorage(t *testing.T) {
	first := &memory.DictionaryStringVector{Indices: []uint32{0, 99}, Offsets: []uint32{0, 1}, Data: []byte("a"), Validity: memory.Bitmap{1}, DictionaryDomain: "same", Encoding: memory.EncodingDictionary}
	second := &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte("b"), DictionaryDomain: "same", Encoding: memory.EncodingDictionary}
	out := &memory.DictionaryStringVector{}
	for _, r := range []struct {
		v   *memory.DictionaryStringVector
		row int
	}{{first, 0}, {second, 0}, {first, 1}, {first, 0}} {
		if err := appendSortDictionaryRow(out, r.v, r.row, r.v.IsValid(r.row)); err != nil {
			t.Fatal(err)
		}
	}
	finishSortDictionary(out)
	identity := out.DictionaryDomain
	for i, want := range []string{"a", "b", "", "a"} {
		if i == 2 {
			if out.IsValid(i) {
				t.Fatal("NULL lost")
			}
			continue
		}
		if string(out.DictionaryValue(out.Indices[i])) != want {
			t.Fatal(i, out)
		}
	}
	if len(identity) != 64 || string(first.Data) != "a" || string(second.Data) != "b" {
		t.Fatal("bad identity or mutated input")
	}
	finishSortDictionary(out)
	if out.DictionaryDomain != identity {
		t.Fatal("unstable identity")
	}
	out.Data[0] = 'z'
	if string(first.Data) != "a" {
		t.Fatal("dictionary remained borrowed")
	}
	if err := appendSortDictionaryRow(out, first, 1, true); err == nil {
		t.Fatal("accepted invalid valid code")
	}
}

func TestSortDictionaryNormalizesEffectiveWindow(t *testing.T) {
	for _, prefix := range []string{"", "prefix"} {
		src := &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{uint32(len(prefix)), uint32(len(prefix) + 1)}, Data: []byte(prefix + "ax"), DictionaryDomain: "first", Encoding: memory.EncodingDictionary}
		next := &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte("b"), DictionaryDomain: "second", Encoding: memory.EncodingDictionary}
		out := &memory.DictionaryStringVector{}
		if err := appendSortDictionaryRow(out, src, 0, true); err != nil {
			t.Fatal(err)
		}
		if err := appendSortDictionaryRow(out, next, 0, true); err != nil {
			t.Fatal(err)
		}
		if string(out.Data) != "ab" || string(out.DictionaryValue(out.Indices[1])) != "b" || out.Offsets[0] != 0 || string(src.Data) != prefix+"ax" {
			t.Fatal(out, src)
		}
	}
	for _, tc := range []struct {
		offsets, data, value uint64
		want                 bool
	}{{math.MaxUint32 - 1, 0, 1, true}, {math.MaxUint32, 0, 1, false}, {1, math.MaxUint32, 0, true}, {1, math.MaxUint32, 1, false}, {1, 1, math.MaxUint64, false}} {
		if got := sortDictionaryCanAppend(tc.offsets, tc.data, tc.value); got != tc.want {
			t.Fatal(tc, got)
		}
	}
}

func TestResidentAndTopKDictionaryWindowAndScope(t *testing.T) {
	for _, top := range []bool{false, true} {
		for _, missing := range []bool{false, true} {
			first := memory.NewBatch()
			first.Rows = 1
			first.Int64s["key"] = &memory.Int64Vector{Values: []int64{2}}
			first.DictStrings["value"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{1, 2, 3}, Data: []byte("xab-tail"), DictionaryDomain: "first", Encoding: memory.EncodingDictionary}
			second := memory.NewBatch()
			second.Rows = 1
			second.Int64s["key"] = &memory.Int64Vector{Values: []int64{1}}
			value := "b"
			if missing {
				value = "c"
			}
			second.DictStrings["value"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte(value + "-tail"), DictionaryDomain: "second", Encoding: memory.EncodingDictionary}
			if !missing {
				second.DictStrings["value"].Data = []byte("ba-tail")
				second.DictStrings["value"].Offsets = []uint32{0, 1, 2}
			}
			secondData := string(second.DictStrings["value"].Data)
			schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeString, Encoding: memory.EncodingDictionary})
			src, err := inmemory.New(schema, first, second)
			if err != nil {
				t.Fatal(err)
			}
			manager := memory.NewBudgetManager(1 << 20)
			var owner source.Source
			if top {
				owner, err = NewTopKSourceWithManager(src, 2, []SortKey{{Column: "key"}}, manager)
			} else {
				owner, err = NewSortSource(src, []SortKey{{Column: "key"}}, manager)
			}
			if err != nil {
				t.Fatal(err)
			}
			stream, err := source.OpenBatchStream(context.Background(), owner, testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			dst := memory.NewBatch()
			ok, err := stream.Next(context.Background(), dst)
			if missing {
				if !errors.Is(err, errSortMixedDictionaries) || ok {
					t.Fatal("unbudgeted mixed value must be rejected", top, ok, err)
				}
			} else {
				if err != nil || !ok {
					t.Fatal(top, ok, err)
				}
				d := dst.DictStrings["value"]
				if string(d.DictionaryValue(d.Indices[0])) != "b" || string(d.DictionaryValue(d.Indices[1])) != "a" {
					t.Fatal(top, d)
				}
				expectedDictionary := first.DictStrings["value"]
				if top {
					expectedDictionary = second.DictStrings["value"]
				}
				if &d.Data[0] != &expectedDictionary.Data[0] {
					t.Fatal("shared path copied dictionary without admission")
				}
			}
			if string(first.DictStrings["value"].Data) != "xab-tail" || string(second.DictStrings["value"].Data) != secondData {
				t.Fatal("source mutated")
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 {
				t.Fatal(manager.Used())
			}
		}
	}
}

func TestResidentDictionaryGrowthCannotExceedAdmission(t *testing.T) {
	var batches []*memory.Batch
	for part := 0; part < 2; part++ {
		b := memory.NewBatch()
		b.Rows = 1024
		b.Int64s["key"] = &memory.Int64Vector{Values: make([]int64, 1024)}
		b.DictStrings["value"] = &memory.DictionaryStringVector{Indices: make([]uint32, 1024), Offsets: []uint32{0, 1024}, Data: []byte(strings.Repeat(string(rune('a'+part)), 1024)), DictionaryDomain: string(rune('a' + part)), Encoding: memory.EncodingDictionary}
		batches = append(batches, b)
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeString, Encoding: memory.EncodingDictionary}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(128 << 10)
	owner, err := NewSortSource(src, []SortKey{{Column: "key"}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(context.Background(), owner, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	dst := memory.NewBatch()
	ok, err := stream.Next(context.Background(), dst)
	if err == nil && ok && memory.BatchBytes(dst) > manager.Used() {
		t.Fatal("unadmitted dictionary amplification", memory.BatchBytes(dst), manager.Used())
	}
	if !errors.Is(err, errSortMixedDictionaries) || ok {
		t.Fatal("resident mixed values require dynamic accounting before support", ok, err)
	}
}
