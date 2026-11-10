package memory

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestColumnarRunEmptyOffsetCanonicalization(t *testing.T) {
	for _, offsets := range [][]uint32{nil, {}, {0}, {3}} {
		for _, rows := range []int{0, 1, 3} {
			b, schema := emptyListFixture(rows, false, nil)
			child := b.StringLists["list"].Values
			child.Offsets = offsets
			child.Data = []byte("unused")
			roundTripEmptyList(t, schema, b)
			if !reflect.DeepEqual(child.Offsets, offsets) || string(child.Data) != "unused" {
				t.Fatal("encoder changed source storage")
			}
		}
		b := NewBatch()
		b.Strings["s"] = &StringViewVector{Offsets: offsets, Data: []byte("unused")}
		roundTripEmptyList(t, NewSchema(Field{Name: "s", Type: TypeString}), b)
	}
	b, schema := emptyListFixture(1, false, nil)
	// The normalized wire format still requires its sentinel; accepting absent
	// source offsets must not loosen corruption checks in the decoder.
	encoded, err := EncodeColumnarRun(context.Background(), schema, b, NewBudgetManager(0))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	for _, count := range []uint32{0, 2, math.MaxUint32} {
		payload := append([]byte(nil), encoded.Payload...)
		const childCountOffset = columnarRunHeader + 4 + 4 + 2*4 + 4
		binary.LittleEndian.PutUint32(payload[childCountOffset:], count)
		manager := NewBudgetManager(0)
		got, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: payload}}, manager)
		if got != nil || !errors.Is(err, ErrSpillCorrupt) || manager.Used() != 0 {
			t.Fatal("invalid child count", count, got, err, manager.Used())
		}
	}
}

func TestColumnarRunEmptyOffsetsKeepMalformedRejection(t *testing.T) {
	for _, tc := range []string{"parent sentinel", "nonempty child", "dictionary sentinel", "decreasing child", "nonempty string"} {
		t.Run(tc, func(t *testing.T) {
			b, schema := emptyListFixture(1, false, nil)
			switch tc {
			case "parent sentinel":
				b.StringLists["list"].Offsets = nil
			case "nonempty child":
				b.StringLists["list"].Offsets[1] = 1
			case "decreasing child":
				b.StringLists["list"].Offsets[1] = 2
				b.StringLists["list"].Values.Offsets = []uint32{0, 1, 0}
			case "dictionary sentinel":
				b = NewBatch()
				schema = NewSchema(Field{Name: "d", Type: TypeString, Encoding: EncodingDictionary})
				b.DictStrings["d"] = &DictionaryStringVector{DictionaryDomain: "empty"}
			case "nonempty string":
				b = NewBatch()
				b.Rows = 1
				schema = NewSchema(Field{Name: "s", Type: TypeString})
				b.Strings["s"] = &StringViewVector{}
			}
			manager := NewBudgetManager(1)
			if got, err := EncodeColumnarRun(context.Background(), schema, b, manager); got != nil || err == nil || errors.Is(err, ErrBudgetExceeded) || manager.Used() != 0 {
				t.Fatal("must reject before reservation", got, err, manager.Used())
			}
		})
	}
}

func TestCloneBatchRangeListNonzeroBaseAndHiddenNullPayload(t *testing.T) {
	b := NewBatch()
	b.Rows = 4
	b.StringLists["list"] = &StringListVector{
		Offsets: []uint32{0, 1, 1, 2, 3}, Validity: Bitmap{0b1011}, Encoding: EncodingView,
		Values: &StringViewVector{Offsets: []uint32{2, 3, 5, 6}, Data: []byte("xxABBZtrailing"), Validity: Bitmap{0b101}, Encoding: EncodingView},
	}
	if err := b.StringLists["list"].Validate(4); err != nil {
		t.Fatal(err)
	}
	for _, r := range [][2]int{{1, 1}, {1, 2}, {2, 1}, {3, 1}, {4, 0}} {
		got, err := CloneBatchRange(b, r[0], r[1])
		if err != nil {
			t.Fatal(err)
		}
		estimate, err := EstimateBatchRangeBytes(b, r[0], r[1])
		if err != nil || estimate != BatchBytes(got) {
			t.Fatal(estimate, BatchBytes(got), err)
		}
		out := got.StringLists["list"]
		if err := out.Validate(r[1]); err != nil {
			t.Fatal(err)
		}
		for row := 0; row < r[1]; row++ {
			original := b.StringLists["list"]
			srcRow := r[0] + row
			if out.IsValid(row) != original.IsValid(srcRow) {
				t.Fatal("parent NULL changed")
			}
			// A raw range clone keeps hidden physical values beneath NULL parents;
			// canonical struct gathers may elide them. Neither changes logical NULLs.
			start, end := int(original.Offsets[srcRow]), int(original.Offsets[srcRow+1])
			outStart := int(out.Offsets[row])
			for i := 0; i < end-start; i++ {
				if out.Values.IsValid(outStart+i) != original.Values.IsValid(start+i) || string(out.Values.ValueBytes(outStart+i)) != string(original.Values.ValueBytes(start+i)) {
					t.Fatal("child range changed")
				}
			}
		}
		got.ReleaseOwned()
	}
}
