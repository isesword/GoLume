package memory

import (
	"bytes"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"testing"
)

func binaryDictionaryFixture() (Schema, *Batch) {
	b := NewBatch()
	b.Rows = 4
	b.DictStrings["b"] = &DictionaryStringVector{Indices: []uint32{0, 1, 99, 2}, Offsets: []uint32{0, 2, 2, 3}, Data: []byte{0, 255, 128}, Validity: Bitmap{11}, Encoding: EncodingDictionary, DictionaryDomain: "binary"}
	return NewSchema(Field{Name: "b", Type: TypeBinary, Encoding: EncodingDictionary, Nullable: true}), b
}

func TestArrowBinaryDictionaryRawAndRange(t *testing.T) {
	for _, dict := range []bool{false, true} {
		schema, b := binaryDictionaryFixture()
		if !dict {
			delete(b.DictStrings, "b")
			b.Strings["b"] = &StringViewVector{Offsets: []uint32{0, 2, 2, 2, 3}, Data: []byte{0, 255, 128}, Validity: Bitmap{11}}
			schema = NewSchema(Field{Name: "b", Type: TypeBinary, Nullable: true})
		}
		for _, r := range [][2]int{{0, 4}, {1, 3}, {2, 1}, {4, 0}} {
			rec, err := ArrowRecordRange(am.DefaultAllocator, schema, b, r[0], r[1])
			if err != nil {
				t.Fatal(dict, r, err)
			}
			if rec.Column(0).DataType().ID() != arrow.BINARY {
				t.Fatal("not binary")
			}
			a := rec.Column(0).(*array.Binary)
			want := [][]byte{{0, 255}, {}, nil, {128}}
			for i := 0; i < r[1]; i++ {
				row := r[0] + i
				if a.IsNull(i) != (row == 2) || (row != 2 && !bytes.Equal(a.Value(i), want[row])) {
					t.Fatalf("dict=%v range=%v row=%d", dict, r, row)
				}
			}
			rec.Release()
		}
	}
}

func TestArrowBinaryDictionaryInvalidStorage(t *testing.T) {
	cases := map[string]func(*DictionaryStringVector){
		"short-indices":  func(v *DictionaryStringVector) { v.Indices = v.Indices[:2] },
		"bad-code":       func(v *DictionaryStringVector) { v.Indices[0] = 99 },
		"decreasing":     func(v *DictionaryStringVector) { v.Offsets = []uint32{0, 2, 1, 3} },
		"out-of-bounds":  func(v *DictionaryStringVector) { v.Offsets[3] = 4 },
		"no-offset":      func(v *DictionaryStringVector) { v.Offsets = nil },
		"no-domain":      func(v *DictionaryStringVector) { v.DictionaryDomain = "" },
		"short-validity": func(v *DictionaryStringVector) { v.Validity = Bitmap{} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			for _, r := range [][2]int{{0, 4}, {1, 1}, {4, 0}} {
				schema, b := binaryDictionaryFixture()
				mutate(b.DictStrings["b"])
				rec, err := ArrowRecordRange(am.DefaultAllocator, schema, b, r[0], r[1])
				if err == nil {
					rec.Release()
					t.Fatal("invalid storage accepted", r)
				}
			}
		})
	}
}

func TestArrowBinaryDictionaryEmptyBatch(t *testing.T) {
	schema, b := binaryDictionaryFixture()
	b.Rows = 0
	v := b.DictStrings["b"]
	v.Indices = nil
	v.Offsets = []uint32{0}
	v.Data = nil
	v.Validity = nil
	rec, err := ArrowRecord(am.DefaultAllocator, schema, b)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if rec.NumRows() != 0 {
		t.Fatal(rec.NumRows())
	}
}

func TestArrowBinaryDictionaryBitmapWordBoundary(t *testing.T) {
	for _, rows := range []int{9, 64, 65} {
		schema, b := binaryDictionaryFixture()
		b.Rows = rows
		v := b.DictStrings["b"]
		v.Indices = make([]uint32, rows)
		v.Validity = make(Bitmap, WordsFor(rows))
		for i := range v.Validity {
			v.Validity[i] = ^uint64(0)
		}
		rec, err := ArrowRecord(am.DefaultAllocator, schema, b)
		if err != nil {
			t.Fatal(rows, err)
		}
		rec.Release()
		v.Validity = v.Validity[:len(v.Validity)-1]
		rec, err = ArrowRecordRange(am.DefaultAllocator, schema, b, rows, 0)
		if err == nil {
			rec.Release()
			t.Fatal("short validity accepted", rows)
		}
	}
}

func TestArrowBinaryPlainKeepsPrecedence(t *testing.T) {
	schema, b := binaryDictionaryFixture()
	b.DictStrings["b"].Indices = nil
	b.Strings["b"] = &StringViewVector{Offsets: []uint32{0, 1, 2, 3, 4}, Data: []byte{0, 1, 2, 3}}
	rec, err := ArrowRecord(am.DefaultAllocator, schema, b)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if !bytes.Equal(rec.Column(0).(*array.Binary).Value(3), []byte{3}) {
		t.Fatal("plain precedence changed")
	}
}
