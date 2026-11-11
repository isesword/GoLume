package memory

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func emptyStructGatherFixture() *StructVector {
	b := NewBatch()
	b.Rows = 3
	fields := []Field{}
	for _, typ := range []DataType{TypeInt8, TypeInt16, TypeInt32, TypeInt64, TypeUint8, TypeUint16, TypeUint32, TypeUint64, TypeFloat32, TypeFloat64, TypeBool, TypeDate32, TypeDate64, TypeTimestamp, TypeTime32, TypeTime64, TypeDuration, TypeString, TypeList} {
		name := typ.String()
		field := Field{Name: name, Type: typ, Nullable: true, Encoding: EncodingPlain}
		switch typ {
		case TypeInt8:
			b.Int8s[name] = &Int8Vector{Values: []int8{7, 8, 9}, Validity: Bitmap{3}}
		case TypeInt16:
			b.Int16s[name] = &Int16Vector{Values: []int16{7, 8, 9}, Validity: Bitmap{3}}
		case TypeInt32, TypeDate32, TypeTime32:
			b.Int32s[name] = &Int32Vector{Values: []int32{7, 8, 9}, Validity: Bitmap{3}}
		case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
			b.Int64s[name] = &Int64Vector{Values: []int64{7, 8, 9}, Validity: Bitmap{3}}
		case TypeUint8:
			b.Uint8s[name] = &Uint8Vector{Values: []uint8{7, 8, 9}, Validity: Bitmap{3}}
		case TypeUint16:
			b.Uint16s[name] = &Uint16Vector{Values: []uint16{7, 8, 9}, Validity: Bitmap{3}}
		case TypeUint32:
			b.Uint32s[name] = &Uint32Vector{Values: []uint32{7, 8, 9}, Validity: Bitmap{3}}
		case TypeBool:
			b.Uint32s[name] = &Uint32Vector{Values: []uint32{1, 0, 1}, Validity: Bitmap{3}}
		case TypeUint64:
			b.Uint64s[name] = &Uint64Vector{Values: []uint64{7, 8, 9}, Validity: Bitmap{3}}
		case TypeFloat32:
			b.Float32s[name] = &Float32Vector{Values: []float32{7, 8, 9}, Validity: Bitmap{3}}
		case TypeFloat64:
			b.Float64s[name] = &Float64Vector{Values: []float64{7, 8, 9}, Validity: Bitmap{3}}
		case TypeString:
			field.Encoding = EncodingView
			b.Strings[name] = &StringViewVector{Offsets: []uint32{0, 1, 7, 13}, Data: []byte("aHIDDENHIDDEN"), Validity: Bitmap{3}, Encoding: EncodingView}
		case TypeList:
			field = ListStringField(name, true, true)
			b.StringLists[name] = &StringListVector{Offsets: []uint32{0, 2, 3, 3}, Values: &StringViewVector{Offsets: []uint32{0, 1, 1, 7}, Data: []byte("aHIDDEN"), Validity: Bitmap{5}, Encoding: EncodingView}, Validity: Bitmap{3}, Encoding: EncodingView}
		}
		fields = append(fields, field)
	}
	return &StructVector{Rows: 3, Fields: fields, Values: b, Validity: Bitmap{5}, Encoding: EncodingView}
}

func checkEmptyStructGather(t *testing.T, src, got *StructVector) {
	t.Helper()
	if got == nil || got.Rows != 0 || got.Values == nil || got.Values.Rows != 0 || len(got.Validity) != 0 || got.Encoding != EncodingView {
		t.Fatalf("invalid empty parent: %+v", got)
	}
	if !reflect.DeepEqual(got.Fields, src.Fields) || &got.Fields[0] == &src.Fields[0] {
		t.Fatal("child schema lost or top-level fields aliased")
	}
	if err := got.Validate(StructField("s", true, src.Fields...), 0); err != nil {
		t.Fatal(err)
	}
	// Every declared physical vector exists, with no selected values or NULL bits.
	b := reflect.ValueOf(got.Values).Elem()
	count := 0
	for i := 0; i < b.NumField(); i++ {
		m := b.Field(i)
		if m.Kind() != reflect.Map {
			continue
		}
		for _, key := range m.MapKeys() {
			v := m.MapIndex(key).Elem()
			count++
			if v.FieldByName("Validity").Len() != 0 {
				t.Fatal("empty child owns validity words")
			}
			values := v.FieldByName("Values")
			if values.IsValid() && values.Kind() == reflect.Slice && values.Len() != 0 {
				t.Fatal("empty scalar owns selected values")
			}
		}
	}
	if count != len(src.Fields) {
		t.Fatalf("physical vectors=%d declared fields=%d", count, len(src.Fields))
	}
	s, l := got.Values.Strings["string"], got.Values.StringLists["list"]
	if !reflect.DeepEqual(s.Offsets, []uint32{0}) || len(s.Data) != 0 || !reflect.DeepEqual(l.Offsets, []uint32{0}) || l.Values == nil || !reflect.DeepEqual(l.Values.Offsets, []uint32{0}) || len(l.Values.Data) != 0 {
		t.Fatal("missing empty offsets or copied row payload")
	}
	if &s.Offsets[0] == &src.Values.Strings["string"].Offsets[0] || &l.Offsets[0] == &src.Values.StringLists["list"].Offsets[0] || &l.Values.Offsets[0] == &src.Values.StringLists["list"].Values.Offsets[0] {
		t.Fatal("empty offsets alias source")
	}
	schema := NewSchema(StructField("s", true, src.Fields...))
	batch := NewBatch()
	batch.Structs["s"] = got
	manager := NewBudgetManager(1 << 20)
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, manager)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}, manager)
	if err != nil {
		encoded.Close()
		t.Fatal(err)
	}
	if err := decoded.Batch.Structs["s"].Validate(schema.Fields[0], 0); err != nil {
		t.Fatal(err)
	}
	decoded.Close()
	encoded.Close()
	if manager.Used() != 0 {
		t.Fatal("codec reservation leaked")
	}
}

func TestStructEmptyGatherPreservesEveryDeclaredChild(t *testing.T) {
	for _, encoding := range []Encoding{EncodingPlain, EncodingView} {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("encoding=%s/nullable=%t", encoding, nullable), func(t *testing.T) {
				src := emptyStructGatherFixture()
				for i := range src.Fields {
					src.Fields[i].Nullable = nullable
					if src.Fields[i].Type != TypeString && src.Fields[i].Type != TypeList {
						src.Fields[i].Encoding = encoding
					}
				}
				for i, rows := range [][]int{nil, {}} {
					t.Run(fmt.Sprintf("take-%d", i), func(t *testing.T) {
						got, err := TakeStructRows(src, rows)
						if err != nil {
							t.Fatal(err)
						}
						checkEmptyStructGather(t, src, got)
						for _, field := range src.Fields {
							if field.Type == TypeString || field.Type == TypeList {
								continue
							}
							values := reflect.ValueOf(got.Values).Elem()
							for j := 0; j < values.NumField(); j++ {
								m := values.Field(j)
								if m.Kind() != reflect.Map {
									continue
								}
								v := m.MapIndex(reflect.ValueOf(field.Name))
								if v.IsValid() && Encoding(v.Elem().FieldByName("Encoding").Uint()) != field.Encoding {
									t.Fatalf("%s physical encoding changed", field.Name)
								}
							}
						}
					})
				}
				for _, offset := range []int{0, 1, 3} {
					t.Run(fmt.Sprintf("range-%d", offset), func(t *testing.T) {
						got, err := CloneStructRange(src, offset, 0)
						if err != nil {
							t.Fatal(err)
						}
						checkEmptyStructGather(t, src, got)
					})
				}
				empty, err := CloneStructRange(src, 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				// A zero-row String child may legally omit offsets; normalize it.
				empty.Values.Strings["string"].Offsets = nil
				empty.Values.StringLists["list"].Values.Offsets = nil
				batch := NewBatch()
				batch.Structs["s"] = empty
				clone, err := CloneBatch(batch)
				if err != nil {
					t.Fatal(err)
				}
				// Use the populated fixture for sentinel independence assertions.
				checkEmptyStructGather(t, src, clone.Structs["s"])
			})
		}
	}
}

func TestStructEmptyGatherOwnsDictionaryAndSelectsDeclaredMaps(t *testing.T) {
	for _, shadowed := range []bool{false, true} {
		src := emptyStructGatherFixture()
		src.Values.DictStrings["string"] = &DictionaryStringVector{Indices: []uint32{0, 1, 0}, Offsets: []uint32{0, 1, 2}, Data: []byte("ab"), DictionaryDomain: "stable", Encoding: EncodingDictionary}
		if !shadowed {
			delete(src.Values.Strings, "string")
		}
		src.Values.Int64s["extra"] = &Int64Vector{Values: []int64{91, 92, 93}}
		for _, rows := range [][]int{nil, {0, 1, 2}} {
			got, err := TakeStructRows(src, rows)
			if err != nil {
				t.Fatal(err)
			}
			if got.Values.Int64s["extra"] != nil {
				t.Fatal("gather copied undeclared runtime child")
			}
			if shadowed {
				if got.Values.Strings["string"] == nil || got.Values.DictStrings["string"] != nil {
					t.Fatal("string runtime map did not take precedence")
				}
			} else {
				d := got.Values.DictStrings["string"]
				original := src.Values.DictStrings["string"]
				if d == nil || d.DictionaryDomain != "stable" || d.Encoding != EncodingDictionary || !reflect.DeepEqual(d.Offsets, original.Offsets) || string(d.Data) != "ab" || &d.Data[0] == &original.Data[0] || &d.Offsets[0] == &original.Offsets[0] {
					t.Fatal("dictionary table ownership or metadata lost")
				}
				original.Data[0] = 'z'
				original.Offsets[1] = 0
				if string(d.Data) != "ab" || d.Offsets[1] != 1 {
					t.Fatal("dictionary clone changed with source")
				}
				original.Data[0] = 'a'
				original.Offsets[1] = 1
			}
		}
	}
}

func TestStructEmptyGatherKeepsNullAndReorderedRows(t *testing.T) {
	src := emptyStructGatherFixture()
	rows := []int{2, 0, 1, 0}
	got, err := TakeStructRows(src, rows)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range rows {
		if got.IsValid(i) != (row != 1) {
			t.Fatal("parent NULL changed")
		}
		for _, field := range src.Fields {
			if batchFieldValid(got.Values, field, i) != (row == 0) {
				t.Fatalf("%s row %d child NULL changed", field.Name, i)
			}
		}
	}
	if !reflect.DeepEqual(got.Values.Strings["string"].Offsets, []uint32{0, 0, 1, 1, 2}) || string(got.Values.Strings["string"].Data) != "aa" {
		t.Fatal("hidden NULL string payload copied")
	}
	if !reflect.DeepEqual(got.Values.StringLists["list"].Offsets, []uint32{0, 0, 2, 2, 4}) || got.Values.StringLists["list"].Values.IsValid(1) {
		t.Fatal("list element NULL or selected values lost")
	}
	if _, err := TakeStructRows(src, []int{3}); err == nil {
		t.Fatal("out-of-range row accepted")
	}
	if _, err := TakeStructRows(nil, nil); err == nil {
		t.Fatal("nil struct accepted")
	}
}

var structGatherBenchmarkSink *StructVector

func BenchmarkStructGatherSelection(b *testing.B) {
	for _, count := range []int{0, 3, 1024} {
		b.Run(fmt.Sprintf("rows=%d", count), func(b *testing.B) {
			src := emptyStructGatherFixture()
			rows := make([]int, count)
			for i := range rows {
				rows[i] = i % src.Rows
			}
			b.ReportAllocs()
			for b.Loop() {
				got, err := TakeStructRows(src, rows)
				if err != nil {
					b.Fatal(err)
				}
				structGatherBenchmarkSink = got
			}
		})
	}
}
