package memory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func emptyListFixture(rows int, sentinel bool, validity Bitmap) (*Batch, Schema) {
	b := NewBatch()
	b.Rows = rows
	child := &StringViewVector{Encoding: EncodingView}
	if sentinel {
		child.Offsets = []uint32{0}
	}
	b.StringLists["list"] = &StringListVector{Offsets: make([]uint32, rows+1), Values: child, Validity: validity, Encoding: EncodingView}
	return b, NewSchema(ListStringField("list", true, true))
}

func TestCloneBatchRangeEmptyLists(t *testing.T) {
	for _, sentinel := range []bool{false, true} {
		for _, rows := range []int{0, 1, 3, 70} {
			for _, nulls := range []string{"none", "all", "mixed"} {
				t.Run(fmt.Sprintf("sentinel=%v/rows=%d/%s", sentinel, rows, nulls), func(t *testing.T) {
					var valid Bitmap
					if nulls != "none" {
						valid = make(Bitmap, WordsFor(rows))
						if nulls == "mixed" {
							for i := 0; i < rows; i += 2 {
								Set(valid, i)
							}
						}
					}
					b, schema := emptyListFixture(rows, sentinel, valid)
					v := b.StringLists["list"]
					if err := v.Validate(rows); err != nil {
						t.Fatal(err)
					}
					ranges := [][2]int{{0, rows}, {0, 0}, {rows, 0}}
					if rows > 0 {
						ranges = append(ranges, [2]int{rows - 1, 1}, [2]int{1, rows - 1})
					}
					for _, r := range ranges {
						estimate, err := EstimateBatchRangeBytes(b, r[0], r[1])
						if err != nil {
							t.Fatal(err)
						}
						got, err := CloneBatchRange(b, r[0], r[1])
						if err != nil {
							t.Fatalf("range %v: %v", r, err)
						}
						out := got.StringLists["list"]
						if err := out.Validate(r[1]); err != nil {
							t.Fatal(err)
						}
						if actual := BatchBytes(got); actual != estimate {
							t.Fatalf("range %v bytes=%d estimate=%d", r, actual, estimate)
						}
						if !reflect.DeepEqual(out.Values.Offsets, []uint32{0}) || len(out.Values.Data) != 0 {
							t.Fatalf("noncanonical empty child: %+v", out.Values)
						}
						for i := 0; i < r[1]; i++ {
							if out.IsValid(i) != v.IsValid(r[0]+i) {
								t.Fatalf("row %d NULL lost", i)
							}
						}
						if out == v || out.Values == v.Values {
							t.Fatal("clone aliases source descriptors")
						}
						roundTripEmptyList(t, schema, got)
						out.Offsets[0] = 123
						if v.Offsets[0] != 0 {
							t.Fatal("parent offsets alias")
						}
						out.Values.Offsets[0] = 456
						if sentinel && v.Values.Offsets[0] != 0 {
							t.Fatal("child offsets alias")
						}
						got.ReleaseOwned()
					}
					if !sentinel && v.Values.Offsets != nil {
						t.Fatal("source nil offsets changed")
					}
					roundTripEmptyList(t, schema, b)
				})
			}
		}
	}
}

func roundTripEmptyList(t *testing.T, schema Schema, b *Batch) {
	t.Helper()
	size, err := columnarRunSize(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewBudgetManager(int64(size))
	encoded, err := EncodeColumnarRun(context.Background(), schema, b, manager)
	if err != nil {
		t.Fatal(err)
	}
	if manager.Used() != int64(size) || len(encoded.Payload) != size {
		t.Fatal("encode exact admission mismatch")
	}
	estimate, err := EstimateColumnarRunRangeBytes(schema, b, 0, b.Rows)
	if err != nil || estimate < int64(size) {
		t.Fatal("encoded payload exceeds estimate", size, estimate, err)
	}
	denied := NewBudgetManager(int64(size - 1))
	if got, err := EncodeColumnarRun(context.Background(), schema, b, denied); got != nil || !errors.Is(err, ErrBudgetExceeded) || denied.Used() != 0 {
		t.Fatal("encode denial", got, err, denied.Used())
	}
	decodeManager := NewBudgetManager(0)
	restored := &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}
	decoded, err := DecodeColumnarRun(context.Background(), schema, restored, decodeManager)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Batch.Rows != b.Rows {
		t.Fatal("decoded row count")
	}
	for _, field := range schema.Fields {
		if err := validateRunField(decoded.Batch, field, b.Rows); err != nil {
			t.Fatal(err)
		}
	}
	if v := decoded.Batch.StringLists["list"]; v != nil {
		original := b.StringLists["list"]
		if !reflect.DeepEqual(v.Values.Offsets, []uint32{0}) {
			t.Fatal("decoded missing sentinel")
		}
		for row := 0; row < b.Rows; row++ {
			if v.IsValid(row) != original.IsValid(row) {
				t.Fatal("decode NULL lost")
			}
		}
	}
	payload := append([]byte(nil), encoded.Payload...)
	decoded.Close()
	decoded.Close()
	encoded.Close()
	encoded.Close()
	if manager.Used() != 0 || decodeManager.Used() != 0 {
		t.Fatal("close leaked budget")
	}
	for _, end := range []int{0, len(payload) - 1} {
		restored.Object.Payload = payload[:end]
		if got, err := DecodeColumnarRun(context.Background(), schema, restored, decodeManager); got != nil || !errors.Is(err, ErrSpillCorrupt) || decodeManager.Used() != 0 {
			t.Fatal("decode failure leaked", got, err, decodeManager.Used())
		}
	}
}

func TestCloneBatchRangeEmptyStringAndNestedList(t *testing.T) {
	for _, sentinel := range []bool{false, true} {
		child, schema := emptyListFixture(3, sentinel, Bitmap{0b101})
		b := NewBatch()
		b.Rows = 3
		field := StructField("record", true, schema.Fields...)
		b.Structs["record"] = &StructVector{Rows: 3, Fields: schema.Fields, Values: child, Validity: Bitmap{0b011}, Encoding: EncodingView}
		for _, r := range [][2]int{{0, 3}, {1, 2}, {0, 0}, {2, 0}, {3, 0}} {
			estimate, err := EstimateBatchRangeBytes(b, r[0], r[1])
			if err != nil {
				t.Fatal(err)
			}
			got, err := CloneBatchRange(b, r[0], r[1])
			if err != nil {
				t.Fatal(err)
			}
			if err := got.Structs["record"].Validate(field, r[1]); err != nil {
				t.Fatal(err)
			}
			if BatchBytes(got) > estimate {
				t.Fatal("nested clone undercount")
			}
			out := got.Structs["record"]
			for row := 0; row < r[1]; row++ {
				srcRow := r[0] + row
				if out.IsValid(row) != b.Structs["record"].IsValid(srcRow) || out.Values.StringLists["list"].IsValid(row) != (b.Structs["record"].IsValid(srcRow) && child.StringLists["list"].IsValid(srcRow)) {
					t.Fatal("inherited NULL lost")
				}
			}
			roundTripEmptyList(t, NewSchema(field), got)
			got.ReleaseOwned()
		}
	}
	for _, typ := range []DataType{TypeString, TypeBinary} {
		b := NewBatch()
		b.Strings["s"] = &StringViewVector{}
		estimate, err := EstimateBatchRangeBytes(b, 0, 0)
		if err != nil || estimate != 4 {
			t.Fatal(estimate, err)
		}
		got, err := CloneBatchRange(b, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Strings["s"].Offsets, []uint32{0}) {
			t.Fatal("empty string lacks sentinel")
		}
		schema := NewSchema(Field{Name: "s", Type: typ, Encoding: EncodingView})
		roundTripEmptyList(t, schema, b)
		roundTripEmptyList(t, schema, got)
		got.ReleaseOwned()
	}
}

func TestCloneBatchRangeRejectsMalformedListOffsets(t *testing.T) {
	tests := []struct {
		name  string
		alter func(*StringListVector)
	}{
		{"missing child", func(v *StringListVector) { v.Values = nil }},
		{"missing parent sentinel", func(v *StringListVector) { v.Offsets = nil }},
		{"missing nonempty child offsets", func(v *StringListVector) { v.Offsets = []uint32{0, 0, 1} }},
		{"interior parent spike", func(v *StringListVector) { v.Offsets = []uint32{0, 1, 0} }},
		{"interior parent decrease", func(v *StringListVector) { v.Offsets = []uint32{1, 0, 1}; v.Values.Offsets = []uint32{0, 0} }},
		{"interior child spike", func(v *StringListVector) { v.Offsets = []uint32{0, 1, 2}; v.Values.Offsets = []uint32{0, 1, 0} }},
		{"child past data", func(v *StringListVector) { v.Offsets = []uint32{0, 1, 1}; v.Values.Offsets = []uint32{0, 1} }},
		{"short parent validity", func(v *StringListVector) { v.Validity = Bitmap{} }},
		{"short child validity", func(v *StringListVector) {
			v.Offsets = []uint32{0, 1, 1}
			v.Values.Offsets = []uint32{0, 0}
			v.Values.Validity = Bitmap{}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := emptyListFixture(2, false, nil)
			tc.alter(b.StringLists["list"])
			// Estimation validates endpoints only, keeping repeated range probes
			// constant-time. Clone validates interiors before copying. Admission
			// for malformed interiors is safely released by its caller on error.
			if _, err := EstimateBatchRangeBytes(b, 0, 2); err == nil && !strings.HasPrefix(tc.name, "interior") {
				t.Fatal("estimator accepted malformed list bounds")
			}
			if got, err := CloneBatchRange(b, 0, 2); err == nil || got != nil {
				t.Fatal("clone accepted malformed list", got, err)
			}
		})
	}
	b := NewBatch()
	b.Rows = 2
	b.Strings["s"] = &StringViewVector{Offsets: []uint32{0, 1, 0}}
	if got, err := CloneBatchRange(b, 0, 2); err == nil || got != nil {
		t.Fatal("clone accepted decreasing string")
	}
}

func TestEstimateEmptyListRangeDoesNotAllocate(t *testing.T) {
	for _, sentinel := range []bool{false, true} {
		b, _ := emptyListFixture(16384, sentinel, nil)
		var size int64
		var err error
		if allocs := testing.AllocsPerRun(100, func() { size, err = EstimateBatchRangeBytes(b, 0, b.Rows) }); allocs != 0 || err != nil || size != 4*16385+4 {
			t.Fatal(allocs, size, err)
		}
	}
}

func BenchmarkEmptyListRange(b *testing.B) {
	for _, rows := range []int{1, 1024, 16384} {
		batch, schema := emptyListFixture(rows, true, nil)
		for _, operation := range []string{"Estimate", "Clone", "Encode"} {
			b.Run(fmt.Sprintf("%s/rows=%d", operation, rows), func(b *testing.B) {
				manager := NewBudgetManager(0)
				b.ReportAllocs()
				for b.Loop() {
					switch operation {
					case "Estimate":
						if _, err := EstimateBatchRangeBytes(batch, 0, rows); err != nil {
							b.Fatal(err)
						}
					case "Clone":
						got, err := CloneBatchRange(batch, 0, rows)
						if err != nil {
							b.Fatal(err)
						}
						got.ReleaseOwned()
					case "Encode":
						got, err := EncodeColumnarRun(context.Background(), schema, batch, manager)
						if err != nil {
							b.Fatal(err)
						}
						got.Close()
					}
				}
			})
		}
	}
}
