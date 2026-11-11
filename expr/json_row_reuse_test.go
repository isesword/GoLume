package expr

import (
	"context"
	"encoding/json"
	"fmt"
	"gocolumn/memory"
	"testing"
)

func BenchmarkJSONDecodeRowScratch(b *testing.B) {
	for _, tc := range []struct {
		name, payload string
		fields        []memory.Field
	}{
		{"numeric", `{"a":1,"b":-200,"c":3000000000,"d":4.5,"e":true}`, []memory.Field{{Name: "a", Type: memory.TypeInt8}, {Name: "b", Type: memory.TypeInt16}, {Name: "c", Type: memory.TypeUint64}, {Name: "d", Type: memory.TypeFloat32}, {Name: "e", Type: memory.TypeBool}}},
		{"mixed", `{"id":1,"name":"hello world","tags":["alpha",null,"beta"],"score":3.14}`, []memory.Field{{Name: "id", Type: memory.TypeInt64}, {Name: "name", Type: memory.TypeString}, memory.ListStringField("tags", true, true), {Name: "score", Type: memory.TypeFloat64}}},
		{"nulls", `null`, []memory.Field{{Name: "id", Type: memory.TypeInt64}, {Name: "name", Type: memory.TypeString}, memory.ListStringField("tags", true, true), {Name: "score", Type: memory.TypeFloat64}}},
	} {
		for _, rows := range []int{1, 256, 4096} {
			b.Run(fmt.Sprintf("%s/%d", tc.name, rows), func(b *testing.B) {
				payloads := make([]string, rows)
				for i := range payloads {
					payloads[i] = tc.payload
				}
				input := scalarRegister{typ: memory.TypeString, strView: testStringVector(payloads, nil)}
				manager := memory.NewBudgetManager(64 << 20)
				out := scalarRegister{acquire: func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }}
				b.Cleanup(func() {
					if out.jsonReservation != nil {
						out.jsonReservation.Release()
					}
					if manager.Used() != 0 {
						b.Fatalf("leaked %d", manager.Used())
					}
				})
				b.ReportAllocs()
				b.SetBytes(int64(rows * len(tc.payload)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := evalJSONDecode(&out, &input, tc.fields, rows); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// This workload contains no non-null decoder payload. The per-row allowance
// leaves headroom for the stdlib decoder while catching rebuilt vector metadata.
func TestJSONDecodeNullRowsAllocationBound(t *testing.T) {
	const rows = 256
	fields := []memory.Field{{Name: "n", Type: memory.TypeInt64}, {Name: "s", Type: memory.TypeString}, memory.ListStringField("items", true, true)}
	payloads := make([]string, rows)
	for i := range payloads {
		payloads[i] = "null"
	}
	input := scalarRegister{typ: memory.TypeString, strView: testStringVector(payloads, nil)}
	var out scalarRegister
	allocs := testing.AllocsPerRun(3, func() {
		if err := evalJSONDecode(&out, &input, fields, rows); err != nil {
			panic(err)
		}
	})
	if allocs > float64(rows*16+256) {
		t.Fatalf("JSON row scratch recreated: allocations=%g > %d", allocs, rows*16+256)
	}
}

func TestJSONScratchMetadataReuseAndPayloadRelease(t *testing.T) {
	fields := []memory.Field{{Name: "n", Type: memory.TypeInt64, Nullable: true}, {Name: "s", Type: memory.TypeString, Nullable: true}, memory.ListStringField("items", true, true)}
	scratch := &memory.StructVector{Rows: 1, Fields: fields, Values: memory.NewBatch(), Encoding: memory.EncodingView}
	scratch.Values.Rows = 1
	output := &memory.StructVector{Fields: fields, Values: memory.NewBatch(), Encoding: memory.EncodingView}
	object := map[string]json.RawMessage{"n": []byte("7"), "s": []byte(`"first"`), "items": []byte(`["one",null,"three"]`)}
	if err := jsonStructRow(scratch, fields, object, true, 0, nil); err != nil {
		t.Fatal(err)
	}
	n, s, list := scratch.Values.Int64s["n"], scratch.Values.Strings["s"], scratch.Values.StringLists["items"]
	if err := memory.AppendStructRow(output, scratch, 0, true); err != nil {
		t.Fatal(err)
	}
	clearJSONRowPayloads(scratch)
	if s.Data != nil || list.Values.Data != nil || list.Values.Offsets != nil || list.Values.Validity != nil {
		t.Fatal("scratch retained variable payload")
	}
	for _, obj := range []map[string]json.RawMessage{
		{}, {"n": []byte("9"), "s": []byte(`"after"`), "items": []byte(`["valid"]`)},
		{"n": []byte("11"), "s": []byte(`""`), "items": []byte(`[]`)},
	} {
		if err := jsonStructRow(scratch, fields, obj, true, 1, nil); err != nil {
			t.Fatal(err)
		}
		if n != scratch.Values.Int64s["n"] || s != scratch.Values.Strings["s"] || list != scratch.Values.StringLists["items"] {
			t.Fatal("scratch metadata replaced")
		}
		if err := memory.AppendStructRow(output, scratch, 0, true); err != nil {
			t.Fatal(err)
		}
		clearJSONRowPayloads(scratch)
	}
	if err := output.Validate(memory.StructField("decoded", true, fields...), 4); err != nil {
		t.Fatal(err)
	}
	if string(output.Values.Strings["s"].ValueBytes(0)) != "first" || string(output.Values.Strings["s"].ValueBytes(2)) != "after" {
		t.Fatal("output aliases scratch")
	}
	if n.Validity != nil && !memory.IsSet(n.Validity, 0) {
		t.Fatal("validity did not recover after NULL")
	}
}
