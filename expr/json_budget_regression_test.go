package expr

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"gocolumn/memory"
)

func TestJSONBudgetRejectsBeforePayloadAllocation(t *testing.T) {
	// Unknown fields must count too: they still allocate RawMessage/map storage.
	raw := `{"value":"ok","unknown":"` + strings.Repeat("x", 1<<20) + `"}`
	input := scalarRegister{typ: memory.TypeString, strView: testStringVector([]string{raw}, nil)}
	fields := []memory.Field{{Name: "value", Type: memory.TypeString}}
	manager := memory.NewBudgetManager(1)
	out := scalarRegister{acquire: func(n int64) (memory.Reservation, error) {
		return manager.Reserve(context.Background(), n)
	}}
	if err := evalJSONDecode(&out, &input, fields, 1); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("error=%v", err)
	}
	allocations := testing.AllocsPerRun(10, func() {
		if err := evalJSONDecode(&out, &input, fields, 1); !errors.Is(err, memory.ErrBudgetExceeded) {
			panic(err)
		}
	})
	if allocations != 0 || manager.Used() != 0 || out.strct != nil {
		t.Fatalf("rejected JSON allocated: allocations=%g used=%d result=%v", allocations, manager.Used(), out.strct)
	}
	input.constant, input.strlit = true, raw
	allocations = testing.AllocsPerRun(10, func() {
		if err := evalJSONDecode(&out, &input, fields, 1); !errors.Is(err, memory.ErrBudgetExceeded) {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("rejected constant JSON copied payload: allocations=%g", allocations)
	}
}

func TestJSONScratchDenialRollsBackOutputReservation(t *testing.T) {
	input := scalarRegister{typ: memory.TypeString, strView: testStringVector([]string{`{"v":"abc"}`}, nil)}
	fields := []memory.Field{{Name: "v", Type: memory.TypeString}}
	out := scalarRegister{}
	outputBytes, _, err := jsonAllocationSizes(&out, &input, len(fields), 1)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(outputBytes)
	out.acquire = func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }
	if err := evalJSONDecode(&out, &input, fields, 1); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("error=%v", err)
	}
	if manager.Used() != 0 || out.strct != nil {
		t.Fatalf("failed scratch retained output: used=%d", manager.Used())
	}
}

func TestJSONReservationReplacementAndErrorLifetime(t *testing.T) {
	fields := []memory.Field{
		{Name: "v", Type: memory.TypeString, Nullable: true},
		{Name: "n", Type: memory.TypeInt64, Nullable: true},
		memory.ListStringField("items", true, true),
	}
	input := scalarRegister{typ: memory.TypeString, strView: testStringVector([]string{
		`{"v":"` + strings.Repeat("x", 4097) + `","n":12,"items":["a",null,"long string"]}`,
		`null`, `{"items":[]}`, `{"v":"tail","items":["",null]}`,
	}, nil)}
	manager := memory.NewBudgetManager(4 << 20)
	out := scalarRegister{acquire: func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }}
	defer func() {
		if out.jsonReservation != nil {
			out.jsonReservation.Release()
		}
	}()
	outputBytes, _, err := jsonAllocationSizes(&out, &input, len(fields), 4)
	if err != nil {
		t.Fatal(err)
	}
	for run := 0; run < 5; run++ {
		if err := evalJSONDecode(&out, &input, fields, 4); err != nil {
			t.Fatal(err)
		}
		if manager.Used() != outputBytes {
			t.Fatalf("run=%d used=%d want output-only=%d", run, manager.Used(), outputBytes)
		}
		if err := out.strct.Validate(memory.StructField("result", true, fields...), 4); err != nil {
			t.Fatal(err)
		}
		if buffers := jsonRetainedBufferCapacity(out.strct); buffers > outputBytes {
			t.Fatalf("retained capacity=%d exceeds pre-reserved=%d", buffers, outputBytes)
		}
	}
	old := out.strct
	input.strView = testStringVector([]string{`{"n":"invalid"}`}, nil)
	if err := evalJSONDecode(&out, &input, fields, 1); err == nil {
		t.Fatal("invalid input accepted")
	}
	if out.strct != old || manager.Used() != outputBytes {
		t.Fatalf("failed replacement changed prior result or leaked: used=%d", manager.Used())
	}
}

func TestJSONEstimateChecksShapeAndOverflow(t *testing.T) {
	var out, input scalarRegister
	if _, _, err := jsonAllocationSizes(&out, &input, -1, 0); err == nil {
		t.Fatal("negative shape accepted")
	}
	n := int64(math.MaxInt64 - 1)
	if addJSONAllocation(&n, 2, 1) {
		t.Fatal("overflow accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out.checkContext = ctx.Err
	input.typ = memory.TypeString
	input.constant, input.strlit = true, `{"value":"x"}`
	if _, _, err := jsonAllocationSizes(&out, &input, 1, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func TestJSONCancellationReleasesConstructionReservations(t *testing.T) {
	input := scalarRegister{typ: memory.TypeString, strView: testStringVector([]string{`{"items":["a","b","c"]}`}, nil)}
	fields := []memory.Field{memory.ListStringField("items", false, false)}
	manager := memory.NewBudgetManager(1 << 20)
	checks := 0
	out := scalarRegister{
		acquire: func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) },
		checkContext: func() error {
			checks++
			// Reach the list iteration after sizing, reservation, and decoding.
			if checks >= 5 {
				return context.Canceled
			}
			return nil
		},
	}
	if err := evalJSONDecode(&out, &input, fields, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if manager.Used() != 0 || out.strct != nil || out.jsonReservation != nil {
		t.Fatalf("cancel retained construction: used=%d", manager.Used())
	}
}

func jsonRetainedBufferCapacity(v *memory.StructVector) int64 {
	n := int64(cap(v.Validity)) * 8
	for _, x := range v.Values.Int8s {
		n += int64(cap(x.Values))*1 + int64(cap(x.Validity))*8
	}
	for _, x := range v.Values.Int16s {
		n += int64(cap(x.Values))*2 + int64(cap(x.Validity))*8
	}
	for _, x := range v.Values.Int32s {
		n += int64(cap(x.Values))*4 + int64(cap(x.Validity))*8
	}
	for _, x := range v.Values.Uint8s {
		n += int64(cap(x.Values))*1 + int64(cap(x.Validity))*8
	}
	for _, x := range v.Values.Uint16s {
		n += int64(cap(x.Values))*2 + int64(cap(x.Validity))*8
	}
	for _, x := range v.Values.Uint32s {
		n += int64(cap(x.Values))*4 + int64(cap(x.Validity))*8
	}
	for _, x := range v.Values.Uint64s {
		n += int64(cap(x.Values))*8 + int64(cap(x.Validity))*8
	}
	for _, x := range v.Values.Float32s {
		n += int64(cap(x.Values))*4 + int64(cap(x.Validity))*8
	}
	for _, x := range v.Values.Float64s {
		n += int64(cap(x.Values))*8 + int64(cap(x.Validity))*8
	}
	for _, x := range v.Values.Int64s {
		n += int64(cap(x.Values)+cap(x.Validity)) * 8
	}
	for _, x := range v.Values.Strings {
		n += int64(cap(x.Offsets))*4 + int64(cap(x.Data)) + int64(cap(x.Validity))*8
	}
	for _, x := range v.Values.StringLists {
		n += int64(cap(x.Offsets))*4 + int64(cap(x.Validity))*8
		n += int64(cap(x.Values.Offsets))*4 + int64(cap(x.Values.Data)) + int64(cap(x.Values.Validity))*8
	}
	return n
}

func BenchmarkJSONDecodeBudgetAccounting(b *testing.B) {
	fields := []memory.Field{{Name: "v", Type: memory.TypeString}, memory.ListStringField("items", false, false)}
	values := make([]string, 64)
	for i := range values {
		values[i] = `{"v":"` + strings.Repeat("x", 128) + `","items":["a","bb","ccc"]}`
	}
	input := scalarRegister{typ: memory.TypeString, strView: testStringVector(values, nil)}
	for _, managed := range []bool{false, true} {
		name := "unmanaged"
		if managed {
			name = "managed"
		}
		b.Run(name, func(b *testing.B) {
			manager := memory.NewBudgetManager(8 << 20)
			out := scalarRegister{}
			if managed {
				out.acquire = func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }
			}
			defer func() {
				if out.jsonReservation != nil {
					out.jsonReservation.Release()
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := evalJSONDecode(&out, &input, fields, len(values)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestJSONNativeBudgetDenialAndReplacementOverlap(t *testing.T) {
	fields := []memory.Field{
		{Name: "i8", Type: memory.TypeInt8}, {Name: "i16", Type: memory.TypeInt16},
		{Name: "u8", Type: memory.TypeUint8}, {Name: "u16", Type: memory.TypeUint16},
		{Name: "f32", Type: memory.TypeFloat32},
	}
	input := scalarRegister{typ: memory.TypeString, strView: testStringVector([]string{`{"i8":-128,"i16":32767,"u8":255,"u16":65535,"f32":1.25}`}, nil)}
	var shape scalarRegister
	output, scratch, err := jsonAllocationSizes(&shape, &input, len(fields), 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int64{output - 1, output + scratch - 1, output + scratch} {
		manager := memory.NewBudgetManager(limit)
		out := scalarRegister{acquire: func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }}
		err := evalJSONDecode(&out, &input, fields, 1)
		if limit < output+scratch {
			if !errors.Is(err, memory.ErrBudgetExceeded) || manager.Used() != 0 || out.strct != nil {
				t.Fatalf("limit=%d err=%v used=%d", limit, err, manager.Used())
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if manager.Used() != output || jsonRetainedBufferCapacity(out.strct) > output {
			t.Fatal("native output reservation does not cover retained capacities")
		}
		old := out.strct
		if err := evalJSONDecode(&out, &input, fields, 1); !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatalf("old/new overlap was not enforced: %v", err)
		}
		if out.strct != old || manager.Used() != output {
			t.Fatal("failed replacement changed output or leaked reservation")
		}
		out.strct = nil
		out.validity = nil
		out.jsonReservation.Release()
		if manager.Used() != 0 {
			t.Fatal("native result release leaked budget")
		}
	}
}
