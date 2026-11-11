package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestJSONDecodeEmptyPreservesTypedChildren(t *testing.T) {
	fields := []memory.Field{{Name: "id", Type: memory.TypeInt64}, {Name: "text", Type: memory.TypeString}, memory.ListStringField("tags", true, true)}
	program, err := Compile(memory.NewSchema(memory.Field{Name: "payload", Type: memory.TypeString, Encoding: memory.EncodingView}), JSONDecode(Col("payload"), memory.NewSchema(fields...)))
	if err != nil {
		t.Fatal(err)
	}
	input := memory.NewBatch()
	input.Strings["payload"] = &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
	manager := memory.NewBudgetManager(1 << 20)
	w, err := NewWorkspace(context.Background(), program, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	got, err := w.Evaluate(context.Background(), input, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if err := got[0].Struct.Validate(memory.StructField("decoded", true, fields...), 0); err != nil {
		t.Fatal(err)
	}
}

func TestJSONEmptyAllSupportedChildren(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration, memory.TypeString, memory.TypeList}
	for _, nullable := range []bool{false, true} {
		for _, typ := range types {
			t.Run(fmt.Sprintf("%s/nullable%v", typ, nullable), func(t *testing.T) {
				field := memory.Field{Name: "x", Type: typ, Nullable: nullable}
				if typ == memory.TypeList {
					field = memory.ListStringField("x", nullable, nullable)
				}
				fields := []memory.Field{field}
				program, err := Compile(memory.NewSchema(), JSONDecode(Str("invalid JSON that has no demanded row"), memory.NewSchema(fields...)))
				if err != nil {
					t.Fatal(err)
				}
				manager := memory.NewBudgetManager(1 << 20)
				w, err := NewWorkspace(context.Background(), program, manager)
				if err != nil {
					t.Fatal(err)
				}
				output, err := w.Evaluate(context.Background(), memory.NewBatch(), testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
				got := output[0].Struct
				if err := got.Validate(memory.StructField("result", true, fields...), 0); err != nil {
					t.Fatal(err)
				}
				if got.Rows != 0 || got.Values.Rows != 0 {
					t.Fatal("empty JSON synthesized rows")
				}
				if typ == memory.TypeString {
					if !reflect.DeepEqual(got.Values.Strings["x"].Offsets, []uint32{0}) {
						t.Fatal("missing string offset sentinel")
					}
				}
				if typ == memory.TypeList {
					list := got.Values.StringLists["x"]
					if !reflect.DeepEqual(list.Offsets, []uint32{0}) || !reflect.DeepEqual(list.Values.Offsets, []uint32{0}) {
						t.Fatal("missing list/child offset sentinel")
					}
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				if used := manager.Used(); used != 0 {
					t.Fatalf("retained budget: %d", used)
				}
			})
		}
	}
}

func TestJSONEmptyBudgetAdmissionAndCancellation(t *testing.T) {
	fields := []memory.Field{{Name: "a", Type: memory.TypeInt64}, {Name: "b", Type: memory.TypeString}, memory.ListStringField("list", false, false)}
	empty := scalarRegister{typ: memory.TypeString, constant: true, strlit: "not JSON"}
	output, scratch, err := jsonAllocationSizes(&scalarRegister{}, &empty, len(fields), 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int64{1, output, output + scratch - 1} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			manager := memory.NewBudgetManager(limit)
			out := scalarRegister{acquire: func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }}
			allocs := testing.AllocsPerRun(3, func() {
				if err := evalJSONDecode(&out, &empty, fields, 0); !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatalf("expected budget error, got %v", err)
				}
				if out.strct != nil || manager.Used() != 0 {
					t.Fatal("denied allocation retained output or budget")
				}
			})
			if limit == 1 && allocs != 0 {
				t.Fatalf("pre-admission rejection allocated %g", allocs)
			}
		})
	}
	// Every checkpoint in the empty child initialization must unwind both
	// reservations and preserve a previously successful result.
	count := 0
	probe := scalarRegister{checkContext: func() error { count++; return nil }}
	if err := evalJSONDecode(&probe, &empty, fields, 0); err != nil {
		t.Fatal(err)
	}
	for stop := 1; stop <= count; stop++ {
		manager := memory.NewBudgetManager(1 << 20)
		out := scalarRegister{acquire: func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }}
		previousInput := scalarRegister{typ: memory.TypeString, constant: true, strlit: `{"a":7,"b":"keep","list":["alive"]}`}
		if err := evalJSONDecode(&out, &previousInput, fields, 1); err != nil {
			t.Fatal(err)
		}
		previous, used := out.strct, manager.Used()
		checks := 0
		out.checkContext = func() error {
			checks++
			if checks == stop {
				return context.Canceled
			}
			return nil
		}
		if err := evalJSONDecode(&out, &empty, fields, 0); !errors.Is(err, context.Canceled) {
			t.Fatalf("checkpoint %d got %v", stop, err)
		}
		if out.strct != previous || manager.Used() != used || string(previous.Values.Strings["b"].ValueBytes(0)) != "keep" {
			t.Fatalf("checkpoint %d lost prior result", stop)
		}
		out.checkContext = nil
		if err := evalJSONDecode(&out, &empty, fields, 0); err != nil {
			t.Fatal(err)
		}
		if err := out.strct.Validate(memory.StructField("empty", true, fields...), 0); err != nil {
			t.Fatal(err)
		}
		out.jsonReservation.Release()
		if manager.Used() != 0 {
			t.Fatal("retry leaked budget")
		}
	}
}
