package expr

import (
	"context"
	"errors"
	"testing"

	"gocolumn/memory"
)

// Exercise the public workspace, ownership-preserving copy, and spill codec,
// rather than only inspecting the private JSON evaluator's result maps.
func TestJSONEmptyReviewWorkspaceAlternationAndCodec(t *testing.T) {
	ctx := context.Background()
	fields := []memory.Field{{Name: "n", Type: memory.TypeInt64}, {Name: "s", Type: memory.TypeString}, memory.ListStringField("tags", false, true)}
	field := memory.StructField("decoded", true, fields...)
	schema := memory.NewSchema(field)
	program, err := Compile(memory.NewSchema(memory.Field{Name: "payload", Type: memory.TypeString, Encoding: memory.EncodingView}), JSONDecode(Col("payload"), memory.NewSchema(fields...)))
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(4 << 20)
	w, err := NewWorkspace(ctx, program, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		w.Close()
		if manager.Used() != 0 {
			t.Fatalf("leaked %d bytes", manager.Used())
		}
	}()
	var saved []*memory.StructVector
	for _, rows := range []int{0, 1, 0, 1, 0} {
		input := memory.NewBatch()
		input.Rows = rows
		payloads := []string{}
		if rows != 0 {
			payloads = append(payloads, `{"n":7,"s":"value","tags":["a",null,"b"]}`)
		}
		input.Strings["payload"] = testStringVector(payloads, nil)
		values, err := w.Evaluate(ctx, input, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		got := values[0].Struct
		if err := got.Validate(field, rows); err != nil {
			t.Fatal(err)
		}
		copy, err := memory.CloneStructRange(got, 0, rows)
		if err != nil {
			t.Fatal(err)
		}
		if err := copy.Validate(field, rows); err != nil {
			t.Fatal(err)
		}
		saved = append(saved, copy)
		batch := memory.NewBatch()
		batch.Rows = rows
		batch.Structs["decoded"] = got
		encoded, err := memory.EncodeColumnarRun(ctx, schema, batch, manager)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := memory.DecodeColumnarRun(ctx, schema, &memory.RestoredSpill{Object: memory.SpillObject{Codec: memory.ColumnarRunCodec, Payload: encoded.Payload}}, manager)
		if err != nil {
			encoded.Close()
			t.Fatal(err)
		}
		if err := decoded.Batch.Structs["decoded"].Validate(field, rows); err != nil {
			t.Fatal(err)
		}
		decoded.Close()
		encoded.Close()
	}
	for _, got := range saved {
		if err := got.Validate(field, got.Rows); err != nil {
			t.Fatal(err)
		}
		if got.Rows == 1 && got.Values.Int64s["n"].Values[0] != 7 {
			t.Fatal("copied payload changed")
		}
	}
}

func TestJSONEmptyReviewMalformedLiteralIsNotEvaluated(t *testing.T) {
	program, err := Compile(memory.NewSchema(), JSONDecode(Str("not JSON"), memory.NewSchema(memory.Field{Name: "required", Type: memory.TypeInt8})))
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(1 << 20)
	w, err := NewWorkspace(context.Background(), program, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		w.Close()
		if manager.Used() != 0 {
			t.Fatal("budget leak")
		}
	}()
	input := memory.NewBatch()
	values, err := w.Evaluate(context.Background(), input, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if err := values[0].Struct.Validate(memory.StructField("", true, memory.Field{Name: "required", Type: memory.TypeInt8}), 0); err != nil {
		t.Fatal(err)
	}
	input.Rows = 1
	if _, err := w.Evaluate(context.Background(), input, testKernelSet()); err == nil {
		t.Fatal("nonempty malformed JSON accepted")
	}
}

func TestJSONEmptyReviewReservationAndCancellationRollback(t *testing.T) {
	fields := []memory.Field{{Name: "n", Type: memory.TypeInt64}, {Name: "s", Type: memory.TypeString}, memory.ListStringField("tags", true, true)}
	input := scalarRegister{typ: memory.TypeString, constant: true, strlit: `{"n":7,"s":"kept","tags":["x"]}`}
	for _, mode := range []string{"output", "scratch", "child", "commit"} {
		t.Run(mode, func(t *testing.T) {
			manager := memory.NewBudgetManager(1 << 20)
			out := scalarRegister{acquire: func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }}
			if err := evalJSONDecode(&out, &input, fields, 1); err != nil {
				t.Fatal(err)
			}
			prior, used := out.strct, manager.Used()
			defer func() {
				out.jsonReservation.Release()
				if manager.Used() != 0 {
					t.Fatal("budget leak")
				}
			}()
			calls := 0
			sentinel := errors.New("injected reservation rejection")
			if mode == "output" || mode == "scratch" {
				out.acquire = func(n int64) (memory.Reservation, error) {
					calls++
					if calls == 1 && mode == "output" || calls == 2 && mode == "scratch" {
						return nil, sentinel
					}
					return manager.Reserve(context.Background(), n)
				}
			} else {
				out.checkContext = func() error {
					calls++
					if calls == 2 && mode == "child" || calls == len(fields)+2 && mode == "commit" {
						return context.Canceled
					}
					return nil
				}
			}
			if err := evalJSONDecode(&out, &input, fields, 0); err == nil {
				t.Fatal("expected failure")
			}
			if out.strct != prior || manager.Used() != used {
				t.Fatalf("failed decode changed prior result/accounting: used=%d want=%d", manager.Used(), used)
			}
			if out.strct.Values.Strings["s"].Len() != 1 {
				t.Fatal("previous result lost")
			}
		})
	}
}
