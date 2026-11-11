package expr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gocolumn/memory"
)

// The expected values come directly from the generated source, not from a
// second run of the optimized decoder or its vector append helpers.
func TestJSONRowReviewScalarTransitions(t *testing.T) {
	cases := []struct {
		typ    memory.DataType
		member string
	}{
		{memory.TypeInt8, "Int8s"}, {memory.TypeInt16, "Int16s"}, {memory.TypeInt32, "Int32s"},
		{memory.TypeDate32, "Int32s"}, {memory.TypeTime32, "Int32s"}, {memory.TypeInt64, "Int64s"},
		{memory.TypeDate64, "Int64s"}, {memory.TypeTimestamp, "Int64s"}, {memory.TypeTime64, "Int64s"}, {memory.TypeDuration, "Int64s"},
		{memory.TypeUint8, "Uint8s"}, {memory.TypeUint16, "Uint16s"}, {memory.TypeUint32, "Uint32s"}, {memory.TypeUint64, "Uint64s"},
		{memory.TypeBool, "Uint32s"}, {memory.TypeFloat32, "Float32s"}, {memory.TypeFloat64, "Float64s"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.typ), func(t *testing.T) {
			const rows = 137
			payloads := make([]string, rows)
			valid := make([]bool, rows)
			for i := range payloads {
				valid[i] = i%7 != 3
				switch i % 7 {
				case 0:
					payloads[i] = "null"
				case 1:
					payloads[i] = `{"x":null}`
				case 2:
					payloads[i] = `{}`
				case 3:
					payloads[i] = "invalid but input is NULL"
				default:
					if tc.typ == memory.TypeBool {
						payloads[i] = fmt.Sprintf(`{"x":%t}`, i%2 == 0)
					} else {
						payloads[i] = fmt.Sprintf(`{"x":%d}`, i%100)
					}
				}
			}
			input := scalarRegister{typ: memory.TypeString, strView: testStringVector(payloads, valid)}
			input.validity = input.strView.Validity
			var out scalarRegister
			if err := evalJSONDecode(&out, &input, []memory.Field{{Name: "x", Type: tc.typ, Nullable: true}}, rows); err != nil {
				t.Fatal(err)
			}
			vec := reflect.ValueOf(out.strct.Values).Elem().FieldByName(tc.member).MapIndex(reflect.ValueOf("x"))
			vals := vec.Elem().FieldByName("Values")
			for i := 0; i < rows; i++ {
				wantValid := i%7 >= 4
				gotValid := vec.MethodByName("IsValid").Call([]reflect.Value{reflect.ValueOf(i)})[0].Bool()
				if gotValid != wantValid {
					t.Fatalf("row %d validity=%v", i, gotValid)
				}
				want := float64(0)
				if wantValid {
					want = float64(i % 100)
					if tc.typ == memory.TypeBool {
						want = 0
						if i%2 == 0 {
							want = 1
						}
					}
				}
				x := vals.Index(i)
				var got float64
				switch x.Kind() {
				case reflect.Float32, reflect.Float64:
					got = x.Float()
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					got = float64(x.Uint())
				default:
					got = float64(x.Int())
				}
				if got != want {
					t.Fatalf("row %d value=%v want %v", i, got, want)
				}
			}
		})
	}
}

func TestJSONRowReviewListOracleAndOwnership(t *testing.T) {
	fields := []memory.Field{{Name: "s", Type: memory.TypeString, Nullable: true}, memory.ListStringField("x", true, true)}
	payloads := []string{"null", `{}`, `{"s":"","x":[]}`, `{"s":"first","x":[null,"",null,"a"]}`, `{"s":"second","x":["b","c","d","e"]}`, `{"s":null,"x":null}`}
	for _, n := range []int{65, 1, 129, 0, 63, 64, 3, 257, 2} {
		a := make([]any, n)
		for i := range a {
			if i%3 != 0 {
				a[i] = fmt.Sprintf("item-%d-中", i)
			}
		}
		b, _ := json.Marshal(map[string]any{"s": strings.Repeat("z", n), "x": a})
		payloads = append(payloads, string(b))
		payloads = append(payloads, `{"s":"short","x":["fresh"]}`)
	}
	input := scalarRegister{typ: memory.TypeString, strView: testStringVector(payloads, nil)}
	var out scalarRegister
	if err := evalJSONDecode(&out, &input, fields, len(payloads)); err != nil {
		t.Fatal(err)
	}
	// Destroy the input before checking, to expose borrowed JSON payloads.
	clear(input.strView.Data)
	for row, payload := range payloads {
		var oracle map[string]any
		if err := json.Unmarshal([]byte(payload), &oracle); err != nil {
			t.Fatal(err)
		}
		if out.strct.IsValid(row) != (oracle != nil) {
			t.Fatalf("parent row %d", row)
		}
		s := out.strct.Values.Strings["s"]
		wantS, validS := oracle["s"].(string)
		if s.IsValid(row) != validS || string(s.ValueBytes(row)) != wantS {
			t.Fatalf("string row %d", row)
		}
		list := out.strct.Values.StringLists["x"]
		want, valid := oracle["x"].([]any)
		start, end, gotValid := list.ValueRange(row)
		if gotValid != valid || end-start != len(want) {
			t.Fatalf("list row %d range=%d:%d valid=%v", row, start, end, gotValid)
		}
		for i, item := range want {
			str, ok := item.(string)
			if list.Values.IsValid(start+i) != ok || string(list.Values.ValueBytes(start+i)) != str {
				t.Fatalf("row %d element %d", row, i)
			}
		}
	}
}

func TestJSONRowReviewCancelEveryCheckpoint(t *testing.T) {
	fields := []memory.Field{{Name: "n", Type: memory.TypeInt64}, memory.ListStringField("x", true, true)}
	payload := `{"n":2,"x":[` + strings.Repeat(`"value",`, 300) + `null]}`
	input := scalarRegister{typ: memory.TypeString, strView: testStringVector([]string{"null", payload, `{"n":3,"x":["last"]}`}, nil)}
	checks := 0
	probe := scalarRegister{checkContext: func() error { checks++; return nil }}
	if err := evalJSONDecode(&probe, &input, fields, 3); err != nil {
		t.Fatal(err)
	}
	for stop := 1; stop <= checks; stop++ {
		t.Run(fmt.Sprint(stop), func(t *testing.T) {
			manager := memory.NewBudgetManager(8 << 20)
			out := scalarRegister{acquire: func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }}
			seed := scalarRegister{typ: memory.TypeString, strView: testStringVector([]string{`{"n":99,"x":["keep"]}`}, nil)}
			if err := evalJSONDecode(&out, &seed, fields, 1); err != nil {
				t.Fatal(err)
			}
			defer func() {
				out.jsonReservation.Release()
				if manager.Used() != 0 {
					t.Errorf("leak %d", manager.Used())
				}
			}()
			old := out.strct
			before := manager.Used()
			calls := 0
			out.checkContext = func() error {
				calls++
				if calls == stop {
					return context.Canceled
				}
				return nil
			}
			if err := evalJSONDecode(&out, &input, fields, 3); !errors.Is(err, context.Canceled) {
				t.Fatalf("error %v", err)
			}
			if out.strct != old || manager.Used() != before || old.Values.Int64s["n"].Values[0] != 99 || string(old.Values.StringLists["x"].Values.Data) != "keep" {
				t.Fatal("cancellation changed prior result or charge")
			}
			out.checkContext = nil
			if err := evalJSONDecode(&out, &input, fields, 3); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJSONRowReviewFailureTransactions(t *testing.T) {
	fields := []memory.Field{{Name: "n", Type: memory.TypeInt64}, memory.ListStringField("x", true, false)}
	for _, mode := range []string{"output-budget", "scratch-budget", "field-error", "list-error", "syntax-error"} {
		t.Run(mode, func(t *testing.T) {
			manager := memory.NewBudgetManager(8 << 20)
			reserve := func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }
			out := scalarRegister{acquire: reserve}
			seed := scalarRegister{typ: memory.TypeString, strView: testStringVector([]string{`{"n":99,"x":["keep"]}`}, nil)}
			if err := evalJSONDecode(&out, &seed, fields, 1); err != nil {
				t.Fatal(err)
			}
			defer func() {
				out.jsonReservation.Release()
				if manager.Used() != 0 {
					t.Fatalf("leak=%d", manager.Used())
				}
			}()
			old := out.strct
			before := manager.Used()
			payloads := []string{`{"n":1,"x":["new"]}`, `{"n":2,"x":["second"]}`}
			want := ""
			switch mode {
			case "output-budget", "scratch-budget":
				calls := 0
				fail := 1
				if mode == "scratch-budget" {
					fail = 2
				}
				out.acquire = func(n int64) (memory.Reservation, error) {
					calls++
					if calls == fail {
						return nil, memory.ErrBudgetExceeded
					}
					return reserve(n)
				}
			case "field-error":
				payloads[1] = `{"n":"bad","x":[null]}`
				want = `JSON_DECODE at row 1 field "n": expected integer`
			case "list-error":
				payloads[1] = `{"n":2,"x":["good",null]}`
				want = `JSON_DECODE at row 1 field "x"[1]: null element is not allowed`
			case "syntax-error":
				payloads[1] = `{`
				want = `JSON_DECODE at row 1: unexpected EOF`
			}
			input := scalarRegister{typ: memory.TypeString, strView: testStringVector(payloads, nil)}
			err := evalJSONDecode(&out, &input, fields, 2)
			if want != "" {
				if err == nil || !strings.HasPrefix(err.Error(), want) {
					t.Fatalf("got %v, want %s", err, want)
				}
			} else if !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("got %v", err)
			}
			if out.strct != old || manager.Used() != before || old.Values.Int64s["n"].Values[0] != 99 || string(old.Values.StringLists["x"].Values.Data) != "keep" {
				t.Fatal("failed decode changed old output or reservations")
			}
			out.acquire = reserve
			if err := evalJSONDecode(&out, &seed, fields, 1); err != nil {
				t.Fatal(err)
			}
		})
	}
}
