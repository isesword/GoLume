package expr

import (
	"encoding/json"
	"gocolumn/memory"
	"math"
	"os"
	"strings"
	"testing"
)

func TestReviewNumericCastPolarsOracle(t *testing.T) {
	data, err := os.ReadFile("../testdata/fundamentals/polars-1.35.2.json")
	if err != nil {
		t.Fatal(err)
	}
	type castCase struct {
		ID      string    `json:"id"`
		Input   []*string `json:"input"`
		Target  string    `json:"target_dtype"`
		Results map[string]struct {
			Error  string            `json:"error"`
			Values []json.RawMessage `json:"values"`
		} `json:"results"`
	}
	// Other fixture cases have non-string input; decode only cast records.
	var raw struct {
		Version string            `json:"version"`
		Cases   []json.RawMessage `json:"cases"`
	}
	if err = json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if raw.Version != "1.35.2" {
		t.Fatalf("unexpected reference version %s", raw.Version)
	}
	cases := []castCase{}
	for _, r := range raw.Cases {
		var id struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(r, &id); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(id.ID, "cast/") {
			continue
		}
		var tc castCase
		if err := json.Unmarshal(r, &tc); err != nil {
			t.Fatal(err)
		}
		if len(tc.Input) != 1 {
			t.Fatalf("%s unexpected input shape", tc.ID)
		}
		cases = append(cases, tc)
	}
	if len(cases) != 360 {
		t.Fatalf("expected 360 cast cases, got %d", len(cases))
	}
	types := map[string]memory.DataType{"Int8": memory.TypeInt8, "Int16": memory.TypeInt16, "Int32": memory.TypeInt32, "Int64": memory.TypeInt64, "UInt8": memory.TypeUint8, "UInt16": memory.TypeUint16, "UInt32": memory.TypeUint32, "UInt64": memory.TypeUint64, "Float32": memory.TypeFloat32, "Float64": memory.TypeFloat64}
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	for _, tc := range cases {
		t.Run(tc.ID, func(t *testing.T) {
			typ, ok := types[tc.Target]
			if !ok {
				t.Fatalf("unknown target %s", tc.Target)
			}
			batch := numericCastBatch("")
			if tc.Input[0] == nil {
				batch.Strings["s"].Validity = memory.Bitmap{0}
			} else {
				batch = numericCastBatch(*tc.Input[0])
			}
			for _, mode := range []string{"strict", "try"} {
				var e Expr = CastTo(Col("s"), typ)
				if mode == "try" {
					e = TryCastTo(Col("s"), typ)
				}
				p, err := Compile(schema, e)
				if err != nil {
					t.Fatal(err)
				}
				var w evaluationState
				_, err = evaluateValueTest(p, batch, &w, testKernelSet())
				expected := tc.Results[mode]
				if expected.Error != "" {
					if err == nil {
						t.Errorf("%s expected %s", mode, expected.Error)
					}
					w.Close()
					continue
				}
				if err != nil {
					t.Errorf("%s: %v", mode, err)
					continue
				}
				r := &w.regs[p.root]
				if len(expected.Values) != 1 {
					t.Fatalf("%s unexpected result shape", mode)
				}
				v := expected.Values[0]
				if string(v) == "null" {
					if isValidRegister(r, 0) {
						t.Errorf("%s expected null", mode)
					}
				} else if !isValidRegister(r, 0) {
					t.Errorf("%s unexpected null", mode)
				} else if isSignedInteger(typ) {
					var want int64
					if err := json.Unmarshal(v, &want); err != nil {
						t.Fatal(err)
					}
					got, _ := registerInt64(r, 0)
					if got != want {
						t.Errorf("%s got %d want %d", mode, got, want)
					}
				} else if unsignedInteger(typ) {
					var want uint64
					if err := json.Unmarshal(v, &want); err != nil {
						t.Fatal(err)
					}
					got, _ := registerUint64(r, 0)
					if got != want {
						t.Errorf("%s got %d want %d", mode, got, want)
					}
				} else {
					var want float64
					switch string(v) {
					case `"NaN"`:
						want = math.NaN()
					case `"+Inf"`:
						want = math.Inf(1)
					case `"-Inf"`:
						want = math.Inf(-1)
					default:
						if err := json.Unmarshal(v, &want); err != nil {
							t.Fatal(err)
						}
					}
					got, _ := registerFloat64(r, 0)
					if !(math.IsNaN(got) && math.IsNaN(want)) && (got != want || (got == 0 && math.Signbit(got) != math.Signbit(want))) {
						t.Errorf("%s got %v want %v", mode, got, want)
					}
				}
				w.Close()
			}
		})
	}
}
