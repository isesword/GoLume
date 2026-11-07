package physical

import (
	"context"
	"encoding/json"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"os"
	"testing"
)

func TestDirectionalFillPinnedOracle(t *testing.T) {
	data, err := os.ReadFile("../testdata/fundamentals/fills-polars-1.35.2.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Version string
		Cases   []struct {
			Dtype, Name              string
			Input, Forward, Backward []any
		}
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Version != "1.35.2" {
		t.Fatal(fixture.Version)
	}
	types := map[string]memory.DataType{"Int8": memory.TypeInt8, "Int16": memory.TypeInt16, "Int32": memory.TypeInt32, "Int64": memory.TypeInt64, "UInt8": memory.TypeUint8, "UInt16": memory.TypeUint16, "UInt32": memory.TypeUint32, "UInt64": memory.TypeUint64, "Float32": memory.TypeFloat32, "Float64": memory.TypeFloat64}
	for _, tc := range fixture.Cases {
		t.Run(tc.Dtype+"/"+tc.Name, func(t *testing.T) {
			typ, ok := types[tc.Dtype]
			if !ok {
				t.Fatal(tc.Dtype)
			}
			input := memory.NewBatch()
			input.Rows = len(tc.Input)
			initializeOffsetOutput(input, "v", typ, input.Rows)
			input.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, input.Rows)}
			for i, v := range tc.Input {
				input.Int64s["id"].Values[i] = int64(i)
				if v == nil {
					continue
				}
				n := math.NaN()
				if f, ok := v.(float64); ok {
					n = f
				}
				setOffsetOutput(input, "v", typ, i, windowNumericCell{valid: true, i64: int64(n), u64: uint64(n), f64: n})
			}
			schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: typ, Nullable: true})
			src, err := inmemory.New(schema, input)
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered} {
				manager := memory.NewBudgetManager(1 << 20)
				frame := WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
				es := []WindowExpression{{Function: WindowForwardFill, Column: "v", Alias: "f", Frame: frame}, {Function: WindowBackwardFill, Column: "v", Alias: "b", Frame: frame}}
				fs := []memory.Field{{Name: "f", Type: typ, Nullable: true}, {Name: "b", Type: typ, Nullable: true}}
				win, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, es, fs, manager, WindowSourceOptions{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
				if err != nil {
					t.Fatal(err)
				}
				out := memory.NewBatch()
				seen := 0
				for {
					ok, err := st.Next(context.Background(), out)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					for row := 0; row < out.Rows; row++ {
						for _, name := range []string{"f", "b"} {
							want := tc.Forward[seen+row]
							if name == "b" {
								want = tc.Backward[seen+row]
							}
							got, err := readWindowNumericCell(out, win.Schema(), WindowExpression{Column: name}, row)
							if err != nil {
								t.Fatal(err)
							}
							if got.valid != (want != nil) {
								t.Fatal(got, want)
							}
							if want == nil {
								continue
							}
							if want == "NaN" {
								if !math.IsNaN(got.f64) {
									t.Fatal(got, want)
								}
								continue
							}
							n := want.(float64)
							if windowSigned(typ) {
								if got.i64 != int64(n) {
									t.Fatal(got, want)
								}
							} else if windowUnsigned(typ) {
								if got.u64 != uint64(n) {
									t.Fatal(got, want)
								}
							} else if got.f64 != n {
								t.Fatal(got, want)
							}
						}
					}
					seen += out.Rows
				}
				st.Close()
				if seen != len(tc.Input) || manager.Used() != 0 {
					t.Fatal(seen, manager.Used())
				}
			}
		})
	}
}
