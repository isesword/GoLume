package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"testing"
)

func TestWindowExtremaNativeVectors(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			for _, start := range []WindowBound{{Kind: WindowCurrentRow}, {Kind: WindowPreceding, Offset: 1}, {Kind: WindowUnboundedPreceding}} {
				t.Run(fmt.Sprintf("%s/mode%d/start%d", typ, mode, start.Kind), func(t *testing.T) {
					ctx, _, query := spillOutputQuery(t)
					input := memory.NewBatch()
					input.Rows = 9
					initializeOffsetOutput(input, "v", typ, input.Rows)
					numbers := []int64{0, 7, 3, 0, 2, 9, 0, 0, 4}
					for row, value := range numbers {
						if value != 0 {
							setOffsetOutput(input, "v", typ, row, windowNumericCell{valid: true, i64: value, u64: uint64(value), f64: float64(value)})
						}
					}
					input.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8}}
					input.Int32s["grp"] = &memory.Int32Vector{Values: []int32{0, 0, 0, 0, 0, 1, 1, 1, 1}}
					schema := memory.NewSchema(memory.Field{Name: "v", Type: typ, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "grp", Type: memory.TypeInt32})
					src, err := inmemory.New(schema, input)
					if err != nil {
						t.Fatal(err)
					}
					frame := WindowFrame{Start: start, End: WindowBound{Kind: WindowCurrentRow}}
					expressions := []WindowExpression{{Function: WindowMin, Column: "v", Alias: "lo", Frame: frame}, {Function: WindowMax, Column: "v", Alias: "hi", Frame: frame}}
					fields := []memory.Field{{Name: "lo", Type: typ, Nullable: true}, {Name: "hi", Type: typ, Nullable: true}}
					window, err := NewWindowSourceWithOptions(src, []string{"grp"}, []SortKey{{Column: "id"}}, expressions, fields, query, WindowSourceOptions{Mode: mode})
					if err != nil {
						t.Fatal(err)
					}
					stream, err := source.OpenBatchStream(ctx, window, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := stream.Close(); err != nil {
							t.Error(err)
						}
					})
					out, total := memory.NewBatch(), 0
					for {
						ok, err := stream.Next(ctx, out)
						if err != nil {
							t.Fatal(err)
						}
						if !ok {
							break
						}
						for _, name := range []string{"lo", "hi"} {
							// Compare actual vector representation, not the permissive numeric reader.
							inputMap := reflect.ValueOf(input).Elem()
							outputMap := reflect.ValueOf(out).Elem()
							for index := 0; index < inputMap.NumField(); index++ {
								m := inputMap.Field(index)
								if m.Kind() != reflect.Map {
									continue
								}
								want := m.MapIndex(reflect.ValueOf("v")).IsValid()
								got := outputMap.Field(index).MapIndex(reflect.ValueOf(name)).IsValid()
								if got != want {
									t.Fatalf("%s binding %s=%v, want %v", name, inputMap.Type().Field(index).Name, got, want)
								}
							}
							for row := 0; row < out.Rows; row++ {
								pos := total + row
								lo := 0
								if pos >= 5 {
									lo = 5
								}
								if start.Kind == WindowCurrentRow {
									lo = pos
								}
								if start.Kind == WindowPreceding {
									lo = max(lo, pos-1)
								}
								var want int64
								for j := lo; j <= pos; j++ {
									v := numbers[j]
									if v != 0 && (want == 0 || (name == "lo" && v < want) || (name == "hi" && v > want)) {
										want = v
									}
								}
								got, err := readWindowNumericCell(out, window.Schema(), WindowExpression{Column: name}, row)
								if err != nil {
									t.Fatal(err)
								}
								if got.valid != (want != 0) {
									t.Fatalf("%s row%d valid=%v want %v", name, pos, got.valid, want != 0)
								}
								if got.valid && ((windowSigned(typ) && got.i64 != want) || (windowUnsigned(typ) && got.u64 != uint64(want)) || (windowFloat(typ) && got.f64 != float64(want))) {
									t.Fatalf("%s row%d=%+v want%d", name, pos, got, want)
								}
							}
						}
						total += out.Rows
					}
					if total != input.Rows {
						t.Fatalf("rows=%d", total)
					}
					checkSpillOutputCleanup(t, stream, query)
				})
			}
		}
	}
}

func TestWindowExtremaRejectMismatchedPhysicalFields(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt8})
	src, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	expression := WindowExpression{Function: WindowMin, Column: "v", Alias: "lo", Frame: WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}}
	for _, field := range []memory.Field{{Name: "lo", Type: memory.TypeInt32, Nullable: true}, {Name: "other", Type: memory.TypeInt8, Nullable: true}, {Name: "lo", Type: memory.TypeInt8}} {
		if _, err := NewWindowSource(src, nil, []SortKey{{Column: "v"}}, []WindowExpression{expression}, []memory.Field{field}, nil); err == nil {
			t.Errorf("accepted mismatched output %+v", field)
		}
	}
}

func TestWindowExtremaResidentEmptyAndFollowingFrames(t *testing.T) {
	input := memory.NewBatch()
	input.Rows = 3
	input.Int8s["v"] = &memory.Int8Vector{Values: []int8{5, -4, 2}}
	input.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt8}, memory.Field{Name: "id", Type: memory.TypeInt64})
	src, err := inmemory.New(schema, input)
	if err != nil {
		t.Fatal(err)
	}
	for _, bound := range []WindowBound{{Kind: WindowPreceding, Offset: 2}, {Kind: WindowFollowing, Offset: 2}} {
		for _, function := range []WindowFunction{WindowMin, WindowMax} {
			expression := WindowExpression{Function: function, Column: "v", Alias: "value", Frame: WindowFrame{Start: bound, End: bound}}
			window, err := NewWindowSource(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{expression}, []memory.Field{{Name: "value", Type: memory.TypeInt8, Nullable: true}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := window.NewBatchStream(context.Background(), source.ReadOptions{Kernels: offsetTestKernels()})
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			ok, err := stream.Next(context.Background(), out)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			for row := 0; row < 3; row++ {
				target := row - 2
				if bound.Kind == WindowFollowing {
					target = row + 2
				}
				valid := target >= 0 && target < 3
				v := out.Int8s["value"]
				if v == nil || v.IsValid(row) != valid || (valid && v.Values[row] != input.Int8s["v"].Values[target]) {
					t.Fatalf("bound=%+v function=%d row%d output=%+v", bound, function, row, v)
				}
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestWindowExtremaCancellationAndBudgetCleanup(t *testing.T) {
	input := memory.NewBatch()
	input.Rows = 3
	input.Float32s["v"] = &memory.Float32Vector{Values: []float32{3, 1, 2}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat32}), input)
	if err != nil {
		t.Fatal(err)
	}
	expression := WindowExpression{Function: WindowMin, Column: "v", Alias: "lo", Frame: WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 4096}, End: WindowBound{Kind: WindowCurrentRow}}}
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered} {
		manager := memory.NewBudgetManager(1)
		window, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "v"}}, []WindowExpression{expression}, []memory.Field{{Name: "lo", Type: memory.TypeFloat32, Nullable: true}}, manager, WindowSourceOptions{Mode: mode})
		if err != nil {
			t.Fatal(err)
		}
		stream, err := window.NewBatchStream(context.Background(), source.ReadOptions{Kernels: offsetTestKernels()})
		if err == nil {
			_, err = stream.Next(context.Background(), memory.NewBatch())
			stream.Close()
		}
		if !errors.Is(err, memory.ErrBudgetExceeded) || manager.Used() != 0 {
			t.Fatalf("mode%d err=%v used=%d", mode, err, manager.Used())
		}
		manager = memory.NewBudgetManager(1 << 20)
		window, err = NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "v"}}, []WindowExpression{expression}, []memory.Field{{Name: "lo", Type: memory.TypeFloat32, Nullable: true}}, manager, WindowSourceOptions{Mode: mode})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		stream, err = window.NewBatchStream(ctx, source.ReadOptions{Kernels: offsetTestKernels()})
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		_, err = stream.Next(ctx, memory.NewBatch())
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if manager.Used() != 0 {
			t.Fatalf("leaked %d bytes", manager.Used())
		}
	}
}
