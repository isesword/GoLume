package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestFillReviewLongPartitions(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered} {
			for _, batchRows := range []int{1, 7, 63, 130} {
				t.Run(fmt.Sprintf("%s/%d/%d", typ, mode, batchRows), func(t *testing.T) {
					b := memory.NewBatch()
					b.Rows = 389
					initializeOffsetOutput(b, "v", typ, 389)
					b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, 389)}
					b.Int64s["g"] = &memory.Int64Vector{Values: make([]int64, 389)}
					for i := 0; i < 389; i++ {
						b.Int64s["id"].Values[i] = int64(i)
						b.Int64s["g"].Values[i] = int64(i / 97)
					}
					for _, i := range []int{1, 63, 96, 101, 193, 288, 388} {
						v := int64(i + 10)
						setOffsetOutput(b, "v", typ, i, windowNumericCell{valid: true, i64: v, u64: uint64(v), f64: float64(v)})
					}
					if typ == memory.TypeUint64 {
						b.Uint64s["v"].Values[1] = math.MaxUint64
					}
					if typ == memory.TypeFloat32 {
						b.Float32s["v"].Values[1] = float32(math.NaN())
					}
					if typ == memory.TypeFloat64 {
						b.Float64s["v"].Values[1] = math.NaN()
					}
					schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: typ, Nullable: true})
					input, err := inmemory.New(schema, b)
					if err != nil {
						t.Fatal(err)
					}
					frame := WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
					es := []WindowExpression{{Function: WindowForwardFill, Column: "v", Alias: "f", Frame: frame}, {Function: WindowBackwardFill, Column: "v", Alias: "v", Frame: frame}}
					fs := []memory.Field{{Name: "f", Type: typ, Nullable: true}, {Name: "v", Type: typ, Nullable: true}}
					manager := memory.NewBudgetManager(1 << 20)
					win, err := NewWindowSourceWithOptions(input, []string{"g"}, []SortKey{{Column: "id"}}, es, fs, manager, WindowSourceOptions{Mode: mode})
					if err != nil {
						t.Fatal(err)
					}
					st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: batchRows})
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
							pos := seen + row
							for j, name := range []string{"f", "v"} {
								want := windowNumericCell{}
								step := -1
								if j == 1 {
									step = 1
								}
								for p := pos; p >= pos/97*97 && p < min((pos/97+1)*97, 389); p += step {
									want, err = readWindowNumericCell(b, schema, WindowExpression{Column: "v"}, p)
									if err != nil {
										t.Fatal(err)
									}
									if want.valid {
										break
									}
								}
								got, err := readWindowNumericCell(out, win.Schema(), WindowExpression{Column: name}, row)
								if err != nil {
									t.Fatal(err)
								}
								if got.valid != want.valid || (got.valid && (got.i64 != want.i64 || got.u64 != want.u64 || (!(math.IsNaN(got.f64) && math.IsNaN(want.f64)) && got.f64 != want.f64))) {
									t.Fatalf("row%d %s got%+v want%+v", pos, name, got, want)
								}
							}
						}
						seen += out.Rows
					}
					if seen != 389 {
						t.Fatal(seen)
					}
					st.Close()
					if manager.Used() != 0 {
						t.Fatal("leak", manager.Used())
					}
				})
			}
		}
	}
}
