package physical

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestOffsetWindowsNativeWidths(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered} {
			for _, period := range []int64{-3, -1, 0, 1, 3} {
				input := memory.NewBatch()
				input.Rows = 5
				initializeOffsetOutput(input, "v", typ, 5)
				for i, v := range []int64{4, 2, 7, 9, 11} {
					if i == 2 {
						continue
					}
					setOffsetOutput(input, "v", typ, i, windowNumericCell{valid: true, i64: v, u64: uint64(v), f64: float64(v)})
				}
				input.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3, 4}}
				schema := memory.NewSchema(memory.Field{Name: "v", Type: typ, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt64})
				src, err := inmemory.New(schema, input)
				if err != nil {
					t.Fatal(err)
				}
				bound := WindowBound{Kind: WindowPreceding, Offset: period}
				if period < 0 {
					bound = WindowBound{Kind: WindowFollowing, Offset: -period}
				}
				if period == 0 {
					bound = WindowBound{Kind: WindowCurrentRow}
				}
				expressions := []WindowExpression{{Function: WindowShift, Column: "v", Alias: "s", Frame: WindowFrame{Start: bound, End: bound}}, {Function: WindowDiff, Column: "v", Alias: "d", Frame: WindowFrame{Start: bound, End: bound}}}
				fields := []memory.Field{{Name: "s", Type: typ, Nullable: true}, {Name: "d", Type: offsetOutputType(typ, WindowDiff), Nullable: true}}
				manager := memory.NewBudgetManager(1 << 20)
				win, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, expressions, fields, manager, WindowSourceOptions{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				stream, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
				if err != nil {
					t.Fatal(err)
				}
				out := memory.NewBatch()
				total := 0
				for {
					ok, err := stream.Next(context.Background(), out)
					if err != nil {
						t.Fatalf("%s mode%d p%d: %v", typ, mode, period, err)
					}
					if !ok {
						break
					}
					for i := 0; i < out.Rows; i++ {
						pos := total + i
						target := pos - int(period)
						for _, name := range []string{"s", "d"} {
							cell, err := readWindowNumericCell(out, win.Schema(), WindowExpression{Column: name}, i)
							if err != nil {
								t.Fatalf("native %s/%s: %v", typ, name, err)
							}
							want := windowNumericCell{}
							if target >= 0 && target < 5 {
								want, _ = readWindowNumericCell(input, schema, WindowExpression{Column: "v"}, target)
								if name == "d" {
									current, _ := readWindowNumericCell(input, schema, WindowExpression{Column: "v"}, pos)
									want = offsetDifference(current, want, typ)
								}
							}
							if cell.valid != want.valid {
								t.Fatalf("%s mode%d p%d row%d %s valid %v want%v", typ, mode, period, pos, name, cell.valid, want.valid)
							}
							if cell.valid {
								ft, _ := win.Schema().Field(name)
								switch {
								case windowSigned(ft.Type):
									if cell.i64 != want.i64 {
										t.Fatal(cell, want)
									}
								case windowUnsigned(ft.Type):
									if cell.u64 != want.u64 {
										t.Fatal(cell, want)
									}
								default:
									if cell.f64 != want.f64 {
										t.Fatal(cell, want)
									}
								}
							}
						}
					}
					total += out.Rows
				}
				if total != 5 {
					t.Fatal(total)
				}
				stream.Close()
				if manager.Used() != 0 {
					t.Fatalf("leak%d", manager.Used())
				}
			}
		}
	}
}

func TestOffsetWindowResidentFutureAndBudget(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{8, 3, 5}}
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64})
	src, _ := inmemory.New(schema, b)
	bound := WindowBound{Kind: WindowFollowing, Offset: 1}
	e := WindowExpression{Function: WindowShift, Column: "v", Alias: "s", Frame: WindowFrame{Start: bound, End: bound}}
	fields := []memory.Field{{Name: "s", Type: memory.TypeInt64, Nullable: true}}
	win, err := NewWindowSource(src, nil, []SortKey{{Column: "v"}}, []WindowExpression{e}, fields, memory.NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	stream, _ := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
	defer stream.Close()
	out := memory.NewBatch()
	ok, err := stream.Next(context.Background(), out)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	v := out.Int64s["s"]
	if v.IsValid(0) || !v.IsValid(1) || v.Values[1] != 5 || v.Values[2] != 8 {
		t.Fatal(v)
	}
	e.Frame = WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 10000}, End: WindowBound{Kind: WindowPreceding, Offset: 10000}}
	manager := memory.NewBudgetManager(1024)
	win, err = NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "v"}}, []WindowExpression{e}, fields, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
	if !errors.Is(err, memory.ErrBudgetExceeded) || manager.Used() != 0 {
		t.Fatal(err, manager.Used())
	}
	if offsetDifference(windowNumericCell{valid: true, u64: math.MaxUint64}, windowNumericCell{valid: true, u64: math.MaxUint64 - 1}, memory.TypeUint64).valid {
		t.Fatal("uint64 diff must validate endpoints")
	}
}

func offsetTestKernels() kernel.Set { k := kernel.New(kernel.ModeScalar); return k }

func TestOffsetWindowExactUint64AndNarrowWrap(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered} {
		b := memory.NewBatch()
		b.Rows = 3
		b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
		b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{9007199254740993, math.MaxUint64, 3}}
		b.Int8s["i"] = &memory.Int8Vector{Values: []int8{-128, 127, -128}}
		schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "i", Type: memory.TypeInt8})
		src, _ := inmemory.New(schema, b)
		next := WindowBound{Kind: WindowFollowing, Offset: 1}
		past := WindowBound{Kind: WindowPreceding, Offset: 1}
		ex := []WindowExpression{{Function: WindowShift, Column: "u", Alias: "next", Frame: WindowFrame{Start: next, End: next}}, {Function: WindowDiff, Column: "i", Alias: "diff", Frame: WindowFrame{Start: past, End: past}}}
		fields := []memory.Field{{Name: "next", Type: memory.TypeUint64, Nullable: true}, {Name: "diff", Type: memory.TypeInt8, Nullable: true}}
		w, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, ex, fields, memory.NewBudgetManager(1<<20), WindowSourceOptions{Mode: mode})
		if err != nil {
			t.Fatal(err)
		}
		st, err := source.OpenBatchStream(context.Background(), w, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
		if err != nil {
			t.Fatal(err)
		}
		out := memory.NewBatch()
		row := 0
		for {
			ok, err := st.Next(context.Background(), out)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			for i := 0; i < out.Rows; i++ {
				if row == 0 && out.Uint64s["next"].Values[i] != math.MaxUint64 {
					t.Fatal("uint64 precision lost")
				}
				if row == 1 && out.Int8s["diff"].Values[i] != -1 {
					t.Fatal("int8 wrap lost")
				}
				row++
			}
		}
		st.Close()
	}
}
