package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestDirectionalFillNativeWidths(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered} {
			for _, batchRows := range []int{1, 3, 64} {
				t.Run(fmt.Sprintf("%s/%d/%d", typ, mode, batchRows), func(t *testing.T) {
					b := memory.NewBatch()
					b.Rows = 12
					initializeOffsetOutput(b, "v", typ, 12)
					b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, 12)}
					b.Int64s["g"] = &memory.Int64Vector{Values: make([]int64, 12)}
					for i := 0; i < 12; i++ {
						b.Int64s["id"].Values[i] = int64(i)
						b.Int64s["g"].Values[i] = int64(i / 4)
					}
					for _, i := range []int{1, 3, 5} {
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
								for p := pos; p >= pos/4*4 && p < (pos/4+1)*4; p += step {
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
					if seen != 12 {
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

func TestBackwardFillBudgetAndCleanup(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 4096
	b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
	b.Int64s["v"] = &memory.Int64Vector{Values: make([]int64, b.Rows), Validity: memory.EnsureBitmap(nil, b.Rows)}
	for i := range b.Rows {
		b.Int64s["id"].Values[i] = int64(i)
	}
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true})
	input, _ := inmemory.New(schema, b)
	e := []WindowExpression{{Function: WindowBackwardFill, Column: "v", Alias: "b", Frame: WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}}}
	fields := []memory.Field{{Name: "b", Type: memory.TypeInt64, Nullable: true}}
	for _, manager := range []memory.Manager{nil, memory.NewBudgetManager(0)} {
		if _, err := NewWindowSourceWithOptions(input, nil, []SortKey{{Column: "id"}}, e, fields, manager, WindowSourceOptions{Mode: WindowExecutionOrdered}); err == nil {
			t.Fatal("unbudgeted backward fill accepted")
		}
	}
	manager := memory.NewBudgetManager(64 << 10)
	win, err := NewWindowSourceWithOptions(input, nil, []SortKey{{Column: "id"}}, e, fields, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
	if err != nil {
		t.Fatal(err)
	}
	st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 8})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Next(context.Background(), memory.NewBatch()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	st.Close()
	if manager.Used() != 0 {
		t.Fatal("leak", manager.Used())
	}
}

type fillCloseCounter struct {
	source.BatchStream
	closes int
}

func (s *fillCloseCounter) Close() error { s.closes++; return s.BatchStream.Close() }

func TestBackwardFillSourceCleanup(t *testing.T) {
	for _, failure := range []string{"budget", "cancel", "input", "early-close"} {
		t.Run(failure, func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 1024
			b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
			b.Int64s["v"] = &memory.Int64Vector{Values: make([]int64, b.Rows), Validity: memory.EnsureBitmap(nil, b.Rows)}
			for i := range b.Rows {
				b.Int64s["id"].Values[i] = int64(i)
			}
			src, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true}), b)
			manager := memory.NewBudgetManager(64 << 10)
			e := WindowExpression{Function: WindowBackwardFill, Column: "v", Alias: "b", Frame: WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}}
			win, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{e}, []memory.Field{{Name: "b", Type: memory.TypeInt64, Nullable: true}}, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
			if err != nil {
				t.Fatal(err)
			}
			ordered := stream.(*orderedWindowStream)
			future := ordered.child.(*futureWindowStream)
			counter := &fillCloseCounter{BatchStream: future.child}
			future.child = counter
			if failure == "input" {
				future.child = &failingFutureChild{BatchStream: counter}
			}
			if failure != "early-close" {
				ctx := context.Background()
				if failure == "cancel" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				_, err = stream.Next(ctx, memory.NewBatch())
				if err == nil {
					t.Fatal("missing error")
				}
				if failure == "budget" && !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatal(err)
				}
				if failure == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if counter.closes != 1 {
					t.Fatal("child not closed on lookahead error", counter.closes)
				}
			}
			stream.Close()
			stream.Close()
			if counter.closes != 1 || manager.Used() != 0 {
				t.Fatal(counter.closes, manager.Used())
			}
		})
	}
}
