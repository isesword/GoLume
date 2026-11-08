package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func maskedMathQuery(tb testing.TB, typ memory.DataType, rows, batchRows, nullPct int, selected bool, op string) (*inmemory.Source, expr.Query, uint64) {
	tb.Helper()
	schema := memory.NewSchema(memory.Field{Name: "x", Type: typ, Nullable: nullPct != 0}, memory.Field{Name: "gate", Type: memory.TypeInt64})
	batches := []*memory.Batch{}
	var count uint64
	for base := 0; base < rows; base += batchRows {
		n := min(batchRows, rows-base)
		batch := memory.NewBatch()
		batch.Reset(n)
		f32, f64, g := make([]float32, n), make([]float64, n), make([]int64, n)
		var valid memory.Bitmap
		if nullPct != 0 {
			valid = make(memory.Bitmap, memory.WordsFor(n))
		}
		for i := range f64 {
			row := base + i
			g[i] = int64(row % 100)
			value := float64(row%97) + .25
			if op == "abs" {
				value = -value
			}
			f32[i], f64[i] = float32(value), value
			isValid := (row*37)%100 >= nullPct
			if isValid && nullPct != 0 {
				memory.Set(valid, i)
			}
			if !selected || g[i] >= 50 {
				if op == "abs" {
					value = math.Abs(value)
				} else {
					value = math.Sqrt(value)
				}
			}
			if typ == memory.TypeFloat32 {
				value = float64(float32(value))
			}
			if isValid && value > 7 {
				count++
			}
		}
		batch.Int64s["gate"] = &memory.Int64Vector{Values: g}
		if typ == memory.TypeFloat32 {
			batch.Float32s["x"] = &memory.Float32Vector{Values: f32, Validity: valid}
		} else {
			batch.Float64s["x"] = &memory.Float64Vector{Values: f64, Validity: valid}
		}
		batches = append(batches, batch)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		tb.Fatal(err)
	}
	var value expr.Expr = expr.Col("x").Abs()
	if op == "sqrt" {
		value = expr.Col("x").Sqrt()
	}
	if selected {
		value = expr.If(expr.GE(expr.Col("gate"), expr.I64(50)), value, expr.Col("x"))
	}
	return src, expr.Query{Filter: expr.GT(value, expr.F64(7)), Aggregates: []expr.Aggregate{expr.Count("count")}}, count
}

func TestMathMaskedQuery(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		for _, op := range []string{"abs", "sqrt"} {
			for _, nullPct := range []int{0, 1, 50, 99, 100} {
				for _, selected := range []bool{false, true} {
					for _, batchRows := range []int{31, 65, 1021, 8193} {
						t.Run(fmt.Sprintf("%s/%s/null%d/selected%t/batch%d", typ, op, nullPct, selected, batchRows), func(t *testing.T) {
							src, q, want := maskedMathQuery(t, typ, 8193, batchRows, nullPct, selected, op)
							for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
								opts := DefaultOptions()
								opts.Workers = 1
								opts.PreparedCacheBytes = 0
								opts.KernelMode = mode
								e := New(opts)
								result, err := e.ExecuteTyped(context.Background(), src, q, ExecuteOptions{Materialize: true})
								closeErr := e.Close()
								if err != nil || closeErr != nil {
									t.Fatal(err, closeErr)
								}
								if len(result.Rows) != 1 {
									t.Fatal("result row count", len(result.Rows))
								}
								got, ok := result.Rows[0].Aggregates[0].Value.AsUint64()
								if !ok || got != want {
									t.Fatalf("mode=%s count=%d want=%d", mode, got, want)
								}
							}
						})
					}
				}
			}
		}
	}
}

func BenchmarkMathMaskedQuery(b *testing.B) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		for _, op := range []string{"abs", "sqrt"} {
			for _, nullPct := range []int{0, 1, 50, 99} {
				for _, selected := range []bool{false, true} {
					for _, batchRows := range []int{1021, 65539} {
						b.Run(fmt.Sprintf("%s/%s/null%d/selected%t/batch%d", typ, op, nullPct, selected, batchRows), func(b *testing.B) {
							src, q, _ := maskedMathQuery(b, typ, 65539, batchRows, nullPct, selected, op)
							opts := DefaultOptions()
							opts.Workers = 1
							opts.PreparedCacheBytes = 0
							opts.KernelMode = kernel.ModeAuto
							e := New(opts)
							defer e.Close()
							p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
							if err != nil {
								b.Fatal(err)
							}
							defer p.Close()
							if _, err = p.ExecuteTyped(context.Background(), ExecuteOptions{}); err != nil {
								b.Fatal(err)
							}
							b.ReportAllocs()
							b.ResetTimer()
							for b.Loop() {
								if _, err = p.ExecuteTyped(context.Background(), ExecuteOptions{}); err != nil {
									b.Fatal(err)
								}
							}
						})
					}
				}
			}
		}
	}
}
