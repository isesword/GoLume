package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"strings"
	"testing"
)

func checkedIntegerQuery(tb testing.TB, typ memory.DataType, op kernel.ArithmeticOp, rows, batchRows, nullPct int, selected, poison bool) (*inmemory.Source, expr.Query, uint64) {
	tb.Helper()
	schema := memory.NewSchema(memory.Field{Name: "a", Type: typ, Nullable: nullPct != 0}, memory.Field{Name: "b", Type: typ}, memory.Field{Name: "gate", Type: memory.TypeInt64})
	var batches []*memory.Batch
	var want uint64
	for base := 0; base < rows; base += batchRows {
		n := min(batchRows, rows-base)
		batch := memory.NewBatch()
		batch.Reset(n)
		a, b, g := make([]int64, n), make([]int64, n), make([]int64, n)
		ua, ub := make([]uint64, n), make([]uint64, n)
		var valid memory.Bitmap
		if nullPct != 0 {
			valid = make(memory.Bitmap, memory.WordsFor(n))
		}
		for i := range a {
			row := base + i
			a[i], b[i], ua[i], ub[i] = 6, 2, 6, 2
			g[i] = int64(row % 100)
			active := !selected || g[i] < 50
			isValid := (row*37)%100 >= nullPct
			if isValid && nullPct != 0 {
				memory.Set(valid, i)
			}
			if active && isValid {
				want++
			}
			if poison && (!active || !isValid) {
				switch op {
				case kernel.ArithmeticAdd:
					a[i], b[i], ua[i], ub[i] = math.MaxInt64, 1, math.MaxUint64, 1
				case kernel.ArithmeticSub:
					a[i], b[i], ua[i], ub[i] = math.MinInt64, 1, 0, 1
				case kernel.ArithmeticMul:
					a[i], b[i], ua[i], ub[i] = math.MinInt64, -1, math.MaxUint64, 2
				}
			}
		}
		if typ == memory.TypeInt64 {
			batch.Int64s["a"] = &memory.Int64Vector{Values: a, Validity: valid}
			batch.Int64s["b"] = &memory.Int64Vector{Values: b}
		} else {
			batch.Uint64s["a"] = &memory.Uint64Vector{Values: ua, Validity: valid}
			batch.Uint64s["b"] = &memory.Uint64Vector{Values: ub}
		}
		batch.Int64s["gate"] = &memory.Int64Vector{Values: g}
		batches = append(batches, batch)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		tb.Fatal(err)
	}
	var value expr.Expr
	switch op {
	case kernel.ArithmeticAdd:
		value = expr.Add(expr.Col("a"), expr.Col("b"))
	case kernel.ArithmeticSub:
		value = expr.Sub(expr.Col("a"), expr.Col("b"))
	case kernel.ArithmeticMul:
		value = expr.Mul(expr.Col("a"), expr.Col("b"))
	}
	var zero expr.Expr = expr.I64(0)
	if typ == memory.TypeUint64 {
		zero = expr.I64(0).Cast(memory.TypeUint64)
	}
	if selected {
		value = expr.If(expr.LT(expr.Col("gate"), expr.I64(50)), value, zero)
	}
	return src, expr.Query{Filter: expr.GT(value, zero), Aggregates: []expr.Aggregate{expr.Count("count")}}, want
}

func TestCheckedIntegerQueryMasksAndBudget(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
		for _, op := range []kernel.ArithmeticOp{kernel.ArithmeticAdd, kernel.ArithmeticSub, kernel.ArithmeticMul} {
			for _, nullPct := range []int{0, 1, 50, 99, 100} {
				for _, selected := range []bool{false, true} {
					for _, batchRows := range []int{31, 65, 1021} {
						src, q, want := checkedIntegerQuery(t, typ, op, 2051, batchRows, nullPct, selected, true)
						for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
							opts := DefaultOptions()
							opts.Workers = 1
							opts.PreparedCacheBytes = 0
							opts.KernelMode = mode
							opts.MemoryLimitBytes = 4 << 20
							opts.ExecutionPolicy = ExecutionBounded
							e := New(opts)
							budget, err := e.BeginQueryMemory()
							if err != nil {
								t.Fatal(err)
							}
							result, err := e.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), src, q, ExecuteOptions{Materialize: true})
							budget.Close()
							closeErr := e.Close()
							if err != nil || closeErr != nil {
								t.Fatalf("%s/%s/null%d/selected%t/batch%d/%s: %v %v", typ, op, nullPct, selected, batchRows, mode, err, closeErr)
							}
							got, ok := result.Rows[0].Aggregates[0].Value.AsUint64()
							if !ok || got != want {
								t.Fatalf("%s/%s/null%d/selected%t/batch%d/%s count=%d want=%d", typ, op, nullPct, selected, batchRows, mode, got, want)
							}
							if snap := budget.Snapshot(); snap.UsedBytes != 0 || snap.PeakBytes > opts.MemoryLimitBytes {
								t.Fatalf("memory %+v", snap)
							}
						}
					}
				}
			}
		}
	}
	src, q, _ := checkedIntegerQuery(t, memory.TypeInt64, kernel.ArithmeticMul, 2051, 1021, 50, true, true)
	opts := DefaultOptions()
	opts.Workers = 1
	opts.KernelMode = kernel.ModeAuto
	opts.MemoryLimitBytes = 1024
	opts.ExecutionPolicy = ExecutionBounded
	e := New(opts)
	_, err := e.ExecuteTyped(context.Background(), src, q, ExecuteOptions{})
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("tight budget: %v", err)
	}
	e.Close()
	if e.Memory().Used() != 0 {
		t.Fatal("budget rejection leaked", e.Memory().Used())
	}
}

func TestCheckedIntegerQueryReportsFirstOverflow(t *testing.T) {
	const n = 257
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
		for _, op := range []kernel.ArithmeticOp{kernel.ArithmeticAdd, kernel.ArithmeticSub, kernel.ArithmeticMul} {
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
				schema := memory.NewSchema(memory.Field{Name: "a", Type: typ}, memory.Field{Name: "b", Type: typ})
				batch := memory.NewBatch()
				batch.Reset(n)
				a, b := make([]int64, n), make([]int64, n)
				ua, ub := make([]uint64, n), make([]uint64, n)
				for i := range a {
					a[i], b[i], ua[i], ub[i] = 6, 2, 6, 2
				}
				for _, i := range []int{65, 130, 256} {
					switch op {
					case kernel.ArithmeticAdd:
						a[i], b[i], ua[i], ub[i] = math.MaxInt64, 1, math.MaxUint64, 1
					case kernel.ArithmeticSub:
						a[i], b[i], ua[i], ub[i] = math.MinInt64, 1, 0, 1
					case kernel.ArithmeticMul:
						a[i], b[i], ua[i], ub[i] = math.MinInt64, -1, math.MaxUint64, 2
					}
				}
				if typ == memory.TypeInt64 {
					batch.Int64s["a"] = &memory.Int64Vector{Values: a}
					batch.Int64s["b"] = &memory.Int64Vector{Values: b}
				} else {
					batch.Uint64s["a"] = &memory.Uint64Vector{Values: ua}
					batch.Uint64s["b"] = &memory.Uint64Vector{Values: ub}
				}
				src, err := inmemory.New(schema, batch)
				if err != nil {
					t.Fatal(err)
				}
				var value expr.Expr
				switch op {
				case kernel.ArithmeticAdd:
					value = expr.Add(expr.Col("a"), expr.Col("b"))
				case kernel.ArithmeticSub:
					value = expr.Sub(expr.Col("a"), expr.Col("b"))
				case kernel.ArithmeticMul:
					value = expr.Mul(expr.Col("a"), expr.Col("b"))
				}
				var zero expr.Expr = expr.I64(0)
				if typ == memory.TypeUint64 {
					zero = expr.I64(0).Cast(memory.TypeUint64)
				}
				opts := DefaultOptions()
				opts.Workers = 1
				opts.KernelMode = mode
				opts.MemoryLimitBytes = 4 << 20
				e := New(opts)
				_, err = e.ExecuteTyped(context.Background(), src, expr.Query{Filter: expr.GT(value, zero), Aggregates: []expr.Aggregate{expr.Count("count")}}, ExecuteOptions{})
				e.Close()
				if err == nil || !strings.Contains(err.Error(), "overflows at row 65") {
					t.Fatalf("%s/%s/%s error %v", typ, op, mode, err)
				}
				if e.Memory().Used() != 0 {
					t.Fatal("error leaked", e.Memory().Used())
				}
			}
		}
	}
}

func BenchmarkCheckedIntegerQuery(b *testing.B) {
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
		for _, op := range []kernel.ArithmeticOp{kernel.ArithmeticAdd, kernel.ArithmeticSub, kernel.ArithmeticMul} {
			for _, nullPct := range []int{0, 50} {
				for _, selected := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/%s/null%d/selected%t", typ, op, nullPct, selected), func(b *testing.B) {
						src, q, _ := checkedIntegerQuery(b, typ, op, 65539, 1021, nullPct, selected, false)
						opts := DefaultOptions()
						opts.Workers = 1
						opts.PreparedCacheBytes = 0
						opts.KernelMode = kernel.ModeAuto
						opts.MemoryLimitBytes = 64 << 20
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

// The control has identical scan/aggregate scaffolding but no arithmetic kernel.
func BenchmarkCheckedIntegerControlQuery(b *testing.B) {
	src, _, _ := checkedIntegerQuery(b, memory.TypeInt64, kernel.ArithmeticAdd, 65539, 1021, 50, false, false)
	q := expr.Query{Filter: expr.GT(expr.Col("a"), expr.I64(0)), Aggregates: []expr.Aggregate{expr.Count("count")}}
	opts := DefaultOptions()
	opts.Workers = 1
	opts.PreparedCacheBytes = 0
	opts.KernelMode = kernel.ModeAuto
	opts.MemoryLimitBytes = 64 << 20
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
}
