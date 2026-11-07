package expr

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

func livenessChain(input Expr, depth int, boolean bool) Expr {
	for range depth {
		if boolean {
			input = Not(input)
		} else {
			input = Add(input, I64(1))
		}
	}
	return input
}

func livenessBatch(rows int, nullable bool) *memory.Batch {
	batch := memory.NewBatch()
	batch.Rows = rows
	x := &memory.Int64Vector{Values: make([]int64, rows)}
	c := &memory.Uint32Vector{Values: make([]uint32, rows)}
	if nullable {
		x.Validity = make(memory.Bitmap, memory.WordsFor(rows))
		c.Validity = make(memory.Bitmap, memory.WordsFor(rows))
	}
	for row := range rows {
		x.Values[row] = int64(row % 97)
		c.Values[row] = uint32(row % 2)
		if nullable && row%5 != 0 {
			memory.Set(x.Validity, row)
		}
		if nullable && row%7 != 0 {
			memory.Set(c.Validity, row)
		}
	}
	batch.Int64s["x"] = x
	batch.Uint32s["c"] = c
	return batch
}

func livenessSchema() memory.Schema {
	return memory.NewSchema(
		memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true},
	)
}

func TestLivenessDeepChainsPreserveInputsAndValidity(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, boolean := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode=%v/bool=%v", mode, boolean), func(t *testing.T) {
				input := Col("x")
				if boolean {
					input = Col("c")
				}
				const depth = 63
				p, err := Compile(livenessSchema(), livenessChain(input, depth, boolean))
				if err != nil {
					t.Fatal(err)
				}
				manager := memory.NewBudgetManager(0)
				w, err := NewWorkspace(context.Background(), p, manager)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				for _, rows := range []int{0, 1, 63, 64, 65, 129, 1, 0, 65, 257, 64} {
					for _, nullable := range []bool{true, false, true} {
						batch := livenessBatch(rows, nullable)
						x, c := batch.Int64s["x"], batch.Uint32s["c"]
						xValues, cValues := slices.Clone(x.Values), slices.Clone(c.Values)
						xValidity, cValidity := slices.Clone(x.Validity), slices.Clone(c.Validity)
						out, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
						if err != nil {
							t.Fatalf("rows=%d nullable=%v: %v", rows, nullable, err)
						}
						for row := range rows {
							if boolean {
								v := out[0].Uint32
								if v.IsValid(row) != c.IsValid(row) || (v.IsValid(row) && v.Values[row] != 1-c.Values[row]) {
									t.Fatalf("rows=%d bool row=%d: value=%d valid=%v", rows, row, v.Values[row], v.IsValid(row))
								}
							} else {
								v := out[0].Int64
								if v.IsValid(row) != x.IsValid(row) || (v.IsValid(row) && v.Values[row] != x.Values[row]+depth) {
									t.Fatalf("rows=%d int64 row=%d: value=%d valid=%v", rows, row, v.Values[row], v.IsValid(row))
								}
							}
						}
						if !slices.Equal(x.Values, xValues) || !slices.Equal(c.Values, cValues) || !slices.Equal(x.Validity, xValidity) || !slices.Equal(c.Validity, cValidity) {
							t.Fatal("evaluation mutated a borrowed input")
						}
						if boolean {
							bits, err := w.EvaluateBool(context.Background(), batch, kernel.New(mode))
							if err != nil {
								t.Fatal(err)
							}
							for row := range rows {
								if memory.IsSet(bits.Validity, row) != c.IsValid(row) || memory.IsSet(bits.Truth, row) != (c.IsValid(row) && c.Values[row] == 0) {
									t.Fatalf("compact bool row=%d: truth=%v validity=%v", row, bits.Truth, bits.Validity)
								}
							}
							if rows%64 != 0 && len(bits.Truth) > 0 && bits.Truth[len(bits.Truth)-1]>>uint(rows%64) != 0 {
								t.Fatal("truth bitmap retained nonzero tail bits")
							}
						}
					}
				}
				if err := w.Close(); err != nil || manager.Used() != 0 || w.MemoryBytes() != 0 {
					t.Fatalf("Close: err=%v used=%d retained=%d", err, manager.Used(), w.MemoryBytes())
				}
			})
		}
	}
}

func TestLivenessFixedWidthCastChains(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		t.Run(typ.String(), func(t *testing.T) {
			var expression Expr = Col("x")
			for range 12 {
				expression = CastTo(CastTo(Add(expression, I64(1)), typ), memory.TypeInt64)
			}
			p, err := Compile(livenessSchema(), expression)
			if err != nil {
				t.Fatal(err)
			}
			w, err := NewWorkspace(context.Background(), p, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			for _, rows := range []int{129, 1, 65, 0, 64} {
				batch := livenessBatch(rows, true)
				values, err := w.Evaluate(context.Background(), batch, testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
				for row := range rows {
					input, got := batch.Int64s["x"], values[0].Int64
					if got.IsValid(row) != input.IsValid(row) || (got.IsValid(row) && got.Values[row] != input.Values[row]+12) {
						t.Fatalf("rows=%d row=%d value=%d valid=%v", rows, row, got.Values[row], got.IsValid(row))
					}
				}
			}
		})
	}
}

func TestLivenessProjectionKeepsSharedAndEarlyOutputs(t *testing.T) {
	shared := Add(Col("x"), I64(3))
	left := livenessChain(shared, 11, false)
	right := livenessChain(Mul(shared, I64(2)), 17, false)
	truth := GT(shared, I64(40))
	p, err := CompileExpressions(livenessSchema(), []NamedExpr{
		Named(shared, "early"), Named(left, "left"), Named(right, "right"),
		Named(Add(left, right), "diamond"), Named(shared, "duplicate"),
		Named(truth, "truth"), Named(Not(truth), "opposite"),
		Named(And(truth, Not(truth)), "contradiction"), Named(Col("x"), "borrowed"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.outputs[0].root != p.outputs[4].root {
		t.Fatal("fixture no longer exercises duplicate shared output roots")
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, rows := range []int{65, 130, 1, 64} {
		batch := livenessBatch(rows, true)
		out, err := w.Evaluate(context.Background(), batch, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		for row := range rows {
			x := batch.Int64s["x"].Values[row]
			valid := batch.Int64s["x"].IsValid(row)
			want := []int64{x + 3, x + 14, 2*x + 23, 3*x + 37, x + 3}
			for column, value := range want {
				got := out[column].Int64
				if got.IsValid(row) != valid || (valid && got.Values[row] != value) {
					t.Fatalf("output=%d row=%d value=%d want=%d valid=%v", column, row, got.Values[row], value, got.IsValid(row))
				}
			}
			for column, value := range []bool{x+3 > 40, x+3 <= 40, false} {
				got := out[column+5].Uint32
				if got.IsValid(row) != valid || (valid && (got.Values[row] != 0) != value) {
					t.Fatalf("bool output=%d row=%d value=%d valid=%v", column, row, got.Values[row], got.IsValid(row))
				}
			}
		}
		if rows > 0 && &out[8].Int64.Values[0] != &batch.Int64s["x"].Values[0] {
			t.Fatal("direct fixed-width output stopped borrowing input storage")
		}
	}
}

func TestLivenessDiamondKeepsNonOutputCommonSubexpression(t *testing.T) {
	shared := Add(Col("x"), I64(3))
	left := livenessChain(shared, 19, false)
	right := livenessChain(Mul(shared, shared), 23, false)
	p, err := Compile(livenessSchema(), Sub(right, left))
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, rows := range []int{129, 1, 65, 0, 64} {
		batch := livenessBatch(rows, true)
		out, err := w.Evaluate(context.Background(), batch, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		for row := range rows {
			input, got := batch.Int64s["x"], out[0].Int64
			x := input.Values[row] + 3
			want := x*x + 23 - (x + 19)
			if got.IsValid(row) != input.IsValid(row) || (got.IsValid(row) && got.Values[row] != want) {
				t.Fatalf("rows=%d row=%d value=%d want=%d valid=%v", rows, row, got.Values[row], want, got.IsValid(row))
			}
		}
	}
}

func TestLivenessBooleanDiamondThreeValuedLogic(t *testing.T) {
	// Neither shared input is an output root. The two branches keep both alive
	// across intervening Boolean operations, and OR consumes both branches.
	a, b := GT(Col("x"), I64(40)), Col("c")
	expression := Or(And(a, Not(b)), And(Not(a), b))
	p, err := Compile(livenessSchema(), expression)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, rows := range []int{129, 1, 65, 0, 64} {
		batch := livenessBatch(rows, true)
		out, err := w.EvaluateBool(context.Background(), batch, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		for row := range rows {
			x, c := batch.Int64s["x"], batch.Uint32s["c"]
			valid := x.IsValid(row) && c.IsValid(row)
			truth := valid && ((x.Values[row] > 40) != (c.Values[row] != 0))
			if memory.IsSet(out.Validity, row) != valid || memory.IsSet(out.Truth, row) != truth {
				t.Fatalf("rows=%d row=%d truth=%v valid=%v, want %v/%v", rows, row, memory.IsSet(out.Truth, row), memory.IsSet(out.Validity, row), truth, valid)
			}
		}
	}
}

func TestLivenessConditionalRegionsKeepDemandLocal(t *testing.T) {
	then := CastTo(livenessChain(Col("x"), 12, false), memory.TypeInt8)
	expression := If(Col("c"), CastTo(then, memory.TypeInt64), livenessChain(Col("x"), 32, false))
	p, err := Compile(livenessSchema(), expression)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, rows := range []int{129, 1, 64, 0, 65} {
		batch := livenessBatch(rows, true)
		for row := range rows {
			if batch.Uint32s["c"].Values[row] == 0 {
				// This value would overflow the narrow cast if the THEN region
				// evaluated rows belonging to ELSE.
				batch.Int64s["x"].Values[row] = 1024
			}
		}
		values, err := w.Evaluate(context.Background(), batch, testKernelSet())
		if err != nil {
			t.Fatalf("rows=%d: %v", rows, err)
		}
		for row := range rows {
			input, got, condition := batch.Int64s["x"], values[0].Int64, batch.Uint32s["c"]
			delta := int64(32)
			if condition.IsValid(row) && condition.Values[row] != 0 {
				delta = 12
			}
			if got.IsValid(row) != input.IsValid(row) || (got.IsValid(row) && got.Values[row] != input.Values[row]+delta) {
				t.Fatalf("rows=%d row=%d value=%d valid=%v", rows, row, got.Values[row], got.IsValid(row))
			}
		}
	}
}

func TestLivenessAdmissionRetryAndCancellation(t *testing.T) {
	p, err := Compile(livenessSchema(), livenessChain(Col("x"), 64, false))
	if err != nil {
		t.Fatal(err)
	}
	manager := &scalarTestManager{base: memory.NewBudgetManager(0)}
	w, err := NewWorkspace(context.Background(), p, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	small := livenessBatch(65, true)
	if _, err := w.Evaluate(context.Background(), small, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	used := manager.Used()
	manager.limit = used + 1
	if _, err := w.Evaluate(context.Background(), livenessBatch(4096, true), testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("growth error=%v", err)
	}
	if manager.Used() != used {
		t.Fatalf("rejected growth changed retained reservation: got=%d want=%d", manager.Used(), used)
	}
	if _, err := w.Evaluate(context.Background(), small, testKernelSet()); err != nil {
		t.Fatalf("small retry: %v", err)
	}
	manager.limit = 0
	if _, err := w.Evaluate(context.Background(), livenessBatch(4096, false), testKernelSet()); err != nil {
		t.Fatalf("growth retry: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.Evaluate(ctx, small, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v", err)
	}
	if manager.Used() != 0 || w.MemoryBytes() != 0 {
		t.Fatalf("cancel leaked: used=%d retained=%d", manager.Used(), w.MemoryBytes())
	}
}

func TestLivenessChainPayloadBoundedByLiveValues(t *testing.T) {
	const rows = 65536
	for _, boolean := range []bool{false, true} {
		t.Run(fmt.Sprintf("bool=%v", boolean), func(t *testing.T) {
			payload := func(depth int) int64 {
				input := Col("x")
				if boolean {
					input = Col("c")
				}
				p, err := Compile(livenessSchema(), livenessChain(input, depth, boolean))
				if err != nil {
					t.Fatal(err)
				}
				full, err := p.WorkspaceBytes(rows)
				if err != nil {
					t.Fatal(err)
				}
				metadata, err := p.WorkspaceBytes(0)
				if err != nil {
					t.Fatal(err)
				}
				return full - metadata
			}
			shallow, deep := payload(8), payload(64)
			if shallow <= 0 || deep > 2*shallow {
				t.Fatalf("chain payload grows with dead values: depth8=%d depth64=%d", shallow, deep)
			}
		})
	}
}
