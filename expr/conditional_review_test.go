package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

// Each arm has a different owner/type pattern. The expected values are an
// independent oracle, not another invocation of the conditional evaluator.
func TestConditionalReviewTypedScratchAndNullSelection(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	arms := []WhenThen{
		{When: EQ(Col("x"), I64(0)), Then: Null(memory.TypeInt8)},
		{When: EQ(Col("x"), I64(1)), Then: CastTo(I64(-7), memory.TypeInt8)},
		{When: EQ(Col("x"), I64(2)), Then: CastTo(I64(65535), memory.TypeUint16)},
		{When: EQ(Col("x"), I64(3)), Then: CastTo(F64(1.5), memory.TypeFloat32)},
		{When: EQ(Col("x"), I64(4)), Then: CastTo(I64(17), memory.TypeUint64)},
	}
	e := Case(arms, F64(-99))
	p, err := CompileExpressions(schema, []NamedExpr{Named(e, "a"), Named(e, "duplicate"), Named(Coalesce(Null(memory.TypeInt16), CastTo(I64(23), memory.TypeInt16), I64(42)), "coalesced")})
	if err != nil {
		t.Fatal(err)
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("duplicate conditional root lost")
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		var states [2]evaluationState
		for _, rows := range []int{130, 1, 65, 0, 129, 257, 63} {
			b := memory.NewBatch()
			b.Rows = rows
			x := &memory.Int64Vector{Values: make([]int64, rows)}
			b.Int64s["x"] = x
			for state := range states {
				for row := range rows {
					x.Values[row] = int64((row + state) % 6)
				}
				out, err := evaluateProjectionTest(p, b, &states[state], kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				for row := range rows {
					selector := int(x.Values[row])
					want := []float64{0, -7, 65535, 1.5, 17, -99}[selector]
					for _, col := range out[:2] {
						v := col.Float64
						if v.IsValid(row) != (selector != 0) || (selector != 0 && v.Values[row] != want) {
							t.Fatalf("rows=%d state=%d row=%d got=%v valid=%v want=%v", rows, state, row, v.Values[row], v.IsValid(row), want)
						}
					}
					if out[2].Int64.Values[row] != 23 {
						t.Fatal("coalesce did not select first valid value")
					}
				}
			}
		}
		for i := range states {
			states[i].Close()
		}
	}
}

func TestConditionalReviewAdmissionRetry(t *testing.T) {
	p, b := conditionalMemoryFixture(t, "case", 8, 130)
	// Fail each admission once, including metadata growth and later arm types.
	for failAt := 1; failAt < 100; failAt++ {
		budget := memory.NewBudgetManager(0)
		calls := 0
		rejected := false
		var w evaluationState
		w.SetReserveHook(func(n int64) (memory.Reservation, error) {
			calls++
			if calls == failAt {
				rejected = true
				return nil, memory.ErrBudgetExceeded
			}
			return budget.Reserve(context.Background(), n)
		})
		err := evaluateInstructions(p.instructions, b, &w, testKernelSet())
		if !rejected {
			if err != nil {
				t.Fatal(err)
			}
			w.Close()
			if budget.Used() != 0 {
				t.Fatal("success leak")
			}
			break
		}
		if !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatalf("admission %d: %v", failAt, err)
		}
		if budget.Used() != w.MemoryBytes() {
			t.Fatalf("admission %d retained accounting: %d != %d", failAt, budget.Used(), w.MemoryBytes())
		}
		if err = evaluateInstructions(p.instructions, b, &w, testKernelSet()); err != nil {
			t.Fatalf("retry %d: %v", failAt, err)
		}
		root := &w.regs[p.root]
		for row := range b.Rows {
			if !isValidRegister(root, row) || root.i64[row] != 2*int64(row%8) {
				t.Fatalf("retry %d row %d", failAt, row)
			}
		}
		if budget.Used() != w.MemoryBytes() {
			t.Fatalf("retry %d accounting", failAt)
		}
		w.Close()
		w.Close()
		if budget.Used() != 0 {
			t.Fatalf("admission %d leak %d", failAt, budget.Used())
		}
	}
}

func TestConditionalReviewGuardFallbackAndCancellation(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	bad := CastTo(I64(1000), memory.TypeInt8)
	cases := []Expr{
		Case([]WhenThen{{When: Bool(true), Then: Null(memory.TypeInt8)}, {When: Bool(true), Then: bad}}, bad),
		If(Bool(true), I64(7), CastTo(bad, memory.TypeInt64)),
		If(Bool(true), If(Bool(false), CastTo(bad, memory.TypeInt64), I64(7)), CastTo(bad, memory.TypeInt64)),
	}
	for i, e := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			p, err := Compile(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			budget := memory.NewBudgetManager(0)
			var w evaluationState
			w.SetMemoryManager(context.Background(), budget)
			b := memory.NewBatch()
			b.Rows = 65
			b.Int64s["x"] = &memory.Int64Vector{Values: make([]int64, 65)}
			if err = evaluateInstructions(p.instructions, b, &w, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			for row := range b.Rows {
				r := &w.regs[p.root]
				if i == 0 {
					if isValidRegister(r, row) {
						t.Fatal("NULL CASE arm fell through")
					}
				} else {
					v, err := registerInt64(r, row)
					if err != nil || v != 7 {
						t.Fatal("guarded nested result", v, err)
					}
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			w.ctx = ctx
			if err = evaluateInstructions(p.instructions, b, &w, testKernelSet()); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if budget.Used() != 0 || w.MemoryBytes() != 0 {
				t.Fatal("cancellation leaked")
			}
			w.ctx = context.Background()
			if err = evaluateInstructions(p.instructions, b, &w, testKernelSet()); err != nil {
				t.Fatal("retry after cancellation", err)
			}
			w.Close()
			if budget.Used() != 0 {
				t.Fatal("close leaked")
			}
		})
	}
}

func TestConditionalReviewBooleanTailAndBorrowedValidity(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true})
	expressions := []Expr{If(Col("c"), Not(Col("c")), Bool(true)), Case([]WhenThen{{When: Col("c"), Then: Null(memory.TypeBool)}}, Bool(true)), Coalesce(Col("c"), Bool(true))}
	for kind, e := range expressions {
		p, err := Compile(schema, e)
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			var w evaluationState
			for _, rows := range []int{130, 1, 65, 0, 129} {
				b := memory.NewBatch()
				b.Rows = rows
				c := &memory.Uint32Vector{Values: make([]uint32, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows))}
				b.Uint32s["c"] = c
				for row := range rows {
					c.Values[row] = uint32(row % 2)
					if row%3 != 0 {
						memory.Set(c.Validity, row)
					}
				}
				result, err := evaluateBoolTest(p, b, &w, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				for row := range rows {
					valid, truth := true, true
					selected := row%3 != 0 && row%2 == 1
					switch kind {
					case 0:
						truth = !selected
					case 1:
						valid = !selected
					case 2:
						truth = row%3 == 0 || row%2 == 1
					}
					if memory.IsSet(result.Validity, row) != valid || (valid && memory.IsSet(result.Truth, row) != truth) {
						t.Fatalf("kind=%d rows=%d row=%d", kind, rows, row)
					}
					if c.IsValid(row) != (row%3 != 0) || c.Values[row] != uint32(row%2) {
						t.Fatal("borrowed input modified")
					}
				}
				if rows%64 != 0 && len(result.Truth) > 0 {
					mask := ^uint64(0) << uint(rows%64)
					if result.Truth[len(result.Truth)-1]&mask != 0 || result.Validity[len(result.Validity)-1]&mask != 0 {
						t.Fatal("dirty bitmap tail")
					}
				}
			}
			w.Close()
		}
	}
}

func TestConditionalReviewCancellationDuringAdmission(t *testing.T) {
	p, b := conditionalMemoryFixture(t, "case", 8, 130)
	for cancelAt := 2; cancelAt < 100; cancelAt++ {
		ctx, cancel := context.WithCancel(context.Background())
		budget := memory.NewBudgetManager(0)
		calls := 0
		canceled := false
		var w evaluationState
		w.ctx = ctx
		w.SetReserveHook(func(n int64) (memory.Reservation, error) {
			calls++
			if calls == cancelAt {
				canceled = true
				cancel()
			}
			return budget.Reserve(context.Background(), n)
		})
		err := evaluateInstructions(p.instructions, b, &w, testKernelSet())
		cancel()
		if !canceled {
			if err != nil {
				t.Fatal(err)
			}
			w.Close()
			if budget.Used() != 0 {
				t.Fatal("success leak")
			}
			break
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation admission %d: %v", cancelAt, err)
		}
		if budget.Used() != 0 || w.MemoryBytes() != 0 {
			t.Fatalf("cancellation admission %d retained %d bytes", cancelAt, budget.Used())
		}
		w.ctx = context.Background()
		if err = evaluateInstructions(p.instructions, b, &w, testKernelSet()); err != nil {
			t.Fatalf("cancellation retry %d: %v", cancelAt, err)
		}
		w.Close()
		if budget.Used() != 0 {
			t.Fatal("retry leaked")
		}
	}
}
