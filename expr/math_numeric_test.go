package expr

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"strings"
	"testing"
)

func TestMathExpressionIntegerSemantics(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Rows = 5
	batch.Int64s["a"] = &memory.Int64Vector{Values: []int64{-5, 5, math.MinInt64, 7, 2}}
	batch.Int64s["b"] = &memory.Int64Vector{Values: []int64{2, -2, -1, 0, 63}}
	p, err := CompileExpressions(schema, []NamedExpr{Named(Col("a").Abs(), "abs"), Named(Col("a").FloorDiv(Col("b")), "div"), Named(Col("a").Mod(Col("b")), "mod")})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD, kernel.ModeAuto} {
		var w evaluationState
		got, err := evaluateProjectionTest(p, batch, &w, kernel.New(mode))
		if err != nil {
			t.Fatal(err)
		}
		if got[0].Int64.Values[2] != math.MinInt64 {
			t.Fatal("abs minimum changed")
		}
		if got[1].Int64.Values[0] != -3 || got[1].Int64.Values[1] != -3 || got[1].Int64.Values[2] != math.MinInt64 {
			t.Fatal(got[1].Int64.Values)
		}
		if memory.IsSet(got[1].Int64.Validity, 3) || memory.IsSet(got[2].Int64.Validity, 3) {
			t.Fatal("zero division not null")
		}
		if got[2].Int64.Values[0] != 1 || got[2].Int64.Values[1] != -1 {
			t.Fatal(got[2].Int64.Values)
		}
		_ = w.Close()
	}
}
func TestMathExpressionFloatSemantics(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeFloat64})
	batch := memory.NewBatch()
	batch.Rows = 7
	batch.Float64s["a"] = &memory.Float64Vector{Values: []float64{-2.5, 2.5, -1, 0, math.Copysign(0, -1), math.Inf(1), math.NaN()}}
	p, err := CompileExpressions(schema, []NamedExpr{Named(Col("a").Round(), "round"), Named(Col("a").Sqrt(), "sqrt"), Named(Col("a").Log(), "log"), Named(Col("a").Floor(), "floor"), Named(Col("a").Ceil(), "ceil"), Named(Col("a").Pow(F64(2)), "pow")})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD, kernel.ModeAuto} {
		var w evaluationState
		got, err := evaluateProjectionTest(p, batch, &w, kernel.New(mode))
		if err != nil {
			t.Fatal(err)
		}
		if got[0].Float64.Values[0] != -2 || got[0].Float64.Values[1] != 2 {
			t.Fatal("round ties")
		}
		if !math.IsNaN(got[1].Float64.Values[2]) || !math.Signbit(got[1].Float64.Values[4]) {
			t.Fatal("sqrt domain/negative zero")
		}
		if !math.IsInf(got[2].Float64.Values[3], -1) || !math.IsInf(got[2].Float64.Values[4], -1) {
			t.Fatal("log zero")
		}
		if got[3].Float64.Values[0] != -3 || got[4].Float64.Values[0] != -2 || got[5].Float64.Values[0] != 6.25 {
			t.Fatal("math result")
		}
		_ = w.Close()
	}
}
func TestMathTypeAndErrorMetadata(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt8}, memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "s", Type: memory.TypeInt64}, memory.Field{Name: "f", Type: memory.TypeFloat32})
	for _, tc := range []struct {
		e                  Expr
		typ                memory.DataType
		nullable, mayError bool
	}{
		{Col("i").Abs(), memory.TypeInt8, false, false},
		{Col("i").FloorDiv(I64(2)), memory.TypeInt8, true, false},
		{Col("u").Mod(I64(3)), memory.TypeUint64, true, false},
		{Col("u").Pow(I64(2)), memory.TypeUint64, false, false},
		{Col("i").Sqrt(), memory.TypeFloat64, false, false},
		{Col("f").Sqrt(), memory.TypeFloat32, false, false},
		{Col("s").Pow(I64(-1)), memory.TypeInt64, false, true},
	} {
		b, err := Bind(schema, tc.e)
		if err != nil {
			t.Fatal(err)
		}
		if b.Field.Type != tc.typ || b.Field.Nullable != tc.nullable || b.MayError != tc.mayError {
			t.Fatalf("%s got=%s nullable=%v mayError=%v", tc.e, b.Field.Type, b.Field.Nullable, b.MayError)
		}
	}
	if _, err := Compile(schema, Col("u").Mod(Col("s"))); err == nil {
		t.Fatal("lossy mixed signed/uint64 implicit conversion")
	}
	if _, err := Compile(schema, Sqrt(Str("4"))); err == nil {
		t.Fatal("string math accepted")
	}
}
func TestMathUint64PrecisionAndNativeWidths(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "u", Type: memory.TypeUint64})
	batch := memory.NewBatch()
	batch.Rows = 2
	batch.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64, 9007199254740993}}
	p, err := CompileExpressions(schema, []NamedExpr{Named(Col("u").Mod(I64(2)), "m"), Named(Col("u").FloorDiv(I64(1)), "d"), Named(Col("u").Pow(I64(1)), "p")})
	if err != nil {
		t.Fatal(err)
	}
	var w evaluationState
	defer w.Close()
	got, err := evaluateProjectionTest(p, batch, &w, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Uint64.Values[1] != 1 || got[1].Uint64.Values[1] != 9007199254740993 || got[2].Uint64.Values[0] != math.MaxUint64 {
		t.Fatal("lost integral precision")
	}
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		e := CastTo(I64(5), typ).Mod(CastTo(I64(2), typ))
		p, err := Compile(memory.NewSchema(), e)
		if err != nil {
			t.Fatal(err)
		}
		empty := memory.NewBatch()
		empty.Rows = 65
		var state evaluationState
		got, err := evaluateValueTest(p, empty, &state, kernel.New(kernel.ModeScalar))
		if err != nil {
			t.Fatal(err)
		}
		if got.Type != typ {
			t.Fatalf("%s got %s", typ, got.Type)
		}
		v, err := registerFloat64(state.regs[p.root], 64)
		if err != nil || v != 1 {
			t.Fatal(typ, v, err)
		}
		_ = state.Close()
	}
}
func TestMathGuardedPowerAndReuse(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Rows = 3
	batch.Int64s["a"] = &memory.Int64Vector{Values: []int64{2, 3, 4}}
	batch.Int64s["b"] = &memory.Int64Vector{Values: []int64{-1, 2, -3}}
	e := If(GE(Col("b"), I64(0)), Col("a").Pow(Col("b")), I64(0))
	p, err := Compile(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	var w evaluationState
	defer w.Close()
	got, err := evaluateValueTest(p, batch, &w, kernel.New(kernel.ModeSIMD))
	if err != nil {
		t.Fatal(err)
	}
	if got.Int64.Values[0] != 0 || got.Int64.Values[1] != 9 || got.Int64.Values[2] != 0 {
		t.Fatal(got.Int64.Values)
	}
	unguarded, _ := Compile(schema, Col("a").Pow(Col("b")))
	if _, err := evaluateValueTest(unguarded, batch, &evaluationState{}, kernel.New(kernel.ModeScalar)); err == nil || !strings.Contains(err.Error(), "row 0") {
		t.Fatal(err)
	}
	// Reused output null bits must clear when the next batch has no zero divisors.
	p, _ = Compile(schema, Col("a").Mod(Col("b")))
	batch.Int64s["b"].Values[0] = 0
	if _, err := evaluateValueTest(p, batch, &w, kernel.New(kernel.ModeScalar)); err != nil {
		t.Fatal(err)
	}
	batch.Int64s["b"].Values[0] = 2
	got, err = evaluateValueTest(p, batch, &w, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	if got.Int64.Validity != nil && !memory.IsSet(got.Int64.Validity, 0) {
		t.Fatal("stale null")
	}
}
func TestMathBudgetAndLiveness(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeFloat64})
	var e Expr = Col("x")
	for i := 0; i < 64; i++ {
		e = Sqrt(Abs(e))
	}
	p, err := Compile(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	if owners := storageOwnerCount(p.instructions); owners > 3 {
		t.Fatalf("math storage liveness owners=%d", owners)
	}
	manager := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, manager)
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range []int{0, 1, 63, 64, 65, 4097, 3, 0} {
		batch := memory.NewBatch()
		batch.Rows = rows
		values := make([]float64, rows)
		for i := range values {
			values[i] = 16
		}
		batch.Float64s["x"] = &memory.Float64Vector{Values: values}
		if _, err := w.Evaluate(context.Background(), batch, kernel.New(kernel.ModeSIMD)); err != nil {
			t.Fatal(err)
		}
		if manager.Used() != w.MemoryBytes() {
			t.Fatalf("charge=%d retained=%d", manager.Used(), w.MemoryBytes())
		}
	}
	_ = w.Close()
	if manager.Used() != 0 {
		t.Fatal("leaked", manager.Used())
	}
	tiny := memory.NewBudgetManager(1)
	failed, _ := NewWorkspace(context.Background(), p, tiny)
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Float64s["x"] = &memory.Float64Vector{Values: []float64{4}}
	if _, err := failed.Evaluate(context.Background(), batch, kernel.New(kernel.ModeScalar)); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	_ = failed.Close()
	if tiny.Used() != 0 {
		t.Fatal("failed budget leaked")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewWorkspace(ctx, p, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestMathMixedUnsignedPrecision(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "v", Type: memory.TypeUint32})
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{9007199254740993}}
	batch.Uint32s["v"] = &memory.Uint32Vector{Values: []uint32{2}}
	for _, tc := range []struct {
		e    Expr
		want uint64
	}{{Mod(Col("u"), Col("v")), 1}, {FloorDiv(Col("u"), Col("v")), 4503599627370496}, {Pow(Col("u"), Col("v")), 18014398509481985}} {
		p, err := Compile(schema, tc.e)
		if err != nil {
			t.Fatal(err)
		}
		var w evaluationState
		got, err := evaluateValueTest(p, batch, &w, kernel.New(kernel.ModeScalar))
		if err != nil {
			t.Fatal(err)
		}
		if got.Type != memory.TypeUint64 || got.Uint64.Values[0] != tc.want {
			t.Fatalf("%s result %+v", tc.e, got)
		}
		_ = w.Close()
	}
}
