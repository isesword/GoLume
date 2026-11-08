package expr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"gocolumn/kernel"
	"gocolumn/memory"
)

func TestCompactLiteralConsumerPlan(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	for _, tc := range []struct {
		expression Expr
		compact    bool
	}{
		{CastTo(I64(5), memory.TypeFloat64), true},
		{GT(Add(Col("x"), I64(1)), I64(5)), true},
		{Concat(Str("prefix"), Str("suffix")), true},
		{Slice(Str("abc"), I64(1), I64(2)), true},
		{Add(Col("x"), I64(5)), false},
		{Abs(I64(5)), false},
		{Mod(Col("x"), I64(5)), false},
	} {
		p, err := Compile(schema, tc.expression)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, in := range p.instructions {
			if in.compactLiteral {
				found = true
			}
		}
		if found != tc.compact {
			t.Fatalf("%v compact=%v", tc.expression, found)
		}
	}
	// Shared uses choose one safe representation, including exported aliases.
	p, err := CompileExpressions(schema, []NamedExpr{Named(I64(5), "root"), Named(CastTo(I64(5), memory.TypeFloat64), "cast"), Named(Add(Col("x"), I64(5)), "dense")})
	if err != nil {
		t.Fatal(err)
	}
	if p.instructions[p.outputs[0].root].compactLiteral {
		t.Fatal("dense consumer lost its vector")
	}
}

func TestCompactLiteralPayloadAndExport(t *testing.T) {
	text := strings.Repeat("雪\x00a", 37)
	p, err := CompileExpressions(memory.NewSchema(), []NamedExpr{
		Named(Str(text), "s"), Named(Str(text), "alias"), Named(I64(math.MinInt64), "i"),
		Named(F64(math.Copysign(0, -1)), "f"), Named(Date32(-123), "date"),
		Named(Timestamp(42), "timestamp"), Named(Null(memory.TypeString), "null"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	manager := memory.NewBudgetManager(0)
	w, err := NewWorkspace(ctx, p, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, rows := range []int{0, 1, 65, 3, 129, 0, 64} {
		batch := memory.NewBatch()
		batch.Rows = rows
		if err := w.execute(ctx, batch, testKernelSet()); err != nil {
			t.Fatal(err)
		}
		for _, output := range p.outputs[:6] {
			if !w.storage.regs[output.root].constant {
				t.Fatal("literal expanded before export")
			}
		}
		values, err := w.Evaluate(ctx, batch, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		if values[0].String.Len() != rows || values[2].Int64.Len() != rows {
			t.Fatal("export length")
		}
		if rows > 0 && &values[0].String.Offsets[0] != &values[1].String.Offsets[0] {
			t.Fatal("duplicate root rematerialized")
		}
		for row := range rows {
			if string(values[0].String.ValueBytes(row)) != text || values[2].Int64.Values[row] != math.MinInt64 ||
				math.Float64bits(values[3].Float64.Values[row]) != math.Float64bits(math.Copysign(0, -1)) ||
				values[4].Int32.Values[row] != -123 || values[5].Int64.Values[row] != 42 || values[6].String.IsValid(row) {
				t.Fatalf("row %d", row)
			}
		}
		if manager.Used() != w.MemoryBytes() {
			t.Fatalf("used=%d retained=%d", manager.Used(), w.MemoryBytes())
		}
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("leaked reservations")
	}
}

func TestCompactLiteralNoRowPayload(t *testing.T) {
	text := strings.Repeat("long literal", 128)
	p, err := Compile(memory.NewSchema(), ByteLength(Str(text)))
	if err != nil {
		t.Fatal(err)
	}
	var w evaluationState
	manager := memory.NewBudgetManager(0)
	w.SetMemoryManager(context.Background(), manager)
	defer w.Close()
	b := memory.NewBatch()
	b.Rows = 4097
	if err := evaluateInstructions(p.instructions, b, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	r := w.ownedRegister(0)
	if !r.constant || len(r.literal) != len(text) || cap(r.data) != 0 || cap(r.offsets) != 0 {
		t.Fatal("string literal has row payload")
	}
	want, err := p.WorkspaceBytes(b.Rows)
	if err != nil {
		t.Fatal(err)
	}
	if manager.Used() != want || w.MemoryBytes() != want {
		t.Fatalf("used=%d retained=%d estimate=%d", manager.Used(), w.MemoryBytes(), want)
	}
	kernels := testKernelSet()
	if got := testing.AllocsPerRun(10, func() {
		if err := evaluateInstructions(p.instructions, b, &w, kernels); err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("warm allocations=%g", got)
	}
}

func TestCompactLiteralExportBudgetRetry(t *testing.T) {
	for _, expression := range []Expr{I64(77), Str(strings.Repeat("x", 31))} {
		p, err := Compile(memory.NewSchema(), expression)
		if err != nil {
			t.Fatal(err)
		}
		const rows = 65
		fixed, err := p.WorkspaceBytes(rows)
		if err != nil {
			t.Fatal(err)
		}
		m := &scalarTestManager{base: memory.NewBudgetManager(0), limit: fixed + int64(unsafe.Sizeof(ScalarValue{}))}
		w, err := NewWorkspace(context.Background(), p, m)
		if err != nil {
			t.Fatal(err)
		}
		b := memory.NewBatch()
		b.Rows = rows
		if _, err := w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatalf("export budget error=%v", err)
		}
		if w.closed || !w.storage.regs[p.root].constant || m.Used() != w.MemoryBytes() {
			t.Fatal("failed materialization damaged retryable state")
		}
		m.limit = 0
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		if out[0].Type == memory.TypeInt64 && out[0].Int64.Values[64] != 77 {
			t.Fatal("retry value")
		}
		if m.Used() != w.MemoryBytes() {
			t.Fatal("retry accounting")
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("retry leaked")
		}
	}
}

func TestCompactLiteralGuardedErrorAndScratchReuse(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	bad := CastTo(Str("not a number"), memory.TypeInt64)
	p, err := Compile(schema, If(GT(Col("x"), I64(0)), bad, I64(7)))
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 65
	b.Int64s["x"] = &memory.Int64Vector{Values: make([]int64, 65)}
	w, _ := NewWorkspace(context.Background(), p, nil)
	if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	b.Int64s["x"].Values[64] = 1
	if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "row 64") {
		t.Fatalf("guarded error=%v", err)
	}
	if !w.closed {
		t.Fatal("semantic error did not close workspace")
	}
	// Streaming arms reuse the same owner across different typed literal leaves.
	p, err = Compile(schema, If(GT(Col("x"), I64(0)), CastTo(I64(-1), memory.TypeFloat64), CastTo(CastTo(F64(3), memory.TypeUint64), memory.TypeFloat64)))
	if err != nil {
		t.Fatal(err)
	}
	w, _ = NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	for range 3 {
		v, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		if v[0].Float64.Values[0] != 3 || v[0].Float64.Values[64] != -1 {
			t.Fatal("scratch payload type contamination")
		}
	}
}

func TestCompactLiteralConcurrentProgram(t *testing.T) {
	p, err := CompileExpressions(memory.NewSchema(), []NamedExpr{Named(Str("immutable"), "s"), Named(CastTo(F64(42), memory.TypeUint64), "u")})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for worker := range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			w, err := NewWorkspace(context.Background(), p, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer w.Close()
			for n := range 10 {
				b := memory.NewBatch()
				b.Rows = 1 + worker*17 + n
				v, err := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeAuto))
				if err != nil {
					t.Error(err)
					return
				}
				if string(v[0].String.ValueBytes(b.Rows-1)) != "immutable" || v[1].Uint64.Values[b.Rows-1] != 42 {
					t.Error(fmt.Sprint("worker ", worker))
					return
				}
			}
		}()
	}
	group.Wait()
}

func TestCompactLiteralReusesDeadDenseOwner(t *testing.T) {
	p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), GT(Add(Col("x"), I64(1)), I64(50)))
	if err != nil {
		t.Fatal(err)
	}
	if p.instructions[1].compactLiteral || !p.instructions[3].compactLiteral || p.instructions[1].storageOwner != p.instructions[3].storageOwner {
		t.Fatal("compact literal did not reuse dead dense capacity")
	}
	b := memory.NewBatch()
	b.Rows = 129
	b.Int64s["x"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
	for row := range b.Rows {
		b.Int64s["x"].Values[row] = int64(row)
	}
	m := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, m)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for range 3 {
		v, err := w.EvaluateBool(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		for row := range b.Rows {
			if memory.IsSet(v.Truth, row) != (row+1 > 50) {
				t.Fatal("wrong row", row)
			}
		}
		owner := w.storage.ownedRegister(3)
		if !owner.constant || owner.i64[0] != 1 || owner.i64[b.Rows-1] != 1 {
			t.Fatal("compact reuse filled dense payload")
		}
		if m.Used() != w.MemoryBytes() {
			t.Fatal("mixed representation owner accounting")
		}
	}
}
