package expr

import (
	"context"
	"errors"
	"math"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

func TestUnifiedValuePredicateComposition(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	batch := memory.NewBatch()
	batch.Rows = 4
	batch.Int64s["a"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}, Validity: memory.Bitmap{0b1011}}
	batch.Strings["s"] = testStringVector([]string{"one", "two", "three", "four"}, []bool{false, true, true, true})
	cases := []Expr{GT(Add(Col("a"), I64(1)), I64(2)), Contains(Upper(Col("s")), Upper(Str("O"))), IsNull(Add(Col("a"), I64(1))), Not(EQ(Length(Concat(Col("s"), Str("!"))), I64(4)))}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, expression := range cases {
			scalar, err := Compile(schema, expression)
			if err != nil {
				t.Fatal(err)
			}
			predicate, _, err := CompilePredicate(schema, expression)
			if err != nil {
				t.Fatal(err)
			}
			var valueWorkspace evaluationState
			var filterWorkspace evaluationState
			value, err := evaluateValueTest(scalar, batch, &valueWorkspace, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			result, err := evaluateBoolTest(predicate, batch, &filterWorkspace, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < batch.Rows; row++ {
				valid := value.Uint32.Validity == nil || memory.IsSet(value.Uint32.Validity, row)
				if valid != memory.IsSet(result.Validity, row) {
					t.Fatalf("%s validity row %d", expression, row)
				}
				if valid && (value.Uint32.Values[row] != 0) != memory.IsSet(result.Truth, row) {
					t.Fatalf("%s truth row %d", expression, row)
				}
			}
			_ = valueWorkspace.Close()
			filterWorkspace.Release()
		}
	}
}

func TestConditionalRegionsGuardErrorsAndReuse(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString}, memory.Field{Name: "n", Type: memory.TypeTimestamp, Nullable: true})
	parse := CastTo(Col("s"), memory.TypeTimestamp)
	expressions := []Expr{
		If(Col("c"), parse, CastTo(I64(9), memory.TypeTimestamp)),
		Case([]WhenThen{{When: Col("c"), Then: parse}}, CastTo(I64(9), memory.TypeTimestamp)),
		Coalesce(Col("n"), parse),
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, expression := range expressions {
			program, err := Compile(schema, expression)
			if err != nil {
				t.Fatal(err)
			}
			budget := memory.NewBudgetManager(1 << 24)
			workspace, err := NewWorkspace(context.Background(), program, budget)
			if err != nil {
				t.Fatal(err)
			}
			for _, rows := range []int{130, 1, 65, 0, 129} {
				batch := memory.NewBatch()
				batch.Rows = rows
				c := make([]uint32, rows)
				n := make([]int64, rows)
				text := make([]string, rows)
				cv := make(memory.Bitmap, memory.WordsFor(rows))
				nv := make(memory.Bitmap, memory.WordsFor(rows))
				for row := 0; row < rows; row++ {
					text[row] = "invalid"
					n[row] = 9
					if row%3 == 0 {
						c[row] = 1
						text[row] = "1970-01-01T00:00:00Z"
					} else {
						memory.Set(nv, row)
					}
					if row%3 != 2 {
						memory.Set(cv, row)
					}
				}
				batch.Uint32s["c"] = &memory.Uint32Vector{Values: c, Validity: cv}
				batch.Int64s["n"] = &memory.Int64Vector{Values: n, Validity: nv}
				batch.Strings["s"] = testStringVector(text, nil)
				values, err := workspace.Evaluate(context.Background(), batch, kernel.New(mode))
				if err != nil {
					t.Fatalf("%s rows=%d: %v", expression, rows, err)
				}
				for row := 0; row < rows; row++ {
					want := int64(9)
					if row%3 == 0 {
						want = 0
					}
					if got := values[0].Int64.Values[row]; got != want {
						t.Fatalf("%s row %d=%d want %d", expression, row, got, want)
					}
				}
			}
			if budget.Used() == 0 {
				t.Fatal("expected owned expression storage")
			}
			_ = workspace.Close()
			if budget.Used() != 0 {
				t.Fatalf("region workspace leaked %d", budget.Used())
			}
		}
	}
}

func TestConditionalStringMergeAndNullDefaults(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString})
	batch := memory.NewBatch()
	batch.Rows = 4
	batch.Uint32s["c"] = &memory.Uint32Vector{Values: []uint32{1, 0, 0, 1}, Validity: memory.Bitmap{0b1011}}
	batch.Strings["s"] = testStringVector([]string{"α", "b", "c", "界"}, nil)
	program, err := CompileExpressions(schema, []NamedExpr{Named(If(Col("c"), Concat(Col("s"), Str("!")), Str("fallback")), "s"), Named(Case([]WhenThen{{When: Col("c"), Then: I64(5)}}, nil), "n")})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := NewWorkspace(context.Background(), program, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	values, err := workspace.Evaluate(context.Background(), batch, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range []string{"α!", "fallback", "fallback", "界!"} {
		if got := string(values[0].String.ValueBytes(row)); got != want {
			t.Fatalf("row %d=%q want %q", row, got, want)
		}
	}
	if values[1].Int64.Validity[0] != 0b1001 {
		t.Fatalf("CASE default NULL validity=%b", values[1].Int64.Validity)
	}
}

func TestConditionalCSEDoesNotHoistFallibleValues(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	bad := CastTo(Col("s"), memory.TypeTimestamp)
	branch := If(Bool(false), bad, CastTo(I64(7), memory.TypeTimestamp))
	p, err := CompileExpressions(schema, []NamedExpr{Named(branch, "a"), Named(branch, "b")})
	if err != nil {
		t.Fatal(err)
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("same-region conditional CSE lost")
	}
	b := memory.NewBatch()
	b.Rows = 1
	b.Strings["s"] = testStringVector([]string{"bad"}, nil)
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	v, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if v[0].Int64.Values[0] != 7 || v[1].Int64.Values[0] != 7 {
		t.Fatal("conditional value mismatch")
	}
	direct, err := Compile(schema, If(Bool(true), bad, CastTo(I64(7), memory.TypeTimestamp)))
	if err != nil {
		t.Fatal(err)
	}
	demanded, err := NewWorkspace(context.Background(), direct, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = demanded.Evaluate(context.Background(), b, testKernelSet()); err == nil {
		t.Fatal("demanded parse error was suppressed")
	}
}

func TestInThreeValuedEqualityAndDemand(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeFloat64, Nullable: true})
	b := memory.NewBatch()
	b.Rows = 5
	b.Float64s["x"] = &memory.Float64Vector{Values: []float64{0, math.Copysign(0, -1), math.NaN(), 3, 0}, Validity: memory.Bitmap{0b01111}}
	p, _, err := CompilePredicate(schema, In(Col("x"), F64(0), Null(memory.TypeFloat64)))
	if err != nil {
		t.Fatal(err)
	}
	var w evaluationState
	defer w.Release()
	v, err := evaluateBoolTest(p, b, &w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if v.Truth[0] != 0b00011 || v.Validity[0] != 0b00011 {
		t.Fatalf("IN value=%b valid=%b", v.Truth, v.Validity)
	}
	bad := CastTo(F64(math.NaN()), memory.TypeInt64)
	scalar, err := Compile(memory.NewSchema(), In(I64(1), I64(1), bad))
	if err != nil {
		t.Fatal(err)
	}
	one := memory.NewBatch()
	one.Rows = 1
	var sw evaluationState
	defer sw.Close()
	if _, err := evaluateValueTest(scalar, one, &sw, testKernelSet()); err != nil {
		t.Fatalf("matched IN evaluated later candidate: %v", err)
	}
	scalar, err = Compile(memory.NewSchema(), In(I64(2), I64(1), bad))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evaluateValueTest(scalar, one, &sw, testKernelSet()); err == nil {
		t.Fatal("unmatched IN suppressed demanded candidate error")
	}
}

func TestFloatIntegerCastBoundaries(t *testing.T) {
	for _, tc := range []struct {
		typ, target memory.DataType
		value       float64
		want        int64
		fails       bool
	}{
		{memory.TypeFloat32, memory.TypeInt64, 1.5, 1, false},
		{memory.TypeFloat64, memory.TypeInt64, math.NaN(), 0, true},
		{memory.TypeFloat64, memory.TypeInt64, 0x1p63, 0, true},
		{memory.TypeFloat64, memory.TypeInt64, -0x1p63, math.MinInt64, false},
		{memory.TypeFloat64, memory.TypeUint64, 0x1p64, 0, true},
		{memory.TypeFloat64, memory.TypeUint64, math.Inf(1), 0, true},
	} {
		schema := memory.NewSchema(memory.Field{Name: "v", Type: tc.typ})
		b := memory.NewBatch()
		b.Rows = 1
		if tc.typ == memory.TypeFloat32 {
			b.Float32s["v"] = &memory.Float32Vector{Values: []float32{float32(tc.value)}}
		} else {
			b.Float64s["v"] = &memory.Float64Vector{Values: []float64{tc.value}}
		}
		p, err := Compile(schema, CastTo(Col("v"), tc.target))
		if err != nil {
			t.Fatal(err)
		}
		var w evaluationState
		v, err := evaluateValueTest(p, b, &w, testKernelSet())
		if (err != nil) != tc.fails {
			t.Fatalf("%s->%s(%g) err=%v", tc.typ, tc.target, tc.value, err)
		}
		if !tc.fails && v.Int64.Values[0] != tc.want {
			t.Fatalf("cast=%d want %d", v.Int64.Values[0], tc.want)
		}
		_ = w.Close()
	}
}

func TestUnifiedWorkspaceOwnershipAndAdmission(t *testing.T) {
	p, err := Compile(memory.NewSchema(), If(Bool(true), Str("value"), Str("unused")))
	if err != nil {
		t.Fatal(err)
	}
	q, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	fallback := memory.NewBudgetManager(1)
	w, err := NewWorkspace(ctx, p, fallback)
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 2
	if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	if fallback.Used() != 0 || q.Used() == 0 {
		t.Fatal("opening query owner lost")
	}
	other, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer other.Close()
	if _, err := w.Evaluate(memory.WithQueryBudget(context.Background(), other), b, testKernelSet()); err == nil {
		t.Fatal("owner drift accepted")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.Evaluate(cancelled, b, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if q.Used() != 0 {
		t.Fatalf("cancel leaked %d", q.Used())
	}
	_ = w.Close()
}

func TestJSONResultMetadataCannotMutateProgram(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "json", Type: memory.TypeString})
	target := memory.NewSchema(memory.ListStringField("items", true, true))
	program, err := Compile(schema, JSONDecode(Col("json"), target))
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Strings["json"] = testStringVector([]string{`{"items":["value"]}`}, nil)
	workspace, err := NewWorkspace(context.Background(), program, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	values, err := workspace.Evaluate(context.Background(), batch, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	values[0].Struct.Fields[0].Children[0].Name = "mutated"
	if program.ResultField().Children[0].Children[0].Name != "item" {
		t.Fatal("runtime result leaked program metadata")
	}
	other, err := NewWorkspace(context.Background(), program, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	values, err = other.Evaluate(context.Background(), batch, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if values[0].Struct.Fields[0].Children[0].Name != "item" {
		t.Fatal("runtime schema mutation crossed workspace ownership")
	}

}

func TestBooleanSubexpressionComparedAsValue(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true})
	batch := memory.NewBatch()
	batch.Rows = 2
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{0, 1}, Validity: memory.Bitmap{2}}
	p, err := Compile(schema, EQ(IsNull(Col("x")), I64(1)))
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	result, err := w.EvaluateBool(context.Background(), batch, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if result.Truth[0] != 1 || result.Validity[0] != 3 {
		t.Fatalf("boolean expression comparison=%v", result)
	}
}
