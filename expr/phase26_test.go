package expr

import (
	"gocolumn/memory"
	"math"
	"testing"
)

func TestProjectionProgramTypedCoercionCSEAndCast(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "b", Type: memory.TypeFloat64},
		memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingView},
		memory.Field{Name: "d", Type: memory.TypeDate32},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp},
	)
	shared := Add(Col("a"), Col("b"))
	program, err := CompileProjection(schema, []NamedExpr{
		Named(shared, "sum"),
		Named(Mul(shared, F64(2)), "twice"),
		Named(Concat(Col("s"), Str("!")), "label"),
		Named(CastTo(Col("a"), memory.TypeString), "a_text"),
		Named(Col("d"), "d"),
		Named(Col("ts"), "ts"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// a,b,a+b,2,(a+b)*2,s,"!",concat,cast,d,ts = 11. Without CSE,
	// the shared a+b subtree would require three additional instructions.
	if got := len(program.Instructions); got != 11 {
		t.Fatalf("instruction count=%d, want 11 (shared subtree must be reused)", got)
	}

	valid := memory.EnsureBitmap(nil, 3)
	memory.Set(valid, 0)
	memory.Set(valid, 2)
	batch := memory.NewBatch()
	batch.Int64s["a"] = &memory.Int64Vector{Values: []int64{2, 100, 4}, Validity: valid}
	batch.Float64s["b"] = &memory.Float64Vector{Values: []float64{0.5, 1.5, 2.25}}
	batch.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 3, 6}, Data: []byte("abbccc"), Encoding: memory.EncodingView}
	batch.Int32s["d"] = &memory.Int32Vector{Values: []int32{10, 11, 12}}
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{1000, 2000, 3000}}
	batch.Reset(3)

	var ws ScalarWorkspace
	values, err := program.Evaluate(batch, &ws, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if values[0].Type != memory.TypeFloat64 || math.Abs(values[0].Float64.Values[0]-2.5) > 1e-9 || math.Abs(values[0].Float64.Values[2]-6.25) > 1e-9 {
		t.Fatalf("sum=%+v", values[0])
	}
	if values[0].Float64.IsValid(1) {
		t.Fatalf("mixed numeric expression must propagate NULL")
	}
	if got := values[1].Float64.Values[2]; math.Abs(got-12.5) > 1e-9 {
		t.Fatalf("twice=%v", got)
	}
	if got := string(values[2].String.ValueBytes(2)); got != "ccc!" {
		t.Fatalf("concat=%q", got)
	}
	if got := string(values[3].String.ValueBytes(0)); got != "2" {
		t.Fatalf("cast string=%q", got)
	}
	if values[3].String.IsValid(1) {
		t.Fatalf("cast must preserve NULL")
	}
	if values[4].Type != memory.TypeDate32 || values[4].Int32.Values[2] != 12 {
		t.Fatalf("date32=%+v", values[4])
	}
	if values[5].Type != memory.TypeTimestamp || values[5].Int64.Values[2] != 3000 {
		t.Fatalf("timestamp=%+v", values[5])
	}
}

func TestTypedPredicateFloatStringDateTimestamp(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "f", Type: memory.TypeFloat64, Nullable: true},
		memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingView},
		memory.Field{Name: "d", Type: memory.TypeDate32},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp},
	)
	valid := memory.EnsureBitmap(nil, 3)
	memory.Set(valid, 0)
	memory.Set(valid, 2)
	batch := memory.NewBatch()
	batch.Float64s["f"] = &memory.Float64Vector{Values: []float64{1.5, 9, 3.5}, Validity: valid}
	batch.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3}, Data: []byte("ccc"), Encoding: memory.EncodingView}
	batch.Int32s["d"] = &memory.Int32Vector{Values: []int32{10, 20, 30}}
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{100, 200, 300}}
	batch.Reset(3)

	pred := And(
		GT(Col("f"), F64(2.0)),
		And(EQ(Col("s"), Str("c")), And(GE(Col("d"), Date32(20)), LE(Col("ts"), Timestamp(300)))),
	)
	program, _, err := CompilePredicate(schema, pred)
	if err != nil {
		t.Fatal(err)
	}
	var ws EvalWorkspace
	out, err := program.Evaluate(batch, &ws, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if memory.CountBitmap(out.Truth) != 1 || !memory.IsSet(out.Truth, 2) {
		t.Fatalf("truth=%v", out.Truth)
	}
	if memory.IsSet(out.Validity, 1) {
		t.Fatalf("NULL float comparison must make conjunction unknown when no false operand dominates")
	}
}
