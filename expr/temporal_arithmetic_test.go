package expr

import (
	"gocolumn/memory"
	"math"
	"strings"
	"testing"
	"time"
)

func TestTimestampUnitLiteralsNormalizeAndRejectOverflow(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp})
	batch := memory.NewBatch()
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{2 * int64(time.Second)}}
	batch.Reset(1)

	for _, tt := range []struct {
		name string
		lit  TimestampUnitLiteral
		want int64
	}{
		{name: "seconds", lit: TimestampSeconds(2), want: 2 * int64(time.Second)},
		{name: "milliseconds", lit: TimestampMillis(2000), want: 2 * int64(time.Second)},
		{name: "microseconds", lit: TimestampMicros(2_000_000), want: 2 * int64(time.Second)},
		{name: "negative milliseconds", lit: TimestampMillis(-1500), want: -1500 * int64(time.Millisecond)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := tt.lit.Nanoseconds(); err != nil || got != tt.want {
				t.Fatalf("Nanoseconds() = %d, %v; want %d, nil", got, err, tt.want)
			}
			p, err := CompileScalar(memory.Schema{}, tt.lit)
			if err != nil {
				t.Fatal(err)
			}
			value, err := p.Evaluate(batch, &ScalarWorkspace{}, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			if value.Type != memory.TypeTimestamp || value.Int64.Values[0] != tt.want {
				t.Fatalf("scalar=%+v", value)
			}
		})
	}

	predicate, _, err := CompilePredicate(schema, EQ(Col("ts"), TimestampMillis(2000)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := predicate.Evaluate(batch, &EvalWorkspace{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(result.Truth, 0) {
		t.Fatalf("normalized literal did not match: truth=%v", result.Truth)
	}

	for _, bad := range []Expr{
		TimestampSeconds(math.MaxInt64),
		TimestampMillis(math.MaxInt64),
		TimestampMicros(math.MinInt64),
		TimestampUnitLiteral{Value: 1},
	} {
		if _, err := CompileScalar(memory.Schema{}, bad); err == nil {
			t.Fatalf("%s should fail to compile", bad)
		}
	}
	if _, _, err := CompilePredicate(schema, EQ(Col("ts"), TimestampMillis(math.MaxInt64))); err == nil {
		t.Fatal("overflowing timestamp predicate literal should fail to compile")
	}
}

func TestTemporalArithmeticPreservesTypesAndNulls(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "d", Type: memory.TypeDate32, Nullable: true},
		memory.Field{Name: "d64", Type: memory.TypeDate64},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp},
	)
	valid := memory.EnsureBitmap(nil, 2)
	memory.Set(valid, 0)
	batch := memory.NewBatch()
	batch.Int32s["d"] = &memory.Int32Vector{Values: []int32{10, math.MaxInt32}, Validity: valid}
	batch.Int64s["d64"] = &memory.Int64Vector{Values: []int64{1_000, 2_000}}
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{2 * int64(time.Second), 3 * int64(time.Second)}}
	batch.Reset(2)

	program, err := CompileProjection(schema, []NamedExpr{
		Named(Add(Col("d"), I64(2)), "date_plus_days"),
		Named(Sub(Col("d"), I64(3)), "date_minus_days"),
		Named(Add(I64(7), Col("d64")), "date64_plus_millis"),
		Named(Sub(Col("ts"), I64(int64(time.Second))), "timestamp_minus_nanos"),
	})
	if err != nil {
		t.Fatal(err)
	}
	values, err := program.Evaluate(batch, &ScalarWorkspace{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if values[0].Type != memory.TypeDate32 || values[0].Int32.Values[0] != 12 || values[1].Int32.Values[0] != 7 {
		t.Fatalf("date32 arithmetic=%+v / %+v", values[0], values[1])
	}
	if values[0].Int32.IsValid(1) || values[1].Int32.IsValid(1) {
		t.Fatal("NULL date32 input must remain NULL without evaluating its overflow-prone payload")
	}
	if values[2].Type != memory.TypeDate64 || values[2].Int64.Values[0] != 1007 {
		t.Fatalf("date64 arithmetic=%+v", values[2])
	}
	if values[3].Type != memory.TypeTimestamp || values[3].Int64.Values[0] != int64(time.Second) {
		t.Fatalf("timestamp arithmetic=%+v", values[3])
	}

	for _, bad := range []Expr{
		Add(Col("d"), F64(1)),
		Sub(I64(1), Col("ts")),
		Mul(Col("ts"), I64(2)),
	} {
		if _, err := CompileScalar(schema, bad); err == nil {
			t.Fatalf("%s should be rejected", bad)
		}
	}
}

func TestCheckedArithmeticRejectsNumericAndTemporalOverflow(t *testing.T) {
	for _, tt := range []struct {
		name   string
		schema memory.Schema
		batch  *memory.Batch
		expr   Expr
	}{
		{
			name:   "int64 add",
			schema: memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64}),
			batch:  batchWithInt64(map[string][]int64{"a": {math.MaxInt64}, "b": {1}}),
			expr:   Add(Col("a"), Col("b")),
		},
		{
			name:   "int64 multiply",
			schema: memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64}),
			batch:  batchWithInt64(map[string][]int64{"a": {math.MinInt64}, "b": {-1}}),
			expr:   Mul(Col("a"), Col("b")),
		},
		{
			name:   "uint64 add",
			schema: memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeUint64}, memory.Field{Name: "b", Type: memory.TypeUint64}),
			batch:  batchWithUint64(map[string][]uint64{"a": {math.MaxUint64}, "b": {1}}),
			expr:   Add(Col("a"), Col("b")),
		},
		{
			name:   "date32 add",
			schema: memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeDate32}),
			batch:  batchWithDate32("d", math.MaxInt32),
			expr:   Add(Col("d"), I64(1)),
		},
		{
			name:   "date64 add",
			schema: memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeDate64}),
			batch:  batchWithInt64(map[string][]int64{"d": {math.MaxInt64}}),
			expr:   Add(Col("d"), I64(1)),
		},
		{
			name:   "timestamp subtract",
			schema: memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}),
			batch:  batchWithInt64(map[string][]int64{"ts": {math.MinInt64}}),
			expr:   Sub(Col("ts"), I64(1)),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			program, err := CompileScalar(tt.schema, tt.expr)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := program.Evaluate(tt.batch, &ScalarWorkspace{}, testKernelSet()); err == nil || !strings.Contains(err.Error(), "overflows at row 0") {
				t.Fatalf("Evaluate error = %v, want checked overflow at row 0", err)
			}
		})
	}
}

func batchWithInt64(columns map[string][]int64) *memory.Batch {
	batch := memory.NewBatch()
	rows := 0
	for name, values := range columns {
		batch.Int64s[name] = &memory.Int64Vector{Values: values}
		rows = len(values)
	}
	batch.Reset(rows)
	return batch
}

func batchWithUint64(columns map[string][]uint64) *memory.Batch {
	batch := memory.NewBatch()
	rows := 0
	for name, values := range columns {
		batch.Uint64s[name] = &memory.Uint64Vector{Values: values}
		rows = len(values)
	}
	batch.Reset(rows)
	return batch
}

func batchWithDate32(name string, value int32) *memory.Batch {
	batch := memory.NewBatch()
	batch.Int32s[name] = &memory.Int32Vector{Values: []int32{value}}
	batch.Reset(1)
	return batch
}
