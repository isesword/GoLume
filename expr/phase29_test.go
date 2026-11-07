package expr

import (
	"gocolumn/memory"
	"testing"
	"time"
)

func TestStringAndCalendarProjectionExpressions(t *testing.T) {
	stamp := time.Date(2024, time.February, 29, 23, 58, 59, 123, time.UTC)
	beforeEpoch := time.Date(1969, time.December, 31, 1, 2, 3, 0, time.UTC)
	valid := memory.EnsureBitmap(nil, 4)
	memory.Set(valid, 0)
	memory.Set(valid, 1)
	memory.Set(valid, 2)

	batch := memory.NewBatch()
	batch.Strings["s"] = &memory.StringViewVector{
		Offsets:  []uint32{0, 8, 18, 18, 18},
		Data:     []byte("Go语言prefix-end"),
		Validity: valid,
		Encoding: memory.EncodingView,
	}
	batch.Int32s["d"] = &memory.Int32Vector{Values: []int32{daysUTC(stamp), daysUTC(beforeEpoch), 0, 0}}
	batch.Int64s["d64"] = &memory.Int64Vector{Values: []int64{stamp.UnixMilli(), beforeEpoch.UnixMilli(), 0, 0}}
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{stamp.UnixNano(), beforeEpoch.UnixNano(), stamp.UnixNano(), 0}, Validity: valid}
	batch.Reset(4)

	schema := memory.NewSchema(
		memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
		memory.Field{Name: "d", Type: memory.TypeDate32},
		memory.Field{Name: "d64", Type: memory.TypeDate64},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true},
	)
	program, err := CompileExpressions(schema, []NamedExpr{
		Length(Col("s")).Alias("len"),
		Year(Col("d")).Alias("date_year"),
		Month(Col("d64")).Alias("date64_month"),
		Day(Col("ts")).Alias("day"),
		Hour(Col("ts")).Alias("hour"),
		Minute(Col("ts")).Alias("minute"),
		Second(Col("ts")).Alias("second"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var workspace evaluationState
	values, err := evaluateProjectionTest(program, batch, &workspace, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if got := values[0].Int64.Values; got[0] != 4 || got[1] != 10 || got[2] != 0 || got[3] != 0 {
		t.Fatalf("length=%v", got)
	}
	if values[0].Int64.IsValid(3) {
		t.Fatal("NULL string length must be NULL")
	}
	if got := values[1].Int64.Values; got[0] != 2024 || got[1] != 1969 {
		t.Fatalf("date years=%v", got)
	}
	if got := values[2].Int64.Values; got[0] != 2 || got[1] != 12 {
		t.Fatalf("date64 months=%v", got)
	}
	if got := values[3].Int64.Values; got[0] != 29 || got[1] != 31 {
		t.Fatalf("days=%v", got)
	}
	if got := values[4].Int64.Values; got[0] != 23 || got[1] != 1 {
		t.Fatalf("hours=%v", got)
	}
	if got := values[5].Int64.Values; got[0] != 58 || got[1] != 2 {
		t.Fatalf("minutes=%v", got)
	}
	if got := values[6].Int64.Values; got[0] != 59 || got[1] != 3 {
		t.Fatalf("seconds=%v", got)
	}
}

func TestStringAndCalendarPredicates(t *testing.T) {
	stamp := time.Date(2024, time.February, 29, 23, 58, 59, 0, time.UTC)
	valid := memory.EnsureBitmap(nil, 4)
	memory.Set(valid, 0)
	memory.Set(valid, 1)
	memory.Set(valid, 2)
	batch := memory.NewBatch()
	batch.Strings["s"] = &memory.StringViewVector{
		Offsets:  []uint32{0, 8, 18, 18, 18},
		Data:     []byte("Go语言prefix-end"),
		Validity: valid,
		Encoding: memory.EncodingView,
	}
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{stamp.UnixNano(), stamp.AddDate(-1, 0, 0).UnixNano(), stamp.UnixNano(), 0}, Validity: valid}
	batch.Reset(4)
	schema := memory.NewSchema(
		memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true},
	)

	predicate := And(Contains(Col("s"), Str("语")), EQ(Year(Col("ts")), I64(2024)))
	program, _, err := CompilePredicate(schema, predicate)
	if err != nil {
		t.Fatal(err)
	}
	var workspace evaluationState
	result, err := evaluateBoolTest(program, batch, &workspace, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(result.Truth, 0) || memory.IsSet(result.Truth, 1) || memory.IsSet(result.Truth, 2) {
		t.Fatalf("contains/year truth=%v", result.Truth)
	}
	if memory.IsSet(result.Validity, 3) {
		t.Fatal("NULL string/timestamp predicate must remain unknown")
	}

	program, _, err = CompilePredicate(schema, And(StartsWith(Col("s"), Str("prefix")), EndsWith(Col("s"), Str("end"))))
	if err != nil {
		t.Fatal(err)
	}
	result, err = evaluateBoolTest(program, batch, &workspace, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(result.Truth, 1) || memory.IsSet(result.Truth, 0) {
		t.Fatalf("prefix/suffix truth=%v", result.Truth)
	}

	program, _, err = CompilePredicate(schema, EQ(Length(Col("s")), I64(4)))
	if err != nil {
		t.Fatal(err)
	}
	result, err = evaluateBoolTest(program, batch, &workspace, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(result.Truth, 0) || memory.IsSet(result.Truth, 1) || memory.IsSet(result.Truth, 2) {
		t.Fatalf("length truth=%v", result.Truth)
	}
}

func TestStringAndCalendarExpressionTypeErrors(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "i", Type: memory.TypeInt64},
		memory.Field{Name: "d", Type: memory.TypeDate32},
		memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingView},
	)
	if _, err := Compile(schema, Length(Col("i"))); err == nil {
		t.Fatal("expected LENGTH type error")
	}
	if _, err := Compile(schema, Hour(Col("d"))); err == nil {
		t.Fatal("expected HOUR type error")
	}
	if _, _, err := CompilePredicate(schema, Contains(Col("i"), Str("1"))); err == nil {
		t.Fatal("expected CONTAINS type error")
	}
	if _, _, err := CompilePredicate(schema, EQ(Year(Col("s")), I64(2024))); err == nil {
		t.Fatal("expected YEAR type error")
	}
}

func daysUTC(t time.Time) int32 {
	u := t.UTC()
	midnight := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
	return int32(midnight.Unix() / int64(24*time.Hour/time.Second))
}
