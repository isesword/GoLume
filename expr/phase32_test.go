package expr

import (
	"math"
	"strings"
	"testing"
	"time"

	"gocolumn/memory"
)

func TestStringTemporalCastsNormalizeOffsetsAndPropagateNulls(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "stamp", Type: memory.TypeString, Nullable: true},
		memory.Field{Name: "span", Type: memory.TypeString, Nullable: true},
	)
	batch := memory.NewBatch()
	batch.Strings["stamp"] = phase32StringVector([]string{
		"2024-03-10T01:59:59-08:00",
		"2024-03-10T03:00:00-07:00",
		"2024-02-29T16:00:00.123456789",
		"not evaluated",
	}, []bool{true, true, true, false})
	batch.Strings["span"] = phase32StringVector([]string{"1h30m", "-250ms", "3ns", "not evaluated"}, []bool{true, true, true, false})
	batch.Reset(4)

	program, err := CompileExpressions(schema, []NamedExpr{
		Named(CastTo(Col("stamp"), memory.TypeTimestamp), "ts"),
		Named(CastTo(Col("span"), memory.TypeDuration), "duration"),
	})
	if err != nil {
		t.Fatal(err)
	}
	values, err := evaluateProjectionTest(program, batch, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if values[0].Type != memory.TypeTimestamp || values[1].Type != memory.TypeDuration {
		t.Fatalf("projection types=%s,%s", values[0].Type, values[1].Type)
	}
	wantTimestamps := []int64{
		time.Date(2024, time.March, 10, 9, 59, 59, 0, time.UTC).UnixNano(),
		time.Date(2024, time.March, 10, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2024, time.February, 29, 16, 0, 0, 123456789, time.UTC).UnixNano(),
		0,
	}
	if got := values[0].Int64.Values; !equalPhase32Int64(got, wantTimestamps) {
		t.Fatalf("timestamps=%v want=%v", got, wantTimestamps)
	}
	if got, want := values[1].Int64.Values, []int64{int64(90 * time.Minute), int64(-250 * time.Millisecond), 3, 0}; !equalPhase32Int64(got, want) {
		t.Fatalf("durations=%v want=%v", got, want)
	}
	if values[0].Int64.IsValid(3) || values[1].Int64.IsValid(3) {
		t.Fatal("NULL strings must stay NULL without parsing their payload")
	}
}

func TestStringTemporalCastsRejectInvalidAndOutOfRangeValues(t *testing.T) {
	for _, tt := range []struct {
		name   string
		target memory.DataType
		value  string
	}{
		{name: "date only", target: memory.TypeTimestamp, value: "2024-02-29"},
		{name: "named timezone", target: memory.TypeTimestamp, value: "2024-02-29T00:00:00 Asia/Shanghai"},
		{name: "outside nanoseconds", target: memory.TypeTimestamp, value: "2263-01-01T00:00:00Z"},
		{name: "invalid duration", target: memory.TypeDuration, value: "P1D"},
		{name: "duration overflow", target: memory.TypeDuration, value: "999999999999999999999h"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: "text", Type: memory.TypeString})
			batch := memory.NewBatch()
			batch.Strings["text"] = phase32StringVector([]string{tt.value}, nil)
			batch.Reset(1)
			program, err := Compile(schema, CastTo(Col("text"), tt.target))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := evaluateValueTest(program, batch, &evaluationState{}, testKernelSet()); err == nil || !strings.Contains(err.Error(), "row 0") {
				t.Fatalf("cast error=%v, want row-specific parse error", err)
			}
		})
	}
}

func TestDurationArithmeticPreservesTypesNullsAndOverflowChecks(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "ts", Type: memory.TypeTimestamp},
		memory.Field{Name: "baseline", Type: memory.TypeTimestamp},
		memory.Field{Name: "duration", Type: memory.TypeDuration, Nullable: true},
	)
	valid := memory.EnsureBitmap(nil, 2)
	memory.Set(valid, 0)
	batch := memory.NewBatch()
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{10 * int64(time.Second), 20 * int64(time.Second)}}
	batch.Int64s["baseline"] = &memory.Int64Vector{Values: []int64{6 * int64(time.Second), 11 * int64(time.Second)}}
	batch.Int64s["duration"] = &memory.Int64Vector{Values: []int64{1500 * int64(time.Millisecond), math.MaxInt64}, Validity: valid}
	batch.Reset(2)

	program, err := CompileExpressions(schema, []NamedExpr{
		Named(Add(Col("ts"), Col("duration")), "end"),
		Named(Sub(Col("ts"), Col("baseline")), "elapsed"),
		Named(Add(Col("duration"), Duration(2*time.Second)), "total"),
		Named(Sub(Duration(5*time.Second), Col("duration")), "remaining"),
	})
	if err != nil {
		t.Fatal(err)
	}
	values, err := evaluateProjectionTest(program, batch, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if values[0].Type != memory.TypeTimestamp || values[1].Type != memory.TypeDuration || values[2].Type != memory.TypeDuration || values[3].Type != memory.TypeDuration {
		t.Fatalf("duration arithmetic types=%s,%s,%s,%s", values[0].Type, values[1].Type, values[2].Type, values[3].Type)
	}
	if got, want := values[0].Int64.Values, []int64{11500 * int64(time.Millisecond), 0}; !equalPhase32Int64(got, want) {
		t.Fatalf("end=%v want=%v", got, want)
	}
	if got, want := values[1].Int64.Values, []int64{4 * int64(time.Second), 9 * int64(time.Second)}; !equalPhase32Int64(got, want) {
		t.Fatalf("elapsed=%v want=%v", got, want)
	}
	if got, want := values[2].Int64.Values, []int64{3500 * int64(time.Millisecond), 0}; !equalPhase32Int64(got, want) {
		t.Fatalf("total=%v want=%v", got, want)
	}
	if got, want := values[3].Int64.Values, []int64{3500 * int64(time.Millisecond), 0}; !equalPhase32Int64(got, want) {
		t.Fatalf("remaining=%v want=%v", got, want)
	}
	for i := 0; i < 2; i++ {
		if i == 0 {
			continue
		}
		if values[0].Int64.IsValid(i) || values[2].Int64.IsValid(i) || values[3].Int64.IsValid(i) {
			t.Fatalf("duration NULL did not propagate at row %d", i)
		}
	}

	overflowSchema := memory.NewSchema(
		memory.Field{Name: "ts", Type: memory.TypeTimestamp},
		memory.Field{Name: "d", Type: memory.TypeDuration},
	)
	overflowBatch := memory.NewBatch()
	overflowBatch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{math.MaxInt64}}
	overflowBatch.Int64s["d"] = &memory.Int64Vector{Values: []int64{1}}
	overflowBatch.Reset(1)
	overflow, err := Compile(overflowSchema, Add(Col("ts"), Col("d")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evaluateValueTest(overflow, overflowBatch, &evaluationState{}, testKernelSet()); err == nil || !strings.Contains(err.Error(), "overflows at row 0") {
		t.Fatalf("timestamp + duration overflow=%v", err)
	}

	for _, bad := range []Expr{
		Add(Date32(1), Duration(time.Second)),
		Add(Duration(time.Second), I64(1)),
		Sub(Duration(time.Second), Timestamp(0)),
		Add(Timestamp(0), Timestamp(1)),
	} {
		if _, err := Compile(memory.Schema{}, bad); err == nil {
			t.Fatalf("%s should be rejected", bad)
		}
	}
}

func TestDurationLiteralPredicate(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "duration", Type: memory.TypeDuration, Nullable: true})
	valid := memory.EnsureBitmap(nil, 3)
	memory.Set(valid, 0)
	memory.Set(valid, 2)
	batch := memory.NewBatch()
	batch.Int64s["duration"] = &memory.Int64Vector{Values: []int64{time.Second.Nanoseconds(), 0, 2 * time.Second.Nanoseconds()}, Validity: valid}
	batch.Reset(3)
	program, _, err := CompilePredicate(schema, EQ(Col("duration"), Duration(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := evaluateBoolTest(program, batch, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(result.Truth, 0) || memory.IsSet(result.Truth, 1) || memory.IsSet(result.Truth, 2) {
		t.Fatalf("duration predicate truth=%v", result.Truth)
	}
	if memory.IsSet(result.Validity, 1) {
		t.Fatalf("NULL duration comparison must be unknown: validity=%v", result.Validity)
	}
}

func phase32StringVector(values []string, valid []bool) *memory.StringViewVector {
	v := &memory.StringViewVector{Offsets: make([]uint32, len(values)+1), Encoding: memory.EncodingView}
	if valid != nil {
		v.Validity = memory.EnsureBitmap(nil, len(values))
	}
	for i, value := range values {
		v.Data = append(v.Data, value...)
		v.Offsets[i+1] = uint32(len(v.Data))
		if valid != nil && valid[i] {
			memory.Set(v.Validity, i)
		}
	}
	if valid == nil {
		v.Validity = nil
	}
	return v
}

func equalPhase32Int64(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
