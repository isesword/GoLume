package expr

import (
	"context"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestTimeOfDayReviewWallClockNotElapsed(t *testing.T) {
	for _, tc := range []struct {
		zone, instant string
		ns, want      int64
	}{
		{"utc", "", -1, 86399999999999},
		{"utc", "", 0, 0},
		// 03:00 local on a spring-forward day is 3 clock hours, not 2 elapsed
		// hours after local midnight. Both autumn fold instants are 01:30.
		{"new_york", "2024-03-10T07:00:00Z", 0, 10800000000000},
		{"new_york", "2024-11-03T05:30:00Z", 0, 5400000000000},
		{"new_york", "2024-11-03T06:30:00Z", 0, 5400000000000},
		{"paris", "1900-01-01T00:00:00.000000001Z", 0, 561000000001},
	} {
		ns := tc.ns
		if tc.instant != "" {
			ns = civilTestUTC(t, tc.instant)
		}
		p, e := Compile(memory.NewSchema(), TimeOfDayInZone(Timestamp(ns), timezoneFixture(t, tc.zone)))
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		b.Reset(3)
		m := memory.NewBudgetManager(0)
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
		if e != nil {
			t.Fatal(e)
		}
		if out[0].Type != memory.TypeTime64 {
			t.Fatal("lost logical time", out[0].Type)
		}
		for i := 0; i < 3; i++ {
			if !out[0].Int64.IsValid(i) || out[0].Int64.Values[i] != tc.want {
				t.Fatal(tc, i, out[0].Int64.Values[i])
			}
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("literal leak")
		}
	}
}

func TestTimeOfDayReviewExtremeOffsets(t *testing.T) {
	// Independent Gregorian datetime oracle used by the formatter review. All
	// arithmetic here is fixed integer expected clock fields, not production calls.
	for _, tc := range []struct {
		offset int32
		want   [2]int64
	}{
		{math.MaxInt32, [2]int64{12410145224192, 10883854775807}},
		{math.MinInt32, [2]int64{75515145224192, 73988854775807}},
	} {
		z := civilTestZone(t, civilTestData(0, []int32{tc.offset}, nil, nil, nil, ""))
		p, e := Compile(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}), TimeOfDayInZone(Col("ts"), z))
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		b.Reset(2)
		b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{math.MinInt64, math.MaxInt64}}
		m := memory.NewBudgetManager(0)
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
		if e != nil {
			t.Fatal(e)
		}
		for row := 0; row < 2; row++ {
			v := out[0].Int64.Values[row]
			if v != tc.want[row] || v < 0 || v >= 86400000000000 {
				t.Fatal(tc.offset, row, v, tc.want[row])
			}
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func TestTimeOfDayReviewTypedArrowAndAdmission(t *testing.T) {
	z := timezoneFixture(t, "utc")
	p, e := CompileExpressions(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true, ArrowType: "timestamp[ns, tz=UTC]"}), []NamedExpr{TimeOfDayInZone(Col("ts"), z).Alias("clock")})
	if e != nil {
		t.Fatal(e)
	}
	field := p.OutputField(0)
	if field.Type != memory.TypeTime64 || !field.Nullable || field.ArrowType != "" || field.Encoding != memory.EncodingPlain {
		t.Fatal("computed metadata", field)
	}
	matched := false
	for _, in := range p.instructions {
		if in.Code == scalarZonedExtract && in.Op == OpTimeOfDay {
			matched = true
			n, e := fixedRegisterBytes(in, 65)
			if e != nil || n != 65*8+2*8 {
				t.Fatal("Time64 must cost 8B plus bitmap", n, e)
			}
		}
	}
	if !matched {
		t.Fatal("missing time instruction")
	}
	b := memory.NewBatch()
	b.Reset(3)
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{-1, 0, 1}, Validity: memory.Bitmap{5}}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	out, e := w.EvaluateSelected(context.Background(), b, []int{0, 1, 2}, kernel.New(kernel.ModeScalar))
	if e != nil {
		t.Fatal(e)
	}
	exported := memory.NewBatch()
	exported.Reset(3)
	exported.Int64s["clock"] = out[0].Int64
	rec, e := memory.ArrowRecord(am.DefaultAllocator, memory.NewSchema(field), exported)
	if e != nil {
		t.Fatal(e)
	}
	defer rec.Release()
	w.Close()
	if m.Used() != 0 {
		t.Fatal("workspace leak")
	}
	typ, ok := rec.Schema().Field(0).Type.(*arrow.Time64Type)
	if !ok || typ.Unit != arrow.Nanosecond {
		t.Fatal("Arrow time64 ns lost", rec.Schema())
	}
	arr, ok := rec.Column(0).(*array.Time64)
	if !ok || arr.IsNull(0) || int64(arr.Value(0)) != 86399999999999 || !arr.IsNull(1) || int64(arr.Value(2)) != 1 {
		t.Fatal("owned Arrow clock values after workspace close")
	}
	for _, kind := range []memory.DataType{memory.TypeInt64, memory.TypeTime64, memory.TypeDate32, memory.TypeDuration} {
		if _, e := Compile(memory.NewSchema(memory.Field{Name: "bad", Type: kind}), TimeOfDayInZone(Col("bad"), z)); e == nil {
			t.Fatal("wrong input type accepted", kind)
		}
	}
	if _, e := Compile(memory.NewSchema(), Unary{Op: OpTimeOfDay, Value: Timestamp(0)}); e == nil {
		t.Fatal("bare Unary silently enabled")
	}
	if OpISOWeekYear != 54 || OpISOWeek != 55 || OpISOWeekday != 56 || OpUTCOffset != 57 || OpTimeOfDay != 58 {
		t.Fatal("operator values changed")
	}
}
