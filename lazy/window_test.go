package lazy

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"strings"
	"testing"
)

func TestWindowsRankAndRollingKeepInputOrderAcrossPartitions(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "grp", Type: memory.TypeInt32},
		memory.Field{Name: "score", Type: memory.TypeInt64},
		memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true},
	)
	b1 := memory.NewBatch()
	b1.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	b1.Int32s["grp"] = &memory.Int32Vector{Values: []int32{1, 2, 1}}
	b1.Int64s["score"] = &memory.Int64Vector{Values: []int64{20, 5, 10}}
	b1.Int64s["value"] = &memory.Int64Vector{Values: []int64{1, 0, 3}, Validity: windowValidity(3, 0, 2)}
	b1.Reset(3)
	b2 := memory.NewBatch()
	b2.Int64s["id"] = &memory.Int64Vector{Values: []int64{3, 4, 5}}
	b2.Int32s["grp"] = &memory.Int32Vector{Values: []int32{2, 1, 2}}
	b2.Int64s["score"] = &memory.Int64Vector{Values: []int64{5, 10, 7}}
	b2.Int64s["value"] = &memory.Int64Vector{Values: []int64{4, 5, 6}}
	b2.Reset(3)
	src, err := inmemory.New(schema, b1, b2)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(src).WithWindows(
		NewWindow(PartitionBy("grp"), OrderBy(Asc("score"))),
		RowNumber("rn"),
		Rank("rank"),
		DenseRank("dense"),
		RollingCountRows(RowsBetween(Preceding(1), CurrentRow), "rows"),
		RollingCount("value", RowsBetween(Preceding(1), CurrentRow), "count"),
		RollingSum("value", RowsBetween(Preceding(1), CurrentRow), "sum"),
	).CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got := frameInt64Column(t, frame, "id"); !equalI64(got, []int64{0, 1, 2, 3, 4, 5}) {
		t.Fatalf("window reordered input rows: %v", got)
	}
	if got := frameInt64Column(t, frame, "rn"); !equalI64(got, []int64{3, 1, 1, 2, 2, 3}) {
		t.Fatalf("row number=%v", got)
	}
	if got := frameInt64Column(t, frame, "rank"); !equalI64(got, []int64{3, 1, 1, 1, 1, 3}) {
		t.Fatalf("rank=%v", got)
	}
	if got := frameInt64Column(t, frame, "dense"); !equalI64(got, []int64{2, 1, 1, 1, 1, 2}) {
		t.Fatalf("dense rank=%v", got)
	}
	if got := frameInt64Column(t, frame, "rows"); !equalI64(got, []int64{2, 1, 1, 2, 2, 2}) {
		t.Fatalf("rolling rows=%v", got)
	}
	if got := frameInt64Column(t, frame, "count"); !equalI64(got, []int64{2, 0, 1, 1, 2, 2}) {
		t.Fatalf("rolling count=%v", got)
	}
	values, valid := frameInt64WithValidity(t, frame, "sum")
	if !equalI64(values, []int64{6, 0, 3, 4, 8, 10}) || !equalBools(valid, []bool{true, false, true, true, true, true}) {
		t.Fatalf("rolling sum values=%v valid=%v", values, valid)
	}
	field, ok := frame.Schema.Field("sum")
	if !ok || field.Type != memory.TypeInt64 || !field.Nullable {
		t.Fatalf("sum field=%+v present=%v", field, ok)
	}
}

func TestWindowFilterIsStrictSemanticBoundary(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	b.Reset(3)
	src, _ := inmemory.New(schema, b)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("x"))), Rank("rank")).Filter(Col("x").Gt(Lit(1)))
	explain, err := frame.ExplainOptimized()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(explain, "pushed_predicate=") {
		t.Fatalf("window-crossing filter was pushed down:\n%s", explain)
	}
	result, err := frame.CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if got := frameInt64Column(t, result, "rank"); !equalI64(got, []int64{2, 3}) {
		t.Fatalf("rank after filter=%v want [2 3]", got)
	}
}

func TestWindowExpressionOrderAndFloatMinMax(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "name", Type: memory.TypeString, Encoding: memory.EncodingView},
		memory.Field{Name: "x", Type: memory.TypeFloat64, Nullable: true},
	)
	b := memory.NewBatch()
	b.Strings["name"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 7, 9}, Data: []byte("a中中zz"), Encoding: memory.EncodingView}
	b.Float64s["x"] = &memory.Float64Vector{Values: []float64{1, math.NaN(), math.Inf(1)}}
	b.Reset(3)
	src, _ := inmemory.New(schema, b)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(src).WithWindows(
		NewWindow(nil, OrderBy(DescExpr(Length(Col("name"))))),
		RowNumber("rn"),
		RollingMin("x", RowsBetween(UnboundedPreceding, CurrentRow), "min"),
		RollingMax("x", RowsBetween(UnboundedPreceding, CurrentRow), "max"),
		RollingAvg("x", RowsBetween(UnboundedPreceding, CurrentRow), "avg"),
	).CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got := frameInt64Column(t, frame, "rn"); !equalI64(got, []int64{3, 1, 2}) {
		t.Fatalf("expression order row numbers=%v", got)
	}
	if _, ok := frame.Schema.Field("__gocolumn_order_key_0"); ok {
		t.Fatal("internal ordering expression leaked to output schema")
	}
	min := frameFloat64Column(t, frame, "min")
	max := frameFloat64Column(t, frame, "max")
	avg := frameFloat64Column(t, frame, "avg")
	if min[0] != 1 || !math.IsNaN(min[1]) || !math.IsInf(min[2], 1) {
		t.Fatalf("min=%v", min)
	}
	if !math.IsNaN(max[0]) || !math.IsNaN(max[1]) || !math.IsNaN(max[2]) {
		t.Fatalf("max must treat NaN as greatest: %v", max)
	}
	if !math.IsNaN(avg[0]) || !math.IsNaN(avg[1]) || !math.IsNaN(avg[2]) {
		t.Fatalf("avg must propagate NaN: %v", avg)
	}
}

func TestWindowFramesValidationOverflowAndBudget(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{math.MaxInt64, 1}}
	b.Reset(2)
	src, _ := inmemory.New(schema, b)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	invalid := session.FromSource(src).WithWindows(NewWindow(nil, nil), RowNumber("rn"))
	if _, err := invalid.Schema(); err == nil || !strings.Contains(err.Error(), "ordering") {
		t.Fatalf("missing-order error=%v", err)
	}
	badFrame := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("x"))), RollingSum("x", RowsBetween(Following(1), Preceding(1)), "sum"))
	if _, err := badFrame.Schema(); err == nil || !strings.Contains(err.Error(), "start is after") {
		t.Fatalf("invalid frame error=%v", err)
	}
	overflow := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("x"))), RollingSum("x", RowsBetween(UnboundedPreceding, CurrentRow), "sum"))
	if _, err := overflow.CollectFrame(context.Background()); err == nil || !strings.Contains(err.Error(), "overflows int64") {
		t.Fatalf("overflow error=%v", err)
	}
	tiny := NewSession(engine.Options{MemoryLimitBytes: 1})
	defer tiny.Close()
	if _, err := tiny.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("x"))), RowNumber("rn")).CollectFrame(context.Background()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("budget error=%v", err)
	}
}

func TestWindowProjectionEliminationAndEmptyInput(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "grp", Type: memory.TypeInt32},
		memory.Field{Name: "score", Type: memory.TypeInt64},
		memory.Field{Name: "payload", Type: memory.TypeInt64},
	)
	b := memory.NewBatch()
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{1}}
	b.Int32s["grp"] = &memory.Int32Vector{Values: []int32{1}}
	b.Int64s["score"] = &memory.Int64Vector{Values: []int64{10}}
	b.Int64s["payload"] = &memory.Int64Vector{Values: []int64{99}}
	b.Reset(1)
	src, _ := inmemory.New(schema, b)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	pruned := session.FromSource(src).WithWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("score"))), RowNumber("rn")).Select("id")
	explain, err := pruned.ExplainOptimized()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(explain, "WINDOW") || strings.Contains(explain, "grp") || strings.Contains(explain, "score") || strings.Contains(explain, "payload") {
		t.Fatalf("unused window was not eliminated:\n%s", explain)
	}
	result, err := pruned.CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if got := frameInt64Column(t, result, "id"); !equalI64(got, []int64{1}) {
		t.Fatalf("pruned result=%v", got)
	}

	empty, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}))
	emptyFrame, err := session.FromSource(empty).WithWindows(NewWindow(nil, OrderBy(Asc("x"))), RowNumber("rn"), RollingCountRows(RowsBetween(CurrentRow, CurrentRow), "count")).CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer emptyFrame.Close()
	if emptyFrame.Rows != 0 || len(emptyFrame.Batches) != 1 {
		t.Fatalf("empty window rows=%d batches=%d", emptyFrame.Rows, len(emptyFrame.Batches))
	}
	if _, err := emptyFrame.Batches[0].Int64("rn"); err != nil {
		t.Fatalf("empty rank output missing: %v", err)
	}
	if _, err := emptyFrame.Batches[0].Int64("count"); err != nil {
		t.Fatalf("empty count output missing: %v", err)
	}
}

func TestWindowNumericResultTypesAndCancellation(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "i", Type: memory.TypeInt32},
		memory.Field{Name: "u", Type: memory.TypeUint32},
		memory.Field{Name: "f", Type: memory.TypeFloat64},
	)
	b := memory.NewBatch()
	b.Int32s["i"] = &memory.Int32Vector{Values: []int32{3, 1}}
	b.Uint32s["u"] = &memory.Uint32Vector{Values: []uint32{2, 5}}
	b.Float64s["f"] = &memory.Float64Vector{Values: []float64{1.5, 2.5}}
	b.Reset(2)
	src, _ := inmemory.New(schema, b)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(src).WithWindows(
		NewWindow(nil, OrderBy(Asc("i"))),
		RollingSum("i", RowsBetween(UnboundedPreceding, CurrentRow), "isum"),
		RollingSum("u", RowsBetween(UnboundedPreceding, CurrentRow), "usum"),
		RollingMin("i", RowsBetween(UnboundedPreceding, CurrentRow), "imin"),
		RollingMax("u", RowsBetween(UnboundedPreceding, CurrentRow), "umax"),
		RollingAvg("f", RowsBetween(UnboundedPreceding, CurrentRow), "favg"),
	).CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got := frameInt64Column(t, frame, "isum"); !equalI64(got, []int64{4, 1}) {
		t.Fatalf("int sum=%v", got)
	}
	if got := frameUint64Column(t, frame, "usum"); len(got) != 2 || got[0] != 7 || got[1] != 5 {
		t.Fatalf("uint sum=%v", got)
	}
	if got := frameInt32Column(t, frame, "imin"); !equalI32(got, []int32{1, 1}) {
		t.Fatalf("int min=%v", got)
	}
	if got := frameUint32Column(t, frame, "umax"); len(got) != 2 || got[0] != 5 || got[1] != 5 {
		t.Fatalf("uint max=%v", got)
	}
	if got := frameFloat64Column(t, frame, "favg"); len(got) != 2 || got[0] != 2 || got[1] != 2.5 {
		t.Fatalf("float avg=%v", got)
	}
	for _, name := range []string{"isum", "usum", "imin", "umax", "favg"} {
		field, ok := frame.Schema.Field(name)
		if !ok {
			t.Fatalf("missing output field %q", name)
		}
		if name == "isum" && field.Type != memory.TypeInt64 || name == "usum" && field.Type != memory.TypeUint64 || name == "imin" && field.Type != memory.TypeInt32 || name == "umax" && field.Type != memory.TypeUint32 || name == "favg" && field.Type != memory.TypeFloat64 {
			t.Fatalf("field %q type=%s", name, field.Type)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("i"))), RowNumber("rn")).CollectFrame(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled execution error=%v", err)
	}
}

func windowValidity(rows int, validRows ...int) memory.Bitmap {
	bits := memory.EnsureBitmap(nil, rows)
	for _, row := range validRows {
		memory.Set(bits, row)
	}
	return bits
}

func frameInt64WithValidity(t *testing.T, frame Frame, name string) ([]int64, []bool) {
	t.Helper()
	var values []int64
	var valid []bool
	for _, batch := range frame.Batches {
		vector, err := batch.Int64(name)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, vector.Values...)
		for row := range vector.Values {
			valid = append(valid, vector.IsValid(row))
		}
	}
	return values, valid
}

func frameFloat64Column(t *testing.T, frame Frame, name string) []float64 {
	t.Helper()
	var out []float64
	for _, batch := range frame.Batches {
		vector, err := batch.Float64(name)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, vector.Values...)
	}
	return out
}

func frameUint64Column(t *testing.T, frame Frame, name string) []uint64 {
	t.Helper()
	var out []uint64
	for _, batch := range frame.Batches {
		vector, err := batch.Uint64(name)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, vector.Values...)
	}
	return out
}

func frameUint32Column(t *testing.T, frame Frame, name string) []uint32 {
	t.Helper()
	var out []uint32
	for _, batch := range frame.Batches {
		vector, err := batch.Uint32(name)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, vector.Values...)
	}
	return out
}

func equalBools(left, right []bool) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
