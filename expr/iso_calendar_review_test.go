package expr

import (
	"context"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"testing"
)

func isoReviewOutputs(value Expr, zone TimeZone) []NamedExpr {
	return []NamedExpr{ISOWeekYearInZone(value, zone).Alias("year"), ISOWeekInZone(value, zone).Alias("week"), ISOWeekdayInZone(value, zone).Alias("weekday"), UTCOffsetInZone(value, zone).Alias("offset")}
}

func TestISOCalendarReviewSelectedIdentity(t *testing.T) {
	// Both raw handles deliberately have the same label. Rules, not labels,
	// determine the ISO year/week pair on opposite sides of a local Monday.
	east := civilTestZone(t, civilTestData(0, []int32{3600}, nil, nil, nil, ""))
	west := civilTestZone(t, civilTestData(0, []int32{-3600}, nil, nil, nil, ""))
	outputs := isoReviewOutputs(Col("ts"), east)
	for _, n := range isoReviewOutputs(Col("ts"), west) {
		outputs = append(outputs, Named(n.Expr, "west_"+n.Alias))
	}
	outputs = append(outputs, ISOWeekYearInZone(Col("ts"), east).Alias("copied_year"))
	p, err := CompileExpressions(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}), outputs)
	if err != nil {
		t.Fatal(err)
	}
	if !p.SupportsSelection() {
		t.Fatal("zoned selected path unavailable")
	}
	if p.outputs[0].root != p.outputs[8].root {
		t.Fatal("copied handle failed CSE")
	}
	for i := 0; i < 8; i++ {
		for j := 0; j < i; j++ {
			if p.outputs[i].root == p.outputs[j].root {
				t.Fatal("part or same-label rules merged", i, j)
			}
		}
	}
	b := memory.NewBatch()
	b.Reset(3)
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{civilTestUTC(t, "2021-01-03T23:30:00Z"), 0, civilTestUTC(t, "2021-01-04T01:30:00Z")}, Validity: memory.Bitmap{5}}
	// Table is independent of the implementation's Time.ISOWeek call.
	want := [][8]int64{{2021, 1, 1, 3600, 2020, 53, 7, -3600}, {}, {2021, 1, 1, 3600, 2021, 1, 1, -3600}}
	m := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, m)
	if err != nil {
		t.Fatal(err)
	}
	rows := []int{0, 1, 2}
	out, err := w.EvaluateSelected(context.Background(), b, rows, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	for col, v := range out {
		for j, row := range rows {
			valid := row != 1
			if v.Int64.IsValid(j) != valid {
				t.Fatal("selection NULL", col, j)
			}
			wc := col
			if wc == 8 {
				wc = 0
			}
			if valid && v.Int64.Values[j] != want[row][wc] {
				t.Fatal("selection value", col, j, v.Int64.Values[j], want[row][wc])
			}
		}
	}
	// Existing selected contract forbids duplicates and reordering; use separate
	// workspaces because a non-budget selection error closes the workspace.
	for _, invalid := range [][]int{{2, 0}, {0, 0}} {
		im := memory.NewBudgetManager(0)
		iw, err := NewWorkspace(context.Background(), p, im)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := iw.EvaluateSelected(context.Background(), b, invalid, kernel.New(kernel.ModeScalar)); err == nil {
			t.Fatal("invalid selection admitted", invalid)
		}
		iw.Close()
		if im.Used() != 0 {
			t.Fatal("invalid selection leak")
		}
	}
	// Reuse the same workspace after selected gather, in original input order.
	out, err = w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	if out[4].Int64.Values[0] != 2020 || out[4].Int64.Values[2] != 2021 {
		t.Fatal("dense after selected")
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("leak", m.Used())
	}
}

func TestISOCalendarReviewExtremeOffsets(t *testing.T) {
	// Expected triples independently calculated using Gregorian/ISO calendar
	// arithmetic in Python datetime from floor(ns/1e9)+offset, not Go Time.ISOWeek.
	for _, tc := range []struct {
		offset int32
		want   [2][3]int64
	}{
		{math.MaxInt32, [2][3]int64{{1745, 40, 7}, {2330, 18, 4}}},
		{math.MinInt32, [2][3]int64{{1609, 36, 3}, {2194, 12, 7}}},
	} {
		zone := civilTestZone(t, civilTestData(0, []int32{tc.offset}, nil, nil, nil, ""))
		p, err := CompileExpressions(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}), isoReviewOutputs(Col("ts"), zone))
		if err != nil {
			t.Fatal(err)
		}
		b := memory.NewBatch()
		b.Reset(2)
		b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{math.MinInt64, math.MaxInt64}}
		m := memory.NewBudgetManager(0)
		w, err := NewWorkspace(context.Background(), p, m)
		if err != nil {
			t.Fatal(err)
		}
		out, err := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
		if err != nil {
			t.Fatal(err)
		}
		for row := 0; row < 2; row++ {
			for col := 0; col < 4; col++ {
				want := int64(tc.offset)
				if col < 3 {
					want = tc.want[row][col]
				}
				if !out[col].Int64.IsValid(row) || out[col].Int64.Values[row] != want {
					t.Fatal(tc.offset, row, col, out[col].Int64.Values[row], want)
				}
			}
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func TestISOCalendarReviewOldOpNumbersAndUnaryRejection(t *testing.T) {
	// Every operator in the 1902-file parent, in its original numeric order.
	old := []Op{OpInvalid, OpGT, OpGE, OpLT, OpLE, OpEQ, OpAnd, OpOr, OpNot, OpIsNull, OpIsNotNull, OpAdd, OpSub, OpMul, OpDiv, OpConcat, OpContains, OpStartsWith, OpEndsWith, OpLength, OpYear, OpMonth, OpDay, OpHour, OpMinute, OpSecond, OpByteLength, OpTrim, OpTrimStart, OpTrimEnd, OpLower, OpUpper, OpFind, OpReplace, OpReplaceAll, OpSlice, OpRegexMatch, OpRegexExtract, OpSplit, OpRegexExtractAll, OpAbs, OpRound, OpFloor, OpCeil, OpSqrt, OpLog, OpMod, OpFloorDiv, OpPow, OpBitAnd, OpBitOr, OpBitXor, OpBitNot, OpListLength}
	for i, op := range old {
		if int(op) != i {
			t.Fatal("old operator renumbered", i, op)
		}
	}
	for _, op := range []Op{OpISOWeekYear, OpISOWeek, OpISOWeekday, OpUTCOffset} {
		if int(op) < len(old) {
			t.Fatal("new operator not appended", op)
		}
		if _, err := Compile(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}), Unary{Op: op, Value: Col("ts")}); err == nil {
			t.Fatal("bare Unary unexpectedly supports zoned-only operator", op)
		}
	}
}
