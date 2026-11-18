package physical

import (
	"context"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"testing"
)

func TestPercentRankResidentAllExistingMixedFunctions(t *testing.T) {
	b := memory.NewBatch()
	b.Reset(8)
	b.Int64s["g"] = &memory.Int64Vector{Values: []int64{0, 0, 0, 0, 1, 1, 1, 1}}
	b.Int64s["k"] = &memory.Int64Vector{Values: []int64{0, 0, 1, 2, 0, 1, 1, 2}}
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{2, 0, 4, 8, 1, 3, 0, 7}, Validity: memory.Bitmap{0xbd}}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{2, 0, 4, 8, 1, 3, 0, 7}, Validity: memory.Bitmap{0xbd}}
	b.Float64s["f"] = &memory.Float64Vector{Values: []float64{1e30, 0, -1e30, 1, 2, 3, 0, 7}, Validity: memory.Bitmap{0xbd}}
	sc := memory.NewSchema(memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "k", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "u", Type: memory.TypeUint64, Nullable: true}, memory.Field{Name: "f", Type: memory.TypeFloat64, Nullable: true})
	input, e := inmemory.New(sc, b)
	if e != nil {
		t.Fatal(e)
	}
	trailing := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 1}, End: WindowBound{Kind: WindowCurrentRow}}
	current := WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
	offset := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 1}, End: WindowBound{Kind: WindowPreceding, Offset: 1}}
	ex := []WindowExpression{
		{Function: WindowRowNumber, Alias: "rn"}, {Function: WindowRank, Alias: "rank"}, {Function: WindowDenseRank, Alias: "dense"},
		{Function: WindowCountRows, Alias: "cr", Frame: trailing}, {Function: WindowCountValues, Column: "v", Alias: "cv", Frame: trailing},
		{Function: WindowSum, Column: "v", Alias: "si", Frame: trailing}, {Function: WindowSum, Column: "u", Alias: "su", Frame: trailing},
		{Function: WindowSum, Column: "f", Alias: "sf", Frame: trailing}, {Function: WindowAvg, Column: "f", Alias: "af", Frame: trailing},
		{Function: WindowMin, Column: "v", Alias: "lo", Frame: trailing}, {Function: WindowMax, Column: "f", Alias: "hi", Frame: trailing},
		{Function: WindowShift, Column: "v", Alias: "v", Frame: offset}, {Function: WindowDiff, Column: "v", Alias: "diff", Frame: offset},
		{Function: WindowForwardFill, Column: "v", Alias: "ff", Frame: current}, {Function: WindowBackwardFill, Column: "v", Alias: "bf", Frame: current},
	}
	fields := make([]memory.Field, len(ex))
	for i, x := range ex {
		typ := memory.TypeInt64
		if x.Alias == "su" {
			typ = memory.TypeUint64
		}
		if x.Alias == "sf" || x.Alias == "af" || x.Alias == "hi" {
			typ = memory.TypeFloat64
		}
		fields[i] = memory.Field{Name: x.Alias, Type: typ, Nullable: i >= 5}
	}
	run := func(pr bool) map[string][]windowNumericCell {
		t.Helper()
		expressions := append([]WindowExpression(nil), ex...)
		outputs := append([]memory.Field(nil), fields...)
		if pr {
			expressions = append(expressions, WindowExpression{Function: WindowPercentRank, Alias: "pr"})
			outputs = append(outputs, memory.Field{Name: "pr", Type: memory.TypeFloat64})
		}
		m := memory.NewBudgetManager(8 << 20)
		win, e := NewWindowSource(input, []string{"g"}, []SortKey{{Column: "k"}}, expressions, outputs, m)
		if e != nil {
			t.Fatal(e)
		}
		st, e := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 3})
		if e != nil {
			t.Fatal(e)
		}
		defer st.Close()
		out := memory.NewBatch()
		result := make(map[string][]windowNumericCell)
		for {
			ok, e := st.Next(context.Background(), out)
			if e != nil {
				t.Fatal(e)
			}
			if !ok {
				break
			}
			for _, f := range fields {
				for row := 0; row < out.Rows; row++ {
					c, e := readWindowNumericCell(out, win.Schema(), WindowExpression{Function: WindowShift, Column: f.Name}, row)
					if e != nil {
						t.Fatal(e)
					}
					result[f.Name] = append(result[f.Name], c)
				}
			}
		}
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
		if m.Used() != 0 {
			t.Fatal("mixed leak", m.Used())
		}
		return result
	}
	legacy, rank := run(false), run(true)
	if !reflect.DeepEqual(legacy, rank) {
		t.Fatal("mixed existing functions changed", legacy, rank)
	}
}
