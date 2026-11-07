package physical

import (
	"context"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

// Distinct missing-value patterns ensure lookahead positions are independent.
func TestFillReviewIndependentColumns(t *testing.T) {
	for _, batchRows := range []int{1, 11, 64, 129} {
		b := memory.NewBatch()
		b.Rows = 521
		for _, name := range []string{"g", "id", "a", "b"} {
			b.Int64s[name] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
		}
		for _, name := range []string{"a", "b"} {
			b.Int64s[name].Validity = memory.EnsureBitmap(nil, b.Rows)
		}
		for i := 0; i < b.Rows; i++ {
			b.Int64s["g"].Values[i] = int64(i / 103)
			b.Int64s["id"].Values[i] = int64(i)
			for col, name := range []string{"a", "b"} {
				b.Int64s[name].Values[i] = int64(i + col*1000)
				if (i+col*13)%71 == 0 {
					memory.Set(b.Int64s[name].Validity, i)
				}
			}
		}
		schema := memory.NewSchema(memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeInt64, Nullable: true})
		src, err := inmemory.New(schema, b)
		if err != nil {
			t.Fatal(err)
		}
		frame := WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
		es := []WindowExpression{{Function: WindowBackwardFill, Column: "a", Alias: "a", Frame: frame}, {Function: WindowBackwardFill, Column: "b", Alias: "b", Frame: frame}, {Function: WindowForwardFill, Column: "a", Alias: "prev", Frame: frame}}
		fs := []memory.Field{{Name: "a", Type: memory.TypeInt64, Nullable: true}, {Name: "b", Type: memory.TypeInt64, Nullable: true}, {Name: "prev", Type: memory.TypeInt64, Nullable: true}}
		mgr := memory.NewBudgetManager(8 << 20)
		win, err := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "id"}}, es, fs, mgr, WindowSourceOptions{Mode: WindowExecutionOrdered})
		if err != nil {
			t.Fatal(err)
		}
		// Reopening proves neither aliases nor lookahead modify reusable input.
		for repeat := 0; repeat < 2; repeat++ {
			st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: batchRows})
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			seen := 0
			for {
				ok, err := st.Next(context.Background(), out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				for row := 0; row < out.Rows; row++ {
					pos := seen + row
					for j, name := range []string{"a", "b", "prev"} {
						input := name
						step := 1
						if j == 2 {
							input = "a"
							step = -1
						}
						target := -1
						for p := pos; p >= pos/103*103 && p < min((pos/103+1)*103, b.Rows); p += step {
							if memory.IsSet(b.Int64s[input].Validity, p) {
								target = p
								break
							}
						}
						got, err := readWindowNumericCell(out, win.Schema(), WindowExpression{Column: name}, row)
						if err != nil {
							t.Fatal(err)
						}
						if got.valid != (target >= 0) || target >= 0 && got.i64 != b.Int64s[input].Values[target] {
							t.Fatalf("batch=%d repeat=%d row=%d column=%s got=%+v target=%d", batchRows, repeat, pos, name, got, target)
						}
					}
				}
				seen += out.Rows
			}
			if seen != b.Rows {
				t.Fatal(seen)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if mgr.Used() != 0 {
				t.Fatal("lease leak", mgr.Used())
			}
		}
	}
}
