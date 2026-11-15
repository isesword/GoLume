package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestListLengthFilteredProjection(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, stringList := range []bool{false, true} {
			b := memory.NewBatch()
			b.Rows = 4
			b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3}}
			offsets := []uint32{0, 2, 2, 3, 6}
			valid := memory.Bitmap{11}
			field := memory.ListInt64Field("xs", true, true)
			if stringList {
				field = memory.ListStringField("xs", true, true)
				b.StringLists["xs"] = &memory.StringListVector{Offsets: offsets, Validity: valid, Values: &memory.StringViewVector{Offsets: make([]uint32, 7), Validity: memory.Bitmap{21}}}
			} else {
				b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: offsets, Validity: valid, Values: &memory.Int64Vector{Values: make([]int64, 6), Validity: memory.Bitmap{21}}}
			}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, field), b)
			if err != nil {
				t.Fatal(err)
			}
			opts := engine.DefaultOptions()
			opts.KernelMode = mode
			opts.PreparedCacheBytes = 0
			s := NewSession(opts)
			// The first filter selects rows before the nested-input projection. A second
			// predicate on ListLength checks generic comparison, not string specialization.
			q := s.FromSource(src).Filter(expr.GT(expr.Col("id"), expr.I64(0))).SelectExprs(expr.Named(ListLength(expr.Col("xs")), "n"))
			for repeat := 0; repeat < 2; repeat++ {
				df, err := q.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				var row int
				for _, batch := range df.batches() {
					v := batch.Int64s["n"]
					for i, x := range v.Values {
						want := []int64{0, 0, 3}[row]
						if v.IsValid(i) != (row != 1) || v.IsValid(i) && x != want {
							t.Fatal("wrong selected lengths", row, x)
						}
						row++
					}
				}
				df.Close()
				if row != 3 {
					t.Fatal(row)
				}
			}
			df, err := s.FromSource(src).Filter(expr.GT(ListLength(expr.Col("xs")), expr.I64(1))).SelectExprs(expr.Named(ListLength(expr.Col("xs")), "n")).Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var total int
			for _, b := range df.batches() {
				total += b.Rows
			}
			df.Close()
			if total != 2 {
				t.Fatal(total)
			}
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("memory leak")
			}
			s.Close()
		}
	}
}
