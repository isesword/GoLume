package physical

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// Exercise nested payload while the sort key remains scalar. The four parent
// rows include empty, NULL, a negative value, and a NULL child.
func TestRebuiltInt64ListOrderPayload(t *testing.T) {
	b := memory.NewBatch()
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{3, 1, 4, 2}}
	b.Int64Lists["list"] = &memory.Int64ListVector{
		Offsets: []uint32{0, 2, 2, 2, 3}, Validity: memory.Bitmap{0b1011}, Encoding: memory.EncodingView,
		Values: &memory.Int64Vector{Values: []int64{7, 0, -4}, Validity: memory.Bitmap{0b101}, Encoding: memory.EncodingPlain},
	}
	b.Reset(4)
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.ListInt64Field("list", true, true)), b)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"resident-sort", "resident-topk", "spill-sort"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			var stream source.BatchStream
			var query *memory.QueryBudget
			switch mode {
			case "resident-sort", "resident-topk":
				manager := memory.NewBudgetManager(8 << 20)
				stream = openResidentDictionaryOrder(t, ctx, src, mode == "resident-topk", 3, SortKey{Column: "key"}, manager)
			case "spill-sort":
				s, q, _, c := openSortLifecycle(t, 16<<20, src, t.TempDir())
				s.chunkRows = 2
				stream, query, ctx = s, q, c
			}
			out := memory.NewBatch()
			got := []string{}
			for {
				ok, err := stream.Next(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				v := out.Int64Lists["list"]
				if v == nil || v.Values == nil {
					t.Fatal("native list missing")
				}
				for row := 0; row < out.Rows; row++ {
					key := out.Int64s["key"].Values[row]
					lo, hi, valid := v.ValueRange(row)
					if !valid {
						got = append(got, fmt.Sprintf("%d:null", key))
						continue
					}
					x := fmt.Sprintf("%d:[", key)
					for i := lo; i < hi; i++ {
						if i != lo {
							x += ","
						}
						if !v.Values.IsValid(i) {
							x += "null"
						} else {
							x += fmt.Sprint(v.Values.Values[i])
						}
					}
					got = append(got, x+"]")
				}
			}
			want := []string{"1:[]", "2:[-4]", "3:[7,null]", "4:null"}
			if mode == "resident-topk" {
				want = want[:3]
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v want %v", got, want)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if query != nil && query.Used() != 0 {
				t.Fatalf("leaked budget %d", query.Used())
			}
		})
	}
}
