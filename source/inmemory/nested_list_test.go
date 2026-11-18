package inmemory

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"testing"
)

func nestedMemoryFixture(t *testing.T, depth int) (memory.Schema, *memory.Batch) {
	t.Helper()
	leaf := memory.Field{Name: "item", Type: memory.TypeInt64, Nullable: true}
	f, e := memory.NestedListField("items", true, memory.Field{Name: "item", Type: memory.TypeList, Nullable: true, Children: []memory.Field{leaf}})
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Rows = 5
	b.Arrays["items"] = &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 0, 0, 1, 2, 4}, Validity: memory.Bitmap{30}, Child: &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 1, 3, 4}, Validity: memory.Bitmap{14}, Child: &memory.ArrayNode{Kind: memory.TypeInt64, Int64: &memory.Int64Vector{Values: []int64{999, 7, 999, -8}, Validity: memory.Bitmap{11}}}}}
	for i := 0; i < depth; i++ {
		f = memory.StructField("inner", true, f)
		v := &memory.StructVector{Rows: 5, Fields: f.Children, Values: b}
		if i == depth-1 {
			v.Validity = memory.Bitmap{15}
		}
		b = memory.NewBatch()
		b.Rows = 5
		b.Structs["inner"] = v
	}
	s, e := memory.NewSchemaWithNestedListsChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	return s, b
}
func TestNestedListInMemoryViewsAndOwned(t *testing.T) {
	for _, depth := range []int{0, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			schema, input := nestedMemoryFixture(t, depth)
			src, e := New(schema, input)
			if e != nil {
				t.Fatal(e)
			}
			m := memory.NewBudgetManager(0)
			opts := streamOptions(1)
			opts.MemoryManager = m
			stream, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				t.Fatal(e)
			}
			defer stream.Close()
			om := memory.NewBudgetManager(0)
			var copies []*memory.Batch
			var leases []memory.Reservation
			for row := 0; row < 5; row++ {
				out := memory.NewBatch()
				ok, e := stream.Next(context.Background(), out)
				if e != nil || !ok {
					t.Fatal(row, ok, e)
				}
				o, l, e := memory.CloneCollectBatchNestedLists(context.Background(), om, out, schema)
				if e != nil {
					t.Fatal(e)
				}
				copies = append(copies, o)
				leases = append(leases, l)
			}
			stream.Close()
			if m.Used() != 0 {
				t.Fatal("view charge")
			}
			input.ClearColumns()
			for row, b := range copies {
				cur := b
				visible := true
				for d := 0; d < depth; d++ {
					v := cur.Structs["inner"]
					if v == nil || v.Rows != 1 {
						t.Fatal("Struct axes")
					}
					visible = visible && v.IsValid(0)
					cur = v.Values
				}
				a := cur.Arrays["items"]
				if a == nil {
					t.Fatal("missing array")
				}
				if !visible {
					if a.IsValid(0) {
						t.Fatal("ancestor folding")
					}
				} else {
					if a.IsValid(0) != (row != 0) {
						t.Fatal("outer validity")
					}
					switch row {
					case 1:
						if a.Offsets[1] != 0 {
							t.Fatal("empty outer")
						}
					case 2:
						if a.Child.Len() != 1 || a.Child.IsValid(0) {
							t.Fatal("NULL inner")
						}
					case 3:
						if a.Child.Len() != 1 || !a.Child.IsValid(0) || a.Child.Offsets[1] != 0 {
							t.Fatal("empty inner")
						}
					case 4:
						if a.Child.Len() != 2 || a.Child.Child.Int64.Values[0] != 7 || a.Child.Child.Int64.IsValid(1) || a.Child.Child.Int64.Values[2] != -8 {
							t.Fatal("leaf oracle")
						}
					}
				}
				b.ReleaseOwned()
				leases[row].Release()
			}
			if om.Used() != 0 {
				t.Fatal("owned charge")
			}
		})
	}
}
func TestNestedListInMemoryUndeclaredRejected(t *testing.T) {
	_, b := nestedMemoryFixture(t, 0)
	old := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64})
	b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, 5)}
	if _, e := New(old, b); e == nil {
		t.Fatal("undeclared new map accepted under old schema")
	}
}
