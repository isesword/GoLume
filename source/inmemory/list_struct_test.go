package inmemory

import (
	"context"
	"errors"
	"gocolumn/memory"
	"testing"
)

func inMemoryStructListFixture(t *testing.T) (memory.Schema, *memory.Batch) {
	t.Helper()
	f, e := memory.ListStructField("items", false, true, memory.Field{Name: "n", Type: memory.TypeInt64})
	if e != nil {
		t.Fatal(e)
	}
	child := memory.NewBatch()
	child.Rows = 3
	child.Int64s["n"] = &memory.Int64Vector{Values: []int64{99, 88, 17}, Validity: memory.Bitmap{4}}
	v := &memory.StructListVector{Offsets: []uint32{0, 1, 1, 2, 3}, Validity: memory.Bitmap{14}, Values: &memory.StructVector{Rows: 3, Values: child, Fields: f.Children[0].Children, Validity: memory.Bitmap{4}}}
	current := memory.NewBatch()
	current.Rows = 4
	current.StructLists["items"] = v
	field := f
	for i := 0; i < 7; i++ {
		field = memory.StructField("inner", true, field)
		node := &memory.StructVector{Rows: 4, Fields: field.Children, Values: current}
		if i == 6 {
			node.Validity = memory.Bitmap{14}
		}
		current = memory.NewBatch()
		current.Rows = 4
		current.Structs["inner"] = node
	}
	field.Name = "root"
	b := memory.NewBatch()
	b.Rows = 4
	b.Structs["root"] = current.Structs["inner"]
	other := f
	other.Name = "other"
	other.Nullable = true
	b.StructLists["other"] = v
	schema, e := memory.NewSchemaWithStructListsChecked(field, other)
	if e != nil {
		t.Fatal(e)
	}
	return schema, b
}
func inMemoryStructListLeaf(b *memory.Batch) *memory.Batch {
	v := b.Structs["root"]
	for i := 1; i < 7; i++ {
		v = v.Values.Structs["inner"]
	}
	return v.Values
}
func inMemoryStructListCheck(t *testing.T, b *memory.Batch, row int, canonical bool) {
	t.Helper()
	if b.Rows != 1 {
		t.Fatal("bounded view rows")
	}
	for _, v := range []*memory.StructListVector{inMemoryStructListLeaf(b).StructLists["items"], b.StructLists["other"]} {
		if v.IsValid(0) != (row != 0) {
			t.Fatal("List parent domain", row)
		}
		first, last := int(v.Offsets[0]), int(v.Offsets[1])
		if a, z, valid := v.ValueRange(0); row == 0 && (a != 0 || z != 0 || valid) {
			t.Fatal("NULL safe accessor must not expose hidden span")
		}
		want := 0
		if row == 0 && !canonical {
			want = 1
		}
		if row >= 2 {
			want = 1
		}
		if last-first != want {
			t.Fatal("list span", row, first, last)
		}
		if row == 2 && v.Values.IsValid(first) {
			t.Fatal("NULL element lost")
		}
		if row == 3 && (!v.Values.IsValid(first) || v.Values.Values.Int64s["n"].Values[first] != 17) {
			t.Fatal("required child value")
		}
	}
}
func TestStructListInMemoryBoundedOwnedAndCleanup(t *testing.T) {
	schema, b := inMemoryStructListFixture(t)
	src, e := New(schema, b)
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	opts := streamOptions(1)
	opts.MemoryManager = m
	st, e := src.NewBatchStream(context.Background(), opts)
	if e != nil {
		t.Fatal(e)
	}
	ownedM := memory.NewBudgetManager(0)
	var owned []*memory.Batch
	var leases []memory.Reservation
	for row := 0; row < 4; row++ {
		out := memory.NewBatch()
		ok, e := st.Next(context.Background(), out)
		if e != nil || !ok {
			t.Fatal(ok, e)
		}
		inMemoryStructListCheck(t, out, row, false)
		copy, lease, e := memory.CloneCollectBatchStructLists(context.Background(), ownedM, out, schema)
		if e != nil {
			t.Fatal(e)
		}
		owned = append(owned, copy)
		leases = append(leases, lease)
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("view scratch leak")
	}
	b.StructLists["other"].Values.Values.Int64s["n"].Values[2] = -999
	b.Structs["root"].Validity[0] = 0
	for row, out := range owned {
		inMemoryStructListCheck(t, out, row, true)
		out.ReleaseOwned()
		leases[row].Release()
	}
	if ownedM.Used() != 0 {
		t.Fatal("owned cleanup")
	}
	schema, b = inMemoryStructListFixture(t)
	src, e = New(schema, b)
	if e != nil {
		t.Fatal(e)
	}
	small := memory.NewBudgetManager(1)
	opts.MemoryManager = small
	st, e = src.NewBatchStream(context.Background(), opts)
	if e == nil {
		_, e = st.Next(context.Background(), memory.NewBatch())
		st.Close()
	}
	if !errors.Is(e, memory.ErrBudgetExceeded) || small.Used() != 0 {
		t.Fatal("inmemory budget", e, small.Used())
	}
	ctx, cancel := context.WithCancel(context.Background())
	opts.MemoryManager = m
	st, e = src.NewBatchStream(ctx, opts)
	if e != nil {
		t.Fatal(e)
	}
	cancel()
	ok, e := st.Next(ctx, memory.NewBatch())
	st.Close()
	if ok || !errors.Is(e, context.Canceled) || m.Used() != 0 {
		t.Fatal("inmemory cancel", ok, e, m.Used())
	}
}
func TestStructListInMemoryRequiredAndHiddenStorage(t *testing.T) {
	for _, kind := range []string{"visible_required", "hidden_offset", "undeclared_cycle", "missing_list"} {
		t.Run(kind, func(t *testing.T) {
			schema, b := inMemoryStructListFixture(t)
			v := b.StructLists["other"]
			switch kind {
			case "visible_required":
				v.Values.Values.Int64s["n"].Validity[0] = 0
			case "hidden_offset":
				v.Offsets[1] = 2
			case "undeclared_cycle":
				v.Values.Values.StructLists["cycle"] = v
			case "missing_list":
				delete(b.StructLists, "other")
			}
			if _, e := New(schema, b); e == nil {
				t.Fatal("invalid Struct-list source accepted", kind)
			}
		})
	}
}
