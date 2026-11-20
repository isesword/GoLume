package memory

import (
	"context"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"reflect"
	"testing"
)

func n38ReviewTree(depth int, kind DataType) (Field, *ArrayNode) {
	f := Field{Name: "item", Type: kind, Nullable: true, Encoding: EncodingPlain}
	v := &ArrayNode{Kind: kind, Encoding: EncodingView}
	if kind == TypeInt64 {
		v.Int64 = &Int64Vector{Values: []int64{7, 8, 9}, Encoding: EncodingPlain}
	} else {
		f.Encoding = EncodingView
		v.Strings = &StringViewVector{Offsets: []uint32{0, 1, 4, 4}, Data: []byte("a界"), Encoding: EncodingView}
	}
	for i := 0; i < depth; i++ {
		f = Field{Name: "item", Type: TypeList, Nullable: true, Encoding: EncodingView, Children: []Field{f}}
		v = &ArrayNode{Kind: TypeList, Encoding: EncodingView, Offsets: []uint32{0, 1, 2, 3}, Child: v}
	}
	f.Name = "v"
	return f, v
}
func n38ReviewValue(v *ArrayNode, row int) any {
	if !v.IsValid(row) {
		return nil
	}
	switch v.Kind {
	case TypeInt64:
		return v.Int64.Values[row]
	case TypeString:
		return string(v.Strings.ValueBytes(row))
	default:
		a := []any{}
		for i := v.Offsets[row]; i < v.Offsets[row+1]; i++ {
			a = append(a, n38ReviewValue(v.Child, int(i)))
		}
		return a
	}
}
func TestN38ReviewOwnedDepthAndArrow(t *testing.T) {
	for _, depth := range []int{2, 3} {
		for _, kind := range []DataType{TypeInt64, TypeString} {
			f, v := n38ReviewTree(depth, kind)
			if e := v.ValidateWithContext(f, 3, NestedListParentDomain{}, nil); e != nil {
				t.Fatal(depth, kind, e)
			}
			want := []any{n38ReviewValue(v, 2), n38ReviewValue(v, 0), n38ReviewValue(v, 2)}
			m := NewBudgetManager(1 << 20)
			o, e := TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, []int{2, 0, 2})
			if e != nil {
				t.Fatal(depth, kind, e)
			}
			if m.Used() == 0 {
				t.Fatal("owner uncharged")
			}
			leaf := v
			for leaf.Kind == TypeList {
				leaf = leaf.Child
			}
			if kind == TypeInt64 {
				leaf.Int64.Values[2] = -99
			} else {
				leaf.Strings.Data[0] = 'Z'
			}
			for i, w := range want {
				if !reflect.DeepEqual(n38ReviewValue(o.Vector, i), w) {
					t.Fatal("source alias or wrong repeated selection")
				}
			}
			b := NewBatch()
			b.Rows = 3
			b.Arrays["v"] = o.Vector
			s := NewSchema(o.Field)
			alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			rec, e := ArrowRecord(alloc, s, b)
			if e != nil {
				t.Fatal(depth, kind, e)
			}
			if rec.NumRows() != 3 {
				t.Fatal("Arrow rows")
			}
			rec.Release()
			alloc.AssertSize(t, 0)
			r, e := CloneNestedListRangeOwned(context.Background(), m, o.Vector, o.Field, NestedListParentDomain{}, 1, 2)
			if e != nil {
				t.Fatal(e)
			}
			borrowed, e := borrowArrayRange(r.Vector, 1, 1, nil)
			if e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(n38ReviewValue(borrowed, 0), want[2]) {
				t.Fatal("borrow coordinate mismatch")
			}
			r.Close()
			o.Close()
			o.Close()
			if m.Used() != 0 {
				t.Fatal("owner leak", m.Used())
			}
		}
	}
}
func TestN38ReviewHiddenStructureAndRequired(t *testing.T) {
	for axis := 0; axis < 3; axis++ {
		f, v := n38ReviewTree(3, TypeInt64)
		n := v
		for j := 0; j < axis; j++ {
			n = n.Child
		}
		n.Validity = Bitmap{6}
		// All descendants of row zero are hidden. A required terminal NULL there is legal.
		fp := &f
		for fp.Type == TypeList {
			fp = &fp.Children[0]
		}
		fp.Nullable = false
		leaf := v.Child.Child.Child
		leaf.Int64.Validity = Bitmap{6}
		if e := v.ValidateWithContext(f, 3, NestedListParentDomain{}, nil); e != nil {
			t.Fatal("hidden required NULL", axis, e)
		}
		n.Offsets[1] = 4 // hidden spans still require valid structural bounds.
		if e := v.ValidateWithContext(f, 3, NestedListParentDomain{}, nil); e == nil {
			t.Fatal("hidden malformed offsets accepted", axis)
		}
	}
	f, v := n38ReviewTree(3, TypeInt64)
	f.Children[0].Children[0].Children[0].Nullable = false
	v.Child.Child.Child.Int64.Validity = Bitmap{6}
	if e := v.ValidateWithContext(f, 3, NestedListParentDomain{}, nil); e == nil {
		t.Fatal("visible required NULL accepted")
	}
	f, v = n38ReviewTree(4, TypeInt64)
	if e := v.ValidateWithContext(f, 3, NestedListParentDomain{}, nil); e == nil {
		t.Fatal("depth4 accepted")
	}
	f, v = n38ReviewTree(3, TypeInt64)
	f.Children[0].Children[0].Children[0].DictionaryDomain = "invalid"
	if e := v.ValidateWithContext(f, 3, NestedListParentDomain{}, nil); e == nil {
		t.Fatal("invalid metadata accepted")
	}
}
