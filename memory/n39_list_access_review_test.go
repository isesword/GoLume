package memory

import (
	"context"
	"reflect"
	"testing"
)

func TestN39ReviewNullableRecursiveOwner(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		f, v := n38ReviewTree(2, kind)
		f.Nullable = false
		v.Validity = Bitmap{5}
		m := NewBudgetManager(1 << 20)
		want := n38ReviewValue(v, 2)
		o, e := TakeNullableArrayRowsOwned(context.Background(), m, v, f, []int{-1, 2, 1, 2})
		if e != nil {
			t.Fatal(kind, e)
		}
		if !o.Field.Nullable || o.Vector.Len() != 4 || o.Vector.IsValid(0) || o.Vector.IsValid(2) {
			t.Fatal("nullable root contract")
		}
		if kind == TypeInt64 {
			v.Child.Child.Int64.Values[2] = -77
		} else {
			v.Child.Child.Strings.Offsets[3] = 3
		}
		for _, i := range []int{1, 3} {
			if !reflect.DeepEqual(n38ReviewValue(o.Vector, i), want) {
				t.Fatal("duplicate/source alias", kind, i)
			}
		}
		f.Children[0].Name = "poison"
		if o.Field.Children[0].Name != "item" {
			t.Fatal("metadata alias")
		}
		o.Close()
		o.Close()
		if m.Used() != 0 {
			t.Fatal("leak", m.Used())
		}
	}
}
func TestN39ReviewNullableRequiredAndHiddenShape(t *testing.T) {
	for _, bad := range []int{-2, 3} {
		f, v := n38ReviewTree(2, TypeInt64)
		m := NewBudgetManager(1 << 20)
		if o, e := TakeNullableArrayRowsOwned(context.Background(), m, v, f, []int{bad}); e == nil {
			o.Close()
			t.Fatal("invalid selection", bad)
		}
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
	f, v := n38ReviewTree(2, TypeInt64)
	f.Children[0].Children[0].Nullable = false
	v.Child.Child.Int64.Validity = Bitmap{6}
	m := NewBudgetManager(1 << 20)
	o, e := TakeNullableArrayRowsOwned(context.Background(), m, v, f, []int{-1, 1, 1})
	if e != nil {
		t.Fatal("unselected required NULL", e)
	}
	o.Close()
	if o, e = TakeNullableArrayRowsOwned(context.Background(), m, v, f, []int{0}); e == nil {
		o.Close()
		t.Fatal("selected required NULL accepted")
	}
	v.Validity = Bitmap{6}
	v.Child.Offsets[1] = 4
	if o, e = TakeNullableArrayRowsOwned(context.Background(), m, v, f, []int{-1}); e == nil {
		o.Close()
		t.Fatal("hidden malformed structure accepted")
	}
	if m.Used() != 0 {
		t.Fatal("failure leak", m.Used())
	}
}

type n39ReviewCancelManager struct {
	*BudgetManager
	cancel context.CancelFunc
}

func (m *n39ReviewCancelManager) Reserve(ctx context.Context, n int64) (Reservation, error) {
	r, e := m.BudgetManager.Reserve(ctx, n)
	if e == nil {
		m.cancel()
	}
	return r, e
}
func TestN39ReviewNullableCancelAfterReserveAndBitmap(t *testing.T) {
	f, v := n38ReviewTree(2, TypeString)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &n39ReviewCancelManager{NewBudgetManager(1 << 20), cancel}
	o, e := TakeNullableArrayRowsOwned(ctx, m, v, f, []int{2, 0, -1, 2})
	if e == nil {
		o.Close()
		t.Fatal("post-reserve cancellation published")
	}
	if ctx.Err() == nil || m.Used() != 0 {
		t.Fatal("cancel cleanup", e, m.Used())
	}
	f, v = n38ReviewTree(2, TypeInt64)
	v.Child.Validity = Bitmap{}
	plain := NewBudgetManager(1 << 20)
	if o, e = TakeNullableArrayRowsOwned(context.Background(), plain, v, f, []int{-1}); e == nil {
		o.Close()
		t.Fatal("hidden short bitmap accepted")
	}
	if plain.Used() != 0 {
		t.Fatal("bitmap failure leak")
	}
}
