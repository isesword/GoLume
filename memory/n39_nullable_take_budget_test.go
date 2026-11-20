package memory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Independent fixture/oracle: output is never computed by an engine helper.
func n39Tree(depth int, text bool) (*ArrayNode, Field, []any) {
	var vals []any
	if text {
		vals = []any{"alpha", nil, strings.Repeat("界", 24000)}
	} else {
		vals = []any{int64(-7), nil, int64(91)}
	}
	leaf := &ArrayNode{Encoding: EncodingView}
	lf := Field{Name: "item", Nullable: true, Encoding: EncodingView}
	if text {
		leaf.Kind = TypeString
		lf.Type = TypeString
		leaf.Strings = &StringViewVector{Offsets: []uint32{0, 5, 5, uint32(5 + len(vals[2].(string)))}, Data: []byte("alpha" + vals[2].(string)), Validity: Bitmap{5}, Encoding: EncodingView}
	} else {
		leaf.Kind = TypeInt64
		lf.Type = TypeInt64
		leaf.Int64 = &Int64Vector{Values: []int64{-7, 888, 91}, Validity: Bitmap{5}, Encoding: EncodingView}
	}
	v := &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 2, 2, 3}, Validity: Bitmap{5}, Child: leaf, Encoding: EncodingView}
	f := Field{Name: "item", Type: TypeList, Nullable: true, Encoding: EncodingView, Children: []Field{lf}}
	rows := []any{[]any{vals[0], nil}, nil, []any{vals[2]}}
	if depth == 2 {
		v = &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 2, 2, 3}, Validity: Bitmap{5}, Child: v, Encoding: EncodingView}
		f = Field{Name: "items", Type: TypeList, Nullable: true, Encoding: EncodingView, Children: []Field{f}}
		rows = []any{[]any{rows[0], nil}, nil, []any{rows[2]}}
	} else {
		f.Name = "items"
	}
	return v, f, rows
}
func n39Read(v *ArrayNode, r int) any {
	if !v.IsValid(r) {
		return nil
	}
	if v.Kind == TypeInt64 {
		return v.Int64.Values[r]
	}
	if v.Kind == TypeString {
		return string(v.Strings.ValueBytes(r))
	}
	out := []any{}
	for i := v.Offsets[r]; i < v.Offsets[r+1]; i++ {
		out = append(out, n39Read(v.Child, int(i)))
	}
	return out
}
func n39Check(t *testing.T, o *OwnedArray, want []any) {
	t.Helper()
	if o == nil || !o.Field.Nullable || o.Vector.Len() != len(want) {
		t.Fatal("owner/schema/length")
	}
	for i, w := range want {
		if !reflect.DeepEqual(n39Read(o.Vector, i), w) {
			t.Fatalf("row %d mismatch", i)
		}
	}
}
func n39Poison(v *ArrayNode) {
	if v == nil {
		return
	}
	for i := range v.Offsets {
		v.Offsets[i] = 0
	}
	for i := range v.Validity {
		v.Validity[i] = 0
	}
	if v.Int64 != nil {
		for i := range v.Int64.Values {
			v.Int64.Values[i] = 777
		}
	}
	if v.Strings != nil {
		for i := range v.Strings.Data {
			v.Strings.Data[i] = 'x'
		}
	}
	n39Poison(v.Child)
}

type n39Probe struct {
	Manager
	calls  int
	fail   bool
	cancel context.CancelFunc
	peak   int64
}

func (m *n39Probe) Reserve(ctx context.Context, n int64) (Reservation, error) {
	m.calls++
	if m.fail {
		return nil, ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	if e == nil && m.cancel != nil {
		m.cancel()
	}
	return r, e
}
func TestN39NullableTakeBudgetOwner(t *testing.T) {
	for _, depth := range []int{1, 2} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("depth%d_string%v", depth, text), func(t *testing.T) {
				src, f, rows := n39Tree(depth, text)
				sel := []int{2, -1, 0, 2, 1, 0}
				want := []any{rows[2], nil, rows[0], rows[2], nil, rows[0]}
				m := &n39Probe{Manager: NewBudgetManager(8 << 20)}
				o, e := TakeNullableArrayRowsOwned(context.Background(), m, src, f, sel)
				if e != nil {
					t.Fatal(e)
				}
				n39Check(t, o, want)
				charge := o.Bytes
				if m.calls != 1 || m.Used() != charge {
					t.Fatal("not exactly one owned reservation")
				}
				t.Logf("depth=%d string=%v duplicateRows=%d bytes=%d reserveCalls=%d", depth, text, len(sel), charge, m.calls)
				single, e := TakeNullableArrayRowsOwned(context.Background(), m, src, f, []int{2})
				if e != nil {
					t.Fatal(e)
				}
				if charge <= single.Bytes {
					t.Fatal("duplicates not charged")
				}
				if m.Used() != charge+single.Bytes {
					t.Fatal("owners not additive")
				}
				single.Close()
				n39Poison(src)
				f.Children[0].Name = "poison"
				n39Check(t, o, want)
				if o.Field.Children[0].Name == "poison" {
					t.Fatal("borrowed schema")
				}
				o.Close()
				o.Close()
				if m.Used() != 0 || o.Vector != nil {
					t.Fatal("close leak")
				}
				for _, delta := range []int64{0, -1} {
					t.Run(fmt.Sprintf("quota%d", delta), func(t *testing.T) {
						s, f, _ := n39Tree(depth, text)
						m := NewBudgetManager(charge + delta)
						o, e := TakeNullableArrayRowsOwned(context.Background(), m, s, f, sel)
						if delta == 0 {
							if e != nil {
								t.Fatal(e)
							}
							n39Check(t, o, want)
							o.Close()
						} else if !errors.Is(e, ErrBudgetExceeded) || o != nil {
							t.Fatalf("quota-1 owner=%v err=%v", o, e)
						}
						if m.Used() != 0 {
							t.Fatal("quota cleanup")
						}
					})
				}
				for _, cancel := range []bool{false, true} {
					t.Run(fmt.Sprintf("reserveCancel%v", cancel), func(t *testing.T) {
						s, f, _ := n39Tree(depth, text)
						ctx, stop := context.WithCancel(context.Background())
						defer stop()
						m := &n39Probe{Manager: NewBudgetManager(charge), fail: !cancel}
						expected := ErrBudgetExceeded
						if cancel {
							m.cancel = stop
							expected = context.Canceled
						}
						o, e := TakeNullableArrayRowsOwned(ctx, m, s, f, sel)
						if !errors.Is(e, expected) || o != nil || m.Used() != 0 || m.calls != 1 {
							t.Fatalf("fault owner=%v err=%v used=%d calls=%d", o, e, m.Used(), m.calls)
						}
					})
				}
			})
		}
	}
}

func TestN39NullableTakeSelectionContracts(t *testing.T) {
	for _, depth := range []int{1, 2} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			for _, indices := range [][]int{{-2}, {3}, {-1, -2}} {
				s, f, _ := n39Tree(depth, false)
				m := NewBudgetManager(1 << 20)
				o, e := TakeNullableArrayRowsOwned(context.Background(), m, s, f, indices)
				if e == nil || o != nil || m.Used() != 0 {
					t.Fatal("bad selection accepted")
				}
			}
			// Nullable take preserves its root-optional contract even for required schema.
			s, f, _ := n39Tree(depth, false)
			f.Nullable = false
			m := NewBudgetManager(1 << 20)
			o, e := TakeNullableArrayRowsOwned(context.Background(), m, s, f, []int{-1, 1})
			if e != nil {
				t.Fatal(e)
			}
			n39Check(t, o, []any{nil, nil})
			o.Close()
			// Required NULL descendants only matter when selected and visible.
			s, f, _ = n39Tree(depth, false)
			leaf := &f
			for leaf.Type == TypeList {
				leaf = &leaf.Children[0]
			}
			leaf.Nullable = false
			o, e = TakeNullableArrayRowsOwned(context.Background(), m, s, f, []int{-1, 2})
			if e != nil {
				t.Fatal("unselected required null", e)
			}
			o.Close()
			o, e = TakeNullableArrayRowsOwned(context.Background(), m, s, f, []int{0})
			if e == nil || o != nil {
				t.Fatal("selected required descendant null accepted")
			}
			// Even sentinel-only selection cannot hide malformed structure.
			s, f, _ = n39Tree(depth, false)
			s.Offsets[2] = 9
			o, e = TakeNullableArrayRowsOwned(context.Background(), m, s, f, []int{-1})
			if e == nil || o != nil || m.Used() != 0 {
				t.Fatal("hidden malformed structure accepted")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			s, f, _ = n39Tree(depth, false)
			o, e = TakeNullableArrayRowsOwned(ctx, m, s, f, []int{-1})
			if !errors.Is(e, context.Canceled) || o != nil || m.Used() != 0 {
				t.Fatal("pre-cancel", e)
			}
		})
	}
}

// The owner executes synchronously: count its context checks without timers or
// nondeterministic goroutines, including checks while copying large strings.
type n39CancelChecks struct {
	context.Context
	checks, at int
}

func (c *n39CancelChecks) Err() error {
	c.checks++
	if c.at > 0 && c.checks >= c.at {
		return context.Canceled
	}
	return nil
}
func TestN39NullableTakeEveryContextCheck(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("string%v", text), func(t *testing.T) {
			s, f, _ := n39Tree(2, text)
			ctx := &n39CancelChecks{Context: context.Background()}
			m := NewBudgetManager(8 << 20)
			o, e := TakeNullableArrayRowsOwned(ctx, m, s, f, []int{2, -1, 0, 2})
			if e != nil {
				t.Fatal(e)
			}
			o.Close()
			checks := ctx.checks
			t.Logf("context checkpoints=%d", checks)
			for at := 1; at <= checks; at++ {
				t.Run(fmt.Sprintf("check%03d", at), func(t *testing.T) {
					s, f, _ := n39Tree(2, text)
					ctx := &n39CancelChecks{Context: context.Background(), at: at}
					m := NewBudgetManager(8 << 20)
					o, e := TakeNullableArrayRowsOwned(ctx, m, s, f, []int{2, -1, 0, 2})
					if !errors.Is(e, context.Canceled) || o != nil || m.Used() != 0 {
						t.Fatalf("check=%d owner=%v err=%v used=%d", at, o, e, m.Used())
					}
				})
			}
		})
	}
}
