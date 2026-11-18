package memory

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestNestedListTakeRangeEstimate(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		for _, r := range [][2]int{{0, 0}, {0, 7}, {1, 4}, {4, 1}, {5, 1}, {7, 0}} {
			t.Run(fmt.Sprint(kind, r), func(t *testing.T) {
				f, v := nestedListTestFixture(kind)
				n, e := EstimateNestedListTakeRangeBytes(context.Background(), v, f, NestedListParentDomain{}, r[0], r[1])
				if e != nil {
					t.Fatal(e)
				}
				rows := make([]int, r[1])
				for i := range rows {
					rows[i] = r[0] + r[1] - 1 - i
				}
				m := NewBudgetManager(n)
				o, e := TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, rows)
				if e != nil {
					t.Fatal(e)
				}
				nestedListTestCheck(t, o, kind, rows, false)
				if o.Bytes != n || m.Used() != n {
					t.Fatal("range permutation exact fee", n, o.Bytes, m.Used())
				}
				o.Close()
				if m.Used() != 0 {
					t.Fatal("release")
				}
				short := NewBudgetManager(n - 1)
				bad, e := TakeNestedListRowsOwned(context.Background(), short, v, f, NestedListParentDomain{}, rows)
				if bad != nil || !errors.Is(e, ErrBudgetExceeded) || short.Used() != 0 {
					t.Fatal("exact minus one", e)
				}
			})
		}
	}
}

func nestedEstimateTreeCheck(t *testing.T, v *StructVector, f Field, depth int, rows []int) {
	t.Helper()
	for i, r := range rows {
		if v.IsValid(i) != (r != 5) {
			t.Fatal("outer NULL", i, r)
		}
	}
	for i := 0; i < depth; i++ {
		f = f.Children[0]
		if i+1 < depth {
			v = v.Values.Structs[f.Name]
		}
	}
	nestedListTestCheck(t, &OwnedNestedList{Vector: v.Values.Arrays[f.Name], Field: f}, TypeInt64, rows, true)
}
func TestNestedListsTreeTakeRangeEstimate(t *testing.T) {
	for _, depth := range []int{1, 8} {
		for _, r := range [][2]int{{0, 0}, {0, 7}, {1, 4}, {5, 1}, {7, 0}} {
			t.Run(fmt.Sprint(depth, r), func(t *testing.T) {
				s, b := nestedBatchFixture(t, depth)
				f := s.Fields[0]
				v := b.Structs[f.Name]
				n, e := EstimateNestedListsStructTakeRangeBytes(context.Background(), v, f, r[0], r[1])
				if e != nil {
					t.Fatal(e)
				}
				rows := make([]int, r[1])
				for i := range rows {
					rows[i] = r[0] + r[1] - 1 - i
				}
				m := NewBudgetManager(n)
				o, e := TakeNestedListsStructRowsOwned(context.Background(), m, v, f, rows)
				if e != nil {
					t.Fatal(e)
				}
				nestedEstimateTreeCheck(t, o.Vector, f, depth, rows)
				actual := o.Bytes
				if actual > n || m.Used() != actual {
					t.Fatal("tree estimate underbounds owner", n, actual, m.Used())
				}
				o.Close()
				for _, limit := range []int64{actual, actual - 1} {
					mm := NewBudgetManager(limit)
					oo, err := TakeNestedListsStructRowsOwned(context.Background(), mm, v, f, rows)
					if limit == actual {
						if err != nil {
							t.Fatal(err)
						}
						oo.Close()
					} else if oo != nil || !errors.Is(err, ErrBudgetExceeded) {
						t.Fatal("tree -1", err)
					}
					if mm.Used() != 0 {
						t.Fatal("tree release")
					}
				}
			})
		}
	}
}
func TestNestedListTakeEstimateDemandedAndHidden(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		f, v := nestedListTestFixture(kind)
		f.Children[0].Children[0].Nullable = false
		for _, r := range [][2]int{{0, 0}, {0, 4}, {5, 2}} {
			if _, e := EstimateNestedListTakeRangeBytes(context.Background(), v, f, NestedListParentDomain{}, r[0], r[1]); e != nil {
				t.Fatal("unselected required NULL", e)
			}
		}
		if _, e := EstimateNestedListTakeRangeBytes(context.Background(), v, f, NestedListParentDomain{}, 4, 1); e == nil {
			t.Fatal("selected required NULL accepted")
		}
		v.Child.Offsets[1] = 99
		if _, e := EstimateNestedListTakeRangeBytes(context.Background(), v, f, NestedListParentDomain{}, 0, 0); e == nil {
			t.Fatal("empty selection hid bad storage")
		}
	}
}

func TestNestedListTakeEstimateEmptyInnerAndRepeats(t *testing.T) {
	for _, count := range []int{63, 64, 65, 4097} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f, _ := NestedListField("n", false, ListInt64Field("item", false, false))
			v := &ArrayNode{Kind: TypeList, Offsets: []uint32{0, uint32(count)}, Child: &ArrayNode{Kind: TypeList, Offsets: make([]uint32, count+1), Child: &ArrayNode{Kind: TypeInt64, Int64: &Int64Vector{}}}}
			n, e := EstimateNestedListTakeRangeBytes(context.Background(), v, f, NestedListParentDomain{}, 0, 1)
			if e != nil {
				t.Fatal(e)
			}
			o, e := TakeNestedListRowsOwned(context.Background(), NewBudgetManager(n), v, f, NestedListParentDomain{}, []int{0})
			if e != nil {
				t.Fatal(e)
			}
			if o.Bytes != n || o.Vector.Child.Len() != count || o.Vector.Child.Child.Len() != 0 {
				t.Fatal("empty inner axis", n, o.Bytes)
			}
			o.Close()
			m := NewBudgetManager(0)
			out, e := TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, []int{0, 0, 0})
			if e != nil {
				t.Fatal(e)
			}
			if out.Vector.Child.Len() != count*3 || out.Bytes <= n {
				t.Fatal("repeated R1 must charge independently", n, out.Bytes)
			}
			actual := out.Bytes
			out.Close()
			short := NewBudgetManager(actual - 1)
			bad, e := TakeNestedListRowsOwned(context.Background(), short, v, f, NestedListParentDomain{}, []int{0, 0, 0})
			if bad != nil || !errors.Is(e, ErrBudgetExceeded) || short.Used() != 0 {
				t.Fatal("repeat -1", e)
			}
		})
	}
}

func TestNestedListsTreeEstimateRequiredDomain(t *testing.T) {
	s, b := nestedBatchFixture(t, 8)
	f := s.Fields[0]
	v := b.Structs[f.Name]
	var required func(*Field, *StructVector)
	required = func(f *Field, v *StructVector) {
		if f.Children[0].Type == TypeStruct {
			required(&f.Children[0], v.Values.Structs[f.Children[0].Name])
		} else {
			f.Children[0].Children[0].Children[0].Nullable = false
		}
		v.Fields = append([]Field(nil), f.Children...)
	}
	required(&f, v)
	for _, r := range [][2]int{{0, 0}, {0, 4}, {5, 2}} {
		if _, e := EstimateNestedListsStructTakeRangeBytes(context.Background(), v, f, r[0], r[1]); e != nil {
			t.Fatal("unselected/ancestor hidden value", e)
		}
	}
	if _, e := EstimateNestedListsStructTakeRangeBytes(context.Background(), v, f, 4, 1); e == nil {
		t.Fatal("active NULL accepted")
	}
}
func TestNestedListEstimateRunningCancel(t *testing.T) {
	f, _ := NestedListField("n", false, ListInt64Field("item", false, false))
	v := &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 4097}, Child: &ArrayNode{Kind: TypeList, Offsets: make([]uint32, 4098), Child: &ArrayNode{Kind: TypeInt64, Int64: &Int64Vector{}}}}
	m := NewBudgetManager(0)
	ctx := &listStructTestCancelContext{Context: context.Background(), manager: m, target: ".nestedListRangeOwnedBytes", stop: 100}
	if _, e := EstimateNestedListTakeRangeBytes(ctx, v, f, NestedListParentDomain{}, 0, 1); !errors.Is(e, context.Canceled) || ctx.checks != 100 || m.Used() != 0 {
		t.Fatal("running range estimator cancellation", e, ctx.checks)
	}
	if _, e := EstimateNestedListTakeRangeBytes(context.Background(), v, f, NestedListParentDomain{}, 0, 1); e != nil {
		t.Fatal("retry", e)
	}
}

func TestNestedListsTreeRepeatedTakeEveryReserve(t *testing.T) {
	s, b := nestedBatchFixture(t, 8)
	f := s.Fields[0]
	src := b.Structs[f.Name]
	rows := []int{6, 4, 6, 5, 0, 2, 3, 1}
	probe := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	out, e := TakeNestedListsStructRowsOwned(context.Background(), probe, src, f, rows)
	if e != nil {
		t.Fatal(e)
	}
	nestedEstimateTreeCheck(t, out.Vector, f, 8, rows)
	calls := probe.calls
	out.Close()
	for at := 1; at <= calls; at++ {
		m := &recursiveCollectTestManager{Manager: NewBudgetManager(0), failAt: at}
		bad, err := TakeNestedListsStructRowsOwned(context.Background(), m, src, f, rows)
		if bad != nil || !errors.Is(err, ErrBudgetExceeded) || m.Used() != 0 {
			t.Fatal("Take fault", at, err, m.Used())
		}
		m.failAt = 0
		good, err := TakeNestedListsStructRowsOwned(context.Background(), m, src, f, rows)
		if err != nil {
			t.Fatal(err)
		}
		nestedEstimateTreeCheck(t, good.Vector, f, 8, rows)
		good.Close()
		if m.Used() != 0 {
			t.Fatal("retry")
		}
	}
}
