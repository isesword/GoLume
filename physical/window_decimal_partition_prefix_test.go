package physical

import (
	"context"
	"errors"
	"math"
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func n36PhysicalInput(t *testing.T, parts []int64, pvalid []bool, keys []int64, values []string, page int) *inmemory.Source {
	t.Helper()
	v, e := memory.Decimal128Field("v", true, 38, 0)
	if e != nil {
		t.Fatal(e)
	}
	schema := memory.NewSchema(memory.Field{Name: "p", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "k", Type: memory.TypeInt64}, v)
	var pages []*memory.Batch
	for lo := 0; lo < len(parts); lo += page {
		hi := min(len(parts), lo+page)
		b := memory.NewBatch()
		b.Rows = hi - lo
		p := &memory.Int64Vector{Values: append([]int64(nil), parts[lo:hi]...), Validity: make(memory.Bitmap, memory.WordsFor(b.Rows))}
		k := &memory.Int64Vector{Values: append([]int64(nil), keys[lo:hi]...)}
		d := &memory.Decimal128Vector{Precision: 38, Validity: make(memory.Bitmap, memory.WordsFor(b.Rows)), Values: make([]memory.Decimal128, b.Rows)}
		for i := lo; i < hi; i++ {
			if pvalid == nil || pvalid[i] {
				memory.Set(p.Validity, i-lo)
			}
			if values[i] != "NULL" {
				x, ok := new(big.Int).SetString(values[i], 10)
				if !ok {
					t.Fatal(values[i])
				}
				d.Values[i-lo] = decimal128.FromBigInt(x)
				memory.Set(d.Validity, i-lo)
			}
		}
		b.Int64s["p"] = p
		b.Int64s["k"] = k
		b.Decimals["v"] = d
		pages = append(pages, b)
	}
	src, e := inmemory.New(schema, pages...)
	if e != nil {
		t.Fatal(e)
	}
	return src.WithOrdering(source.OrderingColumn{Name: "p"}, source.OrderingColumn{Name: "k"})
}
func n36PhysicalSpec() PartitionedDecimalPrefixAverage {
	return PartitionedDecimalPrefixAverage{Column: "v", PartitionBy: "p", OrderBy: "k", Alias: "avg", Precision: 38, Rounding: aggregate.DecimalAverageHalfEven, Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}}
}
func n36PhysicalOpen(t *testing.T, src source.Source, spec PartitionedDecimalPrefixAverage) (source.BatchStream, *memory.BudgetManager) {
	t.Helper()
	op, e := NewPartitionedDecimalPrefixAverageSource(src, spec)
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(2 << 20)
	st, e := op.NewBatchStream(context.Background(), source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), MemoryManager: m})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := st.Close(); e != nil {
			t.Error(e)
		}
		if m.Used() != 0 {
			t.Errorf("budget leak %d", m.Used())
		}
	})
	return st, m
}
func TestN36PartitionPrefixPhysicalResetWide(t *testing.T) {
	max := "99999999999999999999999999999999999999"
	parts := []int64{99, 88, math.MinInt64, math.MinInt64, -1, math.MaxInt64, math.MaxInt64}
	valid := []bool{false, false, true, true, true, true, true}
	keys := []int64{0, 1, -9, -8, 0, math.MinInt64, math.MaxInt64}
	values := []string{"NULL", "3", max, max, "-5", "2", "3"}
	want := []string{"NULL", "3", max, max, "-5", "2", "2"}
	for _, page := range []int{1, 2, 7} {
		st, _ := n36PhysicalOpen(t, n36PhysicalInput(t, parts, valid, keys, values, page), n36PhysicalSpec())
		dst := memory.NewBatch()
		seen := 0
		for {
			ok, e := st.Next(context.Background(), dst)
			if e != nil {
				t.Fatal(e)
			}
			if !ok {
				break
			}
			a := dst.Decimals["avg"]
			for i := 0; i < dst.Rows; i++ {
				if a.IsValid(i) != (want[seen] != "NULL") || a.IsValid(i) && a.Values[i].BigInt().String() != want[seen] {
					t.Fatalf("row%d got%v/%v want%s", seen, a.Values[i], a.IsValid(i), want[seen])
				}
				seen++
			}
		}
		if seen != len(want) {
			t.Fatal("rows missing")
		}
		st.Close()
	}
}
func TestN36PartitionPrefixPhysicalOrderAtomic(t *testing.T) {
	cases := []struct {
		name string
		p, k []int64
		pv   []bool
	}{{"duplicate", []int64{0, 0}, []int64{1, 1}, nil}, {"descending-key", []int64{0, 0}, []int64{2, 1}, nil}, {"return-partition", []int64{1, 0}, []int64{1, 2}, nil}, {"null-after-valid", []int64{1, 0}, []int64{1, 2}, []bool{true, false}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, page := range []int{1, 2} {
				st, _ := n36PhysicalOpen(t, n36PhysicalInput(t, tc.p, tc.pv, tc.k, []string{"1", "2"}, page), n36PhysicalSpec())
				dst := memory.NewBatch()
				if page == 1 {
					ok, e := st.Next(context.Background(), dst)
					if !ok || e != nil {
						t.Fatal(e)
					}
				}
				ok, e := st.Next(context.Background(), dst)
				if ok || e == nil || dst.Rows != 0 || len(dst.Decimals) != 0 {
					t.Fatalf("bad page exposed: %v %v %+v", ok, e, dst)
				}
				st.Close()
			}
		})
	}
}
func TestN36PartitionPrefixPhysicalOverflowCancel(t *testing.T) {
	src := n36PhysicalInput(t, []int64{0, 0}, nil, []int64{1, 2}, []string{"1", "99"}, 2)
	cfg := n36PhysicalSpec()
	cfg.Precision = 1
	st, _ := n36PhysicalOpen(t, src, cfg)
	dst := memory.NewBatch()
	ok, e := st.Next(context.Background(), dst)
	if ok || !errors.Is(e, aggregate.ErrDecimalAverageOverflow) || dst.Rows != 0 {
		t.Fatalf("overflow page published %v %v", ok, e)
	}
	st2, _ := n36PhysicalOpen(t, src, n36PhysicalSpec())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, e = st2.Next(ctx, dst)
	if ok || !errors.Is(e, context.Canceled) || dst.Rows != 0 {
		t.Fatalf("cancel %v %v", ok, e)
	}
}
func TestN36PartitionPrefixPhysicalWitness(t *testing.T) {
	for _, w := range [][]source.OrderingColumn{nil, {{Name: "k"}, {Name: "p"}}, {{Name: "p", NullsLast: true}, {Name: "k"}}, {{Name: "p", Descending: true}, {Name: "k"}}, {{Name: "p"}, {Name: "k", Descending: true}}} {
		src := n36PhysicalInput(t, []int64{0}, nil, []int64{1}, []string{"1"}, 1).WithOrdering(w...)
		if _, e := NewPartitionedDecimalPrefixAverageSource(src, n36PhysicalSpec()); e == nil {
			t.Fatalf("accepted false witness %+v", w)
		}
	}
	for _, mutate := range []func(*PartitionedDecimalPrefixAverage){func(s *PartitionedDecimalPrefixAverage) { s.PartitionBy = "k" }, func(s *PartitionedDecimalPrefixAverage) { s.Precision = 0 }, func(s *PartitionedDecimalPrefixAverage) { s.Rounding = 0 }, func(s *PartitionedDecimalPrefixAverage) { s.Alias = "p" }} {
		s := n36PhysicalSpec()
		mutate(&s)
		if _, e := NewPartitionedDecimalPrefixAverageSource(n36PhysicalInput(t, []int64{0}, nil, []int64{1}, []string{"1"}, 1), s); e == nil {
			t.Fatal("accepted invalid specification")
		}
	}
}
