package lazy

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"math/big"
	"strings"
	"testing"
)

func n36PublicInput(t *testing.T, parts []int64, pvalid []bool, keys []int64, values []string, page int) *inmemory.Source {
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

func TestN36PartitionPrefixPublicExactProjection(t *testing.T) {
	max := "99999999999999999999999999999999999999"
	values := []string{"NULL", "2", "3", max, max, "-9", "-2", "NULL", "-3", "7"}
	parts := []int64{9, 8, 7, math.MinInt64, math.MinInt64, -3, 0, 0, 0, math.MaxInt64}
	pv := []bool{false, false, false, true, true, true, true, true, true, true}
	keys := []int64{1, 2, 3, -8, -7, 100, -10, -9, -8, 0}
	for mode := aggregate.DecimalAverageTowardZero; mode <= aggregate.DecimalAverageHalfAwayFromZero; mode++ {
		for _, page := range []int{1, 3, 10} {
			t.Run(fmt.Sprintf("mode%d_page%d", mode, page), func(t *testing.T) {
				s := n28PublicSession(16 << 20)
				defer s.Close()
				q := s.FromSource(n36PublicInput(t, parts, pv, keys, values, page)).Select("v", "k", "p").WithPartitionedDecimalPrefixAverage("v", "p", "k", "prefix", 38, 0, mode).Select("prefix")
				schema, e := q.Schema()
				if e != nil || len(schema.Fields) != 1 || schema.Fields[0].DecimalPrecision != 38 {
					t.Fatalf("schema %v %v", schema, e)
				}
				plan, e := q.ExplainPhysical(context.Background())
				if e != nil || !strings.Contains(plan, "Prefix") {
					t.Fatalf("explain %s %v", plan, e)
				}
				var want []*big.Int
				for _, segment := range [][]string{values[:3], values[3:5], values[5:6], values[6:9], values[9:]} {
					want = append(want, n28PublicOracle(segment, 0, 0, mode)...)
				}
				d, e := q.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer d.Close()
				n28PublicCheck(t, d.Column, 38, 0, want)
				s.Close()
				n28PublicCheck(t, d.Column, 38, 0, want)
			})
		}
	}
}
func TestN36PartitionPrefixPublicNonElision(t *testing.T) {
	for _, action := range []string{"collect", "count"} {
		t.Run(action, func(t *testing.T) {
			s := n28PublicSession(16 << 20)
			defer s.Close()
			q := s.FromSource(n36PublicInput(t, []int64{0, 0}, nil, []int64{1, 1}, []string{"1", "2"}, 1)).WithPartitionedDecimalPrefixAverage("v", "p", "k", "prefix", 38, 0, aggregate.DecimalAverageHalfEven).Select("k")
			if action == "count" {
				q = q.Agg(Count("n"))
			}
			d, e := q.Collect(context.Background())
			d.Close()
			if e == nil {
				t.Fatal("projection/count removed prefix order validation")
			}
		})
	}
}
func TestN36PartitionPrefixPublicRejectUpstream(t *testing.T) {
	s := n28PublicSession(16 << 20)
	defer s.Close()
	base := s.FromSource(n36PublicInput(t, []int64{0}, nil, []int64{1}, []string{"1"}, 1))
	for name, q := range map[string]LazyFrame{"filter_true": base.Filter(expr.Bool(true)), "sort": base.Sort(Asc("p"), Asc("k")), "limit": base.Limit(1), "topk": base.TopK(1, Asc("k"))} {
		t.Run(name, func(t *testing.T) {
			q = q.WithPartitionedDecimalPrefixAverage("v", "p", "k", "prefix", 38, 0, aggregate.DecimalAverageHalfEven)
			if _, e := q.Schema(); e == nil {
				t.Fatal("Schema accepted unsupported upstream")
			}
			if _, e := q.ExplainOptimized(); e == nil {
				t.Fatal("optimizer erased forbidden plan")
			}
			if _, e := q.Prepare(context.Background()); e == nil {
				t.Fatal("Prepare accepted unsupported upstream")
			}
		})
	}
}

func TestN36PartitionPrefixPublicBorrowedBatches(t *testing.T) {
	s := n28PublicSession(16 << 20)
	defer s.Close()
	q := s.FromSource(n36PublicInput(t, []int64{0, 0, 1, 1}, nil, []int64{1, 2, -1, 0}, []string{"2", "3", "NULL", "7"}, 1)).WithPartitionedDecimalPrefixAverage("v", "p", "k", "prefix", 38, 0, aggregate.DecimalAverageHalfEven)
	want := []*big.Int{big.NewInt(2), big.NewInt(2), nil, big.NewInt(7)}
	r, e := q.CollectBatches(context.Background(), BatchOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	seen := 0
	var old BatchView
	for r.Next(context.Background()) {
		if old.Valid() {
			t.Fatal("old view survived Next")
		}
		v := r.Batch()
		n := int(v.Len())
		n28PublicCheck(t, v.Column, 38, 0, want[seen:seen+n])
		seen += n
		old = v
	}
	if r.Err() != nil || seen != len(want) {
		t.Fatalf("batch result %d %v", seen, r.Err())
	}
	r.Close()
	if old.Valid() {
		t.Fatal("old view survived Close")
	}
}
