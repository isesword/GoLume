package lazy

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
)

func TestN22DecimalResidentAverageUnorderedOracle(t *testing.T) {
	for _, n := range []int{0, 67, 195} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			const p, sc = 37, 37
			s := n18Session(t, engine.ExecutionMemoryOnly)
			r := n17Record(n)
			defer r.Release()
			f, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
			if e != nil {
				t.Fatal(e)
			}
			q := f.GroupBy("k").AggConfigured(Sum("d", "sum"), Count("rows"), CountColumn("d", "valid"), AvgDecimal128("d", p, sc, expr.DecimalRoundHalfEven, "mean"), AvgDecimal128("d", p, sc, expr.DecimalRoundFloor, "floor"))
			b, e := s.Engine().BeginQueryMemory()
			if e != nil {
				t.Fatal(e)
			}
			defer b.Close()
			df, e := q.Collect(memory.WithQueryBudget(context.Background(), b))
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			want := map[n17Key]n17Want{}
			for _, w := range n17Oracle(n, false, false) {
				want[w.key] = w
			}
			values := map[n17Key][]*big.Int{}
			for i := 0; i < n; i++ {
				k := n17Key{int64(i%67 - 33), true}
				if i%17 == 0 {
					k = n17Key{}
				}
				if i%7 != 0 {
					values[k] = append(values[k], big.NewInt(int64(i%13-6)))
				}
			}
			kc, e := df.Column("k")
			if e != nil {
				t.Fatal(e)
			}
			chunks, e := kc.Int64Chunks()
			if e != nil {
				t.Fatal(e)
			}
			order := make([]n17Key, 0, len(want))
			seen := map[n17Key]bool{}
			for _, c := range chunks {
				for i := 0; i < c.Len(); i++ {
					v, ok, e := c.Value(i)
					if !ok {
						v = 0
					}
					k := n17Key{v, ok}
					if _, found := want[k]; e != nil || !found || seen[k] {
						t.Fatal(k, e)
					}
					seen[k] = true
					order = append(order, k)
				}
			}
			if len(order) != len(want) {
				t.Fatal(len(order), len(want))
			}
			for _, name := range []string{"mean", "floor", "sum"} {
				prec, scale := int32(p), int32(sc)
				if name == "sum" {
					prec, scale = 38, 38
				}
				h, ok := df.Schema().Field(name)
				if !ok || h.Type != memory.TypeDecimal || !h.Nullable || h.DecimalPrecision != prec || h.DecimalScale != scale {
					t.Fatal(name, h)
				}
				c, e := df.Column(name)
				if e != nil {
					t.Fatal(e)
				}
				cs, e := c.Decimal128Chunks()
				if e != nil {
					t.Fatal(e)
				}
				pos := 0
				for _, ch := range cs {
					if ch.Precision() != prec || ch.Scale() != scale {
						t.Fatal(name, "metadata")
					}
					for i := 0; i < ch.Len(); i++ {
						if pos >= len(order) {
							t.Fatal("extra row", name)
						}
						k := order[pos]
						exp := want[k].sum
						if name == "mean" {
							exp = n20AverageOracle(values[k], 38, scale, expr.DecimalRoundHalfEven)
						}
						if name == "floor" {
							exp = n20AverageOracle(values[k], 38, scale, expr.DecimalRoundFloor)
						}
						v, ok, e := ch.Value(i)
						if e != nil || ok != (exp != nil) || ok && v.BigInt().Cmp(exp) != 0 {
							t.Fatal(name, pos, v, ok, exp, e)
						}
						pos++
					}
				}
				if pos != len(order) {
					t.Fatal(name, pos, len(order))
				}
			}
			for _, name := range []string{"rows", "valid"} {
				c, e := df.Column(name)
				if e != nil {
					t.Fatal(e)
				}
				cs, e := c.Uint64Chunks()
				if e != nil {
					t.Fatal(e)
				}
				pos := 0
				for _, ch := range cs {
					for i := 0; i < ch.Len(); i++ {
						if pos >= len(order) {
							t.Fatal("extra row", name)
						}
						w := want[order[pos]]
						exp := w.rows
						if name == "valid" {
							exp = w.valid
						}
						v, ok, e := ch.Value(i)
						if e != nil || !ok || v != exp {
							t.Fatal(name, pos, v, ok, exp, e)
						}
						pos++
					}
				}
				if pos != len(order) {
					t.Fatal(name, pos, len(order))
				}
			}
			df.Close()
			if b.Used() != 0 {
				t.Fatal("retained query bytes", b.Used())
			}
		})
	}
}
