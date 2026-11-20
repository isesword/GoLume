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

func TestDecimalGroupedAverageOrderedOracle(t *testing.T) {
	for _, desc := range []bool{false, true} {
		for _, nullLast := range []bool{false, true} {
			for _, n := range []int{0, 67, 195} {
				t.Run(fmt.Sprintf("n%d/desc%t/nullLast%t", n, desc, nullLast), func(t *testing.T) {
					const precision, scale int32 = 37, 37
					ctx := context.Background()
					s := n18Session(t, engine.ExecutionMemoryOnly)
					r := n17Record(n)
					defer r.Release()
					base, err := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
					if err != nil {
						t.Fatal(err)
					}
					key := Asc("k")
					if desc {
						key = Desc("k")
					}
					key.NullsLast = nullLast
					q := base.Sort(key).GroupBy("k").AggConfigured(Sum("d", "sum"), Count("rows"), AvgDecimal128("d", precision, scale, expr.DecimalRoundHalfEven, "mean"), CountColumn("d", "valid"), Sum("e", "other"), AvgDecimal128("d", precision, scale, expr.DecimalRoundFloor, "floor"))
					budget, err := s.Engine().BeginQueryMemory()
					if err != nil {
						t.Fatal(err)
					}
					defer budget.Close()
					df, err := q.Collect(memory.WithQueryBudget(ctx, budget))
					if err != nil {
						t.Fatal(err)
					}
					defer df.Close()

					want := n17Oracle(n, desc, nullLast)
					samples := map[n17Key][]*big.Int{}
					for i := 0; i < n; i++ {
						k := n17Key{int64(i%67 - 33), true}
						if i%17 == 0 {
							k = n17Key{}
						}
						if i%7 != 0 {
							samples[k] = append(samples[k], big.NewInt(int64(i%13-6)))
						}
					}
					keys, err := df.Column("k")
					if err != nil {
						t.Fatal(err)
					}
					ks, err := keys.Int64Chunks()
					if err != nil {
						t.Fatal(err)
					}
					pos := 0
					for _, ch := range ks {
						for i := 0; i < ch.Len(); i++ {
							v, ok, e := ch.Value(i)
							if e != nil || pos >= len(want) || ok != want[pos].key.valid || ok && v != want[pos].key.k {
								t.Fatal("key", pos, v, ok, e)
							}
							pos++
						}
					}
					if pos != len(want) {
						t.Fatal("key row count", pos, len(want))
					}
					for _, name := range []string{"mean", "floor", "sum", "other"} {
						p, sc := precision, scale
						if name == "sum" {
							p, sc = 38, 38
						} else if name == "other" {
							p, sc = 38, 0
						}
						f, ok := df.Schema().Field(name)
						if !ok || f.Type != memory.TypeDecimal || !f.Nullable || f.DecimalPrecision != p || f.DecimalScale != sc {
							t.Fatal("schema", name, f)
						}
						c, e := df.Column(name)
						if e != nil {
							t.Fatal(e)
						}
						chs, e := c.Decimal128Chunks()
						if e != nil {
							t.Fatal(e)
						}
						pos = 0
						for _, ch := range chs {
							if ch.Precision() != p || ch.Scale() != sc {
								t.Fatal("chunk metadata", name, ch.Precision(), ch.Scale())
							}
							for i := 0; i < ch.Len(); i++ {
								if pos >= len(want) {
									t.Fatal("extra row", name, pos)
								}
								exp := want[pos].sum
								switch name {
								case "mean":
									exp = n20AverageOracle(samples[want[pos].key], 38, sc, expr.DecimalRoundHalfEven)
								case "floor":
									exp = n20AverageOracle(samples[want[pos].key], 38, sc, expr.DecimalRoundFloor)
								case "other":
									exp = want[pos].other
								}
								v, valid, err := ch.Value(i)
								if err != nil || valid != (exp != nil) || valid && v.BigInt().Cmp(exp) != 0 {
									t.Fatal(name, pos, v, valid, exp, err)
								}
								pos++
							}
						}
						if pos != len(want) {
							t.Fatal("decimal length", name, pos, len(want))
						}
					}
					for _, name := range []string{"rows", "valid"} {
						c, e := df.Column(name)
						if e != nil {
							t.Fatal(e)
						}
						chs, e := c.Uint64Chunks()
						if e != nil {
							t.Fatal(e)
						}
						pos = 0
						for _, ch := range chs {
							for i := 0; i < ch.Len(); i++ {
								if pos >= len(want) {
									t.Fatal("extra count", name, pos)
								}
								exp := want[pos].rows
								if name == "valid" {
									exp = want[pos].valid
								}
								v, ok, err := ch.Value(i)
								if err != nil || !ok || v != exp {
									t.Fatal(name, pos, v, ok, exp, err)
								}
								pos++
							}
						}
						if pos != len(want) {
							t.Fatal("count length", name, pos, len(want))
						}
					}
					df.Close()
					if budget.Used() != 0 {
						t.Fatal("budget still charged", budget.Used())
					}
				})
			}
		}
	}
}

func TestDecimalGroupedAverageUnorderedAndLegacyReject(t *testing.T) {
	r := n17Record(195)
	defer r.Release()
	a := AvgDecimal128("d", 37, 37, expr.DecimalRoundHalfEven, "mean")
	for _, tc := range []struct {
		name   string
		policy engine.ExecutionPolicy
		legacy bool
	}{
		{"unordered bounded", engine.ExecutionBounded, false},
		{"unordered auto", engine.ExecutionAuto, false},
		{"ordered legacy", engine.ExecutionMemoryOnly, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := n18Session(t, tc.policy)
			base, err := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
			if err != nil {
				t.Fatal(err)
			}
			q := base.GroupBy("k").AggConfigured(a)
			if tc.legacy {
				q = base.Sort(Asc("k")).GroupBy("k").Agg(Avg("d", "mean"))
			}
			df, err := q.Collect(context.Background())
			if err == nil {
				df.Close()
				t.Fatal("unexpectedly accepted")
			}
		})
	}
}
