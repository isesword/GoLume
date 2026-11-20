package lazy

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/aggregate"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
)

func n34PublicParquet(t *testing.T, keys []int64, keyValid []bool, values []string, scale int32) string {
	return n34PublicParquetRows(t, keys, keyValid, values, scale, 128)
}
func n34PublicParquetRows(t *testing.T, keys []int64, keyValid []bool, values []string, scale int32, rowGroup int64) string {
	t.Helper()
	if len(keys) != len(values) {
		t.Fatal("fixture lengths")
	}
	dt := &arrow.Decimal128Type{Precision: 38, Scale: scale}
	schema := arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "d", Type: dt, Nullable: true}, {Name: "keep", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rb := array.NewRecordBuilder(am.DefaultAllocator, schema)
	defer rb.Release()
	for i, k := range keys {
		if keyValid != nil && !keyValid[i] {
			rb.Field(0).(*array.Int64Builder).AppendNull()
		} else {
			rb.Field(0).(*array.Int64Builder).Append(k)
		}
		if values[i] == "NULL" {
			rb.Field(1).(*array.Decimal128Builder).AppendNull()
		} else {
			n, ok := new(big.Int).SetString(values[i], 10)
			if !ok {
				t.Fatal(values[i])
			}
			rb.Field(1).(*array.Decimal128Builder).Append(decimal128.FromBigInt(n))
		}
		rb.Field(2).(*array.Int64Builder).Append(int64(i % 2))
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer tbl.Release()
	path := filepath.Join(t.TempDir(), "input.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	e = pqarrow.WriteTable(tbl, f, rowGroup, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	ce := f.Close()
	if e != nil {
		t.Fatal(e)
	}
	if ce != nil && !errors.Is(ce, os.ErrClosed) {
		t.Fatal(ce)
	}
	return path
}
func n34PublicFrame(t *testing.T, path string) LazyFrame {
	t.Helper()
	o := engine.DefaultOptions()
	o.Workers = 1
	o.Pipeline.Workers = 1
	o.PreparedCacheBytes = 0
	s := NewSession(o)
	t.Cleanup(func() {
		if e := s.Close(); e != nil {
			t.Error(e)
		}
	})
	f, e := s.ScanParquet(path)
	if e != nil {
		t.Fatal(e)
	}
	return f
}
func n34PublicOptions(t *testing.T, mode engine.DecimalBoundedAggregateMode) DecimalBoundedTerminalOptions {
	t.Helper()
	q, e := memory.NewEngineBudget(32 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 32 << 20})
	if e != nil {
		t.Fatal(e)
	}
	store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if q.Used() != 0 || store.Used() != 0 {
			t.Errorf("leak memory/disk%d/%d", q.Used(), store.Used())
		}
		if e := store.Close(); e != nil {
			t.Error(e)
		}
		if e := q.Close(); e != nil {
			t.Error(e)
		}
	})
	return DecimalBoundedTerminalOptions{Budget: q, Store: store, TableCapacity: 16, Mode: mode, AveragePrecision: 38, AverageScale: 4, AverageRounding: aggregate.DecimalAverageHalfEven}
}

type n34PublicKey struct {
	key   int64
	valid bool
}
type n34PublicValue struct {
	sum         *big.Int
	rows, count uint64
}

func n34PublicAvg(sum *big.Int, count uint64, inScale, outScale int32) *big.Int {
	n := new(big.Int).Abs(sum)
	d := new(big.Int).SetUint64(count)
	delta := outScale - inScale
	if delta >= 0 {
		n.Mul(n, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(delta)), nil))
	} else {
		d.Mul(d, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-delta)), nil))
	}
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(n, d, r)
	c := new(big.Int).Lsh(r, 1).Cmp(d)
	if c > 0 || c == 0 && q.Bit(0) != 0 {
		q.Add(q, big.NewInt(1))
	}
	if sum.Sign() < 0 {
		q.Neg(q)
	}
	return q
}
func n34PublicCoeff(v memory.Decimal128) *big.Int {
	n := new(big.Int).SetUint64(uint64(v.HighBits()))
	n.Lsh(n, 64)
	n.Add(n, new(big.Int).SetUint64(v.LowBits()))
	if v.HighBits() < 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	return n
}
func TestN34DecimalBoundedTerminalPublicModes(t *testing.T) {
	for _, mode := range []engine.DecimalBoundedAggregateMode{engine.DecimalBoundedSumCounts, engine.DecimalBoundedAverageCounts, engine.DecimalBoundedSumAverageCounts} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			keys := make([]int64, 1100)
			valid := make([]bool, len(keys))
			values := make([]string, len(keys))
			want := map[n34PublicKey]*n34PublicValue{}
			for i := range keys {
				keys[i] = int64(i%553) - 276
				valid[i] = i%101 != 0
				values[i] = fmt.Sprint(i%149 - 74)
				if i%17 == 0 {
					values[i] = "NULL"
				}
				if i%2 == 0 {
					continue
				}
				k := n34PublicKey{keys[i], valid[i]}
				if !k.valid {
					k.key = 0
				}
				v := want[k]
				if v == nil {
					v = &n34PublicValue{sum: new(big.Int)}
					want[k] = v
				}
				v.rows++
				if values[i] != "NULL" {
					n, _ := new(big.Int).SetString(values[i], 10)
					v.sum.Add(v.sum, n)
					v.count++
				}
			}
			f := n34PublicFrame(t, n34PublicParquet(t, keys, valid, values, 2)).Filter(expr.Col("keep").Eq(expr.I64(1))).Select("d", "k")
			opts := n34PublicOptions(t, mode)
			if mode == engine.DecimalBoundedSumCounts {
				opts.AveragePrecision = 0
				opts.AverageScale = 0
				opts.AverageRounding = 0
			}
			out, e := f.CollectDecimalBounded(context.Background(), "k", "d", opts)
			if out != nil {
				t.Cleanup(func() {
					if e := out.Close(context.Background()); e != nil {
						t.Error(e)
					}
				})
			}
			if e != nil {
				t.Fatal(e)
			}
			if out.Stats.Flushes <= 8 || out.Stats.MergePasses < 2 || opts.Store.Used() == 0 {
				t.Fatalf("not actual multipass spill: %+v", out.Stats)
			}
			order := make([]n34PublicKey, 0, len(want))
			for k := range want {
				order = append(order, k)
			}
			sort.Slice(order, func(i, j int) bool {
				if order[i].valid != order[j].valid {
					return !order[i].valid
				}
				return order[i].key < order[j].key
			})
			seen := 0
			for {
				b, ok, e := out.Next(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				if b.Mode != mode || b.Precision != 38 || b.Scale != 2 {
					t.Fatal("metadata mismatch")
				}
				for i := 0; i < b.Rows; i++ {
					if seen >= len(order) {
						t.Fatal("extra row")
					}
					r := b.Records[i]
					k := order[seen]
					v := want[k]
					if r.KeyValid != k.valid || k.valid && r.Key != k.key || r.CountRows != v.rows || r.CountValid != v.count {
						t.Fatalf("key/count mismatch %+v", r)
					}
					if mode != engine.DecimalBoundedAverageCounts {
						if r.SumValid != (v.count != 0) || r.SumValid && n34PublicCoeff(r.Sum).Cmp(v.sum) != 0 {
							t.Fatal("SUM oracle mismatch")
						}
					} else if r.SumValid {
						t.Fatal("AVG-only SUM")
					}
					if mode != engine.DecimalBoundedSumCounts {
						if r.AverageValid != (v.count != 0) || r.AverageValid && n34PublicCoeff(r.Average).Cmp(n34PublicAvg(v.sum, v.count, 2, 4)) != 0 {
							t.Fatal("AVG oracle mismatch")
						}
					}
					seen++
				}
			}
			if seen != len(order) {
				t.Fatal("missing groups")
			}
		})
	}
}
func TestN34DecimalBoundedTerminalPublicBoundaries(t *testing.T) {
	path := n34PublicParquet(t, []int64{1, 2, 3}, nil, []string{"3", "NULL", "5"}, 0)
	base := n34PublicFrame(t, path)
	for name, f := range map[string]LazyFrame{"sort": base.Sort(Asc("k")), "limit": base.Limit(1), "topk": base.TopK(1, Asc("k")), "computed": base.SelectExprs(expr.Col("k").Add(expr.I64(1)).Alias("k"), expr.Col("d").Alias("d")), "missing": base.Select("k")} {
		t.Run(name, func(t *testing.T) {
			opts := n34PublicOptions(t, engine.DecimalBoundedAverageCounts)
			out, e := f.CollectDecimalBounded(context.Background(), "k", "d", opts)
			if out != nil {
				defer out.Close(context.Background())
			}
			if e == nil {
				t.Fatal("unsupported plan accepted")
			}
		})
	}
	for _, filter := range []expr.Expr{expr.Bool(false), expr.Col("d").IsNull()} {
		t.Run(fmt.Sprint(filter), func(t *testing.T) {
			opts := n34PublicOptions(t, engine.DecimalBoundedAverageCounts)
			out, e := base.Filter(filter).CollectDecimalBounded(context.Background(), "k", "d", opts)
			if e != nil {
				t.Fatal(e)
			}
			defer out.Close(context.Background())
			b, ok, e := out.Next(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			if _, falseFilter := filter.(expr.BoolLiteral); falseFilter {
				if ok {
					t.Fatal("filtered-empty emitted")
				}
			} else {
				if !ok || b.Rows != 1 || b.Records[0].Key != 2 || b.Records[0].AverageValid || b.Records[0].CountValid != 0 {
					t.Fatal("IS NULL semantics")
				}
			}
		})
	}
}
func TestN34DecimalBoundedTerminalPublicFinalFailure(t *testing.T) {
	max := "99999999999999999999999999999999999999"
	f := n34PublicFrame(t, n34PublicParquet(t, []int64{0, 0}, nil, []string{max, max}, 0))
	opts := n34PublicOptions(t, engine.DecimalBoundedSumCounts)
	opts.AveragePrecision = 0
	opts.AverageScale = 0
	opts.AverageRounding = 0
	out, e := f.CollectDecimalBounded(context.Background(), "k", "d", opts)
	if out != nil {
		defer out.Close(context.Background())
	}
	if !errors.Is(e, aggregate.ErrDecimalSumOverflow) {
		t.Fatalf("lost sum error provenance: %v", e)
	}
	opts2 := n34PublicOptions(t, engine.DecimalBoundedAverageCounts)
	opts2.AverageScale = 0
	out, e = f.CollectDecimalBounded(context.Background(), "k", "d", opts2)
	if e != nil {
		t.Fatal(e)
	}
	defer out.Close(context.Background())
	b, ok, e := out.Next(context.Background())
	if e != nil || !ok || n34PublicCoeff(b.Records[0].Average).String() != max {
		t.Fatal("AVG-only wide SUM failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opts3 := n34PublicOptions(t, engine.DecimalBoundedAverageCounts)
	bad, e := f.CollectDecimalBounded(ctx, "k", "d", opts3)
	if bad != nil {
		defer bad.Close(context.Background())
	}
	if !errors.Is(e, context.Canceled) {
		t.Fatalf("lost cancel provenance:%v", e)
	}
}
