package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/arrowrecord"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

func n15CheckCount(t *testing.T, df DataFrame, name string, want uint64) {
	t.Helper()
	f, ok := df.Schema().Field(name)
	if !ok || f.Type != memory.TypeUint64 || f.Nullable {
		t.Fatal("count metadata", f)
	}
	c, e := df.Column(name)
	if e != nil {
		t.Fatal(e)
	}
	chunks, e := c.Uint64Chunks()
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, ch := range chunks {
		for i := 0; i < ch.Len(); i++ {
			v, valid, e := ch.Value(i)
			if e != nil || !valid || v != want {
				t.Fatal("count oracle", v, valid, e, want)
			}
			n++
		}
	}
	if n != 1 {
		t.Fatal("count rows", n)
	}
}
func n15Mixed(q LazyFrame) LazyFrame {
	return q.Agg(Count("rows"), Sum("d", "sum"), CountColumn("d", "valid"), Sum("d", "alias"), Count("rows2"), CountColumn("e", "other"))
}
func n15Check(t *testing.T, df DataFrame, scale int32, sum *big.Int, rows, valid uint64) {
	t.Helper()
	names := []string{"rows", "sum", "valid", "alias", "rows2", "other"}
	sc := df.Schema()
	if len(sc.Fields) != len(names) {
		t.Fatal(sc)
	}
	for i, n := range names {
		if sc.Fields[i].Name != n {
			t.Fatal("output order", sc)
		}
	}
	n14CheckSum(t, df, "sum", scale, sum)
	n14CheckSum(t, df, "alias", scale, sum)
	n15CheckCount(t, df, "rows", rows)
	n15CheckCount(t, df, "rows2", rows)
	n15CheckCount(t, df, "valid", valid)
	n15CheckCount(t, df, "other", valid)
}
func TestDecimalSumCountExactGlobal(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	neg := new(big.Int).Neg(new(big.Int).Set(max))
	for _, scale := range []int32{0, 38} {
		for _, values := range [][]*big.Int{nil, {nil, nil}, {big.NewInt(9), nil, big.NewInt(-3), big.NewInt(0)}, {max, max, neg}, {max, neg, max}, {neg, max, max}} {
			for _, batch := range []int{1, 2, 64} {
				for _, filter := range []int{0, 1, 2} {
					t.Run(fmt.Sprintf("s%d-n%d-b%d-f%d", scale, len(values), batch, filter), func(t *testing.T) {
						opts := engine.DefaultOptions()
						opts.PreparedCacheBytes = 0
						opts.MemoryLimitBytes = 64 << 20
						s := NewSession(opts)
						defer s.Close()
						rec := n14DecimalRecord(38, scale, values)
						defer rec.Release()
						src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, rec)
						if e != nil {
							t.Fatal(e)
						}
						defer src.Close()
						obs := &nestedTopKObserved{Source: src, stable: true, batchRows: batch}
						q := s.FromSource(obs)
						if filter == 1 {
							q = q.Filter(expr.IsNotNull(expr.Col("d")))
						}
						if filter == 2 {
							q = q.Filter(expr.Bool(false))
						}
						var sum *big.Int
						var rows, valid uint64
						for _, v := range values {
							if filter == 2 || filter == 1 && v == nil {
								continue
							}
							rows++
							if v != nil {
								valid++
								if sum == nil {
									sum = new(big.Int)
								}
								sum.Add(sum, v)
							}
						}
						df, e := n15Mixed(q).Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						n15Check(t, df, scale, sum, rows, valid)
						df.Close()
						if obs.opens != 1 || obs.closes != 1 {
							t.Fatal("stream", obs.opens, obs.closes)
						}
						ctx, cancel := context.WithCancel(context.Background())
						cancel()
						df, e = n15Mixed(q).Collect(ctx)
						if e == nil {
							df.Close()
						}
						if !errors.Is(e, context.Canceled) {
							t.Fatal("cancel", e)
						}
						s.Close()
						if s.Engine().Memory().Used() != 0 {
							t.Fatal("retained")
						}
					})
				}
			}
		}
	}
}
func TestDecimalSumCountIOAndBudget(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.MemoryLimitBytes = 64 << 20
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.Spill = memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	rec := n14DecimalRecord(38, 38, []*big.Int{big.NewInt(9), nil, big.NewInt(-3)})
	defer rec.Release()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, rec)
	if e != nil {
		t.Fatal(e)
	}
	in := filepath.Join(t.TempDir(), "in.parquet")
	if _, e = q.SinkParquet(context.Background(), in, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	read, e := s.ScanParquetWithOptions(in, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	mixed := n15Mixed(read)
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	df, e := mixed.Collect(memory.WithQueryBudget(context.Background(), budget))
	if e != nil {
		t.Fatal(e)
	}
	n15Check(t, df, 38, big.NewInt(6), 3, 2)
	df.Close()
	if budget.Used() != 0 || budget.Snapshot().SpilledBytes != 0 {
		t.Fatal("query balance", budget.Snapshot())
	}
	budget.Close()
	ip := filepath.Join(t.TempDir(), "mixed.arrow")
	if _, e = mixed.SinkIPC(context.Background(), ip, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(ip)
	if e != nil {
		t.Fatal(e)
	}
	rd, e := ipc.NewFileReader(f)
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for i := 0; i < rd.NumRecords(); i++ {
		r, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		for col, a := range r.Columns() {
			switch col {
			case 1, 3:
				d := a.(*array.Decimal128)
				dt := d.DataType().(*arrow.Decimal128Type)
				if dt.Precision != 38 || dt.Scale != 38 {
					t.Fatal(dt)
				}
				for j := 0; j < d.Len(); j++ {
					if d.IsNull(j) || d.Value(j).BigInt().Cmp(big.NewInt(6)) != 0 {
						t.Fatal("IPC decimal")
					}
				}
			default:
				c := a.(*array.Uint64)
				want := uint64(2)
				if col == 0 || col == 4 {
					want = 3
				}
				for j := 0; j < c.Len(); j++ {
					if c.IsNull(j) || c.Value(j) != want {
						t.Fatal("IPC count")
					}
				}
			}
		}
		n += int(r.NumRows())
		r.Release()
	}
	rd.Close()
	f.Close()
	if n != 1 {
		t.Fatal(n)
	}
	out := filepath.Join(t.TempDir(), "mixed.parquet")
	if _, e = mixed.SinkParquet(context.Background(), out, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	again, e := s.ScanParquetWithOptions(out, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	df, e = again.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	n15Check(t, df, 38, big.NewInt(6), 3, 2)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("IO retained")
	}
}

func TestDecimalSumCountBoundariesAndAdmission(t *testing.T) {
	for _, limit := range []int64{1, 64 << 20} {
		opts := engine.DefaultOptions()
		opts.PreparedCacheBytes = 0
		opts.MemoryLimitBytes = limit
		s := NewSession(opts)
		rec := n14DecimalRecord(38, 0, []*big.Int{big.NewInt(1), nil})
		q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, rec)
		rec.Release()
		if e != nil {
			t.Fatal(e)
		}
		if limit == 1 {
			df, e := n15Mixed(q).Collect(context.Background())
			if e == nil {
				df.Close()
				t.Fatal("one-byte admission accepted")
			}
			if !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatal("wrong admission error", e)
			}
		} else {
			for _, bad := range []LazyFrame{q.Agg(CountColumn("d", "n")), q.Agg(Sum("d", "s"), CountColumn("", "n")), q.Agg(Sum("d", "s"), CountColumn("missing", "n")), q.Agg(Sum("d", "s"), Avg("d", "a")), q.GroupBy("e").Agg(Sum("d", "s"), Count("n")), q.WithColumns(expr.Named(expr.I64(1), "i")).Agg(Sum("d", "s"), CountColumn("i", "n"))} {
				df, e := bad.Collect(context.Background())
				if e == nil {
					df.Close()
					t.Fatal("unsupported scope accepted")
				}
			}
			max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
			r := n14DecimalRecord(38, 0, []*big.Int{max, max})
			overflow, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
			r.Release()
			if e != nil {
				t.Fatal(e)
			}
			df, e := n15Mixed(overflow).Collect(context.Background())
			if e == nil {
				df.Close()
				t.Fatal("mixed final overflow accepted")
			}
			df, e = n15Mixed(q).Collect(context.Background())
			if e != nil {
				t.Fatal("failure poisoned subsequent query", e)
			}
			n15Check(t, df, 0, big.NewInt(1), 2, 1)
			df.Close()
		}
		s.Close()
		if s.Engine().Memory().Used() != 0 {
			t.Fatal("failure cleanup")
		}
	}
}
