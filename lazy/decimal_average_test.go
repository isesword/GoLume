package lazy

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"gocolumn/aggregate"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/arrowrecord"
)

var n20Modes = []expr.DecimalRoundingMode{expr.DecimalRoundTowardZero, expr.DecimalRoundAwayFromZero, expr.DecimalRoundFloor, expr.DecimalRoundCeil, expr.DecimalRoundHalfEven, expr.DecimalRoundHalfAwayFromZero}

// Independent unbounded integer oracle: no aggregate kernel/finalizer is called.
func n20AverageOracle(values []*big.Int, inScale, outScale int32, mode expr.DecimalRoundingMode) *big.Int {
	sum := new(big.Int)
	n := int64(0)
	for _, v := range values {
		if v != nil {
			sum.Add(sum, v)
			n++
		}
	}
	if n == 0 {
		return nil
	}
	negative := sum.Sign() < 0
	sum.Abs(sum)
	den := big.NewInt(n)
	delta := outScale - inScale
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(absN20(delta))), nil)
	if delta >= 0 {
		sum.Mul(sum, pow)
	} else {
		den.Mul(den, pow)
	}
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(sum, den, r)
	inc := false
	if r.Sign() != 0 {
		switch mode {
		case expr.DecimalRoundAwayFromZero:
			inc = true
		case expr.DecimalRoundFloor:
			inc = negative
		case expr.DecimalRoundCeil:
			inc = !negative
		case expr.DecimalRoundHalfEven, expr.DecimalRoundHalfAwayFromZero:
			cmp := new(big.Int).Lsh(r, 1).Cmp(den)
			inc = cmp > 0 || cmp == 0 && (mode == expr.DecimalRoundHalfAwayFromZero || q.Bit(0) == 1)
		}
	}
	if inc {
		q.Add(q, big.NewInt(1))
	}
	if negative {
		q.Neg(q)
	}
	return q
}
func absN20(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}
func n20CheckAverage(t *testing.T, df DataFrame, name string, p, scale int32, want *big.Int) {
	t.Helper()
	f, ok := df.Schema().Field(name)
	if !ok || f.Type != memory.TypeDecimal || f.DecimalPrecision != p || f.DecimalScale != scale || !f.Nullable {
		t.Fatal("AVG schema", f)
	}
	c, e := df.Column(name)
	if e != nil {
		t.Fatal(e)
	}
	chunks, e := c.Decimal128Chunks()
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, ch := range chunks {
		if ch.Precision() != p || ch.Scale() != scale {
			t.Fatal("chunk metadata")
		}
		for i := 0; i < ch.Len(); i++ {
			v, valid, e := ch.Value(i)
			if e != nil || valid != (want != nil) || valid && v.BigInt().Cmp(want) != 0 {
				t.Fatal("AVG oracle", name, v, valid, e, want)
			}
			n++
		}
	}
	if n != 1 {
		t.Fatal("global row count", n)
	}
}
func TestDecimalAverageConfiguredOracle(t *testing.T) {
	datasets := [][]*big.Int{nil, {nil, nil}, {big.NewInt(2), nil, big.NewInt(3)}, {big.NewInt(-2), nil, big.NewInt(-3)}, {big.NewInt(149), big.NewInt(150), nil}, {big.NewInt(-149), big.NewInt(-150)}, {big.NewInt(7), big.NewInt(-7)}}
	for _, mode := range n20Modes {
		for _, scales := range [][2]int32{{0, 0}, {2, 1}, {0, 3}} {
			for di, values := range datasets {
				for _, batch := range []int{1, 64} {
					t.Run(fmt.Sprintf("m%d-s%d-%d-d%d-b%d", mode, scales[0], scales[1], di, batch), func(t *testing.T) {
						opts := engine.DefaultOptions()
						opts.PreparedCacheBytes = 0
						opts.MemoryLimitBytes = 64 << 20
						s := NewSession(opts)
						defer s.Close()
						rec := n14DecimalRecord(38, scales[0], values)
						defer rec.Release()
						src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, rec)
						if e != nil {
							t.Fatal(e)
						}
						defer src.Close()
						observed := &nestedTopKObserved{Source: src, stable: true, batchRows: batch}
						base := s.FromSource(observed)
						q := base.AggConfigured(AvgDecimal128("d", 9, scales[1], mode, "mean"), AvgDecimal128("d", 10, scales[1], mode, "alias"), Count("rows"), CountColumn("d", "valid"), Sum("e", "sum"))
						budget, e := s.Engine().BeginQueryMemory()
						if e != nil {
							t.Fatal(e)
						}
						defer budget.Close()
						df, e := q.Collect(memory.WithQueryBudget(context.Background(), budget))
						if e != nil {
							t.Fatal(e)
						}
						want := n20AverageOracle(values, scales[0], scales[1], mode)
						n20CheckAverage(t, df, "mean", 9, scales[1], want)
						n20CheckAverage(t, df, "alias", 10, scales[1], want)
						valid := uint64(0)
						for _, v := range values {
							if v != nil {
								valid++
							}
						}
						n15CheckCount(t, df, "rows", uint64(len(values)))
						n15CheckCount(t, df, "valid", valid)
						var sum *big.Int
						if valid > 0 {
							sum = new(big.Int).SetUint64(valid)
						}
						n14CheckSum(t, df, "sum", 0, sum)
						df.Close()
						if budget.Used() != 0 || observed.opens != 1 || observed.closes != 1 {
							t.Fatal("stream/query cleanup", budget.Used(), observed.opens, observed.closes)
						}
					})
				}
			}
		}
	}
}
func TestDecimalAverageWideAndFailureIsolation(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.MemoryLimitBytes = 64 << 20
	s := NewSession(opts)
	defer s.Close()
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	rec := n14DecimalRecord(38, 0, []*big.Int{max, max, nil})
	defer rec.Release()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, rec)
	if e != nil {
		t.Fatal(e)
	}
	avg := q.AggConfigured(AvgDecimal128("d", 38, 0, expr.DecimalRoundHalfEven, "mean"), CountColumn("d", "n"))
	df, e := avg.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	n20CheckAverage(t, df, "mean", 38, 0, max)
	n15CheckCount(t, df, "n", 2)
	df.Close()
	failed, e := q.AggConfigured(AvgDecimal128("d", 38, 0, expr.DecimalRoundHalfEven, "mean"), Sum("d", "sum")).Collect(context.Background())
	if e == nil {
		failed.Close()
		t.Fatal("sibling SUM overflow hidden")
	}
	if !errors.Is(e, aggregate.ErrDecimalSumOverflow) {
		t.Fatal(e)
	}
	for _, bad := range []LazyFrame{q.Agg(Avg("d", "bare")), q.GroupBy("e").AggConfigured(AvgDecimal128("d", 38, 0, expr.DecimalRoundHalfEven, "grouped")), q.AggConfigured(AvgDecimal128("d", 1, 0, expr.DecimalRoundHalfEven, "overflow"))} {
		df, e = bad.Collect(context.Background())
		if e == nil {
			df.Close()
			t.Fatal("unsupported/overflow accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	df, e = avg.Collect(ctx)
	if e == nil {
		df.Close()
		t.Fatal("cancel accepted")
	}
	if !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		df, e = avg.Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		n20CheckAverage(t, df, "mean", 38, 0, max)
		df.Close()
	}
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("retained after errors")
	}
}
func TestDecimalAverageOptionsCopyAndFilters(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.MemoryLimitBytes = 64 << 20
	s := NewSession(opts)
	defer s.Close()
	rec := n14DecimalRecord(38, 1, []*big.Int{big.NewInt(95), nil})
	defer rec.Release()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, rec)
	if e != nil {
		t.Fatal(e)
	}
	specs := []expr.AggregateSpec{AvgDecimal128("d", 2, 0, expr.DecimalRoundTowardZero, "mean")}
	avg := q.AggConfigured(specs...)
	specs[0] = AvgDecimal128("missing", 0, 0, 0, "bad")
	df, e := avg.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	n20CheckAverage(t, df, "mean", 2, 0, big.NewInt(9))
	df.Close()
	for _, filter := range []expr.Expr{expr.Bool(false), expr.IsNotNull(expr.Col("d"))} {
		df, e = q.Filter(filter).AggConfigured(AvgDecimal128("d", 3, 2, expr.DecimalRoundHalfEven, "mean")).Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		want := big.NewInt(950)
		if filter.String() == expr.Bool(false).String() {
			want = nil
		}
		n20CheckAverage(t, df, "mean", 3, 2, want)
		df.Close()
	}
	for _, bad := range []expr.DecimalAverageSpec{AvgDecimal128("d", 0, 0, expr.DecimalRoundHalfEven, "a"), AvgDecimal128("d", 39, 0, expr.DecimalRoundHalfEven, "a"), AvgDecimal128("d", 1, -1, expr.DecimalRoundHalfEven, "a"), AvgDecimal128("d", 1, 2, expr.DecimalRoundHalfEven, "a"), AvgDecimal128("d", 1, 0, 0, "a"), AvgDecimal128("d", 1, 0, 255, "a")} {
		if _, e = q.Filter(expr.Bool(false)).AggConfigured(bad).Schema(); e == nil {
			t.Fatal("invalid config accepted for empty output", bad)
		}
	}
	for _, bad := range []LazyFrame{q.AggConfigured(AvgDecimal128("missing", 3, 0, expr.DecimalRoundHalfEven, "a")), q.AggConfigured(AvgDecimal128("d", 3, 0, expr.DecimalRoundHalfEven, "dup"), Count("dup"))} {
		if _, e = bad.Schema(); e == nil {
			t.Fatal("bad binding accepted")
		}
	}
}
func TestDecimalAverageNativeParquetIPC(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.MemoryLimitBytes = 64 << 20
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.Spill = memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	r := n14DecimalRecord(38, 2, []*big.Int{big.NewInt(123), nil, big.NewInt(456)})
	defer r.Release()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
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
	avg := read.AggConfigured(AvgDecimal128("d", 6, 3, expr.DecimalRoundHalfEven, "mean"), CountColumn("d", "n"))
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	df, e := avg.Collect(memory.WithQueryBudget(context.Background(), budget))
	if e != nil {
		t.Fatal(e)
	}
	n20CheckAverage(t, df, "mean", 6, 3, big.NewInt(2895))
	df.Close()
	if budget.Used() != 0 || budget.Snapshot().SpilledBytes != 0 {
		t.Fatal("fixed AVG state budget", budget.Snapshot())
	}
	budget.Close()
	path := filepath.Join(t.TempDir(), "mean.arrow")
	if _, e = avg.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	rd, e := ipc.NewFileReader(f)
	if e != nil {
		f.Close()
		t.Fatal(e)
	}
	n := 0
	for i := 0; i < rd.NumRecords(); i++ {
		record, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		a := record.Column(0).(*array.Decimal128)
		dt := a.DataType().(*arrow.Decimal128Type)
		if dt.Precision != 6 || dt.Scale != 3 {
			t.Fatal("IPC metadata", dt)
		}
		for j := 0; j < a.Len(); j++ {
			if a.IsNull(j) || a.Value(j).BigInt().Cmp(big.NewInt(2895)) != 0 {
				t.Fatal("IPC coefficient")
			}
			n++
		}
		count := record.Column(1).(*array.Uint64)
		if count.Len() != 1 || count.IsNull(0) || count.Value(0) != 2 {
			t.Fatal("IPC count")
		}
		record.Release()
	}
	rd.Close()
	f.Close()
	if n != 1 {
		t.Fatal(n)
	}
	out := filepath.Join(t.TempDir(), "mean.parquet")
	if _, e = avg.SinkParquet(context.Background(), out, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
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
	n20CheckAverage(t, df, "mean", 6, 3, big.NewInt(2895))
	n15CheckCount(t, df, "n", 2)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("IO retained")
	}
}
func TestDecimalAverageOneByteAdmission(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.MemoryLimitBytes = 1
	s := NewSession(opts)
	defer s.Close()
	r := n14DecimalRecord(38, 0, []*big.Int{big.NewInt(1)})
	defer r.Release()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	df, e := q.AggConfigured(AvgDecimal128("d", 3, 0, expr.DecimalRoundHalfEven, "a")).Collect(context.Background())
	if e == nil {
		df.Close()
		t.Fatal("one byte admitted")
	}
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal(e)
	}
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("admission retained")
	}
}
