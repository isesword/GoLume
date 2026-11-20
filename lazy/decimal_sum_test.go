package lazy

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/arrowrecord"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

func n14DecimalRecord(p, s int32, values []*big.Int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: &arrow.Decimal128Type{Precision: p, Scale: s}, Nullable: true}, {Name: "e", Type: &arrow.Decimal128Type{Precision: 1, Scale: 0}, Nullable: true}}, nil)
	rb := array.NewRecordBuilder(am.DefaultAllocator, schema)
	defer rb.Release()
	for _, v := range values {
		if v == nil {
			rb.Field(0).(*array.Decimal128Builder).AppendNull()
			rb.Field(1).(*array.Decimal128Builder).AppendNull()
		} else {
			rb.Field(0).(*array.Decimal128Builder).Append(decimal128.FromBigInt(v))
			rb.Field(1).(*array.Decimal128Builder).Append(decimal128.FromI64(1))
		}
	}
	return rb.NewRecordBatch()
}
func n14CheckSum(t *testing.T, df DataFrame, name string, scale int32, want *big.Int) {
	t.Helper()
	f, ok := df.Schema().Field(name)
	if !ok || f.Type.String() != "decimal" || f.DecimalPrecision != 38 || f.DecimalScale != scale || !f.Nullable {
		t.Fatal("sum schema", f)
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
		if ch.Precision() != 38 || ch.Scale() != scale {
			t.Fatal("chunk metadata")
		}
		for i := 0; i < ch.Len(); i++ {
			v, valid, e := ch.Value(i)
			if e != nil || valid != (want != nil) || valid && v.BigInt().Cmp(want) != 0 {
				t.Fatal("sum oracle", name, v, valid, e, want)
			}
			n++
		}
	}
	if n != 1 {
		t.Fatal("global rows", n)
	}
}
func TestDecimalSumExactGlobal(t *testing.T) {
	for _, ps := range [][2]int32{{1, 0}, {38, 0}, {38, 38}} {
		max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(ps[0])), nil), big.NewInt(1))
		neg := new(big.Int).Neg(new(big.Int).Set(max))
		for _, values := range [][]*big.Int{{}, {nil, nil}, {big.NewInt(1), nil, big.NewInt(-1)}, {max, max, neg}, {max, neg, max}, {neg, max, max}} {
			for _, rows := range []int{1, 2, 64} {
				for _, workers := range []int{1, 3} {
					t.Run(fmt.Sprintf("p%d-s%d-n%d-b%d-w%d", ps[0], ps[1], len(values), rows, workers), func(t *testing.T) {
						opts := engine.DefaultOptions()
						opts.Workers = workers
						opts.PreparedCacheBytes = 0
						opts.MemoryLimitBytes = 64 << 20
						s := NewSession(opts)
						defer s.Close()
						rec := n14DecimalRecord(ps[0], ps[1], values)
						defer rec.Release()
						src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, rec)
						if e != nil {
							t.Fatal(e)
						}
						defer src.Close()
						obs := &nestedTopKObserved{Source: src, stable: true, batchRows: rows}
						q := s.FromSource(obs).Agg(Sum("d", "a"), Sum("d", "alias"), Sum("e", "other"))
						sc, e := q.Schema()
						if e != nil || len(sc.Fields) != 3 {
							t.Fatal(sc, e)
						}
						df, e := q.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						if obs.opens != 1 || obs.closes != 1 {
							t.Fatal("SUM must consume one stream", obs.opens, obs.closes)
						}
						var want *big.Int
						count := int64(0)
						for _, v := range values {
							if v != nil {
								if want == nil {
									want = new(big.Int)
								}
								want.Add(want, v)
								count++
							}
						}
						n14CheckSum(t, df, "a", ps[1], want)
						n14CheckSum(t, df, "alias", ps[1], want)
						var other *big.Int
						if count > 0 {
							other = big.NewInt(count)
						}
						n14CheckSum(t, df, "other", 0, other)
						df.Close()
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
func TestDecimalSumOverflowAndBoundaries(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	for _, negative := range []bool{false, true} {
		v := new(big.Int).Set(max)
		if negative {
			v.Neg(v)
		}
		s := NewSession(engine.DefaultOptions())
		r := n14DecimalRecord(38, 0, []*big.Int{v, v})
		q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
		r.Release()
		if e != nil {
			t.Fatal(e)
		}
		df, e := q.Agg(Sum("d", "sum")).Collect(context.Background())
		if e == nil {
			df.Close()
			t.Fatal("final precision overflow accepted")
		}
		for _, bad := range []LazyFrame{q.Agg(Avg("d", "avg")), q.Agg(Min("d", "min")), q.GroupBy("e").Agg(Sum("d", "sum"))} {
			df, e = bad.Collect(context.Background())
			if e == nil {
				df.Close()
				t.Fatal("consumer expanded")
			}
		}
		s.Close()
		if s.Engine().Memory().Used() != 0 {
			t.Fatal("error leak")
		}
	}
}
func TestDecimalSumNativeParquetIO(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.Spill = memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	r := n14DecimalRecord(38, 38, []*big.Int{big.NewInt(9), nil, big.NewInt(-3)})
	defer r.Release()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "in.parquet")
	if _, e = q.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	read, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	sum := read.WithColumns(expr.Add(expr.Col("d"), expr.Decimal128(decimal128.FromI64(0), 1, 0)).Alias("d")).Agg(Sum("d", "total"))
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	df, e := sum.Collect(memory.WithQueryBudget(context.Background(), budget))
	if e != nil {
		t.Fatal(e)
	}
	n14CheckSum(t, df, "total", 38, big.NewInt(6))
	df.Close()
	if budget.Snapshot().SpilledBytes != 0 {
		t.Fatal("global state unexpectedly spilled", budget.Snapshot())
	}
	budget.Close()
	ip := filepath.Join(t.TempDir(), "sum.arrow")
	if _, e = sum.SinkIPC(context.Background(), ip, IPCSinkOptions{}); e != nil {
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
		rec, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		a := rec.Column(0).(*array.Decimal128)
		dt := a.DataType().(*arrow.Decimal128Type)
		if dt.Precision != 38 || dt.Scale != 38 {
			t.Fatal("IPC schema")
		}
		for j := 0; j < a.Len(); j++ {
			if !a.IsValid(j) || a.Value(j) != decimal128.FromI64(6) {
				t.Fatal("IPC raw coeff")
			}
			n++
		}
		rec.Release()
	}
	rd.Close()
	f.Close()
	if n != 1 {
		t.Fatal(n)
	}
	out := filepath.Join(t.TempDir(), "sum.parquet")
	if _, e = sum.SinkParquet(context.Background(), out, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
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
	n14CheckSum(t, df, "total", 38, big.NewInt(6))
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("IO retained")
	}
}

func TestDecimalSumLegacyMaterializationRejected(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	r := n14DecimalRecord(38, 0, []*big.Int{big.NewInt(1)})
	defer r.Release()
	src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	prepared, e := s.Engine().Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("d", "total")}}, engine.PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer prepared.Close()
	if _, e = prepared.Execute(context.Background(), engine.ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("legacy uint64 Result accepted Decimal")
	}
	if _, e = prepared.ExecuteTyped(context.Background(), engine.ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("legacy TypedValue accepted Decimal")
	}
	// Rejection must not poison the prepared/global streaming path or leak state.
	df, e := s.FromSource(src).Agg(Sum("d", "total")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	n14CheckSum(t, df, "total", 0, big.NewInt(1))
	df.Close()
	prepared.Close()
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("prepared rejection leak")
	}
}
