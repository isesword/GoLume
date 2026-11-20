package lazy_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/lazy"
	"gocolumn/memory"
	"gocolumn/source/arrowrecord"
	pq "gocolumn/source/parquet"
	"math/big"
	"path/filepath"
	"testing"
)

func n16PublicRecord(alloc am.Allocator, scale int32, values []*big.Int) arrow.RecordBatch {
	b := array.NewRecordBuilder(alloc, arrow.NewSchema([]arrow.Field{{Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: scale}, Nullable: true}}, nil))
	defer b.Release()
	d := b.Field(0).(*array.Decimal128Builder)
	for _, v := range values {
		if v == nil {
			d.AppendNull()
		} else {
			d.Append(decimal128.FromBigInt(v))
		}
	}
	return b.NewRecordBatch()
}
func n16PublicQuery(filter expr.Expr) expr.Query {
	return expr.Query{Filter: filter, Aggregates: []expr.Aggregate{expr.Count("rows"), expr.Sum("d", "total"), expr.CountColumn("d", "valid"), expr.Sum("d", "again")}}
}
func n16PublicCheck(t *testing.T, r engine.DecimalResult, scale int32, want *big.Int, rows, valid uint64, materialize bool) {
	t.Helper()
	if r.Summary.Rows != nil || r.Summary.GroupCount != 1 || r.Summary.SelectedRows != int(rows) || r.Summary.Checksum != 0 {
		t.Fatal("summary", r.Summary)
	}
	if !materialize {
		if len(r.Rows) != 0 {
			t.Fatal("summary materialized")
		}
		return
	}
	if len(r.Rows) != 1 || len(r.Rows[0].Aggregates) != 4 {
		t.Fatal("shape", r)
	}
	for i, a := range r.Rows[0].Aggregates {
		if a.Alias != []string{"rows", "total", "valid", "again"}[i] {
			t.Fatal("alias order", a)
		}
		v := a.Value
		if i == 1 || i == 3 {
			if a.Kind != expr.AggSum || v.Type != memory.TypeDecimal || v.Precision != 38 || v.Scale != scale || v.Valid != (want != nil) {
				t.Fatal("decimal metadata", v)
			}
			coeff, p, s, ok := v.AsDecimal128()
			if ok != (want != nil) || ok && (p != 38 || s != scale) || !ok && (coeff != (memory.Decimal128{}) || p != 0 || s != 0) {
				t.Fatal("decimal accessor metadata", ok, p, s)
			}
			if ok && coeff.BigInt().Cmp(want) != 0 {
				t.Fatal("decimal oracle", coeff, want)
			}
			if _, ok := v.AsUint64(); ok {
				t.Fatal("decimal silently narrowed")
			}
		} else {
			n, ok := v.AsUint64()
			w := rows
			if i == 2 {
				w = valid
			}
			if !ok || n != w || v.Type != memory.TypeUint64 || !v.Valid {
				t.Fatal("count", v, w)
			}
			if _, _, _, ok := v.AsDecimal128(); ok {
				t.Fatal("count silently decimal")
			}
		}
	}
}
func TestDecimalResultPublicEngineAndPrepared(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	neg := new(big.Int).Neg(new(big.Int).Set(max))
	for _, scale := range []int32{0, 38} {
		for _, vals := range [][]*big.Int{nil, {nil, nil}, {max, max, neg, nil}, {neg, max, max, nil}, {big.NewInt(9007199254740993), big.NewInt(-1)}} {
			for _, filtered := range []bool{false, true} {
				t.Run(fmt.Sprintf("s%d-n%d-filter%v", scale, len(vals), filtered), func(t *testing.T) {
					alloc := am.NewCheckedAllocator(am.DefaultAllocator)
					rec := n16PublicRecord(alloc, scale, vals)
					src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, rec)
					rec.Release()
					if e != nil {
						t.Fatal(e)
					}
					opts := engine.DefaultOptions()
					opts.PreparedCacheBytes = 0
					opts.MemoryLimitBytes = 64 << 20
					en := engine.New(opts)
					var pred expr.Expr
					if filtered {
						pred = expr.Bool(false)
					}
					q := n16PublicQuery(pred)
					var sum *big.Int
					var rows, valid uint64
					for _, v := range vals {
						if filtered {
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
					p, e := en.Prepare(context.Background(), src, q, engine.PrepareOptions{})
					if e != nil {
						t.Fatal(e)
					}
					first, e := p.ExecuteDecimal(context.Background(), engine.ExecuteOptions{Materialize: true})
					if e != nil {
						t.Fatal(e)
					}
					n16PublicCheck(t, first, scale, sum, rows, valid, true)
					second, e := en.ExecuteDecimal(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
					if e != nil {
						t.Fatal(e)
					}
					n16PublicCheck(t, second, scale, sum, rows, valid, true)
					summary, e := en.ExecuteDecimal(context.Background(), src, q, engine.ExecuteOptions{})
					if e != nil {
						t.Fatal(e)
					}
					n16PublicCheck(t, summary, scale, sum, rows, valid, false)
					for _, mat := range []bool{false, true} {
						r, e := p.ExecuteDecimal(context.Background(), engine.ExecuteOptions{Materialize: mat})
						if e != nil {
							t.Fatal(e)
						}
						n16PublicCheck(t, r, scale, sum, rows, valid, mat)
					}
					if _, e = p.Execute(context.Background(), engine.ExecuteOptions{Materialize: true}); e == nil {
						t.Fatal("legacy Result expanded")
					}
					if _, e = p.ExecuteTyped(context.Background(), engine.ExecuteOptions{Materialize: true}); e == nil {
						t.Fatal("TypedValue expanded")
					}
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					r, e := p.ExecuteDecimal(ctx, engine.ExecuteOptions{Materialize: true})
					if !errors.Is(e, context.Canceled) || len(r.Rows) != 0 {
						t.Fatal("cancel", e, r)
					}
					if en.Memory().Used() != 0 {
						t.Fatal("public result retains query leases", en.Memory().Used())
					}
					p.Close()
					src.Close()
					en.Close()
					alloc.AssertSize(t, 0)
					n16PublicCheck(t, first, scale, sum, rows, valid, true)
					n16PublicCheck(t, second, scale, sum, rows, valid, true)
					second.Rows[0].Aggregates[1].Value.Coefficient = decimal128.FromI64(123)
					n16PublicCheck(t, first, scale, sum, rows, valid, true)
				})
			}
		}
	}
}
func TestDecimalResultPublicParquetAndLazyOracle(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.MemoryLimitBytes = 64 << 20
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.Spill = memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
	s := lazy.NewSession(opts)
	rec := n16PublicRecord(am.DefaultAllocator, 38, []*big.Int{big.NewInt(9), nil, big.NewInt(-3)})
	frame, e := s.ScanArrowWithOptions(lazy.ArrowScanOptions{NativeDecimal128: true}, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "root.parquet")
	if _, e = frame.SinkParquet(context.Background(), path, lazy.ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	src, e := pq.OpenFileWithOptions(path, pq.Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	q := n16PublicQuery(nil)
	result, e := s.Engine().ExecuteDecimal(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
	if e != nil {
		t.Fatal(e)
	}
	n16PublicCheck(t, result, 38, big.NewInt(6), 3, 2, true)
	df, e := s.FromSource(src).Agg(q.Aggregates...).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for i, a := range result.Rows[0].Aggregates {
		c, e := df.Column(a.Alias)
		if e != nil {
			t.Fatal(e)
		}
		if i == 1 || i == 3 {
			ch, e := c.Decimal128Chunks()
			if e != nil || len(ch) != 1 || ch[0].Len() != 1 {
				t.Fatal("lazy shape", e)
			}
			v, ok, e := ch[0].Value(0)
			if e != nil || !ok || v != a.Value.Coefficient || ch[0].Precision() != 38 || ch[0].Scale() != 38 {
				t.Fatal("lazy Decimal mismatch")
			}
		} else {
			ch, e := c.Uint64Chunks()
			if e != nil || len(ch) != 1 || ch[0].Len() != 1 {
				t.Fatal("lazy count shape", e)
			}
			v, ok, e := ch[0].Value(0)
			if e != nil || !ok || v != a.Value.Uint64 {
				t.Fatal("lazy count mismatch")
			}
		}
	}
	df.Close()
	s.Close()
	n16PublicCheck(t, result, 38, big.NewInt(6), 3, 2, true)
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("retained")
	}
}

func TestDecimalResultPublicDefaultsAndErrors(t *testing.T) {
	rec := n16PublicRecord(am.DefaultAllocator, 0, []*big.Int{big.NewInt(1), nil})
	src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.MemoryLimitBytes = 64 << 20
	en := engine.New(opts)
	defer en.Close()
	q := expr.Query{Aggregates: []expr.Aggregate{expr.Sum("d", "")}}
	r, e := en.ExecuteDecimal(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Rows) != 1 || len(r.Rows[0].Aggregates) != 1 {
		t.Fatal(r)
	}
	a := r.Rows[0].Aggregates[0]
	v, p, s, ok := a.Value.AsDecimal128()
	if a.Alias != "agg_0" || !ok || v != decimal128.FromI64(1) || p != 38 || s != 0 {
		t.Fatal("default alias/pureSUM", a)
	}
	for _, bad := range []expr.Query{{Aggregates: []expr.Aggregate{expr.CountColumn("d", "n")}}, {GroupBy: []string{"d"}, Aggregates: q.Aggregates}, {Aggregates: []expr.Aggregate{expr.Avg("d", "a")}}} {
		r, e := en.ExecuteDecimal(context.Background(), src, bad, engine.ExecuteOptions{Materialize: true})
		if e == nil || len(r.Rows) != 0 {
			t.Fatal("unsupported materialization", r, e)
		}
	}
	if en.Memory().Used() != 0 {
		t.Fatal("failure retained")
	}
	opts.MemoryLimitBytes = 1
	tiny := engine.New(opts)
	r, e = tiny.ExecuteDecimal(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
	if !errors.Is(e, memory.ErrBudgetExceeded) || len(r.Rows) != 0 {
		t.Fatal("admission", r, e)
	}
	if tiny.Memory().Used() != 0 {
		t.Fatal("tiny retained")
	}
	tiny.Close()
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	rec = n16PublicRecord(am.DefaultAllocator, 0, []*big.Int{max, max})
	overflow, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	r, e = en.ExecuteDecimal(context.Background(), overflow, q, engine.ExecuteOptions{Materialize: true})
	overflow.Close()
	if e == nil || len(r.Rows) != 0 {
		t.Fatal("final overflow partial result", r, e)
	}
	if en.Memory().Used() != 0 {
		t.Fatal("overflow retained")
	}
}
