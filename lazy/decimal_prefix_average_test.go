package lazy

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/aggregate"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// n28PublicInput deliberately controls page boundaries and source ordering proof.
func n28PublicInput(t *testing.T, values []string, keys []int64, inScale int32, page int, ordered, extra, nullable bool) *inmemory.Source {
	t.Helper()
	field, e := memory.Decimal128Field("value", true, 38, inScale)
	if e != nil {
		t.Fatal(e)
	}
	fields := []memory.Field{{Name: "sequence", Type: memory.TypeInt64, Nullable: nullable}, field}
	if extra {
		fields = append(fields, memory.Field{Name: "extra", Type: memory.TypeInt64})
	}
	var batches []*memory.Batch
	for start := 0; start < len(values); start += page {
		end := start + page
		if end > len(values) {
			end = len(values)
		}
		b := memory.NewBatch()
		b.Rows = end - start
		k := &memory.Int64Vector{}
		v := &memory.Decimal128Vector{Precision: 38, Scale: inScale, Validity: make(memory.Bitmap, memory.WordsFor(end-start))}
		for i := start; i < end; i++ {
			key := int64(i + 1)
			if keys != nil {
				key = keys[i]
			}
			k.Values = append(k.Values, key)
			x := new(big.Int)
			if values[i] != "NULL" {
				var ok bool
				x, ok = x.SetString(values[i], 10)
				if !ok {
					t.Fatal(values[i])
				}
				memory.Set(v.Validity, i-start)
			}
			v.Values = append(v.Values, decimal128.FromBigInt(x))
		}
		b.Int64s["sequence"] = k
		b.Decimals["value"] = v
		if extra {
			b.Int64s["extra"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
		}
		batches = append(batches, b)
	}
	src, e := inmemory.New(memory.NewSchema(fields...), batches...)
	if e != nil {
		t.Fatal(e)
	}
	if ordered {
		src = src.WithOrdering(source.OrderingColumn{Name: "sequence"})
	}
	return src
}
func n28PublicSession(limit int64) *Session {
	o := engine.DefaultOptions()
	o.Workers = 1
	o.ExecutionPolicy = engine.ExecutionMemoryOnly
	o.MemoryLimitBytes = limit
	o.PreparedCacheBytes = 0
	return NewSession(o)
}

// Exact bigint rational oracle, independent of production accumulators/finalizers.
func n28PublicOracle(values []string, inScale, outScale int32, mode aggregate.DecimalAverageRoundingMode) []*big.Int {
	out := make([]*big.Int, len(values))
	sum := new(big.Int)
	count := int64(0)
	for i, v := range values {
		if v != "NULL" {
			x, _ := new(big.Int).SetString(v, 10)
			sum.Add(sum, x)
			count++
		}
		if count == 0 {
			continue
		}
		num := new(big.Int).Abs(sum)
		den := big.NewInt(count)
		delta := outScale - inScale
		if delta < 0 {
			den.Mul(den, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-delta)), nil))
		} else {
			num.Mul(num, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(delta)), nil))
		}
		q, r := new(big.Int), new(big.Int)
		q.QuoRem(num, den, r)
		inc := false
		if r.Sign() != 0 {
			switch mode {
			case aggregate.DecimalAverageAwayFromZero:
				inc = true
			case aggregate.DecimalAverageFloor:
				inc = sum.Sign() < 0
			case aggregate.DecimalAverageCeil:
				inc = sum.Sign() > 0
			case aggregate.DecimalAverageHalfEven, aggregate.DecimalAverageHalfAwayFromZero:
				c := new(big.Int).Lsh(r, 1).Cmp(den)
				inc = c > 0 || c == 0 && (mode == aggregate.DecimalAverageHalfAwayFromZero || q.Bit(0) == 1)
			}
		}
		if inc {
			q.Add(q, big.NewInt(1))
		}
		if sum.Sign() < 0 {
			q.Neg(q)
		}
		out[i] = q
	}
	return out
}
func n28PublicCheck(t *testing.T, get func(string) (Series, error), p, scale int32, want []*big.Int) {
	t.Helper()
	c, e := get("prefix")
	if e != nil {
		t.Fatal(e)
	}
	chunks, e := c.Decimal128Chunks()
	if e != nil {
		t.Fatal(e)
	}
	at := 0
	for _, v := range chunks {
		if v.Precision() != p || v.Scale() != scale {
			t.Fatal("prefix metadata", v.Precision(), v.Scale())
		}
		for i := 0; i < v.Len(); i++ {
			if at >= len(want) {
				t.Fatal("extra prefix rows")
			}
			x, valid, e := v.Value(i)
			if e != nil || valid != (want[at] != nil) || valid && x.BigInt().Cmp(want[at]) != 0 {
				t.Fatal("independent prefix oracle", at, x, valid, want[at], e)
			}
			at++
		}
	}
	if at != len(want) {
		t.Fatal("prefix rows", at, len(want))
	}
}
func TestN28PublicPrefixExactAndPruning(t *testing.T) {
	for _, values := range [][]string{{"NULL", "2", "3", "NULL", "-7", "1"}, {"-3", "-4", "2", "NULL"}, {"99999999999999999999999999999999999999", "99999999999999999999999999999999999999", "-99999999999999999999999999999999999999"}, {}, {"NULL", "NULL"}} {
		for mode := aggregate.DecimalAverageTowardZero; mode <= aggregate.DecimalAverageHalfAwayFromZero; mode++ {
			for _, page := range []int{1, 3} {
				t.Run(fmt.Sprintf("n%d-first%v-mode%d-page%d", len(values), values, mode, page), func(t *testing.T) {
					s := n28PublicSession(16 << 20)
					defer s.Close()
					q := s.FromSource(n28PublicInput(t, values, nil, 2, page, true, true, false)).Select("value", "sequence").WithDecimalPrefixAverage("value", "sequence", "prefix", 38, 1, mode).Select("prefix")
					schema, e := q.Schema()
					if e != nil {
						t.Fatal(e)
					}
					if len(schema.Fields) != 1 || schema.Fields[0].DecimalPrecision != 38 || schema.Fields[0].DecimalScale != 1 || !schema.Fields[0].Nullable {
						t.Fatal("public schema", schema)
					}
					plan, e := q.ExplainPhysical(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					if !strings.Contains(plan, "Prefix") || !strings.Contains(plan, "sequence") || !strings.Contains(plan, "value") {
						t.Fatal("lost input dependencies/ordering", plan)
					}
					d, e := q.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					want := n28PublicOracle(values, 2, 1, mode)
					n28PublicCheck(t, d.Column, 38, 1, want)
					s.Close()
					n28PublicCheck(t, d.Column, 38, 1, want)
					d.Close()
				})
			}
		}
	}
}
func TestN28PublicPrefixAdmission(t *testing.T) {
	for _, which := range []string{"missing-order", "extra-column", "nullable-key", "filter", "sort", "limit", "topk", "mode0", "precision0", "scale", "alias-key", "unlimited", "bounded"} {
		t.Run(which, func(t *testing.T) {
			limit := int64(16 << 20)
			if which == "unlimited" {
				limit = 0
			}
			s := n28PublicSession(limit)
			defer s.Close()
			src := n28PublicInput(t, []string{}, nil, 0, 1, which != "missing-order", which == "extra-column", which == "nullable-key")
			q := s.FromSource(src)
			if which == "filter" {
				q = q.Filter(expr.Bool(true))
			}
			if which == "limit" {
				q = q.Limit(1)
			}
			if which == "topk" {
				q = q.TopK(1, Asc("sequence"))
			}
			if which == "sort" {
				q = q.Sort(Asc("sequence"))
			}
			if which == "bounded" {
				q = q.WithExecution(ExecutionOptions{Policy: engine.ExecutionBounded})
			}
			p, sc := int32(38), int32(0)
			mode := aggregate.DecimalAverageHalfEven
			alias := "prefix"
			if which == "mode0" {
				mode = 0
			}
			if which == "precision0" {
				p = 0
			}
			if which == "scale" {
				sc = 39
			}
			if which == "alias-key" {
				alias = "sequence"
			}
			q = q.WithDecimalPrefixAverage("value", "sequence", alias, p, sc, mode)
			if _, e := q.Schema(); e == nil {
				t.Fatal("invalid Schema accepted")
			}
			if _, e := q.ExplainOptimized(); e == nil {
				t.Fatal("invalid Explain accepted")
			}
			if _, e := q.ExplainPhysical(context.Background()); e == nil {
				t.Fatal("invalid physical Explain accepted")
			}
			pquery, e := q.Prepare(context.Background())
			if e == nil {
				pquery.Close()
				t.Fatal("invalid Prepare accepted")
			}
		})
	}
}
func TestN28PublicPrefixCrossPageOrder(t *testing.T) {
	for _, keys := range [][]int64{{1, 2, 2, 4}, {1, 3, 2, 4}} {
		t.Run(fmt.Sprint(keys), func(t *testing.T) {
			s := n28PublicSession(16 << 20)
			defer s.Close()
			q := s.FromSource(n28PublicInput(t, []string{"1", "2", "3", "4"}, keys, 0, 2, true, false, false)).WithDecimalPrefixAverage("value", "sequence", "prefix", 8, 2, aggregate.DecimalAverageHalfEven).Select("prefix")
			d, e := q.Collect(context.Background())
			if e == nil {
				d.Close()
				t.Fatal("non-strict cross-page order accepted")
			}
			if !strings.Contains(strings.ToLower(e.Error()), "order") && !strings.Contains(strings.ToLower(e.Error()), "ascending") {
				t.Fatal("wrong error", e)
			}
		})
	}
}
func TestN28PublicPrefixBudgetCancellation(t *testing.T) {
	for _, which := range []string{"cancel", "tiny-budget"} {
		t.Run(which, func(t *testing.T) {
			limit := int64(16 << 20)
			if which == "tiny-budget" {
				limit = 64
			}
			s := n28PublicSession(limit)
			defer s.Close()
			q := s.FromSource(n28PublicInput(t, []string{"2", "3"}, nil, 0, 1, true, false, false)).WithDecimalPrefixAverage("value", "sequence", "prefix", 38, 0, aggregate.DecimalAverageHalfEven)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if which == "cancel" {
				cancel()
			}
			d, e := q.Collect(ctx)
			if e == nil {
				d.Close()
				t.Fatal("missing resource/cancel error")
			}
			if which == "cancel" && !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			if which == "tiny-budget" && !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatal(e)
			}
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("terminal error leaked budget")
			}
		})
	}
}

func TestN28PublicPrefixPreparedOwnership(t *testing.T) {
	s := n28PublicSession(16 << 20)
	defer s.Close()
	values := []string{"NULL", "2", "3", "NULL", "7"}
	q := s.FromSource(n28PublicInput(t, values, nil, 0, 2, true, false, false)).WithDecimalPrefixAverage("value", "sequence", "prefix", 9, 2, aggregate.DecimalAverageHalfEven)
	p, e := q.Prepare(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	first, e := p.Execute(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer first.Close()
	second, e := p.Execute(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	want := n28PublicOracle(values, 0, 2, aggregate.DecimalAverageHalfEven)
	n28PublicCheck(t, first.Column, 9, 2, want)
	n28PublicCheck(t, second.Column, 9, 2, want)
	second.Close()
	p.Close()
	s.Close()
	n28PublicCheck(t, first.Column, 9, 2, want)
	first.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("owned close budget leak", s.Engine().Memory().Used())
	}
}
func TestN28PublicPrefixUnusedStillValidates(t *testing.T) {
	s := n28PublicSession(16 << 20)
	defer s.Close()
	q := s.FromSource(n28PublicInput(t, []string{"2", "3", "7", "9"}, []int64{1, 2, 2, 4}, 0, 2, true, false, false)).WithDecimalPrefixAverage("value", "sequence", "prefix", 9, 2, aggregate.DecimalAverageHalfEven)
	for name, f := range map[string]LazyFrame{"unused": q.Select("sequence"), "count": q.Select("sequence").Agg(Count("n"))} {
		t.Run(name, func(t *testing.T) {
			d, e := f.Collect(context.Background())
			if e == nil {
				d.Close()
				t.Fatal("unused prefix bypassed strict order")
			}
			if !strings.Contains(strings.ToLower(e.Error()), "order") && !strings.Contains(strings.ToLower(e.Error()), "ascending") {
				t.Fatal("not runtime strict-order error", e)
			}
		})
	}
}

func TestN28PublicPrefixTerminalReadback(t *testing.T) {
	s := n28PublicSession(16 << 20)
	defer s.Close()
	values := []string{"NULL", "2", "3", "NULL", "-7"}
	want := n28PublicOracle(values, 0, 2, aggregate.DecimalAverageHalfEven)
	q := s.FromSource(n28PublicInput(t, values, nil, 0, 2, true, false, false)).WithDecimalPrefixAverage("value", "sequence", "prefix", 9, 2, aggregate.DecimalAverageHalfEven)
	reader, e := q.CollectBatches(context.Background(), BatchOptions{})
	if e != nil {
		t.Fatal(e)
	}
	at := 0
	var previous BatchView
	for reader.Next(context.Background()) {
		if at > 0 && previous.Valid() {
			t.Fatal("borrowed previous batch survived advance")
		}
		v := reader.Batch()
		n := int(v.Len())
		n28PublicCheck(t, v.Column, 9, 2, want[at:at+n])
		previous = v
		at += n
	}
	if reader.Err() != nil {
		t.Fatal(reader.Err())
	}
	reader.Close()
	if previous.Valid() {
		t.Fatal("borrow survived reader close")
	}
	if at != len(want) {
		t.Fatal("batch row count")
	}
	check := func(rec arrow.RecordBatch, at int) int {
		ix := rec.Schema().FieldIndices("prefix")
		if len(ix) != 1 {
			t.Fatal("missing output")
		}
		a := rec.Column(ix[0]).(*array.Decimal128)
		dt := a.DataType().(*arrow.Decimal128Type)
		if dt.Precision != 9 || dt.Scale != 2 || !rec.Schema().Field(ix[0]).Nullable {
			t.Fatal("Arrow output metadata")
		}
		for i := 0; i < a.Len(); i++ {
			if at >= len(want) || a.IsValid(i) != (want[at] != nil) || a.IsValid(i) && a.Value(i).BigInt().Cmp(want[at]) != 0 {
				t.Fatal("independent Arrow prefix oracle", at)
			}
			at++
		}
		return at
	}
	for _, kind := range []string{"ipc", "parquet"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "prefix."+kind)
			at := 0
			if kind == "ipc" {
				if _, e := q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
					t.Fatal(e)
				}
				f, e := os.Open(path)
				if e != nil {
					t.Fatal(e)
				}
				r, e := ipc.NewFileReader(f)
				if e != nil {
					t.Fatal(e)
				}
				for i := 0; i < r.NumRecords(); i++ {
					rec, e := r.RecordBatchAt(i)
					if e != nil {
						t.Fatal(e)
					}
					at = check(rec, at)
					rec.Release()
				}
				r.Close()
				f.Close()
			} else {
				if _, e := q.SinkParquet(context.Background(), path, ParquetSinkOptions{}); e != nil {
					t.Fatal(e)
				}
				pr, e := file.OpenParquetFile(path, false)
				if e != nil {
					t.Fatal(e)
				}
				ar, e := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
				if e != nil {
					t.Fatal(e)
				}
				table, e := ar.ReadTable(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				tr := array.NewTableReader(table, 2)
				for tr.Next() {
					at = check(tr.RecordBatch(), at)
				}
				if tr.Err() != nil {
					t.Fatal(tr.Err())
				}
				tr.Release()
				table.Release()
				pr.Close()
			}
			if at != len(want) {
				t.Fatal("sink row count", at)
			}
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("sink budget leak")
			}
		})
	}
}

func TestN28PublicPrefixCountValid(t *testing.T) {
	s := n28PublicSession(16 << 20)
	defer s.Close()
	q := s.FromSource(n28PublicInput(t, []string{"NULL", "2", "3", "7"}, nil, 0, 2, true, false, false)).WithDecimalPrefixAverage("value", "sequence", "prefix", 9, 2, aggregate.DecimalAverageHalfEven).Select("sequence").Agg(Count("n"))
	d, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	c, e := d.Column("n")
	if e != nil {
		t.Fatal(e)
	}
	v, e := c.ToAnySlice()
	if e != nil || len(v) != 1 || v[0] != uint64(4) || c.Type() != memory.TypeUint64 {
		t.Fatal("valid prefix count", v, e)
	}
}
