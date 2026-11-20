package lazy

import (
	"context"
	"errors"
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
	"strings"
	"testing"
)

func n18Check(t *testing.T, df DataFrame, want []n17Want) {
	t.Helper()
	byKey := map[n17Key]n17Want{}
	for _, w := range want {
		byKey[w.key] = w
	}
	c, e := df.Column("k")
	if e != nil {
		t.Fatal(e)
	}
	chs, e := c.Int64Chunks()
	if e != nil {
		t.Fatal(e)
	}
	ordered := []n17Want{}
	seen := map[n17Key]bool{}
	for _, ch := range chs {
		for i := 0; i < ch.Len(); i++ {
			v, valid, e := ch.Value(i)
			if e != nil {
				t.Fatal(e)
			}
			key := n17Key{v, valid}
			if !valid {
				key = n17Key{}
			}
			w, ok := byKey[key]
			if !ok || seen[key] {
				t.Fatal("unexpected/duplicate key", key)
			}
			seen[key] = true
			ordered = append(ordered, w)
		}
	}
	if len(seen) != len(want) {
		t.Fatal("missing groups", len(seen), len(want))
	}
	n17Check(t, df, ordered)
}
func n18Session(t *testing.T, policy engine.ExecutionPolicy) *Session {
	t.Helper()
	opts := engine.DefaultOptions()
	opts.ExecutionPolicy = policy
	opts.MemoryLimitBytes = 64 << 20
	opts.PreparedCacheBytes = 0
	opts.Workers = 3
	s := NewSession(opts)
	t.Cleanup(func() { s.Close() })
	return s
}
func TestDecimalResidentGroupedUnorderedMatrix(t *testing.T) {
	for _, n := range []int{0, 195} {
		for _, batch := range []int{1, 63, 64, 65} {
			for _, filtered := range []bool{false, true} {
				t.Run(fmt.Sprintf("n%d-b%d-filter%v", n, batch, filtered), func(t *testing.T) {
					s := n18Session(t, engine.ExecutionMemoryOnly)
					r := n17Record(n)
					src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, r)
					r.Release()
					if e != nil {
						t.Fatal(e)
					}
					defer src.Close()
					obs := &nestedTopKObserved{Source: src, stable: true, batchRows: batch}
					input := s.FromSource(obs)
					want := n17Oracle(n, false, false)
					if filtered {
						input = input.Filter(expr.Bool(false))
						want = nil
					}
					df, e := n17Grouped(input).Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					n18Check(t, df, want)
					df.Close()
					if obs.opens != 1 || obs.closes != 1 {
						t.Fatal("one pass", obs.opens, obs.closes)
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
func TestDecimalResidentGrouped195DistinctCollect(t *testing.T) {
	const n = 195
	base := n17Record(n)
	defer base.Release()
	kb := array.NewInt64Builder(am.DefaultAllocator)
	for i := 0; i < n; i++ {
		kb.Append(int64((i * 71) % n))
	}
	keys := kb.NewInt64Array()
	kb.Release()
	r := array.NewRecordBatch(base.Schema(), []arrow.Array{keys, base.Column(1), base.Column(2)}, n)
	keys.Release()
	defer r.Release()
	s := n18Session(t, engine.ExecutionMemoryOnly)
	src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	obs := &nestedTopKObserved{Source: src, stable: true, batchRows: 63}
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	df, e := n17Grouped(s.FromSource(obs)).Collect(memory.WithQueryBudget(context.Background(), budget))
	if e != nil {
		t.Fatal(e)
	}
	want := make([]n17Want, n)
	for i := range want {
		want[i] = n17Want{key: n17Key{int64((i * 71) % n), true}, rows: 1}
		if i%7 != 0 {
			want[i].valid = 1
			want[i].sum = big.NewInt(int64(i%13 - 6))
		}
		if i%11 != 0 {
			want[i].other = big.NewInt(int64(i%3 - 1))
		}
	}
	n18Check(t, df, want)
	c, _ := df.Column("k")
	chunks, e := c.Int64Chunks()
	if e != nil {
		t.Fatal(e)
	}
	sizes := []int{}
	for _, ch := range chunks {
		sizes = append(sizes, ch.Len())
		if ch.Len() > 64 {
			t.Fatal("page", ch.Len())
		}
	}
	if len(chunks) < 4 {
		t.Fatal("missing pages", sizes)
	}
	t.Logf("195 distinct groups: chunks=%v peak=%d retained=%d limit=%d", sizes, budget.Snapshot().PeakBytes, budget.Used(), budget.Limit())
	df.Close()
	if budget.Used() != 0 {
		t.Fatal("retained")
	}
}
func TestDecimalResidentGroupedParquetIPC(t *testing.T) {
	s := n18Session(t, engine.ExecutionMemoryOnly)
	r := n17Record(195)
	input, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
	r.Release()
	if e != nil {
		t.Fatal(e)
	}
	in := filepath.Join(t.TempDir(), "in.parquet")
	if _, e = input.SinkParquet(context.Background(), in, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	read, e := s.ScanParquetWithOptions(in, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	q := n17Grouped(read)
	want := n17Oracle(195, false, false)
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	n18Check(t, df, want)
	df.Close()
	ip := filepath.Join(t.TempDir(), "out.arrow")
	if _, e = q.SinkIPC(context.Background(), ip, IPCSinkOptions{}); e != nil {
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
	byKey := map[n17Key]n17Want{}
	for _, w := range want {
		byKey[w.key] = w
	}
	seen := map[n17Key]bool{}
	for i := 0; i < rd.NumRecords(); i++ {
		rec, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		keys := rec.Column(0).(*array.Int64)
		d := rec.Column(1).(*array.Decimal128)
		dt := d.DataType().(*arrow.Decimal128Type)
		if dt.Precision != 38 || dt.Scale != 38 {
			t.Fatal(dt)
		}
		rows := rec.Column(2).(*array.Uint64)
		valid := rec.Column(3).(*array.Uint64)
		other := rec.Column(4).(*array.Decimal128)
		for j := 0; j < int(rec.NumRows()); j++ {
			key := n17Key{}
			if keys.IsValid(j) {
				key = n17Key{keys.Value(j), true}
			}
			w, ok := byKey[key]
			if !ok || seen[key] {
				t.Fatal("IPC duplicate/missing key", key)
			}
			seen[key] = true
			if d.IsValid(j) != (w.sum != nil) || w.sum != nil && d.Value(j).BigInt().Cmp(w.sum) != 0 || rows.IsNull(j) || rows.Value(j) != w.rows || valid.IsNull(j) || valid.Value(j) != w.valid || other.IsValid(j) != (w.other != nil) || w.other != nil && other.Value(j).BigInt().Cmp(w.other) != 0 {
				t.Fatal("IPC exact oracle")
			}
		}
		rec.Release()
	}
	rd.Close()
	f.Close()
	if len(seen) != len(want) {
		t.Fatal("IPC rows")
	}
	out := filepath.Join(t.TempDir(), "out.parquet")
	if _, e = q.SinkParquet(context.Background(), out, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
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
	n18Check(t, df, want)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("IO retained")
	}
}
func TestDecimalResidentGroupedPolicyAndOverflow(t *testing.T) {
	for _, policy := range []engine.ExecutionPolicy{engine.ExecutionAuto, engine.ExecutionBounded} {
		s := n18Session(t, policy)
		r := n17Record(195)
		q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
		r.Release()
		if e != nil {
			t.Fatal(e)
		}
		if df, e := n17Grouped(q).Collect(context.Background()); e == nil {
			df.Close()
			t.Fatal("unordered policy expanded", policy)
		}
		s.Close()
		if s.Engine().Memory().Used() != 0 {
			t.Fatal("policy retained")
		}
	}
	s := n18Session(t, engine.ExecutionMemoryOnly)
	b := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0}, Nullable: true}}, nil))
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	for i, v := range []*big.Int{big.NewInt(1), max, max} {
		key := int64(1)
		if i == 0 {
			key = 0
		}
		b.Field(0).(*array.Int64Builder).Append(key)
		b.Field(1).(*array.Decimal128Builder).Append(decimal128.FromBigInt(v))
	}
	r := b.NewRecordBatch()
	b.Release()
	src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, r)
	r.Release()
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	query := expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("d", "sum"), expr.Count("n")}}
	p, e := s.Engine().Prepare(context.Background(), src, query, engine.PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for _, materialize := range []bool{false, true} {
		r, e := p.ExecuteDecimal(context.Background(), engine.ExecuteOptions{Materialize: materialize})
		if e == nil || !strings.Contains(e.Error(), "requires global") || len(r.Rows) != 0 {
			t.Fatal("grouped exact endpoint expanded", r, e)
		}
	}
	if _, e = p.Execute(context.Background(), engine.ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("legacy materialized")
	}
	if _, e = p.ExecuteTyped(context.Background(), engine.ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("typed materialized")
	}
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	ctx := memory.WithQueryBudget(context.Background(), budget)
	sumField, _ := memory.Decimal128Field("sum", true, 38, 0)
	keyField, _ := src.Schema().Field("k")
	schema := memory.NewSchema(keyField, sumField, memory.Field{Name: "n", Type: memory.TypeUint64})
	st, e := p.NewResidentAggregateStream(ctx, schema, 64)
	if e != nil {
		t.Fatal(e)
	}
	out := memory.NewBatch()
	ok, e := st.Next(ctx, out)
	if e == nil || ok {
		t.Fatal("late overflow emitted first page", ok, e)
	}
	st.Close()
	if budget.Used() != 0 {
		t.Fatal("preflight retained")
	}
}

func TestDecimalResidentGroupedExtremeKeysAndCancellation(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	neg := new(big.Int).Neg(new(big.Int).Set(max))
	lo := int64(-1 << 63)
	hi := int64(1<<63 - 1)
	for _, triple := range [][]*big.Int{{max, max, neg}, {max, neg, max}, {neg, max, max}} {
		s := n18Session(t, engine.ExecutionMemoryOnly)
		b := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: 38}, Nullable: true}, {Name: "e", Type: &arrow.Decimal128Type{Precision: 1, Scale: 0}, Nullable: true}}, nil))
		keys := []int64{lo, hi, lo, 0, hi, lo, 0}
		values := []*big.Int{triple[0], nil, triple[1], big.NewInt(0), nil, triple[2], nil}
		for i, k := range keys {
			if i == 3 || i == 6 {
				b.Field(0).(*array.Int64Builder).AppendNull()
			} else {
				b.Field(0).(*array.Int64Builder).Append(k)
			}
			if values[i] == nil {
				b.Field(1).(*array.Decimal128Builder).AppendNull()
			} else {
				b.Field(1).(*array.Decimal128Builder).Append(decimal128.FromBigInt(values[i]))
			}
			b.Field(2).(*array.Decimal128Builder).Append(decimal128.FromI64(1))
		}
		r := b.NewRecordBatch()
		b.Release()
		src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, r)
		r.Release()
		if e != nil {
			t.Fatal(e)
		}
		obs := &nestedTopKObserved{Source: src, stable: true, batchRows: 1}
		q := n17Grouped(s.FromSource(obs))
		df, e := q.Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		want := []n17Want{{key: n17Key{lo, true}, sum: max, other: big.NewInt(3), rows: 3, valid: 3}, {key: n17Key{hi, true}, sum: nil, other: big.NewInt(2), rows: 2, valid: 0}, {key: n17Key{}, sum: big.NewInt(0), other: big.NewInt(2), rows: 2, valid: 1}}
		n18Check(t, df, want)
		df.Close()
		filtered := n17Grouped(s.FromSource(obs).Filter(expr.IsNotNull(expr.Col("d"))))
		df, e = filtered.Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		want = []n17Want{{key: n17Key{lo, true}, sum: max, other: big.NewInt(3), rows: 3, valid: 3}, {key: n17Key{}, sum: big.NewInt(0), other: big.NewInt(1), rows: 1, valid: 1}}
		n18Check(t, df, want)
		df.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if df, e := q.Collect(ctx); !errors.Is(e, context.Canceled) {
			if e == nil {
				df.Close()
			}
			t.Fatal("cancel identity", e)
		}
		src.Close()
		s.Close()
		if s.Engine().Memory().Used() != 0 {
			t.Fatal("extreme retained")
		}
	}
}
