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
	"gocolumn/physical"
	"gocolumn/source"
	"gocolumn/source/arrowrecord"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

type n17Key struct {
	k     int64
	valid bool
}
type n17Want struct {
	key         n17Key
	sum, other  *big.Int
	rows, valid uint64
}

func n17Record(n int) arrow.RecordBatch {
	b := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: 38}, Nullable: true}, {Name: "e", Type: &arrow.Decimal128Type{Precision: 1, Scale: 0}, Nullable: true}}, nil))
	defer b.Release()
	for i := 0; i < n; i++ {
		if i%17 == 0 {
			b.Field(0).(*array.Int64Builder).AppendNull()
		} else {
			b.Field(0).(*array.Int64Builder).Append(int64(i%67 - 33))
		}
		if i%7 == 0 {
			b.Field(1).(*array.Decimal128Builder).AppendNull()
		} else {
			b.Field(1).(*array.Decimal128Builder).Append(decimal128.FromI64(int64(i%13 - 6)))
		}
		if i%11 == 0 {
			b.Field(2).(*array.Decimal128Builder).AppendNull()
		} else {
			b.Field(2).(*array.Decimal128Builder).Append(decimal128.FromI64(int64(i%3 - 1)))
		}
	}
	return b.NewRecordBatch()
}
func n17Oracle(n int, desc, nullLast bool) []n17Want {
	m := map[n17Key]*n17Want{}
	for i := 0; i < n; i++ {
		key := n17Key{int64(i%67 - 33), true}
		if i%17 == 0 {
			key = n17Key{}
		}
		w := m[key]
		if w == nil {
			w = &n17Want{key: key}
			m[key] = w
		}
		w.rows++
		if i%7 != 0 {
			w.valid++
			if w.sum == nil {
				w.sum = new(big.Int)
			}
			w.sum.Add(w.sum, big.NewInt(int64(i%13-6)))
		}
		if i%11 != 0 {
			if w.other == nil {
				w.other = new(big.Int)
			}
			w.other.Add(w.other, big.NewInt(int64(i%3-1)))
		}
	}
	out := make([]n17Want, 0, len(m))
	for _, w := range m {
		out = append(out, *w)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].key, out[j].key
		if a.valid != b.valid {
			if nullLast {
				return a.valid
			}
			return !a.valid
		}
		if desc {
			return a.k > b.k
		}
		return a.k < b.k
	})
	return out
}
func n17Grouped(q LazyFrame) LazyFrame {
	return q.GroupBy("k").Agg(Sum("d", "sum"), Count("rows"), CountColumn("d", "valid"), Sum("e", "other"))
}
func n17Check(t *testing.T, df DataFrame, want []n17Want) {
	t.Helper()
	sc := df.Schema()
	if len(sc.Fields) != 5 || sc.Fields[0].Name != "k" || !sc.Fields[0].Nullable || sc.Fields[1].DecimalPrecision != 38 || sc.Fields[1].DecimalScale != 38 || sc.Fields[2].Nullable || sc.Fields[3].Nullable || sc.Fields[4].DecimalScale != 0 {
		t.Fatal("metadata", sc)
	}
	c, e := df.Column("k")
	if e != nil {
		t.Fatal(e)
	}
	keys, e := c.Int64Chunks()
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, ch := range keys {
		for i := 0; i < ch.Len(); i++ {
			v, valid, e := ch.Value(i)
			if n >= len(want) || e != nil || valid != want[n].key.valid || valid && v != want[n].key.k {
				t.Fatal("key", n, v, valid, e)
			}
			n++
		}
	}
	if n != len(want) {
		t.Fatal("rows", n, len(want))
	}
	for _, name := range []string{"sum", "other"} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		chs, e := c.Decimal128Chunks()
		if e != nil {
			t.Fatal(e)
		}
		n := 0
		for _, ch := range chs {
			for i := 0; i < ch.Len(); i++ {
				v, valid, e := ch.Value(i)
				w := want[n].sum
				if name == "other" {
					w = want[n].other
				}
				if e != nil || valid != (w != nil) || valid && v.BigInt().Cmp(w) != 0 {
					t.Fatal("sum", name, n, v, valid, w, e)
				}
				n++
			}
		}
		if n != len(want) {
			t.Fatal(n)
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
		n := 0
		for _, ch := range chs {
			for i := 0; i < ch.Len(); i++ {
				v, valid, e := ch.Value(i)
				w := want[n].rows
				if name == "valid" {
					w = want[n].valid
				}
				if e != nil || !valid || v != w {
					t.Fatal("count", name, n, v, w)
				}
				n++
			}
		}
		if n != len(want) {
			t.Fatal(n)
		}
	}
}
func TestDecimalGroupedLazySortMatrix(t *testing.T) {
	for _, n := range []int{0, 195} {
		for _, batch := range []int{1, 63, 64, 65} {
			for _, desc := range []bool{false, true} {
				for _, nullLast := range []bool{false, true} {
					t.Run(fmt.Sprintf("n%d-b%d-d%v-null%v", n, batch, desc, nullLast), func(t *testing.T) {
						opts := engine.DefaultOptions()
						opts.MemoryLimitBytes = 64 << 20
						opts.PreparedCacheBytes = 0
						opts.Workers = 3
						s := NewSession(opts)
						defer s.Close()
						r := n17Record(n)
						src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, r)
						r.Release()
						if e != nil {
							t.Fatal(e)
						}
						defer src.Close()
						obs := &nestedTopKObserved{Source: src, stable: true, batchRows: batch}
						key := Asc("k")
						if desc {
							key = Desc("k")
						}
						key.NullsLast = nullLast
						q := n17Grouped(s.FromSource(obs).Sort(key))
						df, e := q.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						n17Check(t, df, n17Oracle(n, desc, nullLast))
						df.Close()
						empty, e := n17Grouped(s.FromSource(obs).Sort(key).Filter(expr.Bool(false))).Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						n17Check(t, empty, nil)
						empty.Close()
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

type n17OrderedDiagnostic struct{ *n13DiagnosticSource }

func (s *n17OrderedDiagnostic) Ordering() []source.OrderingColumn {
	return s.Source.(source.OrderingProvider).Ordering()
}
func TestDecimalGroupedExternalSortBridge(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	dir := t.TempDir()
	opts.Spill = memory.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	r := n17Record(65)
	src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, r)
	r.Release()
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	observed := &nestedTopKObserved{Source: src, stable: true, batchRows: 1}
	sorted, e := physical.NewSortSourceWithOptions(observed, []physical.SortKey{{Column: "k", NullsLast: true}}, nil, physical.SortSourceOptions{Spillable: true})
	if e != nil {
		t.Fatal(e)
	}
	diag := &n17OrderedDiagnostic{&n13DiagnosticSource{Source: sorted}}
	df, e := n17Grouped(s.FromSource(diag)).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	n17Check(t, df, n17Oracle(65, false, true))
	runs, _ := strconv.Atoi(diag.diagnostics["spill_runs"])
	levels, _ := strconv.Atoi(diag.diagnostics["merge_levels"])
	if runs < 65 || levels < 2 || diag.diagnostics["strategy"] != "external-stable" {
		t.Fatal("not multipass upstream sort", diag.diagnostics)
	}
	t.Log(diag.diagnostics)
	df.Close()
	s.Close()
	recursiveCNoFiles(t, dir)
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("retained")
	}
}

func TestDecimalGroupedNativeParquetIPC(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	r := n17Record(195)
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
	r.Release()
	if e != nil {
		t.Fatal(e)
	}
	in := filepath.Join(t.TempDir(), "input.parquet")
	if _, e = q.SinkParquet(context.Background(), in, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	read, e := s.ScanParquetWithOptions(in, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	key := Desc("k")
	key.NullsLast = true
	grouped := n17Grouped(read.Sort(key))
	want := n17Oracle(195, true, true)
	df, e := grouped.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	n17Check(t, df, want)
	df.Close()
	path := filepath.Join(t.TempDir(), "grouped.arrow")
	if _, e = grouped.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	rd, e := ipc.NewFileReader(f)
	if e != nil {
		t.Fatal(e)
	}
	seen := 0
	for i := 0; i < rd.NumRecords(); i++ {
		rec, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		k := rec.Column(0).(*array.Int64)
		d := rec.Column(1).(*array.Decimal128)
		dt := d.DataType().(*arrow.Decimal128Type)
		if dt.Precision != 38 || dt.Scale != 38 {
			t.Fatal(dt)
		}
		rows := rec.Column(2).(*array.Uint64)
		valid := rec.Column(3).(*array.Uint64)
		other := rec.Column(4).(*array.Decimal128)
		for j := 0; j < int(rec.NumRows()); j++ {
			w := want[seen]
			if k.IsValid(j) != w.key.valid || w.key.valid && k.Value(j) != w.key.k || d.IsValid(j) != (w.sum != nil) || w.sum != nil && d.Value(j).BigInt().Cmp(w.sum) != 0 || rows.IsNull(j) || rows.Value(j) != w.rows || valid.IsNull(j) || valid.Value(j) != w.valid || other.IsValid(j) != (w.other != nil) || w.other != nil && other.Value(j).BigInt().Cmp(w.other) != 0 {
				t.Fatal("IPC oracle", seen)
			}
			seen++
		}
		rec.Release()
	}
	rd.Close()
	f.Close()
	if seen != len(want) {
		t.Fatal(seen)
	}
	out := filepath.Join(t.TempDir(), "grouped.parquet")
	if _, e = grouped.SinkParquet(context.Background(), out, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
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
	n17Check(t, df, want)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("IO retained")
	}
}

func TestDecimalGroupedLateOverflowAndBoundaries(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	b := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0}, Nullable: true}}, nil))
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
	input := s.FromSource(src)
	grouped := input.Sort(Asc("k")).GroupBy("k").Agg(Sum("d", "sum"), Count("rows"))
	if df, e := grouped.Collect(context.Background()); e == nil {
		df.Close()
		t.Fatal("late group overflow accepted")
	}
	dir := t.TempDir()
	for _, kind := range []string{"ipc", "parquet"} {
		path := filepath.Join(dir, "out."+kind)
		var err error
		if kind == "ipc" {
			_, err = grouped.SinkIPC(context.Background(), path, IPCSinkOptions{})
		} else {
			_, err = grouped.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 1})
		}
		if err == nil {
			t.Fatal("late sink overflow accepted")
		}
		entries, e := os.ReadDir(dir)
		if e != nil || len(entries) != 0 {
			t.Fatal("failed sink published or retained temp", entries, e)
		}
	}
	if df, e := input.GroupBy("k").Agg(Sum("d", "sum")).Collect(context.Background()); e == nil {
		df.Close()
		t.Fatal("unproved input accepted")
	}
	sorted, e := physical.NewSortSourceWithOptions(src, []physical.SortKey{{Column: "k"}}, nil, physical.SortSourceOptions{})
	if e != nil {
		t.Fatal(e)
	}
	p, e := s.Engine().Prepare(context.Background(), sorted, expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("d", "sum")}}, engine.PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	for _, materialize := range []bool{false, true} {
		r, e := p.ExecuteDecimal(context.Background(), engine.ExecuteOptions{Materialize: materialize})
		if e == nil || len(r.Rows) != 0 {
			t.Fatal("global Decimal API accepted grouped", e)
		}
	}
	if _, e = p.Execute(context.Background(), engine.ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("legacy materialized grouped")
	}
	if _, e = p.ExecuteTyped(context.Background(), engine.ExecuteOptions{Materialize: true}); e == nil {
		t.Fatal("typed materialized grouped")
	}
	p.Close()
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("late error retained")
	}
}

func TestDecimalGroupedExactCancellationAndNullGroup(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	neg := new(big.Int).Neg(new(big.Int).Set(max))
	for _, triple := range [][]*big.Int{{max, max, neg}, {max, neg, max}, {neg, max, max}} {
		opts := engine.DefaultOptions()
		opts.PreparedCacheBytes = 0
		opts.MemoryLimitBytes = 64 << 20
		s := NewSession(opts)
		b := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: 38}, Nullable: true}, {Name: "e", Type: &arrow.Decimal128Type{Precision: 1, Scale: 0}, Nullable: true}}, nil))
		values := append(append([]*big.Int(nil), triple...), nil, nil, big.NewInt(0), nil)
		for i, v := range values {
			if i >= 5 {
				b.Field(0).(*array.Int64Builder).AppendNull()
			} else {
				key := int64(0)
				if i >= 3 {
					key = 1
				}
				b.Field(0).(*array.Int64Builder).Append(key)
			}
			if v == nil {
				b.Field(1).(*array.Decimal128Builder).AppendNull()
			} else {
				b.Field(1).(*array.Decimal128Builder).Append(decimal128.FromBigInt(v))
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
		key := Asc("k")
		key.NullsLast = true
		df, e := n17Grouped(s.FromSource(obs).Sort(key)).Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		want := []n17Want{{key: n17Key{0, true}, sum: max, other: big.NewInt(3), rows: 3, valid: 3}, {key: n17Key{1, true}, sum: nil, other: big.NewInt(2), rows: 2, valid: 0}, {key: n17Key{}, sum: big.NewInt(0), other: big.NewInt(2), rows: 2, valid: 1}}
		n17Check(t, df, want)
		df.Close()
		src.Close()
		s.Close()
		if s.Engine().Memory().Used() != 0 {
			t.Fatal("retained")
		}
	}
}

func TestDecimalGrouped195DistinctCollectBudget(t *testing.T) {
	const n = 195
	base := n17Record(n)
	defer base.Release()
	kb := array.NewInt64Builder(am.DefaultAllocator)
	for i := 0; i < n; i++ {
		kb.Append(int64(i))
	}
	keys := kb.NewInt64Array()
	kb.Release()
	r := array.NewRecordBatch(base.Schema(), []arrow.Array{keys, base.Column(1), base.Column(2)}, n)
	keys.Release()
	defer r.Release()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	src, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	obs := &nestedTopKObserved{Source: src, stable: true, batchRows: 63}
	q := n17Grouped(s.FromSource(obs).Sort(Asc("k")))
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	df, e := q.Collect(memory.WithQueryBudget(context.Background(), budget))
	if e != nil {
		t.Fatal(e)
	}
	want := make([]n17Want, n)
	for i := range want {
		want[i] = n17Want{key: n17Key{int64(i), true}, rows: 1}
		if i%7 != 0 {
			want[i].valid = 1
			want[i].sum = big.NewInt(int64(i%13 - 6))
		}
		if i%11 != 0 {
			want[i].other = big.NewInt(int64(i%3 - 1))
		}
	}
	n17Check(t, df, want)
	c, e := df.Column("k")
	if e != nil {
		t.Fatal(e)
	}
	chunks, e := c.Int64Chunks()
	if e != nil {
		t.Fatal(e)
	}
	sizes := make([]int, len(chunks))
	for i, ch := range chunks {
		sizes[i] = ch.Len()
		if ch.Len() > 64 {
			t.Fatal("grouped output exceeded bounded page", ch.Len())
		}
	}
	if len(chunks) < 4 {
		t.Fatal("195 groups did not cross64group pages", sizes)
	}
	t.Logf("195 distinct groups: chunks=%v query_peak=%d retained_result=%d limit=%d", sizes, budget.Snapshot().PeakBytes, budget.Used(), budget.Limit())
	df.Close()
	if budget.Used() != 0 {
		t.Fatal("query retained", budget.Used())
	}
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("engine retained")
	}
}
