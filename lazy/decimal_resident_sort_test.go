package lazy

import (
	"context"
	"errors"
	"fmt"
	pqfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
)

func decimalResidentWant(row int, p int32) *big.Int {
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(p)), nil), big.NewInt(1))
	if p == 1 {
		return big.NewInt(int64(row%7 - 3))
	}
	switch row % 9 {
	case 0:
		return max
	case 1:
		return new(big.Int).Neg(max)
	case 2:
		return big.NewInt(9007199254740993)
	case 3:
		return big.NewInt(9007199254740992)
	case 4:
		return new(big.Int).Lsh(big.NewInt(1), 64)
	case 5:
		return new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 64))
	default:
		return big.NewInt(int64(row%3 - 1))
	}
}
func decimalResidentRecord(t *testing.T, p, scale int32) arrow.RecordBatch {
	t.Helper()
	base := residentNestedRecord(t, 0)
	defer base.Release()
	d := array.NewDecimal128Builder(am.DefaultAllocator, &arrow.Decimal128Type{Precision: p, Scale: scale})
	defer d.Release()
	for r := 0; r < 65; r++ {
		d.Append(decimal128.FromBigInt(decimalResidentWant(r, p)))
	}
	da := d.NewDecimal128Array()
	defer da.Release()
	// Put deliberately out-of-precision bits beneath NULLs without exposing them as values.
	values := append([]decimal128.Num(nil), da.Values()...)
	for r := 0; r < 65; r++ {
		if r%7 == 0 {
			values[r] = decimal128.New(0x7fffffffffffffff, ^uint64(0))
		}
	}
	// Builder AppendValues preserves the coefficient payload for invalid entries.
	d.AppendValues(values, func() []bool {
		v := make([]bool, 65)
		for i := range v {
			v[i] = i%7 != 0
		}
		return v
	}())
	native := d.NewArray()
	defer native.Release()
	fields := append([]arrow.Field(nil), base.Schema().Fields()...)
	fields = append(fields, arrow.Field{Name: "amount", Type: native.DataType(), Nullable: true})
	cols := append([]arrow.Array(nil), base.Columns()...)
	cols = append(cols, native)
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 65)
}
func decimalResidentOrder(p int32, desc, first bool) []int {
	rows := make([]int, 65)
	for i := range rows {
		rows[i] = i
	}
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		an, bn := a%7 == 0, b%7 == 0
		if an != bn {
			return an == first
		}
		if an {
			return false
		}
		c := decimalResidentWant(a, p).Cmp(decimalResidentWant(b, p))
		if desc {
			return c > 0
		}
		return c < 0
	})
	return rows
}
func decimalResidentCheck(t *testing.T, df DataFrame, p, scale int32, order []int) {
	t.Helper()
	residentNestedCheck(t, df, order)
	c, e := df.Column("amount")
	if e != nil {
		t.Fatal(e)
	}
	chunks, e := c.Decimal128Chunks()
	if e != nil {
		t.Fatal(e)
	}
	seen := 0
	for _, ch := range chunks {
		if ch.Precision() != p || ch.Scale() != scale {
			t.Fatal("decimal metadata")
		}
		for i := 0; i < ch.Len(); i++ {
			if seen >= len(order) {
				t.Fatal("extra rows")
			}
			row := order[seen]
			v, valid, e := ch.Value(i)
			if e != nil || valid != (row%7 != 0) || valid && v.BigInt().Cmp(decimalResidentWant(row, p)) != 0 {
				t.Fatal("decimal coefficient/null", seen, v, valid, e)
			}
			seen++
		}
	}
	if seen != len(order) {
		t.Fatal("row count", seen)
	}
}
func TestDecimalResidentSortMatrix(t *testing.T) {
	for _, ps := range [][2]int32{{1, 0}, {38, 0}, {38, 38}} {
		for _, workers := range []int{1, 3} {
			for _, desc := range []bool{false, true} {
				for _, first := range []bool{false, true} {
					t.Run(fmt.Sprintf("p%d-s%d-w%d-desc%v-first%v", ps[0], ps[1], workers, desc, first), func(t *testing.T) {
						opts := engine.DefaultOptions()
						opts.Workers = workers
						opts.PreparedCacheBytes = 0
						opts.MemoryLimitBytes = 64 << 20
						s := NewSession(opts)
						defer s.Close()
						rec := decimalResidentRecord(t, ps[0], ps[1])
						q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true, NativeDecimal128: true}, rec)
						rec.Release()
						if e != nil {
							t.Fatal(e)
						}
						resultP := ps[0]
						key := Asc("amount")
						if desc {
							key = Desc("amount")
						}
						if first {
							key = AscNullsFirst("amount")
							if desc {
								key = DescNullsFirst("amount")
							}
						}
						sorted := q.Sort(key)
						budget, e := s.Engine().BeginQueryMemory()
						if e != nil {
							t.Fatal(e)
						}
						df, e := sorted.Collect(memory.WithQueryBudget(context.Background(), budget))
						if e != nil {
							t.Fatal(e)
						}
						order := decimalResidentOrder(ps[0], desc, first)
						// p1 fixture values agree with p2 only for this check's independent source rule.
						if ps[0] == 1 {
							decimalResidentCheckSmall(t, df, resultP, ps[1], order)
						} else {
							decimalResidentCheck(t, df, resultP, ps[1], order)
						}
						if budget.Snapshot().SpilledBytes != 0 {
							t.Fatal("resident spilled")
						}
						budget.Close()
						reader, e := sorted.CollectBatches(context.Background(), BatchOptions{})
						if e != nil {
							t.Fatal(e)
						}
						if !reader.Next(context.Background()) {
							t.Fatal(reader.Err())
						}
						view := reader.Batch()
						reader.Close()
						if view.Valid() {
							t.Fatal("borrow survived close")
						}
						s.Close()
						if ps[0] == 1 {
							decimalResidentCheckSmall(t, df, resultP, ps[1], order)
						} else {
							decimalResidentCheck(t, df, resultP, ps[1], order)
						}
						df.Close()
						if s.Engine().Memory().Used() != 0 {
							t.Fatal("owner leak")
						}
					})
				}
			}
		}
	}
}
func decimalResidentCheckSmall(t *testing.T, df DataFrame, p, scale int32, order []int) {
	t.Helper()
	residentNestedCheck(t, df, order)
	c, e := df.Column("amount")
	if e != nil {
		t.Fatal(e)
	}
	cs, e := c.Decimal128Chunks()
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, ch := range cs {
		if ch.Precision() != p || ch.Scale() != scale {
			t.Fatal("small metadata")
		}
		for i := 0; i < ch.Len(); i++ {
			v, ok, e := ch.Value(i)
			r := order[n]
			if e != nil || ok != (r%7 != 0) || ok && v.BigInt().Cmp(decimalResidentWant(r, 1)) != 0 {
				t.Fatal("small coefficient")
			}
			n++
		}
	}
	if n != 65 {
		t.Fatal(n)
	}
}
func TestDecimalResidentSortIO(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	rec := decimalResidentRecord(t, 38, 38)
	defer rec.Release()
	legacy, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, rec)
	if e != nil {
		t.Fatal(e)
	}
	sc, e := legacy.Schema()
	if e != nil {
		t.Fatal(e)
	}
	f, _ := sc.Field("amount")
	if f.DecimalPrecision != 0 {
		t.Fatal("default became native")
	}
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true, NativeDecimal128: true}, rec)
	if e != nil {
		t.Fatal(e)
	}
	q = q.WithColumns(expr.Add(expr.Col("amount"), expr.Decimal128(decimal128.FromI64(0), 1, 0)).Alias("amount")).Sort(Desc("amount"))
	order := decimalResidentOrder(38, true, false)
	path := filepath.Join(t.TempDir(), "decimal.arrow")
	if _, e = q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	file, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	rd, e := ipc.NewFileReader(file)
	if e != nil {
		t.Fatal(e)
	}
	var records []arrow.RecordBatch
	seen := 0
	for i := 0; i < rd.NumRecords(); i++ {
		r, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		a := r.Column(int(r.NumCols()) - 1).(*array.Decimal128)
		dt := a.DataType().(*arrow.Decimal128Type)
		if dt.Precision != 38 || dt.Scale != 38 {
			t.Fatal("IPC metadata")
		}
		for j := 0; j < a.Len(); j++ {
			row := order[seen]
			if a.IsValid(j) != (row%7 != 0) || a.IsValid(j) && a.Value(j).BigInt().Cmp(decimalResidentWant(row, 38)) != 0 {
				t.Fatal("IPC oracle")
			}
			seen++
		}
		records = append(records, r)
	}
	if seen != 65 {
		t.Fatal(seen)
	}
	re, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true, NativeDecimal128: true}, records...)
	for _, r := range records {
		r.Release()
	}
	rd.Close()
	file.Close()
	if e != nil {
		t.Fatal(e)
	}
	re = re.WithColumns(expr.Add(expr.Col("amount"), expr.Decimal128(decimal128.FromI64(0), 1, 0)).Alias("amount"))
	df, e := re.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	decimalResidentCheck(t, df, 38, 38, order)
	df.Close()
	pp := filepath.Join(t.TempDir(), "decimal.parquet")
	if _, e = q.SinkParquet(context.Background(), pp, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.ScanParquetWithOptions(pp, ParquetScanOptions{NativeNested: true}); e == nil || !strings.Contains(e.Error(), "rejects logical type Decimal") || !strings.Contains(e.Error(), "amount") {
		t.Fatal("mixed NativeNested/root Decimal boundary", e)
	}
	// The complete mixed file was written. Verify it through independent Arrow,
	// not by pretending the current NativeNested reader accepts root Decimal.
	pr, e := pqfile.OpenParquetFile(pp, false)
	if e != nil {
		t.Fatal(e)
	}
	pa, e := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
	if e != nil {
		t.Fatal(e)
	}
	table, e := pa.ReadTable(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for col, field := range table.Schema().Fields() {
		n := 0
		for _, chunk := range table.Column(col).Data().Chunks() {
			for row := 0; row < chunk.Len(); row++ {
				original := order[n]
				switch field.Name {
				case "amount":
					a := chunk.(*array.Decimal128)
					dt := a.DataType().(*arrow.Decimal128Type)
					if dt.Precision != 38 || dt.Scale != 38 || a.IsValid(row) != (original%7 != 0) || a.IsValid(row) && a.Value(row).BigInt().Cmp(decimalResidentWant(original, 38)) != 0 {
						t.Fatal("Parquet Decimal oracle")
					}
				case "seq":
					if chunk.(*array.Int64).Value(row) != int64(original) {
						t.Fatal("Parquet stable ordinal")
					}
				case "ints", "texts":
					if !reflect.DeepEqual(structNestedArrowValue(chunk, row), nestedListBRaw(original, field.Name == "texts")) {
						t.Fatal("Parquet list oracle", field.Name)
					}
				case "outer":
					if !reflect.DeepEqual(structNestedArrowValue(chunk, row), nestedListBOuter(original, 1, 8)) {
						t.Fatal("Parquet recursive oracle")
					}
				case "recordOuter":
					if !reflect.DeepEqual(structNestedArrowValue(chunk, row), listStructBOuter(original, 1, 7)) {
						t.Fatal("Parquet StructList oracle")
					}
				}
				n++
			}
		}
		if n != 65 {
			t.Fatal("Parquet rows", field.Name, n)
		}
	}
	table.Release()
	pr.Close()
	flatPath := filepath.Join(t.TempDir(), "decimal-root-only.parquet")
	if _, e = q.Select("seq", "amount").SinkParquet(context.Background(), flatPath, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	pq, e := s.ScanParquet(flatPath)
	if e != nil {
		t.Fatal(e)
	}
	pd, e := pq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	col, e := pd.Column("amount")
	if e != nil {
		t.Fatal(e)
	}
	chs, e := col.Decimal128Chunks()
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, ch := range chs {
		if ch.Precision() != 38 || ch.Scale() != 38 {
			t.Fatal("flat Parquet metadata")
		}
		for i := 0; i < ch.Len(); i++ {
			a, ok, e := ch.Value(i)
			row := order[n]
			if e != nil || ok != (row%7 != 0) || ok && a.BigInt().Cmp(decimalResidentWant(row, 38)) != 0 {
				t.Fatal("flat Parquet oracle")
			}
			n++
		}
	}
	if n != 65 {
		t.Fatal(n)
	}
	pd.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("IO leaked")
	}
}
func TestDecimalResidentSortBudgetCancel(t *testing.T) {
	for _, tiny := range []bool{false, true} {
		opts := engine.DefaultOptions()
		opts.PreparedCacheBytes = 0
		opts.MemoryLimitBytes = 64 << 20
		if tiny {
			opts.MemoryLimitBytes = 1
		}
		s := NewSession(opts)
		rec := decimalResidentRecord(t, 38, 0)
		q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true, NativeDecimal128: true}, rec)
		rec.Release()
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		if !tiny {
			cancel()
		}
		df, e := q.Sort(Asc("amount")).Collect(ctx)
		cancel()
		if e == nil {
			df.Close()
			t.Fatal("expected failure")
		}
		want := context.Canceled
		if tiny {
			want = memory.ErrBudgetExceeded
		}
		if !errors.Is(e, want) {
			t.Fatal(e)
		}
		s.Close()
		if s.Engine().Memory().Used() != 0 {
			t.Fatal("failure leak")
		}
	}
}

func TestDecimalResidentSortLimitCapability(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	rec := decimalResidentRecord(t, 38, 0)
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true, NativeDecimal128: true}, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"amount", "seq"} {
		for _, n := range []int{0, 3, 65} {
			df, e := q.Sort(Asc(key)).Limit(int64(n)).Collect(context.Background())
			if e != nil {
				t.Fatal(key, n, e)
			}
			order := decimalResidentOrder(38, false, false)
			if key == "seq" {
				for i := range order {
					order[i] = i
				}
			}
			decimalResidentCheck(t, df, 38, 0, order[:n])
			df.Close()
		}
	}
	// The hidden expression key also needs the guard when Decimal payload has
	// been projected away before sorting.
	df, e := q.Select("seq").Sort(AscExpr(expr.CastDecimal128(expr.Col("seq"), 20, 0))).Limit(3).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	c, e := df.Column("seq")
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.ToAnySlice()
	if e != nil || !reflect.DeepEqual(got, []any{int64(0), int64(1), int64(2)}) {
		t.Fatal(got, e)
	}
	df.Close()
	bad, e := q.TopK(3, Asc("amount")).Collect(context.Background())
	if e == nil {
		bad.Close()
		t.Fatal("direct TopK expanded")
	}
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("Sort Limit ownership leak")
	}
}
