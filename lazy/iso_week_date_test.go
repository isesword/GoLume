package lazy

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	parquetformat "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"testing"
)

// Fixed epoch-day expectations, including astronomical years zero and minus one.
var weekDateInputs = [][3]int64{{2020, 53, 5}, {2021, 1, 1}, {2021, 53, 1}, {0, 1, 1}, {-1, 1, 1}, {1970, 1, 4}}
var weekDateDays = []int32{18628, 18631, 0, -719526, -719890, 0}

func weekDateQuery(t *testing.T, s *Session, n int) LazyFrame {
	t.Helper()
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}
	for _, name := range []string{"year", "week", "weekday"} {
		fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	}
	rb := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema(fields, nil))
	for i := 0; i < n; i++ {
		rb.Field(0).(*array.Int64Builder).Append(int64(i))
		for j, mod := range []int{7, 11, 13} {
			b := rb.Field(j + 1).(*array.Int64Builder)
			if i%mod == 0 {
				b.AppendNull()
			} else {
				b.Append(weekDateInputs[i%6][j])
			}
		}
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	var chunks []arrow.RecordBatch
	for start := 0; start < n; start += 65 {
		chunks = append(chunks, rec.NewSlice(int64(start), int64(min(start+65, n))))
	}
	q, e := s.ScanArrow(chunks...)
	for _, c := range chunks {
		c.Release()
	}
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	return q.WithColumns(TryDateFromISOWeek(Col("year"), Col("week"), Col("weekday")).Alias("date"))
}
func weekDateRows(n int) []int {
	r := make([]int, n)
	for i := range r {
		r[i] = i
	}
	return r
}
func weekDateWant(t *testing.T, row int, name string) any {
	if name == "id" {
		return int64(row)
	}
	for j, n := range []string{"year", "week", "weekday"} {
		if name == n {
			if row%[]int{7, 11, 13}[j] == 0 {
				return nil
			}
			return weekDateInputs[row%6][j]
		}
	}
	if name == "date" {
		if row%7 == 0 || row%11 == 0 || row%13 == 0 || row%6 == 2 {
			return nil
		}
		return weekDateDays[row%6]
	}
	t.Fatal("unknown field", name)
	return nil
}
func weekDateCheck(t *testing.T, df DataFrame, rows []int) {
	t.Helper()
	for _, name := range []string{"id", "year", "week", "weekday", "date"} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		typ := memory.TypeInt64
		if name == "date" {
			typ = memory.TypeDate32
			if _, e := c.Date32Chunks(); e != nil {
				t.Fatal(e)
			}
		}
		if c.Type() != typ {
			t.Fatal("logical type", name, c.Type())
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, len(rows))
		for i, r := range rows {
			want[i] = weekDateWant(t, r, name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal(name, got, want)
		}
	}
}
func weekDateArrowSchema(t *testing.T, s *arrow.Schema) {
	t.Helper()
	found := false
	for _, f := range s.Fields() {
		if f.Name == "date" {
			found = true
			if f.Type.ID() != arrow.DATE32 || !f.Nullable {
				t.Fatal("Date32 schema", f)
			}
		}
	}
	if !found {
		t.Fatal("missing date")
	}
}
func TestISOWeekDateLazyTransport(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := weekDateQuery(t, s, 65)
	rows := weekDateRows(65)
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	weekDateCheck(t, df, rows)
	selected, e := q.Filter(expr.GT(Col("id"), expr.I64(31))).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer selected.Close()
	weekDateCheck(t, selected, rows[32:])
	path := filepath.Join(t.TempDir(), "iso-week-date.arrow")
	if _, e = q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
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
	var recs []arrow.RecordBatch
	for i := 0; i < rd.NumRecords(); i++ {
		r, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		weekDateArrowSchema(t, r.Schema())
		recs = append(recs, r)
	}
	iq, e := s.ScanArrow(recs...)
	for _, r := range recs {
		r.Release()
	}
	rd.Close()
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	idf, e := iq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer idf.Close()
	weekDateCheck(t, idf, rows)
	pp := filepath.Join(t.TempDir(), "iso-week-date.parquet")
	result, e := q.SinkParquet(context.Background(), pp, ParquetSinkOptions{RowGroupRows: 17})
	if e != nil {
		t.Fatal(e)
	}
	pq, e := s.ScanParquet(result.Files[0])
	if e != nil {
		t.Fatal(e)
	}
	pdf, e := pq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer pdf.Close()
	weekDateCheck(t, pdf, rows)
	pr, e := file.OpenParquetFile(pp, false)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	ps := pr.MetaData().Schema
	for i := 0; i < ps.NumColumns(); i++ {
		col := ps.Column(i)
		if col.Name() == "date" {
			found = true
			_, ok := col.LogicalType().(schema.DateLogicalType)
			if !ok || col.PhysicalType() != parquetformat.Types.Int32 {
				t.Fatal("Parquet INT32 DATE", col.PhysicalType(), col.LogicalType())
			}
		}
	}
	if !found {
		t.Fatal("missing physical date")
	}
	ar, e := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
	if e != nil {
		pr.Close()
		t.Fatal(e)
	}
	table, e := ar.ReadTable(context.Background())
	if e != nil {
		pr.Close()
		t.Fatal(e)
	}
	weekDateArrowSchema(t, table.Schema())
	for i, field := range table.Schema().Fields() {
		var got []any
		for _, ch := range table.Column(i).Data().Chunks() {
			for j := 0; j < ch.Len(); j++ {
				if ch.IsNull(j) {
					got = append(got, nil)
					continue
				}
				switch v := ch.(type) {
				case *array.Int64:
					got = append(got, v.Value(j))
				case *array.Date32:
					got = append(got, int32(v.Value(j)))
				default:
					t.Fatalf("unexpected parquet array %T", ch)
				}
			}
		}
		want := make([]any, len(rows))
		for j, r := range rows {
			want[j] = weekDateWant(t, r, field.Name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("independent Parquet oracle", field.Name, got, want)
		}
	}
	table.Release()
	pr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cd, ce := q.Collect(ctx)
	if ce == nil {
		cd.Close()
	}
	if !errors.Is(ce, context.Canceled) {
		t.Fatal(ce)
	}
	s.Close()
	weekDateCheck(t, df, rows)
	weekDateCheck(t, idf, rows)
	weekDateCheck(t, pdf, rows)
	df.Close()
	idf.Close()
	pdf.Close()
	selected.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("owner leak")
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1
	tiny := NewSession(opts)
	tq := weekDateQuery(t, tiny, 65)
	td, te := tq.Collect(context.Background())
	if te == nil {
		td.Close()
	}
	tiny.Close()
	if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
		t.Fatal("budget", te)
	}
}
func weekDateMemory(t *testing.T, s *Session, values [][3]int64, validity [3]memory.Bitmap) LazyFrame {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = len(values)
	fields := []memory.Field{{Name: "id", Type: memory.TypeInt64}}
	ids := make([]int64, len(values))
	for i := range ids {
		ids[i] = int64(i)
	}
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	for j, name := range []string{"year", "week", "weekday"} {
		v := make([]int64, len(values))
		for i := range v {
			v[i] = values[i][j]
		}
		b.Int64s[name] = &memory.Int64Vector{Values: v, Validity: validity[j]}
		fields = append(fields, memory.Field{Name: name, Type: memory.TypeInt64, Nullable: validity[j] != nil})
	}
	src, e := inmemory.New(memory.NewSchema(fields...), b)
	if e != nil {
		t.Fatal(e)
	}
	return s.FromSource(src)
}
func TestISOWeekDateLazyNullsErrorsAndGuard(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := weekDateMemory(t, s, [][3]int64{{2021, 53, 1}, {2021, 53, 1}, {2021, 53, 1}, {2020, 53, 5}}, [3]memory.Bitmap{{14}, {13}, {11}})
	df, e := q.SelectExprs(DateFromISOWeek(Col("year"), Col("week"), Col("weekday")).Alias("date")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	c, e := df.Column("date")
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.ToAnySlice()
	if e != nil || !reflect.DeepEqual(got, []any{nil, nil, nil, int32(18628)}) {
		t.Fatal("three nullable inputs", got, e)
	}
	df.Close()
	for _, v := range [][3]int64{{2021, 53, 1}, {2020, 0, 1}, {2020, 54, 1}, {2020, 1, 0}, {2020, 1, 8}, {2020, -1, 1}} {
		q := weekDateMemory(t, s, [][3]int64{v, {2020, 53, 5}}, [3]memory.Bitmap{})
		bad, e := q.WithColumns(DateFromISOWeek(Col("year"), Col("week"), Col("weekday")).Alias("date")).Filter(expr.GT(Col("id"), expr.I64(0))).Collect(context.Background())
		if e == nil {
			bad.Close()
		}
		if !errors.Is(e, expr.ErrISOWeekDate) {
			t.Fatal("strict invalid ISO date lost", v, e)
		}
		good, e := q.WithColumns(TryDateFromISOWeek(Col("year"), Col("week"), Col("weekday")).Alias("date")).Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		c, e := good.Column("date")
		if e != nil {
			t.Fatal(e)
		}
		a, e := c.ToAnySlice()
		if e != nil || !reflect.DeepEqual(a, []any{nil, int32(18628)}) {
			t.Fatal("Try invalid date", a, e)
		}
		good.Close()
		gd, e := q.SelectExprs(expr.If(expr.GT(Col("id"), expr.I64(0)), DateFromISOWeek(Col("year"), Col("week"), Col("weekday")), DateFromISOWeek(expr.I64(1970), expr.I64(1), expr.I64(4))).Alias("date")).Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		gc, e := gd.Column("date")
		if e != nil {
			t.Fatal(e)
		}
		gv, e := gc.ToAnySlice()
		if e != nil || !reflect.DeepEqual(gv, []any{int32(0), int32(18628)}) {
			t.Fatal("inactive invalid ISO date", gv, e)
		}
		gd.Close()
	}
	q = weekDateMemory(t, s, [][3]int64{{0, 1, 1}, {-1, 1, 1}, {9223372036854775807, 1, 1}}, [3]memory.Bitmap{})
	d, e := q.SelectExprs(TryDateFromISOWeek(Col("year"), Col("week"), Col("weekday")).Alias("date")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	c, e = d.Column("date")
	if e != nil {
		t.Fatal(e)
	}
	v, e := c.ToAnySlice()
	if e != nil || !reflect.DeepEqual(v, []any{int32(-719526), int32(-719890), nil}) {
		t.Fatal("proleptic and range", v, e)
	}
	d.Close()
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("error/guard leak")
	}
}
func TestISOWeekDateLazyActualSpill(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 8 << 20
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	q := weekDateQuery(t, s, 260)
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	observer := &weekDateRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			default:
				observer.Err()
				runtime.Gosched()
			}
		}
	}()
	df, e := q.Sort(AscNullsFirst("date"), Asc("id")).Collect(observer)
	close(done)
	<-stopped
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	if budget.Snapshot().SpilledBytes <= 0 || observer.maxRuns < 2 {
		t.Fatal("expected actual runs", budget.Snapshot(), observer.maxRuns)
	}
	t.Logf("ISO week date spill bytes=%d runs=%d", budget.Snapshot().SpilledBytes, observer.maxRuns)
	rows := weekDateRows(260)
	sort.Slice(rows, func(i, j int) bool {
		a, b := weekDateWant(t, rows[i], "date"), weekDateWant(t, rows[j], "date")
		if a == nil || b == nil {
			if a == nil && b == nil {
				return rows[i] < rows[j]
			}
			return a == nil
		}
		if a.(int32) == b.(int32) {
			return rows[i] < rows[j]
		}
		return a.(int32) < b.(int32)
	})
	weekDateCheck(t, df, rows)
	budget.Close()
	s.Close()
	e = filepath.WalkDir(dir, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			t.Errorf("retained spill file %s", path)
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	weekDateCheck(t, df, rows)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("spill owner leak")
	}
}

type weekDateRunContext struct {
	context.Context
	mu      sync.Mutex
	dir     string
	maxRuns int
}

func (c *weekDateRunContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	_ = filepath.WalkDir(c.dir, func(_ string, d fs.DirEntry, e error) error {
		if e == nil && !d.IsDir() {
			info, e := d.Info()
			if e == nil && info.Size() > 0 {
				n++
			}
		}
		return nil
	})
	if n > c.maxRuns {
		c.maxRuns = n
	}
	return c.Context.Err()
}

// Endpoint tuples are independently supplied full-domain Gregorian/ISO oracles.
// This four-row transport test does not change the fixed spill acceptance query.
func TestISOWeekDateLazyFullDomainTransport(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := weekDateMemory(t, s, [][3]int64{{-5877641, 26, 2}, {5881580, 28, 5}, {-5877641, 26, 1}, {5881580, 28, 6}}, [3]memory.Bitmap{}).SelectExprs(TryDateFromISOWeek(Col("year"), Col("week"), Col("weekday")).Alias("date"))
	want := []any{int32(-2147483648), int32(2147483647), nil, nil}
	check := func(df DataFrame) {
		t.Helper()
		c, e := df.Column("date")
		if e != nil {
			t.Fatal(e)
		}
		if c.Type() != memory.TypeDate32 {
			t.Fatal("endpoint type", c.Type())
		}
		v, e := c.ToAnySlice()
		if e != nil || !reflect.DeepEqual(v, want) {
			t.Fatal("full Date32 endpoints", v, want, e)
		}
	}
	original, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer original.Close()
	check(original)
	path := filepath.Join(t.TempDir(), "endpoints.arrow")
	if _, e = q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
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
	weekDateArrowSchema(t, rd.Schema())
	var records []arrow.RecordBatch
	for i := 0; i < rd.NumRecords(); i++ {
		r, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		records = append(records, r)
	}
	iq, e := s.ScanArrow(records...)
	for _, r := range records {
		r.Release()
	}
	rd.Close()
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	ipcResult, e := iq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer ipcResult.Close()
	check(ipcResult)
	pp := filepath.Join(t.TempDir(), "endpoints.parquet")
	result, e := q.SinkParquet(context.Background(), pp, ParquetSinkOptions{RowGroupRows: 2})
	if e != nil {
		t.Fatal(e)
	}
	pq, e := s.ScanParquet(result.Files[0])
	if e != nil {
		t.Fatal(e)
	}
	nativeResult, e := pq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer nativeResult.Close()
	check(nativeResult)
	pr, e := file.OpenParquetFile(pp, false)
	if e != nil {
		t.Fatal(e)
	}
	ps := pr.MetaData().Schema
	if ps.NumColumns() != 1 {
		t.Fatal("endpoint physical columns", ps.NumColumns())
	}
	col := ps.Column(0)
	_, dateAnnotation := col.LogicalType().(schema.DateLogicalType)
	if col.PhysicalType() != parquetformat.Types.Int32 || !dateAnnotation {
		t.Fatal("endpoint INT32 DATE annotation", col.PhysicalType(), col.LogicalType())
	}
	ar, e := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
	if e != nil {
		pr.Close()
		t.Fatal(e)
	}
	table, e := ar.ReadTable(context.Background())
	if e != nil {
		pr.Close()
		t.Fatal(e)
	}
	weekDateArrowSchema(t, table.Schema())
	var arrowValues []any
	for _, ch := range table.Column(0).Data().Chunks() {
		a, ok := ch.(*array.Date32)
		if !ok {
			t.Fatalf("endpoint array %T", ch)
		}
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				arrowValues = append(arrowValues, nil)
			} else {
				arrowValues = append(arrowValues, int32(a.Value(i)))
			}
		}
	}
	if !reflect.DeepEqual(arrowValues, want) {
		t.Fatal("independent Arrow Parquet full-domain values", arrowValues, want)
	}
	table.Release()
	pr.Close()
	s.Close()
	check(original)
	check(ipcResult)
	check(nativeResult)
	original.Close()
	ipcResult.Close()
	nativeResult.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("endpoint transport leak")
	}
}
