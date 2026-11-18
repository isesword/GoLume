package lazy

import (
	"context"
	"encoding/base64"
	"errors"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
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
	"strings"
	"sync"
	"testing"
	"time"
)

func composeCivilZone(t *testing.T, name string) expr.TimeZone {
	t.Helper()
	encoded, e := os.ReadFile(filepath.Join("..", "expr", "testdata", "timezone", name+".tzif.b64"))
	if e != nil {
		t.Fatal(e)
	}
	data, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	if e != nil {
		t.Fatal(e)
	}
	z, e := expr.TimeZoneFromTZData(name, data)
	if e != nil {
		t.Fatal(e)
	}
	clear(data)
	return z
}
func composeCivilNS(t *testing.T, text string) int64 {
	t.Helper()
	v, e := time.Parse(time.RFC3339, text)
	if e != nil {
		t.Fatal(e)
	}
	return v.UnixNano()
}

// Hard-coded Date32, time-of-day nanoseconds and independent UTC expectations.
var composeCivilDays = []int32{19723, 19792, 20030, -1, 19723, 19723}
var composeCivilClocks = []int64{123456789, 9000000000000, 5400000000001, 86399999999999, -1, 86400000000000}
var composeCivilUTC = []string{"2024-01-01T05:00:00.123456789Z", "", "2024-11-03T05:30:00.000000001Z", "1970-01-01T04:59:59.999999999Z", "", ""}

func composeCivilResolver(t *testing.T, fold expr.CivilFoldPolicy) expr.CivilTimeResolver {
	t.Helper()
	r, e := expr.NewCivilTimeResolver(context.Background(), composeCivilZone(t, "new_york"), expr.CivilResolveOptions{Fold: fold, Gap: expr.GapReject})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func composeCivilQuery(t *testing.T, s *Session, n int) LazyFrame {
	t.Helper()
	rb := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "date", Type: arrow.FixedWidthTypes.Date32, Nullable: true}, {Name: "clock", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true}}, nil))
	for i := 0; i < n; i++ {
		rb.Field(0).(*array.Int64Builder).Append(int64(i))
		d := rb.Field(1).(*array.Date32Builder)
		c := rb.Field(2).(*array.Time64Builder)
		if i%7 == 0 {
			d.AppendNull()
		} else {
			d.Append(arrow.Date32(composeCivilDays[i%6]))
		}
		if i%11 == 0 {
			c.AppendNull()
		} else {
			c.Append(arrow.Time64(composeCivilClocks[i%6]))
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
	return q.WithColumns(TryComposeCivilTimestamp(Col("date"), Col("clock"), composeCivilResolver(t, expr.FoldEarlier)).Alias("ts"))
}
func composeCivilRows(n int) []int {
	r := make([]int, n)
	for i := range r {
		r[i] = i
	}
	return r
}
func composeCivilWant(t *testing.T, row int, name string) any {
	switch name {
	case "id":
		return int64(row)
	case "date":
		if row%7 == 0 {
			return nil
		}
		return composeCivilDays[row%6]
	case "clock":
		if row%11 == 0 {
			return nil
		}
		return composeCivilClocks[row%6]
	case "ts":
		if row%7 == 0 || row%11 == 0 || composeCivilUTC[row%6] == "" {
			return nil
		}
		return composeCivilNS(t, composeCivilUTC[row%6])
	}
	t.Fatal("unknown column", name)
	return nil
}
func composeCivilCheckColumns(t *testing.T, df DataFrame, rows []int, names []string) {
	t.Helper()
	types := map[string]memory.DataType{"id": memory.TypeInt64, "date": memory.TypeDate32, "clock": memory.TypeTime64, "ts": memory.TypeTimestamp}
	for _, name := range names {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		if c.Type() != types[name] {
			t.Fatal("logical type", name, c.Type())
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, len(rows))
		for i, row := range rows {
			want[i] = composeCivilWant(t, row, name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s got %v want %v", name, got, want)
		}
	}
}
func composeCivilCheck(t *testing.T, df DataFrame, rows []int) {
	composeCivilCheckColumns(t, df, rows, []string{"id", "date", "clock", "ts"})
}
func TestComposeCivilLazyTransport(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := composeCivilQuery(t, s, 65)
	rows := composeCivilRows(65)
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	composeCivilCheck(t, df, rows)
	selected, e := q.Filter(expr.GT(Col("id"), expr.I64(31))).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer selected.Close()
	composeCivilCheck(t, selected, rows[32:])
	path := filepath.Join(t.TempDir(), "compose.arrow")
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
		composeCivilArrowSchema(t, r.Schema())
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
	composeCivilCheck(t, idf, rows)
	pp := filepath.Join(t.TempDir(), "compose.parquet")
	result, e := q.SinkParquet(context.Background(), pp, ParquetSinkOptions{RowGroupRows: 17})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.ScanParquet(result.Files[0]); e == nil || !strings.Contains(e.Error(), "not supported by native scan") {
		t.Fatalf("native TIME rejection boundary: %v", e)
	}
	pr, e := file.OpenParquetFile(pp, false)
	if e != nil {
		t.Fatal(e)
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
	composeCivilArrowSchema(t, table.Schema())
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
				case *array.Time64:
					got = append(got, int64(v.Value(j)))
				case *array.Timestamp:
					got = append(got, int64(v.Value(j)))
				default:
					t.Fatalf("unexpected Parquet array %T", ch)
				}
			}
		}
		want := make([]any, len(rows))
		for j, row := range rows {
			want[j] = composeCivilWant(t, row, field.Name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("independent Parquet oracle", field.Name, got, want)
		}
	}
	tr := array.NewTableReader(table, 17)
	var pqRecords []arrow.RecordBatch
	for tr.Next() {
		r := tr.RecordBatch()
		r.Retain()
		pqRecords = append(pqRecords, r)
	}
	tr.Release()
	pq, e := s.ScanArrow(pqRecords...)
	for _, r := range pqRecords {
		r.Release()
	}
	table.Release()
	pr.Close()
	if e != nil {
		t.Fatal(e)
	}
	pdf, e := pq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer pdf.Close()
	composeCivilCheck(t, pdf, rows)
	// Removing the unsupported TIME input permits the existing native Timestamp reader.
	projected := q.SelectExprs(Col("id").Alias("id"), Col("date").Alias("date"), Col("ts").Alias("ts"))
	np := filepath.Join(t.TempDir(), "timestamp-only.parquet")
	nr, e := projected.SinkParquet(context.Background(), np, ParquetSinkOptions{RowGroupRows: 17})
	if e != nil {
		t.Fatal(e)
	}
	nq, e := s.ScanParquet(nr.Files[0])
	if e != nil {
		t.Fatal(e)
	}
	ndf, e := nq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer ndf.Close()
	composeCivilCheckColumns(t, ndf, rows, []string{"id", "date", "ts"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cd, ce := q.Collect(ctx)
	if ce == nil {
		cd.Close()
	}
	if !errors.Is(ce, context.Canceled) {
		t.Fatal("cancel", ce)
	}
	s.Close()
	composeCivilCheck(t, df, rows)
	composeCivilCheck(t, idf, rows)
	composeCivilCheck(t, pdf, rows)
	composeCivilCheckColumns(t, ndf, rows, []string{"id", "date", "ts"})
	df.Close()
	idf.Close()
	pdf.Close()
	ndf.Close()
	selected.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("owned leak")
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1
	tiny := NewSession(opts)
	tq := composeCivilQuery(t, tiny, 65)
	td, te := tq.Collect(context.Background())
	if te == nil {
		td.Close()
	}
	tiny.Close()
	if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
		t.Fatal("budget", te)
	}
}
func composeCivilArrowSchema(t *testing.T, s *arrow.Schema) {
	t.Helper()
	seen := map[string]bool{}
	for _, f := range s.Fields() {
		seen[f.Name] = true
		switch f.Name {
		case "date":
			if f.Type.ID() != arrow.DATE32 || !f.Nullable {
				t.Fatal("Date32 metadata", f)
			}
		case "clock":
			ty, ok := f.Type.(*arrow.Time64Type)
			if !ok || ty.Unit != arrow.Nanosecond || !f.Nullable {
				t.Fatal("Time64 ns metadata", f)
			}
		case "ts":
			ty, ok := f.Type.(*arrow.TimestampType)
			if !ok || ty.Unit != arrow.Nanosecond || ty.TimeZone != "UTC" || !f.Nullable {
				t.Fatal("Timestamp UTC ns metadata", f)
			}
		}
	}
	for _, n := range []string{"id", "date", "clock", "ts"} {
		if !seen[n] {
			t.Fatal("missing field", n)
		}
	}
}
func composeCivilMemory(t *testing.T, s *Session, days []int32, clock []int64, dv, cv memory.Bitmap) LazyFrame {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = len(days)
	b.Int32s["date"] = &memory.Int32Vector{Values: days, Validity: dv}
	b.Int64s["clock"] = &memory.Int64Vector{Values: clock, Validity: cv}
	ids := make([]int64, len(days))
	for i := range ids {
		ids[i] = int64(i)
	}
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "date", Type: memory.TypeDate32, Nullable: dv != nil}, memory.Field{Name: "clock", Type: memory.TypeTime64, Nullable: cv != nil}), b)
	if e != nil {
		t.Fatal(e)
	}
	return s.FromSource(src)
}
func TestComposeCivilLazyNullsAndErrors(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	r := composeCivilResolver(t, expr.FoldEarlier)
	// Invalid clock is hidden by date NULL; extreme date is hidden by clock NULL.
	q := composeCivilMemory(t, s, []int32{19723, 2147483647, 0, 19723}, []int64{-1, 0, 0, 1}, memory.Bitmap{10}, memory.Bitmap{9})
	df, e := q.SelectExprs(ComposeCivilTimestamp(Col("date"), Col("clock"), r).Alias("ts")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	c, e := df.Column("ts")
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.ToAnySlice()
	want := []any{nil, nil, nil, composeCivilNS(t, "2024-01-01T05:00:00.000000001Z")}
	if e != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("double nullable", got, e)
	}
	df.Close()
	for _, tc := range []struct {
		name  string
		day   int32
		clock int64
		err   error
	}{{"negative clock", 19723, -1, expr.ErrCivilClock}, {"24h", 19723, 86400000000000, expr.ErrCivilClock}, {"gap", 19792, 9000000000000, expr.ErrCivilGap}, {"date range", 2147483647, 0, expr.ErrCivilRange}} {
		t.Run(tc.name, func(t *testing.T) {
			q := composeCivilMemory(t, s, []int32{tc.day, 19723}, []int64{tc.clock, 0}, nil, nil)
			bad, e := q.WithColumns(ComposeCivilTimestamp(Col("date"), Col("clock"), r).Alias("ts")).Filter(expr.GT(Col("id"), expr.I64(0))).Collect(context.Background())
			if e == nil {
				bad.Close()
			}
			if !errors.Is(e, tc.err) {
				t.Fatal("strict error lost before filter", e, tc.err)
			}
			good, e := q.WithColumns(TryComposeCivilTimestamp(Col("date"), Col("clock"), r).Alias("ts")).Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer good.Close()
			c, e := good.Column("ts")
			if e != nil {
				t.Fatal(e)
			}
			v, e := c.ToAnySlice()
			if e != nil || !reflect.DeepEqual(v, []any{nil, composeCivilNS(t, "2024-01-01T05:00:00Z")}) {
				t.Fatal("Try row error", v, e)
			}
		})
	}
	for _, policy := range []expr.CivilFoldPolicy{expr.FoldReject, expr.FoldEarlier, expr.FoldLater} {
		r := composeCivilResolver(t, policy)
		q := composeCivilMemory(t, s, []int32{20030}, []int64{5400000000001}, nil, nil)
		fd, e := q.SelectExprs(ComposeCivilTimestamp(Col("date"), Col("clock"), r).Alias("ts")).Collect(context.Background())
		if policy == expr.FoldReject {
			if e == nil {
				fd.Close()
			}
			if !errors.Is(e, expr.ErrCivilFold) {
				t.Fatal("fold reject", e)
			}
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		c, e := fd.Column("ts")
		if e != nil {
			t.Fatal(e)
		}
		v, e := c.ToAnySlice()
		text := "2024-11-03T05:30:00.000000001Z"
		if policy == expr.FoldLater {
			text = "2024-11-03T06:30:00.000000001Z"
		}
		if e != nil || !reflect.DeepEqual(v, []any{composeCivilNS(t, text)}) {
			t.Fatal("fold nanoseconds", v, e)
		}
		fd.Close()
	}
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("error paths leak")
	}
}
func TestComposeCivilLazyActualSpill(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 8 << 20
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	q := composeCivilQuery(t, s, 260)
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	observer := &composeCivilRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
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
	df, e := q.Sort(AscNullsFirst("ts"), Asc("id")).Collect(observer)
	close(done)
	<-stopped
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	if budget.Snapshot().SpilledBytes <= 0 || observer.maxRuns < 2 {
		t.Fatal("expected actual runs", budget.Snapshot(), observer.maxRuns)
	}
	t.Logf("civil compose spill bytes=%d runs=%d", budget.Snapshot().SpilledBytes, observer.maxRuns)
	rows := composeCivilRows(260)
	sort.Slice(rows, func(i, j int) bool {
		a, b := composeCivilWant(t, rows[i], "ts"), composeCivilWant(t, rows[j], "ts")
		if a == nil || b == nil {
			if a == nil && b == nil {
				return rows[i] < rows[j]
			}
			return a == nil
		}
		if a.(int64) == b.(int64) {
			return rows[i] < rows[j]
		}
		return a.(int64) < b.(int64)
	})
	composeCivilCheck(t, df, rows)
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
	composeCivilCheck(t, df, rows)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("spill owner leak")
	}
}

type composeCivilRunContext struct {
	context.Context
	mu      sync.Mutex
	dir     string
	maxRuns int
}

func (c *composeCivilRunContext) Err() error {
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
