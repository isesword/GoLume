package lazy

import (
	"context"
	"encoding/base64"
	"errors"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
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

// Expectations are fixed independently of time.ISOWeek and the extraction implementation.
var isoCalendarInstants = []string{"2021-01-04T04:59:00Z", "2021-01-04T05:00:00Z", "2019-12-30T04:59:00Z", "2019-12-30T05:00:00Z", "2024-03-10T07:00:00Z", "2024-11-03T06:00:00Z"}
var isoCalendarValues = [][4]int64{{2020, 53, 7, -18000}, {2021, 1, 1, -18000}, {2019, 52, 7, -18000}, {2020, 1, 1, -18000}, {2024, 10, 7, -14400}, {2024, 44, 7, -18000}}
var isoCalendarNames = []string{"year", "week", "weekday", "offset"}

func isoCalendarZone(t *testing.T, name string) expr.TimeZone {
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
func isoCalendarNS(t *testing.T, text string) int64 {
	t.Helper()
	v, e := time.Parse(time.RFC3339, text)
	if e != nil {
		t.Fatal(e)
	}
	return v.UnixNano()
}
func isoCalendarQuery(t *testing.T, s *Session, n int) LazyFrame {
	t.Helper()
	rb := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true}}, nil))
	for i := 0; i < n; i++ {
		rb.Field(0).(*array.Int64Builder).Append(int64(i))
		b := rb.Field(1).(*array.TimestampBuilder)
		if i%7 == 0 {
			b.AppendNull()
		} else {
			b.Append(arrow.Timestamp(isoCalendarNS(t, isoCalendarInstants[i%6])))
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
	z := isoCalendarZone(t, "new_york")
	return q.WithColumns(ISOWeekYearInZone(Col("ts"), z).Alias("year"), ISOWeekInZone(Col("ts"), z).Alias("week"), ISOWeekdayInZone(Col("ts"), z).Alias("weekday"), UTCOffsetInZone(Col("ts"), z).Alias("offset"))
}
func isoCalendarRows(n int) []int {
	r := make([]int, n)
	for i := range r {
		r[i] = i
	}
	return r
}
func isoCalendarWant(t *testing.T, row int, name string) any {
	if name == "id" {
		return int64(row)
	}
	if row%7 == 0 {
		return nil
	}
	if name == "ts" {
		return isoCalendarNS(t, isoCalendarInstants[row%6])
	}
	for i, n := range isoCalendarNames {
		if name == n {
			return isoCalendarValues[row%6][i]
		}
	}
	t.Fatal("unknown column", name)
	return nil
}
func isoCalendarCheck(t *testing.T, df DataFrame, rows []int) {
	t.Helper()
	for _, name := range append([]string{"id", "ts"}, isoCalendarNames...) {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		expectedType := memory.TypeInt64
		if name == "ts" {
			expectedType = memory.TypeTimestamp
		}
		if c.Type() != expectedType {
			t.Fatal("logical type", name, c.Type())
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, len(rows))
		for i, row := range rows {
			want[i] = isoCalendarWant(t, row, name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s got %v want %v", name, got, want)
		}
	}
}
func TestISOCalendarLazyTransport(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := isoCalendarQuery(t, s, 65)
	rows := isoCalendarRows(65)
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	isoCalendarCheck(t, df, rows)
	selected, e := q.Filter(expr.GT(Col("id"), expr.I64(31))).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer selected.Close()
	isoCalendarCheck(t, selected, rows[32:])
	g, e := q.GroupBy("year", "week").Agg(Count("count"), Sum("id", "sum")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer g.Close()
	wantGroups := map[[2]any][2]int64{}
	for _, row := range rows {
		k := [2]any{isoCalendarWant(t, row, "year"), isoCalendarWant(t, row, "week")}
		v := wantGroups[k]
		v[0]++
		v[1] += int64(row)
		wantGroups[k] = v
	}
	cols := map[string][]any{}
	for _, name := range []string{"year", "week", "count", "sum"} {
		c, e := g.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		cols[name], e = c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
	}
	if len(cols["year"]) != len(wantGroups) {
		t.Fatal("ISO pair group count")
	}
	seen := map[[2]any]bool{}
	for i, y := range cols["year"] {
		k := [2]any{y, cols["week"][i]}
		v, ok := wantGroups[k]
		if !ok || seen[k] || v != [2]int64{int64(cols["count"][i].(uint64)), cols["sum"][i].(int64)} {
			t.Fatal("ISO pair grouping", k)
		}
		seen[k] = true
	}
	path := filepath.Join(t.TempDir(), "iso.arrow")
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
		for _, field := range r.Schema().Fields() {
			for _, name := range isoCalendarNames {
				if field.Name == name && (field.Type.ID() != arrow.INT64 || !field.Nullable) {
					t.Fatal("extracted IPC metadata", field)
				}
			}
		}
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
	isoCalendarCheck(t, idf, rows)
	result, e := q.SinkParquet(context.Background(), filepath.Join(t.TempDir(), "iso.parquet"), ParquetSinkOptions{RowGroupRows: 17})
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
	isoCalendarCheck(t, pdf, rows)
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
	isoCalendarCheck(t, df, rows)
	isoCalendarCheck(t, idf, rows)
	isoCalendarCheck(t, pdf, rows)
	df.Close()
	idf.Close()
	pdf.Close()
	selected.Close()
	g.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("owned leak")
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1
	tiny := NewSession(opts)
	tq := isoCalendarQuery(t, tiny, 65)
	td, te := tq.Collect(context.Background())
	if te == nil {
		td.Close()
	}
	tiny.Close()
	if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
		t.Fatal("budget", te)
	}
}
func TestISOCalendarLazyHistoricalInMemory(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{isoCalendarNS(t, "1900-01-01T00:00:00Z"), 0}, Validity: memory.Bitmap{1}}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}), b)
	if e != nil {
		t.Fatal(e)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	z := isoCalendarZone(t, "paris")
	q := s.FromSource(src).SelectExprs(ISOWeekYearInZone(Col("ts"), z).Alias("year"), ISOWeekInZone(Col("ts"), z).Alias("week"), ISOWeekdayInZone(Col("ts"), z).Alias("weekday"), UTCOffsetInZone(Col("ts"), z).Alias("offset"))
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	s.Close()
	for i, name := range isoCalendarNames {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := []any{[]int64{1900, 1, 1, 561}[i], nil}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("historical seconds", name, got, want)
		}
	}
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("inmemory leak")
	}
}
func TestISOCalendarLazyActualSpill(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 8 << 20
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	q := isoCalendarQuery(t, s, 260)
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	observer := &isoCalendarRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
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
	df, e := q.Sort(AscNullsFirst("year"), AscNullsFirst("week"), Asc("id")).Collect(observer)
	close(done)
	<-stopped
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	if budget.Snapshot().SpilledBytes <= 0 || observer.maxRuns < 2 {
		t.Fatal("expected actual runs", budget.Snapshot(), observer.maxRuns)
	}
	t.Logf("ISO spill bytes=%d runs=%d", budget.Snapshot().SpilledBytes, observer.maxRuns)
	rows := isoCalendarRows(260)
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a%7 == 0 || b%7 == 0 {
			if a%7 == 0 && b%7 == 0 {
				return a < b
			}
			return a%7 == 0
		}
		x, y := isoCalendarValues[a%6], isoCalendarValues[b%6]
		if x[0] != y[0] {
			return x[0] < y[0]
		}
		if x[1] != y[1] {
			return x[1] < y[1]
		}
		return a < b
	})
	isoCalendarCheck(t, df, rows)
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
	isoCalendarCheck(t, df, rows)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("spill owner leak")
	}
}

type isoCalendarRunContext struct {
	context.Context
	mu      sync.Mutex
	dir     string
	maxRuns int
}

func (c *isoCalendarRunContext) Err() error {
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
