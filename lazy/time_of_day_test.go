package lazy

import (
	"context"
	"encoding/base64"
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
	"strings"
	"sync"
	"testing"
	"time"
)

// Fixed local-wall nanoseconds, independent of the extraction implementation.
var timeOfDayInstants = []string{"2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z", "1969-12-31T23:59:59.999999999Z", "2024-01-01T05:00:00.123456789Z", "2024-03-10T06:59:59Z", "2024-03-10T07:00:00Z"}
var timeOfDayValues = []int64{5400000000000, 5400000000000, 68399999999999, 123456789, 7199000000000, 10800000000000}

func timeOfDayZone(t *testing.T, name string) expr.TimeZone {
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
func timeOfDayNS(t *testing.T, text string) int64 {
	t.Helper()
	v, e := time.Parse(time.RFC3339, text)
	if e != nil {
		t.Fatal(e)
	}
	return v.UnixNano()
}
func timeOfDayQuery(t *testing.T, s *Session, n int) LazyFrame {
	t.Helper()
	rb := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true}}, nil))
	for i := 0; i < n; i++ {
		rb.Field(0).(*array.Int64Builder).Append(int64(i))
		b := rb.Field(1).(*array.TimestampBuilder)
		if i%7 == 0 {
			b.AppendNull()
		} else {
			b.Append(arrow.Timestamp(timeOfDayNS(t, timeOfDayInstants[i%6])))
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
	z := timeOfDayZone(t, "new_york")
	return q.WithColumns(TimeOfDayInZone(Col("ts"), z).Alias("tod"))
}
func timeOfDayRows(n int) []int {
	r := make([]int, n)
	for i := range r {
		r[i] = i
	}
	return r
}

func timeOfDayWant(t *testing.T, row int, name string) any {
	if name == "id" {
		return int64(row)
	}
	if row%7 == 0 {
		return nil
	}
	if name == "ts" {
		return timeOfDayNS(t, timeOfDayInstants[row%6])
	}
	if name == "tod" {
		return timeOfDayValues[row%6]
	}
	t.Fatal("unknown column", name)
	return nil
}
func timeOfDayCheck(t *testing.T, df DataFrame, rows []int) {
	t.Helper()
	for _, name := range []string{"id", "ts", "tod"} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		expectedType := memory.TypeInt64
		if name == "ts" {
			expectedType = memory.TypeTimestamp
		}
		if name == "tod" {
			expectedType = memory.TypeTime64
		}
		if c.Type() != expectedType {
			t.Fatal("logical type", name, c.Type())
		}
		if name == "tod" {
			if _, e := c.Time64Chunks(); e != nil {
				t.Fatal("typed Time64 access", e)
			}
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, len(rows))
		for i, row := range rows {
			want[i] = timeOfDayWant(t, row, name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s got %v want %v", name, got, want)
		}
	}
}
func TestTimeOfDayLazyTransport(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := timeOfDayQuery(t, s, 65)
	rows := timeOfDayRows(65)
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	timeOfDayCheck(t, df, rows)
	selected, e := q.Filter(expr.GT(Col("id"), expr.I64(31))).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer selected.Close()
	timeOfDayCheck(t, selected, rows[32:])
	path := filepath.Join(t.TempDir(), "time-of-day.arrow")
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
			if field.Name == "tod" {
				ty, ok := field.Type.(*arrow.Time64Type)
				if !ok || ty.Unit != arrow.Nanosecond || !field.Nullable {
					t.Fatal("IPC Time64 ns metadata", field)
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
	timeOfDayCheck(t, idf, rows)
	pp := filepath.Join(t.TempDir(), "time-of-day.parquet")
	result, e := q.SinkParquet(context.Background(), pp, ParquetSinkOptions{RowGroupRows: 17})
	if e != nil {
		t.Fatal(e)
	}
	// Native TIME scan is intentionally unsupported in this earlier baseline.
	if _, e = s.ScanParquet(result.Files[0]); e == nil || !strings.Contains(e.Error(), "not supported by native scan") {
		t.Fatalf("native TIME rejection boundary: %v", e)
	}
	// Independently read Parquet with Arrow, then admit those typed records back through ScanArrow.
	pr, e := file.OpenParquetFile(pp, false)
	if e != nil {
		t.Fatal(e)
	}
	// Inspect the actual on-disk Parquet schema, not Arrow's reconstructed type.
	physicalFound := false
	pqSchema := pr.MetaData().Schema
	for i := 0; i < pqSchema.NumColumns(); i++ {
		column := pqSchema.Column(i)
		if column.Name() != "tod" {
			continue
		}
		physicalFound = true
		logical, ok := column.LogicalType().(schema.TimeLogicalType)
		if column.PhysicalType() != parquetformat.Types.Int64 || !ok || logical.TimeUnit() != schema.TimeUnitNanos || logical.IsAdjustedToUTC() {
			t.Fatalf("Parquet TIME physical/logical annotation: physical=%v logical=%v", column.PhysicalType(), column.LogicalType())
		}
	}
	if !physicalFound {
		t.Fatal("missing physical TIME column")
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
	found := false
	for i, field := range table.Schema().Fields() {
		if field.Name != "tod" {
			continue
		}
		found = true
		ty, ok := field.Type.(*arrow.Time64Type)
		if !ok || ty.Unit != arrow.Nanosecond || !field.Nullable {
			t.Fatal("Parquet Time64 ns metadata", field)
		}
		var got []any
		for _, ch := range table.Column(i).Data().Chunks() {
			v, ok := ch.(*array.Time64)
			if !ok {
				t.Fatal("Parquet Time64 array", ch.DataType())
			}
			for j := 0; j < v.Len(); j++ {
				if v.IsNull(j) {
					got = append(got, nil)
				} else {
					got = append(got, int64(v.Value(j)))
				}
			}
		}
		want := make([]any, len(rows))
		for j, row := range rows {
			want[j] = timeOfDayWant(t, row, "tod")
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("independent Parquet Time64 oracle", got, want)
		}
	}
	if !found {
		t.Fatal("missing TIME column")
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
	timeOfDayCheck(t, pdf, rows)
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
	timeOfDayCheck(t, df, rows)
	timeOfDayCheck(t, idf, rows)
	timeOfDayCheck(t, pdf, rows)
	df.Close()
	idf.Close()
	pdf.Close()
	selected.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("owned leak")
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1
	tiny := NewSession(opts)
	tq := timeOfDayQuery(t, tiny, 65)
	td, te := tq.Collect(context.Background())
	if te == nil {
		td.Close()
	}
	tiny.Close()
	if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
		t.Fatal("budget", te)
	}
}
func TestTimeOfDayLazyInMemoryAndFold(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{timeOfDayNS(t, timeOfDayInstants[0]), timeOfDayNS(t, timeOfDayInstants[1]), timeOfDayNS(t, timeOfDayInstants[2]), 0}, Validity: memory.Bitmap{7}}
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3}}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt64}), b)
	if e != nil {
		t.Fatal(e)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	z := timeOfDayZone(t, "new_york")
	q := s.FromSource(src).WithColumns(TimeOfDayInZone(Col("ts"), z).Alias("tod"))
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	c, e := df.Column("tod")
	if e != nil {
		t.Fatal(e)
	}
	if c.Type() != memory.TypeTime64 {
		t.Fatal("type")
	}
	got, e := c.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	want := []any{timeOfDayValues[0], timeOfDayValues[1], timeOfDayValues[2], nil}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("fold and negative-epoch nanoseconds", got, want)
	}
	guard, e := s.FromSource(src).SelectExprs(expr.If(expr.GT(Col("id"), expr.I64(1)), TimeOfDayInZone(Col("ts"), z), TimeOfDayInZone(Col("ts"), timeOfDayZone(t, "paris"))).Alias("guard")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	gc, e := guard.Column("guard")
	if e != nil {
		t.Fatal(e)
	}
	if gc.Type() != memory.TypeTime64 {
		t.Fatal("guard lost Time64")
	}
	gv, e := gc.ToAnySlice()
	if e != nil || !reflect.DeepEqual(gv, []any{int64(23400000000000), int64(27000000000000), timeOfDayValues[2], nil}) {
		t.Fatal("guard", gv, e)
	}
	guard.Close()
	s.Close()
	c, e = df.Column("tod")
	if e != nil {
		t.Fatal(e)
	}
	got, e = c.ToAnySlice()
	if e != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("owned Time64 after source close", got, e)
	}
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("inmemory owner leak")
	}
}
func TestTimeOfDayLazyHistoricalSeconds(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{timeOfDayNS(t, "1900-01-01T00:00:00.000000001Z")}}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}), b)
	if e != nil {
		t.Fatal(e)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	df, e := s.FromSource(src).SelectExprs(TimeOfDayInZone(Col("ts"), timeOfDayZone(t, "paris")).Alias("tod")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	c, e := df.Column("tod")
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.ToAnySlice()
	if e != nil || !reflect.DeepEqual(got, []any{int64(561000000001)}) {
		t.Fatal("historical second offset", got, e)
	}
	df.Close()
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("historical owner leak")
	}
}
func TestTimeOfDayLazyActualSpill(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 8 << 20
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	q := timeOfDayQuery(t, s, 260)
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	observer := &timeOfDayRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
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
	df, e := q.Sort(AscNullsFirst("tod"), Asc("id")).Collect(observer)
	close(done)
	<-stopped
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	if budget.Snapshot().SpilledBytes <= 0 || observer.maxRuns < 2 {
		t.Fatal("expected actual runs", budget.Snapshot(), observer.maxRuns)
	}
	t.Logf("time-of-day spill bytes=%d runs=%d", budget.Snapshot().SpilledBytes, observer.maxRuns)
	rows := timeOfDayRows(260)
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a%7 == 0 || b%7 == 0 {
			if a%7 == 0 && b%7 == 0 {
				return a < b
			}
			return a%7 == 0
		}
		x, y := timeOfDayValues[a%6], timeOfDayValues[b%6]
		if x == y {
			return a < b
		}
		return x < y
	})
	timeOfDayCheck(t, df, rows)
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
	timeOfDayCheck(t, df, rows)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("spill owner leak")
	}
}

type timeOfDayRunContext struct {
	context.Context
	mu      sync.Mutex
	dir     string
	maxRuns int
}

func (c *timeOfDayRunContext) Err() error {
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
