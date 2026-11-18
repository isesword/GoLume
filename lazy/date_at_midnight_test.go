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
	"math"
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

// Epoch dates and UTC instants are fixed independent expectations, not computed by the resolver.
var midnightDates = []int32{15337, 15338, 15339, math.MaxInt32}
var midnightUTC = []string{"2011-12-29T10:00:00Z", "", "2011-12-30T10:00:00Z", ""}

func midnightResolver(t *testing.T) expr.CivilTimeResolver {
	t.Helper()
	r, e := expr.NewCivilTimeResolver(context.Background(), midnightZone(t), expr.CivilResolveOptions{Fold: expr.FoldReject, Gap: expr.GapReject})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func midnightQuery(t *testing.T, s *Session, n int) LazyFrame {
	t.Helper()
	rb := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "date", Type: arrow.FixedWidthTypes.Date32, Nullable: true}}, nil))
	for i := 0; i < n; i++ {
		rb.Field(0).(*array.Int64Builder).Append(int64(i))
		b := rb.Field(1).(*array.Date32Builder)
		if i%7 == 0 {
			b.AppendNull()
		} else {
			b.Append(arrow.Date32(midnightDates[i%4]))
		}
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	var chunks []arrow.RecordBatch
	for start := 0; start < n; start += 65 {
		chunks = append(chunks, rec.NewSlice(int64(start), int64(min(n, start+65))))
	}
	q, e := s.ScanArrow(chunks...)
	for _, chunk := range chunks {
		chunk.Release()
	}
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	return q.WithColumns(TryDateAtMidnight(Col("date"), midnightResolver(t)).Alias("ts"))
}
func midnightCheck(t *testing.T, df DataFrame, rows []int) {
	t.Helper()
	for _, name := range []string{"id", "date", "ts"} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		if name == "ts" && c.Type() != memory.TypeTimestamp || name == "date" && c.Type() != memory.TypeDate32 {
			t.Fatal("lost logical type", name, c.Type())
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, len(rows))
		for j, i := range rows {
			switch name {
			case "id":
				want[j] = int64(i)
			case "date":
				if i%7 != 0 {
					want[j] = midnightDates[i%4]
				}
			case "ts":
				if i%7 != 0 && midnightUTC[i%4] != "" {
					v, e := time.Parse(time.RFC3339, midnightUTC[i%4])
					if e != nil {
						t.Fatal(e)
					}
					want[j] = v.UnixNano()
				}
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s got %v want %v", name, got, want)
		}
	}
}
func TestDateAtMidnightLazyTransport(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := midnightQuery(t, s, 65)
	rows := make([]int, 65)
	for i := range rows {
		rows[i] = i
	}
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	midnightCheck(t, df, rows)
	path := filepath.Join(t.TempDir(), "midnight.arrow")
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
			if field.Name == "ts" {
				ty, ok := field.Type.(*arrow.TimestampType)
				if !ok || ty.Unit != arrow.Nanosecond || ty.TimeZone != "UTC" || !field.Nullable {
					t.Fatal("IPC timestamp metadata", field)
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
	midnightCheck(t, idf, rows)
	result, e := q.SinkParquet(context.Background(), filepath.Join(t.TempDir(), "midnight.parquet"), ParquetSinkOptions{RowGroupRows: 17})
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
	midnightCheck(t, pdf, rows)
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
	midnightCheck(t, df, rows)
	midnightCheck(t, idf, rows)
	midnightCheck(t, pdf, rows)
	df.Close()
	idf.Close()
	pdf.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("owned leak")
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1
	tiny := NewSession(opts)
	tq := midnightQuery(t, tiny, 65)
	td, te := tq.Collect(context.Background())
	if te == nil {
		td.Close()
	}
	tiny.Close()
	if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
		t.Fatal("budget", te)
	}
}
func TestDateAtMidnightLazyStrictInMemory(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	b.Int32s["date"] = &memory.Int32Vector{Values: []int32{15338, 15337, 15339}}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "date", Type: memory.TypeDate32}), b)
	if e != nil {
		t.Fatal(e)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := s.FromSource(src)
	r := midnightResolver(t)
	df, e := q.WithColumns(DateAtMidnight(Col("date"), r).Alias("ts")).Filter(expr.GT(Col("id"), expr.I64(0))).Collect(context.Background())
	if e == nil {
		df.Close()
	}
	if !errors.Is(e, expr.ErrCivilGap) {
		t.Fatal("strict error was filtered away", e)
	}
	df, e = q.WithColumns(TryDateAtMidnight(Col("date"), r).Alias("ts")).Filter(expr.GT(Col("id"), expr.I64(0))).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	c, e := df.Column("ts")
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	want := []any{}
	for _, text := range []string{midnightUTC[0], midnightUTC[2]} {
		v, e := time.Parse(time.RFC3339, text)
		if e != nil {
			t.Fatal(e)
		}
		want = append(want, v.UnixNano())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal(got, want)
	}
	df.Close()
	s.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("inmemory leak")
	}
}
func TestDateAtMidnightLazyActualSpill(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 8 << 20
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	q := midnightQuery(t, s, 260)
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	observer := &midnightRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
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
		t.Fatal("expected real external runs", budget.Snapshot(), observer.maxRuns)
	}
	t.Logf("spill bytes=%d runs=%d", budget.Snapshot().SpilledBytes, observer.maxRuns)
	rows := make([]int, 260)
	for i := range rows {
		rows[i] = i
	}
	key := func(i int) string {
		if i%7 == 0 {
			return ""
		}
		return midnightUTC[i%4]
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := key(rows[i]), key(rows[j])
		if a == b {
			return rows[i] < rows[j]
		}
		return a < b
	})
	midnightCheck(t, df, rows)
	budget.Close()
	s.Close()
	midnightNoFiles(t, dir)
	midnightCheck(t, df, rows)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("spill owner leak")
	}
}

func midnightZone(t *testing.T) expr.TimeZone {
	t.Helper()
	encoded, e := os.ReadFile(filepath.Join("..", "expr", "testdata", "timezone", "apia.tzif.b64"))
	if e != nil {
		t.Fatal(e)
	}
	data, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	if e != nil {
		t.Fatal(e)
	}
	zone, e := expr.TimeZoneFromTZData("apia", data)
	if e != nil {
		t.Fatal(e)
	}
	clear(data)
	return zone
}

// Observer only records actual nonempty spill files while the query is running.
type midnightRunContext struct {
	context.Context
	mu      sync.Mutex
	dir     string
	maxRuns int
}

func (c *midnightRunContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	_ = filepath.WalkDir(c.dir, func(_ string, d fs.DirEntry, e error) error {
		if e == nil && !d.IsDir() {
			info, e := d.Info()
			if e == nil && info.Size() > 0 {
				count++
			}
		}
		return nil
	})
	if count > c.maxRuns {
		c.maxRuns = count
	}
	return c.Context.Err()
}
func midnightNoFiles(t *testing.T, dir string) {
	t.Helper()
	e := filepath.WalkDir(dir, func(path string, d fs.DirEntry, e error) error {
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
}
