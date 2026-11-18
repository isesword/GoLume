package lazy

import (
	"context"
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
	"sync"
	"testing"
	"time"
)

func TestCivilLazyStrictProjectionError(t *testing.T) {
	z := zoneLazyHandle(t, "new_york")
	r, e := expr.NewCivilTimeResolver(context.Background(), z, expr.CivilResolveOptions{Fold: expr.FoldReject, Gap: expr.GapReject})
	if e != nil {
		t.Fatal(e)
	}
	rb := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "text", Type: arrow.BinaryTypes.String}}, nil))
	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 1}, nil)
	rb.Field(1).(*array.StringBuilder).AppendValues([]string{"2024-03-10T02:30:00", "2024-01-01T00:00:00"}, nil)
	rec := rb.NewRecordBatch()
	rb.Release()
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q, e := s.ScanArrow(rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	d, e := q.WithColumns(ParseCivilTimestamp(Col("text"), r).Alias("ts")).Filter(expr.GT(Col("id"), expr.I64(0))).Collect(context.Background())
	if e == nil {
		d.Close()
	}
	if !errors.Is(e, expr.ErrCivilGap) {
		t.Fatal("strict projection error was filtered away", e)
	}
	d, e = q.WithColumns(TryParseCivilTimestamp(Col("text"), r).Alias("ts")).Filter(expr.GT(Col("id"), expr.I64(0))).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
}

var civilLazyTexts = []string{"2024-11-03T01:30:00", "2024-03-10T01:59:59", "2024-03-10T03:00:00", "2024-03-10T02:30:00", "bad", "2024-01-01T00:00:00"}
var civilLazyUTC = []string{"2024-11-03T05:30:00Z", "2024-03-10T06:59:59Z", "2024-03-10T07:00:00Z", "", "", "2024-01-01T05:00:00Z"}
var civilLazyDays = []int32{20030, 19792, 19792, 0, 0, 19723}

func civilLazyWant(t *testing.T, row int, name string) any {
	t.Helper()
	i := row % 6
	if name == "id" {
		return int64(row)
	}
	if name == "text" {
		if row%7 == 0 {
			return nil
		}
		return civilLazyTexts[i]
	}
	if row%7 == 0 || civilLazyUTC[i] == "" {
		return nil
	}
	switch name {
	case "date":
		return civilLazyDays[i]
	case "hour":
		return []int64{1, 1, 3, 0, 0, 0}[i]
	default:
		v, e := time.Parse(time.RFC3339, civilLazyUTC[i])
		if e != nil {
			t.Fatal(e)
		}
		return v.UnixNano()
	}
}
func civilLazyQuery(t *testing.T, s *Session) LazyFrame {
	t.Helper()
	z := zoneLazyHandle(t, "new_york")
	r, e := expr.NewCivilTimeResolver(context.Background(), z, expr.CivilResolveOptions{Fold: expr.FoldEarlier, Gap: expr.GapReject})
	if e != nil {
		t.Fatal(e)
	}
	rb := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "text", Type: arrow.BinaryTypes.String, Nullable: true}}, nil))
	for row := 0; row < 65; row++ {
		rb.Field(0).(*array.Int64Builder).Append(int64(row))
		b := rb.Field(1).(*array.StringBuilder)
		if row%7 == 0 {
			b.AppendNull()
		} else {
			b.Append(civilLazyTexts[row%6])
		}
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	q, e := s.ScanArrow(rec, rec, rec, rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	return q.WithColumns(TryParseCivilTimestamp(Col("text"), r).Alias("ts")).WithColumns(DateInZone(Col("ts"), z).Alias("date"), HourInZone(Col("ts"), z).Alias("hour"))
}
func civilLazyCheck(t *testing.T, df DataFrame, rows []int) {
	t.Helper()
	for _, name := range []string{"id", "text", "ts", "date", "hour"} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		if name == "ts" && c.Type() != memory.TypeTimestamp || name == "date" && c.Type() != memory.TypeDate32 {
			t.Fatal("metadata")
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := []any{}
		for _, row := range rows {
			want = append(want, civilLazyWant(t, row, name))
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s got%v want%v", name, got, want)
		}
	}
}
func civilLazyRows() []int {
	rows := []int{}
	for repeat := 0; repeat < 4; repeat++ {
		for row := 0; row < 65; row++ {
			rows = append(rows, row)
		}
	}
	return rows
}
func TestCivilLazyPipeline(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q := civilLazyQuery(t, s)
	rows := civilLazyRows()
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	civilLazyCheck(t, df, rows)
	grouped, e := q.GroupBy("date").Agg(Count("count"), Sum("id", "sum")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer grouped.Close()
	wanted := map[any][2]int64{}
	for _, row := range rows {
		key := civilLazyWant(t, row, "date")
		v := wanted[key]
		v[0]++
		v[1] += int64(row)
		wanted[key] = v
	}
	cols := map[string][]any{}
	for _, n := range []string{"date", "count", "sum"} {
		c, e := grouped.Column(n)
		if e != nil {
			t.Fatal(e)
		}
		cols[n], e = c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
	}
	if len(cols["date"]) != len(wanted) {
		t.Fatal("groups")
	}
	for i, k := range cols["date"] {
		if wanted[k] != [2]int64{int64(cols["count"][i].(uint64)), cols["sum"][i].(int64)} {
			t.Fatal("group oracle")
		}
	}
	path := filepath.Join(t.TempDir(), "civil.arrow")
	if _, e = q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	in, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	rd, e := ipc.NewFileReader(in)
	if e != nil {
		t.Fatal(e)
	}
	var recs []arrow.RecordBatch
	for i := 0; i < rd.NumRecords(); i++ {
		r, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		recs = append(recs, r)
	}
	iq, e := s.ScanArrow(recs...)
	for _, r := range recs {
		r.Release()
	}
	rd.Close()
	in.Close()
	if e != nil {
		t.Fatal(e)
	}
	idf, e := iq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer idf.Close()
	civilLazyCheck(t, idf, rows)
	pp := filepath.Join(t.TempDir(), "civil.parquet")
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
	civilLazyCheck(t, pdf, rows)
	pr, e := file.OpenParquetFile(pp, false)
	if e != nil {
		t.Fatal(e)
	}
	ar, e := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
	if e != nil {
		t.Fatal(e)
	}
	tab, e := ar.ReadTable(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for i, f := range tab.Schema().Fields() {
		if f.Name == "ts" {
			ty, ok := f.Type.(*arrow.TimestampType)
			if !ok || ty.Unit != arrow.Nanosecond || ty.TimeZone != "UTC" || !f.Nullable {
				t.Fatal("UTC Arrow metadata", f)
			}
		}
		got := []any{}
		for _, ch := range tab.Column(i).Data().Chunks() {
			for j := 0; j < ch.Len(); j++ {
				got = append(got, civilSinkArrowValue(t, ch, j))
			}
		}
		want := []any{}
		for _, row := range rows {
			want = append(want, civilLazyWant(t, row, f.Name))
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("independent Parquet oracle", f.Name)
		}
	}
	tab.Release()
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
	reader, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	if !reader.Next(context.Background()) {
		t.Fatal(reader.Err())
	}
	v := reader.Batch()
	reader.Close()
	if v.Valid() {
		t.Fatal("expired batch")
	}
	s.Close()
	civilLazyCheck(t, df, rows)
	civilLazyCheck(t, idf, rows)
	civilLazyCheck(t, pdf, rows)
	df.Close()
	idf.Close()
	pdf.Close()
	grouped.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("owned leak")
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1
	tiny := NewSession(opts)
	tq := civilLazyQuery(t, tiny)
	td, te := tq.Collect(context.Background())
	if te == nil {
		td.Close()
	}
	tiny.Close()
	if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
		t.Fatal("budget", te)
	}
}
func TestCivilLazyActualSpill(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 8 << 20
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	q := civilLazyQuery(t, s)
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	observer := &civilRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
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
		t.Fatal("actual runs", budget.Snapshot(), observer.maxRuns)
	}
	t.Logf("civil actual spill=%d runs=%d disk=%d", budget.Snapshot().SpilledBytes, observer.maxRuns, observer.diskBytes)
	rows := civilLazyRows()
	sort.Slice(rows, func(i, j int) bool {
		a, b := civilLazyWant(t, rows[i], "ts"), civilLazyWant(t, rows[j], "ts")
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
	civilLazyCheck(t, df, rows)
	budget.Close()
	s.Close()
	civilNoSpillFiles(t, dir)
	civilLazyCheck(t, df, rows)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("spill owner")
	}
}

func TestCivilLazyInMemoryStrict(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 2
	b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 19, 38}, Data: []byte("2024-01-01T00:00:002024-11-03T01:30:00")}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), b)
	if e != nil {
		t.Fatal(e)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	r, e := expr.NewCivilTimeResolver(context.Background(), zoneLazyHandle(t, "new_york"), expr.CivilResolveOptions{Fold: expr.FoldLater, Gap: expr.GapReject})
	if e != nil {
		t.Fatal(e)
	}
	df, e := s.FromSource(src).SelectExprs(ParseCivilTimestamp(Col("s"), r).Alias("ts")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	s.Close()
	c, e := df.Column("ts")
	if e != nil {
		t.Fatal(e)
	}
	v, e := c.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	for i, text := range []string{"2024-01-01T05:00:00Z", "2024-11-03T06:30:00Z"} {
		want, e := time.Parse(time.RFC3339, text)
		if e != nil {
			t.Fatal(e)
		}
		if v[i] != want.UnixNano() {
			t.Fatal("memory strict", v)
		}
	}
}

// civilSinkArrowValue is a flat independent Arrow oracle for this test's
// id/text/ts/date/hour schema, not a nested-container decoder.
func civilSinkArrowValue(t *testing.T, a arrow.Array, row int) any {
	t.Helper()
	if a.IsNull(row) {
		return nil
	}
	switch v := a.(type) {
	case *array.Int64:
		return v.Value(row)
	case *array.String:
		return v.Value(row)
	case *array.Date32:
		return int32(v.Value(row))
	case *array.Timestamp:
		return int64(v.Value(row))
	default:
		t.Fatalf("unsupported civil independent Arrow oracle %T", a)
		return nil
	}
}
func civilNoSpillFiles(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			t.Errorf("spill file retained: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Observe transient runs during synchronous context checkpoints: by the first
// output batch the sorter may already have merged and deleted every run.
type civilRunContext struct {
	mu sync.Mutex
	context.Context
	dir       string
	maxRuns   int
	diskBytes int64
}

func (c *civilRunContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.maxRuns < 2 {
		n := 0
		var size int64
		_ = filepath.WalkDir(c.dir, func(_ string, d fs.DirEntry, e error) error {
			if e == nil && !d.IsDir() {
				if info, e := d.Info(); e == nil && info.Size() > 0 {
					n++
					size += info.Size()
				}
			}
			return nil
		})
		if n > c.maxRuns {
			c.maxRuns = n
			c.diskBytes = size
		}
	}
	return c.Context.Err()
}
