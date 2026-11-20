package lazy

import (
	"context"
	"encoding/base64"
	"errors"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func calendarLazyResolver(t *testing.T) expr.CivilTimeResolver {
	t.Helper()
	b, e := os.ReadFile(filepath.Join("..", "expr", "testdata", "timezone", "new_york.tzif.b64"))
	if e != nil {
		t.Fatal(e)
	}
	data, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if e != nil {
		t.Fatal(e)
	}
	z, e := expr.TimeZoneFromTZData("captured", data)
	if e != nil {
		t.Fatal(e)
	}
	r, e := expr.NewCivilTimeResolver(context.Background(), z, expr.CivilResolveOptions{Fold: expr.FoldReject, Gap: expr.GapReject})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestCalendarDaysLazyTransport(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	ctx := context.Background()
	r := calendarLazyResolver(t)
	instant := func(v string) int64 {
		x, e := time.Parse(time.RFC3339Nano, v)
		if e != nil {
			t.Fatal(e)
		}
		return x.UnixNano()
	}
	b := memory.NewBatch()
	b.Reset(3)
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{instant("2024-03-09T14:30:00.123456789Z"), instant("2024-03-09T07:30:00Z"), 0}, Validity: memory.Bitmap{3}}
	b.Int64s["days"] = &memory.Int64Vector{Values: []int64{1, 1, 1}}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}, memory.Field{Name: "days", Type: memory.TypeInt64}), b)
	if e != nil {
		t.Fatal(e)
	}
	base := s.FromSource(src)
	q := base.SelectExprs(TryAddCalendarDays(Col("ts"), Col("days"), r).Alias("next"))
	want := []any{instant("2024-03-10T13:30:00.123456789Z"), nil, nil}
	check := func(df DataFrame) {
		defer df.Close()
		c, e := df.Column("next")
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatal(got, want, e)
		}
	}
	df, e := q.Collect(ctx)
	if e != nil {
		t.Fatal(e)
	}
	check(df)
	reader, e := q.CollectBatches(ctx, BatchOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	var got []any
	for reader.Next(ctx) {
		c, e := reader.Batch().Column("next")
		if e != nil {
			t.Fatal(e)
		}
		v, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		got = append(got, v...)
	}
	if e := reader.Err(); e != nil {
		t.Fatal(e)
	}
	if e := reader.Close(); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
	result, e := q.SinkParquet(ctx, filepath.Join(t.TempDir(), "days.parquet"), ParquetSinkOptions{RowGroupRows: 1})
	if e != nil {
		t.Fatal(e)
	}
	back, e := s.ScanParquet(result.Files[0])
	if e != nil {
		t.Fatal(e)
	}
	df, e = back.Collect(ctx)
	if e != nil {
		t.Fatal(e)
	}
	check(df)
	ipcPath := filepath.Join(t.TempDir(), "days.arrow")
	if _, e := q.SinkIPC(ctx, ipcPath, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	file, e := os.Open(ipcPath)
	if e != nil {
		t.Fatal(e)
	}
	defer file.Close()
	ipcReader, e := ipc.NewFileReader(file)
	if e != nil {
		t.Fatal(e)
	}
	defer ipcReader.Close()
	rec, e := ipcReader.RecordBatchAt(0)
	if e != nil {
		t.Fatal(e)
	}
	arrowFrame, e := s.ScanArrow(rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	arrowResult, e := arrowFrame.Collect(ctx)
	if e != nil {
		t.Fatal(e)
	}
	check(arrowResult)
	strict := base.SelectExprs(AddCalendarDays(Col("ts"), Col("days"), r).Alias("next"))
	if df, e := strict.Collect(ctx); !errors.Is(e, expr.ErrCivilGap) {
		df.Close()
		t.Fatal("strict gap", e)
	}
	// Running the actual optimized Lazy graph must not move this filter before
	// the strict expression and hide the second row's gap.
	postFilter := base.WithColumns(AddCalendarDays(Col("ts"), Col("days"), r).Alias("next")).Filter(expr.EQ(Col("ts"), expr.Timestamp(b.Int64s["ts"].Values[0])))
	if df, e := postFilter.Collect(ctx); !errors.Is(e, expr.ErrCivilGap) {
		df.Close()
		t.Fatal("optimizer hid demanded gap", e)
	}
	filtered, e := q.Filter(expr.EQ(Col("next"), expr.Timestamp(want[0].(int64)))).Collect(ctx)
	if e != nil {
		t.Fatal(e)
	}
	want = want[:1]
	check(filtered)
}
