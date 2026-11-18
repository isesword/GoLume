package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// Fixed date ordinals, not values produced by time.In or another extractor.
var lazyZoneDateDays = map[string][]int32{"new_york": {19792, 19792, 20030, 20030}, "apia": {15337, 15339}}

func dateZoneSource(t *testing.T, name string) *inmemory.Source {
	t.Helper()
	cases := zoneLazyFixtures()[name]
	var batches []*memory.Batch
	for start := 0; start < 130; start += 31 {
		n := min(31, 130-start)
		b := memory.NewBatch()
		b.Reset(n)
		ts, amount := make([]int64, n), make([]int64, n)
		bits := memory.EnsureBitmap(nil, n)
		for i := 0; i < n; i++ {
			r := start + i
			instant, e := time.Parse(time.RFC3339, cases[r%len(cases)].utc)
			if e != nil {
				t.Fatal(e)
			}
			ts[i] = instant.UnixNano()
			amount[i] = int64(r + 1)
			if r%7 != 0 {
				memory.Set(bits, i)
			}
		}
		b.Int64s["ts"] = &memory.Int64Vector{Values: ts, Validity: bits}
		b.Int64s["amount"] = &memory.Int64Vector{Values: amount}
		batches = append(batches, b)
	}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}, memory.Field{Name: "amount", Type: memory.TypeInt64}), batches...)
	if e != nil {
		t.Fatal(e)
	}
	return src
}
func dateZoneGroups(t *testing.T, df DataFrame, name string) {
	t.Helper()
	columns := map[string][]any{}
	for _, n := range []string{"date", "count", "sum"} {
		c, e := df.Column(n)
		if e != nil {
			t.Fatal(e)
		}
		if n == "date" && c.Type() != memory.TypeDate32 {
			t.Fatal("lost Date32", c.Type())
		}
		columns[n], e = c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
	}
	want := map[any][2]int64{}
	days := lazyZoneDateDays[name]
	for row := 0; row < 130; row++ {
		var key any
		if row%7 != 0 {
			key = days[row%len(days)]
		}
		v := want[key]
		v[0]++
		v[1] += int64(row + 1)
		want[key] = v
	}
	if len(columns["date"]) != len(want) {
		t.Fatal("group cardinality")
	}
	seen := map[any]bool{}
	for i, key := range columns["date"] {
		v, ok := want[key]
		if !ok || seen[key] {
			t.Fatal("unexpected/duplicate key", key)
		}
		seen[key] = true
		if columns["count"][i] != uint64(v[0]) || columns["sum"][i] != v[1] {
			t.Fatalf("date %v count=%v sum=%v want=%v", key, columns["count"][i], columns["sum"][i], v)
		}
	}
}
func TestDateInZoneGroupedCalendarPipeline(t *testing.T) {
	for _, name := range []string{"new_york", "apia"} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("%s/%d", name, mode), func(t *testing.T) {
				opts := engine.DefaultOptions()
				opts.KernelMode = mode
				opts.PreparedCacheBytes = 0
				s := NewSession(opts)
				defer func() {
					s.Close()
					if s.Engine().Memory().Used() != 0 {
						t.Error("leak")
					}
				}()
				zone := zoneLazyHandle(t, name)
				src := dateZoneSource(t, name)
				scan := s.FromSource(src)
				projected := scan.WithColumns(DateInZone(Col("ts"), zone).Alias("local_date"))
				// The archived test used an unpublished StructFrom/StructGet round trip.
				// This published-baseline variant retains every date/group/IPC oracle
				// through supported scalar projections, without claiming nested coverage.
				calendar := projected.SelectExprs(Col("local_date").Alias("date"), Col("amount").Alias("amount"))
				schema, e := calendar.Schema()
				if e != nil {
					t.Fatal(e)
				}
				field, _ := schema.Field("date")
				if field.Type != memory.TypeDate32 || !field.Nullable || field.ArrowType != "" {
					t.Fatal("date metadata", field)
				}
				df, e := calendar.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				c, e := df.Column("date")
				if e != nil {
					t.Fatal(e)
				}
				got, e := c.ToAnySlice()
				if e != nil {
					t.Fatal(e)
				}
				want := make([]any, 130)
				days := lazyZoneDateDays[name]
				for row := range want {
					if row%7 != 0 {
						want[row] = days[row%len(days)]
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("row date oracle", got, want)
				}
				df.Close()
				grouped := calendar.GroupBy("date").Agg(Count("count"), Sum("amount", "sum"))
				gs, e := grouped.Schema()
				if e != nil {
					t.Fatal(e)
				}
				gf, _ := gs.Field("date")
				if gf.Type != memory.TypeDate32 || !gf.Nullable {
					t.Fatal("group metadata", gf)
				}
				df, e = grouped.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				dateZoneGroups(t, df, name)
				df.Close()
				path := filepath.Join(t.TempDir(), "dates.arrow")
				if _, e := grouped.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
					t.Fatal(e)
				}
				f, e := os.Open(path)
				if e != nil {
					t.Fatal(e)
				}
				reader, e := ipc.NewFileReader(f)
				if e != nil {
					t.Fatal(e)
				}
				if reader.Schema().Field(0).Type.ID() != arrow.DATE32 {
					t.Fatal("Arrow date metadata", reader.Schema())
				}
				var records []arrow.RecordBatch
				for i := 0; i < reader.NumRecords(); i++ {
					r, e := reader.RecordBatchAt(i)
					if e != nil {
						t.Fatal(e)
					}
					records = append(records, r)
				}
				round, e := s.ScanArrow(records...)
				for _, r := range records {
					r.Release()
				}
				reader.Close()
				f.Close()
				if e != nil {
					t.Fatal(e)
				}
				df, e = round.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				dateZoneGroups(t, df, name)
				df.Close()
				// An active batch stream is closed before exhausting multiple source batches.
				early, e := calendar.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 256, MaxBatchBytes: 4096})
				if e != nil {
					t.Fatal(e)
				}
				if !early.Next(context.Background()) {
					t.Fatal(early.Err())
				}
				early.Close()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				df, e = grouped.Collect(ctx)
				if !errors.Is(e, context.Canceled) {
					if e == nil {
						df.Close()
					}
					t.Fatal(e)
				}
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("early/cancel leak")
				}
				tinyOpts := opts
				tinyOpts.MemoryLimitBytes = 1
				tiny := NewSession(tinyOpts)
				df, e = tiny.FromSource(src).WithColumns(DateInZone(Col("ts"), zone).Alias("date")).GroupBy("date").Agg(Count("n")).Collect(context.Background())
				if !errors.Is(e, memory.ErrBudgetExceeded) {
					if e == nil {
						df.Close()
					}
					tiny.Close()
					t.Fatal(e)
				}
				if tiny.Engine().Memory().Used() != 0 {
					t.Fatal("budget denial leak")
				}
				tiny.Close()
			})
		}
	}
}
func TestDateInZoneSameLabelAndUTCCompatibility(t *testing.T) {
	east, e := TimeZoneFromTZData("same", zoneLazyFixedData(3600))
	if e != nil {
		t.Fatal(e)
	}
	west, e := TimeZoneFromTZData("same", zoneLazyFixedData(-18000))
	if e != nil {
		t.Fatal(e)
	}
	utc, e := LoadTimeZone("UTC")
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Reset(4)
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{0, -1, 1704067200000000000, 0}, Validity: memory.Bitmap{7}}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}), b)
	if e != nil {
		t.Fatal(e)
	}
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	q := s.FromSource(src).SelectExprs(DateInZone(Col("ts"), east).Alias("east"), DateInZone(Col("ts"), west).Alias("west"), DateInZone(Col("ts"), utc).Alias("utc"), Named(expr.CastTo(Col("ts"), memory.TypeDate32), "old_utc"))
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	for name, want := range map[string][]any{"east": {int32(0), int32(0), int32(19723), nil}, "west": {int32(-1), int32(-1), int32(19722), nil}, "utc": {int32(0), int32(-1), int32(19723), nil}, "old_utc": {int32(0), int32(-1), int32(19723), nil}} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		if c.Type() != memory.TypeDate32 {
			t.Fatal(c.Type())
		}
		v, e := c.ToAnySlice()
		if e != nil || !reflect.DeepEqual(v, want) {
			t.Fatal(name, v, want, e)
		}
	}
}
