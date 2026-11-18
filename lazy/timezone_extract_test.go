package lazy

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type zoneLazyCase struct {
	utc   string
	local [6]int64
}

func zoneLazyFixtures() map[string][]zoneLazyCase {
	return map[string][]zoneLazyCase{
		"new_york": {{"2024-03-10T06:59:59Z", [6]int64{2024, 3, 10, 1, 59, 59}}, {"2024-03-10T07:00:00Z", [6]int64{2024, 3, 10, 3, 0, 0}}, {"2024-11-03T05:59:59Z", [6]int64{2024, 11, 3, 1, 59, 59}}, {"2024-11-03T06:00:00Z", [6]int64{2024, 11, 3, 1, 0, 0}}},
		"apia":     {{"2011-12-30T09:59:59Z", [6]int64{2011, 12, 29, 23, 59, 59}}, {"2011-12-30T10:00:00Z", [6]int64{2011, 12, 31, 0, 0, 0}}},
	}
}
func zoneLazySource(t *testing.T, cases []zoneLazyCase) *inmemory.Source {
	t.Helper()
	var batches []*memory.Batch
	for start := 0; start < 130; start += 31 {
		n := min(31, 130-start)
		b := memory.NewBatch()
		b.Reset(n)
		ids, ts := make([]int64, n), make([]int64, n)
		bits := memory.EnsureBitmap(nil, n)
		for i := 0; i < n; i++ {
			r := start + i
			ids[i] = int64(r)
			parsed, e := time.Parse(time.RFC3339, cases[r%len(cases)].utc)
			if e != nil {
				t.Fatal(e)
			}
			ts[i] = parsed.UnixNano()
			if r%7 != 0 {
				memory.Set(bits, i)
			}
		}
		b.Int64s["id"] = &memory.Int64Vector{Values: ids}
		b.Int64s["ts"] = &memory.Int64Vector{Values: ts, Validity: bits}
		batches = append(batches, b)
	}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}), batches...)
	if e != nil {
		t.Fatal(e)
	}
	return src
}
func zoneLazyHandle(t *testing.T, name string) expr.TimeZone {
	t.Helper()
	encoded, e := os.ReadFile(filepath.Join("..", "expr", "testdata", "timezone", name+".tzif.b64"))
	if e != nil {
		t.Fatal(e)
	}
	data, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	if e != nil {
		t.Fatal(e)
	}
	zone, e := TimeZoneFromTZData(name, data)
	if e != nil {
		t.Fatal(e)
	}
	for i := range data {
		data[i] = 0
	}
	if zone.Name() != name {
		t.Fatal(zone.Name())
	}
	return zone
}

var zoneLazyNames = []string{"year", "month", "day", "hour", "minute", "second"}

func zoneLazyCheck(t *testing.T, column func(string) (Series, error), cases []zoneLazyCase, rows []int) {
	t.Helper()
	for part, name := range zoneLazyNames {
		for _, name := range []string{name, "copy_" + name} {
			s, e := column(name)
			if e != nil {
				t.Fatal(e)
			}
			got, e := s.ToAnySlice()
			if e != nil {
				t.Fatal(e)
			}
			want := make([]any, len(rows))
			for i, r := range rows {
				if r%7 != 0 {
					want[i] = cases[r%len(cases)].local[part]
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s got%v want%v", name, got, want)
			}
		}
	}
}
func TestTimeZoneExtractLazyOracle(t *testing.T) {
	for name, cases := range zoneLazyFixtures() {
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
				src := zoneLazySource(t, cases)
				scan := s.FromSource(src)
				nodes := []expr.Expr{YearInZone(Col("ts"), zone), MonthInZone(Col("ts"), zone), DayInZone(Col("ts"), zone), HourInZone(Col("ts"), zone), MinuteInZone(Col("ts"), zone), SecondInZone(Col("ts"), zone)}
				out := []expr.NamedExpr{Col("id").Alias("id")}
				for i, node := range nodes {
					out = append(out, Named(node, zoneLazyNames[i]), Named(node, "copy_"+zoneLazyNames[i]))
				}
				q := scan.Filter(expr.GE(Col("id"), expr.I64(1))).SelectExprs(out...)
				rows := make([]int, 129)
				for i := range rows {
					rows[i] = i + 1
				}
				schema, e := q.Schema()
				if e != nil {
					t.Fatal(e)
				}
				for _, f := range schema.Fields[1:] {
					if f.Type != memory.TypeInt64 || !f.Nullable {
						t.Fatal(f)
					}
				}
				df, e := q.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				zoneLazyCheck(t, df.Column, cases, rows)
				df.Close()
				filtered := q.Filter(expr.GE(Col("hour"), expr.I64(1)))
				var selected []int
				for _, r := range rows {
					if r%7 != 0 && cases[r%len(cases)].local[3] >= 1 {
						selected = append(selected, r)
					}
				}
				df, e = filtered.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				zoneLazyCheck(t, df.Column, cases, selected)
				df.Close()
				stream, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
				if e != nil {
					t.Fatal(e)
				}
				seen := 0
				for stream.Next(context.Background()) {
					v := stream.Batch()
					ids, e := v.Column("id")
					if e != nil {
						t.Fatal(e)
					}
					n := int(ids.Len())
					zoneLazyCheck(t, v.Column, cases, rows[seen:seen+n])
					seen += n
				}
				if e := stream.Err(); e != nil {
					t.Fatal(e)
				}
				stream.Close()
				if seen != len(rows) {
					t.Fatal(seen)
				}
				for _, format := range []string{"ipc", "parquet"} {
					p := filepath.Join(t.TempDir(), "zones."+format)
					var read LazyFrame
					if format == "parquet" {
						if _, e := q.SinkParquet(context.Background(), p, ParquetSinkOptions{}); e != nil {
							t.Fatal(e)
						}
						read, e = s.ScanParquet(p)
					} else {
						if _, e := q.SinkIPC(context.Background(), p, IPCSinkOptions{}); e != nil {
							t.Fatal(e)
						}
						f, err := os.Open(p)
						if err != nil {
							t.Fatal(err)
						}
						reader, err := ipc.NewFileReader(f)
						if err != nil {
							t.Fatal(err)
						}
						var records []arrow.RecordBatch
						for i := 0; i < reader.NumRecords(); i++ {
							r, err := reader.RecordBatchAt(i)
							if err != nil {
								t.Fatal(err)
							}
							records = append(records, r)
						}
						read, e = s.ScanArrow(records...)
						for _, r := range records {
							r.Release()
						}
						reader.Close()
						f.Close()
					}
					if e != nil {
						t.Fatal(e)
					}
					df, e = read.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					zoneLazyCheck(t, df.Column, cases, rows)
					df.Close()
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				df, e = q.Collect(ctx)
				if !errors.Is(e, context.Canceled) {
					if e == nil {
						df.Close()
					}
					t.Fatal(e)
				}
				early, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
				if e != nil {
					t.Fatal(e)
				}
				if !early.Next(context.Background()) {
					t.Fatal(early.Err())
				}
				early.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("early/cancel leak")
				}
			})
		}
	}
}

// A controlled TZif v1 with one fixed-offset rule; no host tzdata is involved.
func zoneLazyFixedData(offset int32) []byte {
	b := make([]byte, 54)
	copy(b, "TZif")
	binary.BigEndian.PutUint32(b[36:40], 1)
	binary.BigEndian.PutUint32(b[40:44], 4)
	binary.BigEndian.PutUint32(b[44:48], uint32(offset))
	copy(b[50:], "FIX\x00")
	return b
}
func TestTimeZoneExtractIdentityUTCGuardsAndAdmission(t *testing.T) {
	a, e := TimeZoneFromTZData("same-label", zoneLazyFixedData(3600))
	if e != nil {
		t.Fatal(e)
	}
	b, e := TimeZoneFromTZData("same-label", zoneLazyFixedData(-18000))
	if e != nil {
		t.Fatal(e)
	}
	utc, e := LoadTimeZone("UTC")
	if e != nil {
		t.Fatal(e)
	}
	cases := []zoneLazyCase{{"2024-01-01T00:00:00Z", [6]int64{2024, 1, 1, 0, 0, 0}}}
	src := zoneLazySource(t, cases)
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	scan := s.FromSource(src)
	q := scan.SelectExprs(HourInZone(Col("ts"), a).Alias("east"), HourInZone(Col("ts"), b).Alias("west"), Hour(Col("ts")).Alias("old"), HourInZone(Col("ts"), utc).Alias("utc"), Named(expr.If(expr.LT(Col("id"), expr.I64(65)), HourInZone(Col("ts"), a), HourInZone(Col("ts"), b)), "guard"))
	for repeat := 0; repeat < 2; repeat++ {
		df, e := q.Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		for _, name := range []string{"east", "west", "old", "utc", "guard"} {
			c, e := df.Column(name)
			if e != nil {
				t.Fatal(e)
			}
			got, e := c.ToAnySlice()
			if e != nil {
				t.Fatal(e)
			}
			want := make([]any, 130)
			for r := range want {
				if r%7 == 0 {
					continue
				}
				v := int64(0)
				if name == "east" || name == "guard" && r < 65 {
					v = 1
				}
				if name == "west" || name == "guard" && r >= 65 {
					v = 19
				}
				want[r] = v
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal(name, got, want)
			}
		}
		df.Close()
	}
	for _, name := range []string{"", "Local", "Definitely/Not-A-Real-Zone"} {
		if _, e := LoadTimeZone(name); e == nil {
			t.Fatal("invalid zone accepted", name)
		}
	}
	if _, e := TimeZoneFromTZData("huge", make([]byte, (1<<20)+1)); e == nil {
		t.Fatal("oversize accepted")
	}
	// Explicit extraction does not change Arrow's existing IANA timestamp admission boundary.
	ab := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "America/New_York"}}}, nil))
	ab.Field(0).(*array.TimestampBuilder).Append(0)
	rec := ab.NewRecordBatch()
	ab.Release()
	_, e = s.ScanArrow(rec)
	rec.Release()
	if e == nil || !strings.Contains(e.Error(), "timezone") {
		t.Fatal("Arrow admission changed", e)
	}
	tinyOpts := opts
	tinyOpts.MemoryLimitBytes = 1
	tiny := NewSession(tinyOpts)
	defer tiny.Close()
	df, e := tiny.FromSource(src).SelectExprs(HourInZone(Col("ts"), a).Alias("h")).Collect(context.Background())
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		if e == nil {
			df.Close()
		}
		t.Fatal(e)
	}
	if tiny.Engine().Memory().Used() != 0 || s.Engine().Memory().Used() != 0 {
		t.Fatal("budget leak")
	}
}
