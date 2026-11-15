package lazy

import (
	"context"
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
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestArrowTimeUnitsCanonicalValues(t *testing.T) {
	b := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "t32", Type: arrow.FixedWidthTypes.Time32s}, {Name: "t64", Type: arrow.FixedWidthTypes.Time64us}}, nil))
	b.Field(0).(*array.Time32Builder).Append(2)
	b.Field(1).(*array.Time64Builder).Append(3)
	rec := b.NewRecordBatch()
	b.Release()
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q, e := s.ScanArrow(rec)
	rec.Release()
	if e != nil {
		t.Fatal(e)
	}
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	for name, want := range map[string][]any{"t32": {int32(2000)}, "t64": {int64(3000)}} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		v, e := c.ToAnySlice()
		if e != nil || !reflect.DeepEqual(v, want) {
			t.Errorf("%s got=%v want=%v error=%v", name, v, want, e)
		}
	}
}

func arrowTimeFields() []arrow.Field {
	return []arrow.Field{
		{Name: "seconds", Type: arrow.FixedWidthTypes.Time32s, Nullable: true}, {Name: "millis", Type: arrow.FixedWidthTypes.Time32ms, Nullable: true},
		{Name: "micros", Type: arrow.FixedWidthTypes.Time64us, Nullable: true}, {Name: "nanos", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
		{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}, Nullable: true}, {Name: "duration", Type: &arrow.DurationType{Unit: arrow.Microsecond}, Nullable: true},
	}
}
func arrowTimeRecord(alloc am.Allocator, n int, nested bool) arrow.RecordBatch {
	fields := arrowTimeFields()
	schema := fields
	if nested {
		schema = []arrow.Field{{Name: "root", Type: arrow.StructOf(fields...), Nullable: true}}
	}
	b := array.NewRecordBuilder(alloc, arrow.NewSchema(schema, nil))
	defer b.Release()
	for row := 0; row < n; row++ {
		var sb *array.StructBuilder
		if nested {
			sb = b.Field(0).(*array.StructBuilder)
			sb.AppendValues([]bool{row%7 != 0})
		}
		for i := range fields {
			var builder array.Builder
			if nested {
				builder = sb.FieldBuilder(i)
			} else {
				builder = b.Field(i)
			}
			v := int64(row + 1)
			valid := row%5 != 0
			switch a := builder.(type) {
			case *array.Time32Builder:
				if valid {
					a.Append(arrow.Time32(v))
				} else {
					a.AppendNull()
				}
			case *array.Time64Builder:
				if valid {
					a.Append(arrow.Time64(v))
				} else {
					a.AppendNull()
				}
			case *array.TimestampBuilder:
				if valid {
					a.Append(arrow.Timestamp(v))
				} else {
					a.AppendNull()
				}
			case *array.DurationBuilder:
				if valid {
					a.Append(arrow.Duration(v))
				} else {
					a.AppendNull()
				}
			}
		}
	}
	return b.NewRecordBatch()
}
func arrowTimeExpected(name string, row int) any {
	if row%5 == 0 {
		return nil
	}
	v := int64(row + 1)
	switch name {
	case "seconds":
		return int32(v * 1000)
	case "millis":
		return int32(v)
	case "micros", "duration":
		return v * 1000
	case "nanos":
		return v
	case "timestamp":
		return v * 1000000000
	}
	panic(name)
}
func arrowTimeCheck(t *testing.T, df DataFrame, rows []int, nested bool, skipDuration ...bool) {
	t.Helper()
	if nested {
		c, e := df.Column("root")
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, len(rows))
		for i, r := range rows {
			if r%7 == 0 {
				continue
			}
			m := map[string]any{}
			for _, f := range arrowTimeFields() {
				m[f.Name] = arrowTimeExpected(f.Name, r)
			}
			want[i] = m
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("nested got=%#v want=%#v", got, want)
		}
		return
	}
	for _, f := range arrowTimeFields() {
		if len(skipDuration) > 0 && skipDuration[0] && f.Name == "duration" {
			continue
		}
		c, e := df.Column(f.Name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, len(rows))
		for i, r := range rows {
			want[i] = arrowTimeExpected(f.Name, r)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s got=%#v want=%#v", f.Name, got, want)
		}
	}
}
func TestArrowTimeUnitsPipelineRoundTrip(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, nested := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode%d/nested%v", mode, nested), func(t *testing.T) {
				alloc := am.NewCheckedAllocator(am.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				rec := arrowTimeRecord(alloc, 132, nested)
				slice := rec.NewSlice(1, 131)
				rec.Release()
				opts := engine.DefaultOptions()
				opts.PreparedCacheBytes = 0
				opts.KernelMode = mode
				s := NewSession(opts)
				defer func() {
					s.Close()
					if s.Engine().Memory().Used() != 0 {
						t.Fatal("leak")
					}
				}()
				q, e := s.ScanArrow(slice)
				slice.Release()
				if e != nil {
					t.Fatal(e)
				}
				rows := make([]int, 130)
				for i := range rows {
					rows[i] = i + 1
				}
				initial, initialErr := q.Collect(context.Background())
				if initialErr != nil {
					t.Fatal(initialErr)
				}
				arrowTimeCheck(t, initial, rows, nested)
				initial.Close()
				if nested {
					q = q.SelectExprs(expr.Col("root").Alias("root"))
				} else {
					q = q.Filter(expr.GE(expr.Col("seconds"), expr.I64(2000)))
					rows = nil
					for r := 1; r <= 130; r++ {
						if r%5 != 0 {
							rows = append(rows, r)
						}
					}
				}
				df, e := q.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				arrowTimeCheck(t, df, rows, nested)
				df.Close()
				// IPC exercises canonical exported units and another Arrow source admission.
				p := filepath.Join(t.TempDir(), "times.arrow")
				if _, e := q.SinkIPC(context.Background(), p, IPCSinkOptions{}); e != nil {
					t.Fatal(e)
				}
				file, e := os.Open(p)
				if e != nil {
					t.Fatal(e)
				}
				reader, e := ipc.NewFileReader(file)
				if e != nil {
					t.Fatal(e)
				}
				exported := reader.Schema().Fields()
				if nested {
					exported = exported[0].Type.(*arrow.StructType).Fields()
				}
				for _, field := range exported {
					switch typ := field.Type.(type) {
					case *arrow.Time32Type:
						if typ.Unit != arrow.Millisecond {
							t.Fatal(field)
						}
					case *arrow.Time64Type:
						if typ.Unit != arrow.Nanosecond {
							t.Fatal(field)
						}
					case *arrow.TimestampType:
						if typ.Unit != arrow.Nanosecond {
							t.Fatal(field)
						}
					case *arrow.DurationType:
						if typ.Unit != arrow.Nanosecond {
							t.Fatal(field)
						}
					}
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
				file.Close()
				if e != nil {
					t.Fatal(e)
				}
				df, e = round.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				arrowTimeCheck(t, df, rows, nested)
				df.Close()
				if !nested {
					p := filepath.Join(t.TempDir(), "times.parquet")
					if _, e := q.SinkParquet(context.Background(), p, ParquetSinkOptions{}); e == nil || !strings.Contains(e.Error(), "DURATION") {
						t.Fatalf("existing duration Parquet boundary: %v", e)
					}
					pqInput := q.SelectExprs(expr.Col("seconds").Alias("seconds"), expr.Col("millis").Alias("millis"), expr.Col("micros").Alias("micros"), expr.Col("nanos").Alias("nanos"), expr.Col("timestamp").Alias("timestamp"))
					if _, e := pqInput.SinkParquet(context.Background(), p, ParquetSinkOptions{}); e != nil {
						t.Fatal(e)
					}
					if _, e := s.ScanParquet(p); e == nil || !strings.Contains(e.Error(), "not supported by native scan") {
						t.Fatalf("existing Time Parquet reader boundary: %v", e)
					}
					timestampPath := filepath.Join(t.TempDir(), "timestamp.parquet")
					if _, e := q.SelectExprs(expr.Col("timestamp").Alias("timestamp")).SinkParquet(context.Background(), timestampPath, ParquetSinkOptions{}); e != nil {
						t.Fatal(e)
					}
					pq, e := s.ScanParquet(timestampPath)
					if e != nil {
						t.Fatal(e)
					}
					df, e = pq.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					c, e := df.Column("timestamp")
					if e != nil {
						t.Fatal(e)
					}
					got, e := c.ToAnySlice()
					if e != nil {
						t.Fatal(e)
					}
					want := make([]any, len(rows))
					for i, r := range rows {
						want[i] = arrowTimeExpected("timestamp", r)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatal("timestamp Parquet", got, want)
					}
					df.Close()
				}
				st, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
				if e != nil {
					t.Fatal(e)
				}
				if !st.Next(context.Background()) {
					t.Fatal(st.Err())
				}
				st.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("early close leak")
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
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("cancel leak")
				}
			})
		}
	}
}
func TestArrowTimeUnitsAdmissionAndBudget(t *testing.T) {
	for _, wide := range []bool{false, true} {
		t.Run(fmt.Sprint(wide), func(t *testing.T) {
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			typ := arrow.DataType(arrow.FixedWidthTypes.Time32s)
			if wide {
				typ = arrow.FixedWidthTypes.Time64us
			}
			b := array.NewRecordBuilder(alloc, arrow.NewSchema([]arrow.Field{{Name: "bad", Type: typ}}, nil))
			if wide {
				b.Field(0).(*array.Time64Builder).Append(arrow.Time64(math.MaxInt64))
			} else {
				b.Field(0).(*array.Time32Builder).Append(arrow.Time32(math.MaxInt32))
			}
			r := b.NewRecordBatch()
			b.Release()
			s := NewSession(engine.DefaultOptions())
			defer s.Close()
			_, e := s.ScanArrow(r)
			r.Release()
			if e == nil || !strings.Contains(e.Error(), "overflow") {
				t.Fatalf("source admission must fail before any guard: %v", e)
			}
			alloc.AssertSize(t, 0)
		})
	}
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	r := arrowTimeRecord(alloc, 130, false)
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.MemoryLimitBytes = 1
	s := NewSession(opts)
	defer s.Close()
	q, e := s.ScanArrow(r)
	r.Release()
	if e != nil {
		t.Fatal(e)
	}
	df, e := q.Collect(context.Background())
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		if e == nil {
			df.Close()
		}
		t.Fatal(e)
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("failed budget leak")
	}
}
