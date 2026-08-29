package parquet

import (
	"context"
	"gocolumn/memory"
	"os"
	"path/filepath"
	"testing"
	"time"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

func TestOpenFileReadsUTF8DateAndNormalizedTimestamps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "typed.parquet")
	stamp := time.Date(2024, time.February, 29, 23, 58, 59, 123456789, time.UTC)
	writeTypedFixture(t, path, stamp)

	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		name string
		typ  memory.DataType
	}{
		{"s", memory.TypeString}, {"d", memory.TypeDate32},
		{"ts_ms", memory.TypeTimestamp}, {"ts_us", memory.TypeTimestamp}, {"ts_ns", memory.TypeTimestamp},
	} {
		field, ok := src.Schema().Field(want.name)
		if !ok || field.Type != want.typ {
			t.Fatalf("field %q=%+v", want.name, field)
		}
	}

	r, err := src.NewReader(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	parts, err := src.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	if err := r.ReadPartition(context.Background(), parts[0], batch); err != nil {
		t.Fatal(err)
	}
	strings, err := batch.String("s")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(strings.ValueBytes(0)); got != "Go语言" {
		t.Fatalf("string row 0=%q", got)
	}
	if got := string(strings.ValueBytes(1)); got != "" {
		t.Fatalf("string row 1=%q", got)
	}
	if strings.IsValid(2) {
		t.Fatal("string row 2 should be NULL")
	}
	if got := string(strings.ValueBytes(3)); got != "prefix-end" {
		t.Fatalf("string row 3=%q", got)
	}
	days, _ := batch.Int32("d")
	if days.Values[0] != int32(stamp.Truncate(24*time.Hour).Unix()/86400) {
		t.Fatalf("date values=%v", days.Values)
	}
	for _, want := range []struct {
		name  string
		value int64
	}{
		{"ts_ms", stamp.UnixMilli() * int64(1e6)},
		{"ts_us", stamp.UnixMicro() * int64(1e3)},
		{"ts_ns", stamp.UnixNano()},
	} {
		values, err := batch.Int64(want.name)
		if err != nil {
			t.Fatal(err)
		}
		if values.Values[0] != want.value {
			t.Fatalf("%s=%d want=%d", want.name, values.Values[0], want.value)
		}
	}
}

func TestOpenFileRejectsRawByteArrayAndNonUTCTimestamp(t *testing.T) {
	for _, tt := range []struct {
		name string
		root func(t *testing.T) *schema.GroupNode
	}{
		{
			name: "raw byte array",
			root: func(t *testing.T) *schema.GroupNode {
				t.Helper()
				root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{
					schema.NewByteArrayNode("raw", arrowparquet.Repetitions.Required, -1),
				}, -1)
				if err != nil {
					t.Fatal(err)
				}
				return root
			},
		},
		{
			name: "non UTC timestamp",
			root: func(t *testing.T) *schema.GroupNode {
				t.Helper()
				ts, err := schema.NewPrimitiveNodeLogical("ts", arrowparquet.Repetitions.Required,
					schema.NewTimestampLogicalTypeWithOpts(schema.WithTSTimeUnitType(schema.TimeUnitMillis)), arrowparquet.Types.Int64, -1, -1)
				if err != nil {
					t.Fatal(err)
				}
				root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{ts}, -1)
				if err != nil {
					t.Fatal(err)
				}
				return root
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unsupported.parquet")
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			w := file.NewParquetWriter(f, tt.root(t))
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenFile(path); err == nil {
				t.Fatal("expected unsupported logical type error")
			}
		})
	}
}

func TestReaderRejectsInvalidUTF8AndTimestampOverflow(t *testing.T) {
	for _, tt := range []struct {
		name  string
		write func(t *testing.T, path string)
	}{
		{
			name: "invalid UTF-8",
			write: func(t *testing.T, path string) {
				t.Helper()
				text, err := schema.NewPrimitiveNodeLogical("s", arrowparquet.Repetitions.Required, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
				if err != nil {
					t.Fatal(err)
				}
				root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{text}, -1)
				if err != nil {
					t.Fatal(err)
				}
				f, err := os.Create(path)
				if err != nil {
					t.Fatal(err)
				}
				w := file.NewParquetWriter(f, root)
				rg := w.AppendRowGroup()
				writeTypedBytes(t, rg, []arrowparquet.ByteArray{[]byte{0xff}}, nil)
				if err := rg.Close(); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "timestamp overflow",
			write: func(t *testing.T, path string) {
				t.Helper()
				ts, err := schema.NewPrimitiveNodeLogical("ts", arrowparquet.Repetitions.Required,
					schema.NewTimestampLogicalTypeWithOpts(schema.WithTSIsAdjustedToUTC(), schema.WithTSTimeUnitType(schema.TimeUnitMillis)), arrowparquet.Types.Int64, -1, -1)
				if err != nil {
					t.Fatal(err)
				}
				root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{ts}, -1)
				if err != nil {
					t.Fatal(err)
				}
				f, err := os.Create(path)
				if err != nil {
					t.Fatal(err)
				}
				w := file.NewParquetWriter(f, root)
				rg := w.AppendRowGroup()
				writeTypedI64(t, rg, []int64{int64(^uint64(0) >> 1)})
				if err := rg.Close(); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "invalid.parquet")
			tt.write(t, path)
			src, err := OpenFile(path)
			if err != nil {
				t.Fatal(err)
			}
			r, err := src.NewReader(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			parts, err := src.Partitions(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := r.ReadPartition(context.Background(), parts[0], memory.NewBatch()); err == nil {
				t.Fatal("expected read failure")
			}
		})
	}
}

func writeTypedFixture(t *testing.T, path string, stamp time.Time) {
	t.Helper()
	text, err := schema.NewPrimitiveNodeLogical("s", arrowparquet.Repetitions.Optional, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	date, err := schema.NewPrimitiveNodeLogical("d", arrowparquet.Repetitions.Required, schema.DateLogicalType{}, arrowparquet.Types.Int32, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	timestamp := func(name string, unit schema.TimeUnitType) *schema.PrimitiveNode {
		t.Helper()
		node, err := schema.NewPrimitiveNodeLogical(name, arrowparquet.Repetitions.Required,
			schema.NewTimestampLogicalTypeWithOpts(schema.WithTSIsAdjustedToUTC(), schema.WithTSTimeUnitType(unit)), arrowparquet.Types.Int64, -1, -1)
		if err != nil {
			t.Fatal(err)
		}
		return node
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{
		text, date, timestamp("ts_ms", schema.TimeUnitMillis), timestamp("ts_us", schema.TimeUnitMicros), timestamp("ts_ns", schema.TimeUnitNanos),
	}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	props := arrowparquet.NewWriterProperties(arrowparquet.WithCompression(compress.Codecs.Uncompressed))
	w := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	rg := w.AppendRowGroup()
	writeTypedBytes(t, rg, []arrowparquet.ByteArray{[]byte("Go语言"), []byte(""), []byte("prefix-end")}, []int16{1, 1, 0, 1})
	days := int32(time.Date(stamp.Year(), stamp.Month(), stamp.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400)
	writeTypedI32(t, rg, []int32{days, days + 1, days + 2, days + 3})
	writeTypedI64(t, rg, []int64{stamp.UnixMilli(), stamp.Add(time.Second).UnixMilli(), stamp.Add(2 * time.Second).UnixMilli(), stamp.Add(3 * time.Second).UnixMilli()})
	writeTypedI64(t, rg, []int64{stamp.UnixMicro(), stamp.Add(time.Second).UnixMicro(), stamp.Add(2 * time.Second).UnixMicro(), stamp.Add(3 * time.Second).UnixMicro()})
	writeTypedI64(t, rg, []int64{stamp.UnixNano(), stamp.Add(time.Second).UnixNano(), stamp.Add(2 * time.Second).UnixNano(), stamp.Add(3 * time.Second).UnixNano()})
	if err := rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeTypedBytes(t *testing.T, rg file.SerialRowGroupWriter, values []arrowparquet.ByteArray, defs []int16) {
	t.Helper()
	column, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := column.(*file.ByteArrayColumnChunkWriter).WriteBatch(values, defs, nil); err != nil {
		t.Fatal(err)
	}
	if err := column.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeTypedI32(t *testing.T, rg file.SerialRowGroupWriter, values []int32) {
	t.Helper()
	column, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := column.(*file.Int32ColumnChunkWriter).WriteBatch(values, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := column.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeTypedI64(t *testing.T, rg file.SerialRowGroupWriter, values []int64) {
	t.Helper()
	column, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := column.(*file.Int64ColumnChunkWriter).WriteBatch(values, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := column.Close(); err != nil {
		t.Fatal(err)
	}
}
