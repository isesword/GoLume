package parquet_test

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/lazy"
	"os"
	"path/filepath"
	"testing"
	"time"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

func TestLazyParquetStringAndTemporalExpressions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.parquet")
	stamp := time.Date(2024, time.February, 29, 23, 58, 59, 0, time.UTC)
	writeLazyTypedFixture(t, path, stamp)

	session := lazy.NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.ScanParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := frame.
		Filter(expr.And(
			expr.And(lazy.Contains(lazy.Col("message"), lazy.Str("语")), lazy.Year(lazy.Col("event_time")).Eq(lazy.Lit(2024))),
			lazy.Col("event_time").Eq(lazy.TimestampMillis(stamp.UnixMilli())),
		)).
		WithColumns(
			lazy.Length(lazy.Col("message")).Alias("characters"),
			lazy.Day(lazy.Col("event_date")).Alias("day"),
			lazy.Hour(lazy.Col("event_time")).Alias("hour"),
		).
		SelectExprs(lazy.Col("characters").Alias("characters"), lazy.Col("day").Alias("day"), lazy.Col("hour").Alias("hour")).
		CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Height() != 1 {
		t.Fatalf("rows=%d", result.Height())
	}
	characters, _ := result.Column("characters")
	days, _ := result.Column("day")
	hours, _ := result.Column("hour")
	chars, _, _ := characters.CopyInt64()
	day, _, _ := days.CopyInt64()
	hour, _, _ := hours.CopyInt64()
	if chars[0] != 4 || day[0] != 29 || hour[0] != 23 {
		t.Fatalf("characters=%v day=%v hour=%v", chars, day, hour)
	}
}

func writeLazyTypedFixture(t *testing.T, path string, stamp time.Time) {
	t.Helper()
	message, err := schema.NewPrimitiveNodeLogical("message", arrowparquet.Repetitions.Required, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	date, err := schema.NewPrimitiveNodeLogical("event_date", arrowparquet.Repetitions.Required, schema.DateLogicalType{}, arrowparquet.Types.Int32, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	timestamp, err := schema.NewPrimitiveNodeLogical("event_time", arrowparquet.Repetitions.Required,
		schema.NewTimestampLogicalTypeWithOpts(schema.WithTSIsAdjustedToUTC(), schema.WithTSTimeUnitType(schema.TimeUnitMillis)), arrowparquet.Types.Int64, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{message, date, timestamp}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root)
	rg := w.AppendRowGroup()
	writeLazyTypedBytes(t, rg, []arrowparquet.ByteArray{[]byte("Go语言"), []byte("skip")})
	day := int32(time.Date(stamp.Year(), stamp.Month(), stamp.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400)
	writeLazyTypedI32(t, rg, []int32{day, day + 1})
	writeLazyTypedI64(t, rg, []int64{stamp.UnixMilli(), stamp.AddDate(-1, 0, 0).UnixMilli()})
	if err := rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeLazyTypedBytes(t *testing.T, rg file.SerialRowGroupWriter, values []arrowparquet.ByteArray) {
	t.Helper()
	column, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := column.(*file.ByteArrayColumnChunkWriter).WriteBatch(values, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := column.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeLazyTypedI32(t *testing.T, rg file.SerialRowGroupWriter, values []int32) {
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

func writeLazyTypedI64(t *testing.T, rg file.SerialRowGroupWriter, values []int64) {
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
