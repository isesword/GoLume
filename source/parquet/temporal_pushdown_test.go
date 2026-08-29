package parquet

import (
	"context"
	"gocolumn/expr"
	"gocolumn/source"
	"os"
	"path/filepath"
	"testing"
	"time"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

func TestApplyPushdownTemporalExtractionsAndExplicitTimestampUnits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "temporal-pushdown.parquet")
	stamps := writeTemporalPushdownFixture(t, path)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name      string
		predicate expr.Expr
		want      []int
	}{
		{
			name:      "timestamp millis statistics normalize to nanos",
			predicate: expr.EQ(expr.Col("ts"), expr.TimestampMillis(stamps[1].UnixMilli())),
			want:      []int{1},
		},
		{
			name:      "year extraction",
			predicate: expr.EQ(expr.Year(expr.Col("ts")), expr.I64(2024)),
			want:      []int{1},
		},
		{
			name:      "date day extraction in a single day bucket",
			predicate: expr.EQ(expr.Day(expr.Col("d")), expr.I64(29)),
			want:      []int{1},
		},
		{
			name:      "timestamp hour extraction in a single hour bucket",
			predicate: expr.EQ(expr.Hour(expr.Col("ts")), expr.I64(23)),
			want:      []int{1},
		},
		{
			name:      "impossible month domain",
			predicate: expr.EQ(expr.Month(expr.Col("d")), expr.I64(13)),
			want:      nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pushed, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{
				Projection: []string{"ts"},
				Predicate:  tt.predicate,
			})
			if err != nil {
				t.Fatal(err)
			}
			parts, err := pushed.Partitions(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if report.PrunedPartitions != 3-len(tt.want) || len(parts) != len(tt.want) {
				t.Fatalf("report=%+v parts=%v", report, parts)
			}
			for i, want := range tt.want {
				if parts[i].ID != want {
					t.Fatalf("parts=%v want IDs=%v", parts, tt.want)
				}
			}
		})
	}
}

func TestApplyPushdownStringPredicatePrunesAllNullRowGroup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "string-null-pushdown.parquet")
	writeTemporalPushdownFixture(t, path)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pushed, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{
		Projection: []string{"d"},
		Predicate:  expr.Contains(expr.Col("s"), expr.Str("needle")),
	})
	if err != nil {
		t.Fatal(err)
	}
	parts, err := pushed.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.PrunedPartitions != 1 || len(parts) != 2 || parts[0].ID != 1 || parts[1].ID != 2 {
		t.Fatalf("report=%+v parts=%v", report, parts)
	}
}

func writeTemporalPushdownFixture(t *testing.T, path string) []time.Time {
	t.Helper()
	text, err := schema.NewPrimitiveNodeLogical("s", arrowparquet.Repetitions.Optional, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	date, err := schema.NewPrimitiveNodeLogical("d", arrowparquet.Repetitions.Required, schema.DateLogicalType{}, arrowparquet.Types.Int32, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	timestamp, err := schema.NewPrimitiveNodeLogical("ts", arrowparquet.Repetitions.Required,
		schema.NewTimestampLogicalTypeWithOpts(schema.WithTSIsAdjustedToUTC(), schema.WithTSTimeUnitType(schema.TimeUnitMillis)), arrowparquet.Types.Int64, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{text, date, timestamp}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	props := arrowparquet.NewWriterProperties(
		arrowparquet.WithCompression(compress.Codecs.Uncompressed),
		arrowparquet.WithDictionaryDefault(false),
	)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	stamps := []time.Time{
		time.Date(2023, time.January, 1, 10, 0, 0, 0, time.UTC),
		time.Date(2024, time.February, 29, 23, 10, 0, 0, time.UTC),
		time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC),
	}
	for index, stamp := range stamps {
		rg := w.AppendRowGroup()
		if index == 0 {
			writeTypedBytes(t, rg, nil, []int16{0, 0})
		} else if index == 1 {
			writeTypedBytes(t, rg, []arrowparquet.ByteArray{[]byte("needle"), []byte("needle again")}, []int16{1, 1})
		} else {
			writeTypedBytes(t, rg, []arrowparquet.ByteArray{[]byte("other"), []byte("value")}, []int16{1, 1})
		}
		day := int32(stamp.Truncate(24*time.Hour).Unix() / int64(24*time.Hour/time.Second))
		writeTypedI32(t, rg, []int32{day, day})
		writeTypedI64(t, rg, []int64{stamp.UnixMilli(), stamp.Add(time.Second).UnixMilli()})
		if err := rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return stamps
}
