package parquet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/expr"
	"gocolumn/source"
)

// This is a real, generated Parquet file. It is deliberately independent of
// the repository's original million-row fixture and measures metadata only.
func metadataCacheFixture(tb testing.TB, groups, columns int, stats bool) *Source {
	tb.Helper()
	fields := schema.FieldList{}
	for i := range columns {
		fields = append(fields, schema.NewInt64Node(fmt.Sprintf("c%d", i), arrowparquet.Repetitions.Optional, -1))
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, fields, -1)
	if err != nil {
		tb.Fatal(err)
	}
	path := filepath.Join(tb.TempDir(), "metadata-cache.parquet")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithDictionaryDefault(false), arrowparquet.WithStats(stats))))
	for group := range groups {
		rg := w.AppendRowGroup()
		for col := range columns {
			column, err := rg.NextColumn()
			if err != nil {
				tb.Fatal(err)
			}
			v := int64(group*10 + col)
			if _, err = column.(*file.Int64ColumnChunkWriter).WriteBatch([]int64{v, v + 1, v + 2}, []int16{1, 1, 0, 1}, nil); err != nil {
				tb.Fatal(err)
			}
			if err = column.Close(); err != nil {
				tb.Fatal(err)
			}
		}
		if err := rg.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	s, err := OpenFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

func metadataCachePredicates() []struct {
	name      string
	predicate expr.Expr
} {
	x, y := expr.Col("c0"), expr.Col("c1")
	var wide expr.Expr = expr.GE(x, expr.I64(0))
	for column := 1; column < 8; column++ {
		wide = expr.And(wide, expr.GE(expr.Col(fmt.Sprintf("c%d", column)), expr.I64(0)))
	}
	return []struct {
		name      string
		predicate expr.Expr
	}{
		{"none", nil},
		{"wide_distinct", wide},
		{"wide_repeated", expr.And(wide, wide)},
		{"single", expr.GE(x, expr.I64(0))},
		{"range", expr.And(expr.GE(x, expr.I64(0)), expr.LT(x, expr.I64(1000)))},
		{"distinct", expr.And(expr.GE(x, expr.I64(0)), expr.LT(y, expr.I64(1000)))},
		{"interleaved", expr.And(expr.And(expr.GE(x, expr.I64(0)), expr.GE(y, expr.I64(0))), expr.And(expr.LT(x, expr.I64(1000)), expr.LT(y, expr.I64(1000))))},
		{"in_null_range", expr.And(expr.And(expr.IsNotNull(x), expr.GE(x, expr.I64(0))), expr.And(expr.LT(x, expr.I64(1000)), expr.In(x, expr.I64(0), expr.I64(1), expr.I64(10))))},
	}
}

func BenchmarkParquetMetadataPushdown(b *testing.B) {
	for _, stats := range []bool{false, true} {
		s := metadataCacheFixture(b, 128, 8, stats)
		for _, tc := range metadataCachePredicates() {
			b.Run(fmt.Sprintf("stats=%v/%s", stats, tc.name), func(b *testing.B) {
				req := source.PushdownRequest{Projection: []string{"c0"}, Predicate: tc.predicate}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if _, _, err := s.ApplyPushdown(context.Background(), req); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
