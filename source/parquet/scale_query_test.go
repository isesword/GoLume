package parquet_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/engine"
	"gocolumn/kernel"
	"gocolumn/lazy"
	"gocolumn/memory"
	"gocolumn/source"
	parquet "gocolumn/source/parquet"
)

func scaleTimestampFixture(t testing.TB, n int, nullable bool, badAt int) string {
	t.Helper()
	var fields schema.FieldList
	rep := arrowparquet.Repetitions.Required
	if nullable {
		rep = arrowparquet.Repetitions.Optional
	}
	for i, unit := range []schema.TimeUnitType{schema.TimeUnitMillis, schema.TimeUnitMicros, schema.TimeUnitNanos} {
		col, err := schema.NewPrimitiveNodeLogical(fmt.Sprintf("ts%d", i), rep, schema.NewTimestampLogicalTypeWithOpts(schema.WithTSIsAdjustedToUTC(), schema.WithTSTimeUnitType(unit)), arrowparquet.Types.Int64, -1, -1)
		if err != nil {
			t.Fatal(err)
		}
		fields = append(fields, col)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, fields, -1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "scale.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithDictionaryDefault(false))))
	rg := w.AppendRowGroup()
	for col, scale := range []int64{1000000, 1000, 1} {
		var values []int64
		var defs []int16
		for row := 0; row < n; row++ {
			if nullable {
				if row%7 == 0 && row != badAt {
					defs = append(defs, 0)
					continue
				}
				defs = append(defs, 1)
			}
			v := int64(row-130) * 1000000000 / scale
			if row == badAt && col == 0 {
				v = math.MaxInt64
			}
			values = append(values, v)
		}
		c, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.(*file.Int64ColumnChunkWriter).WriteBatch(values, defs, nil); err != nil {
			t.Fatal(err)
		}
		if err = c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err = rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestTimestampScaleParquetModes(t *testing.T) {
	ctx := context.Background()
	const n = 259
	for _, nullable := range []bool{false, true} {
		path := scaleTimestampFixture(t, n, nullable, -1)
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			for _, batchRows := range []int{1, 7, 65, 129, 1024} {
				src, err := parquet.OpenFile(path)
				if err != nil {
					t.Fatal(err)
				}
				opts := source.ReadOptions{Kernels: kernel.New(mode), BatchRows: batchRows, BatchBytes: 1 << 20, MemoryManager: memory.NewBudgetManager(8 << 20)}
				stream, err := source.OpenBatchStream(ctx, src, opts)
				if err != nil {
					t.Fatal(err)
				}
				batch := memory.NewBatch()
				row := 0
				for {
					ok, err := stream.Next(ctx, batch)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					for i := 0; i < batch.Rows; i++ {
						for col := 0; col < 3; col++ {
							v, err := batch.Int64(fmt.Sprintf("ts%d", col))
							if err != nil {
								t.Fatal(err)
							}
							valid := !nullable || row%7 != 0
							if v.IsValid(i) != valid || valid && v.Values[i] != int64(row-130)*1000000000 {
								t.Fatalf("%s nullable%v batch%d row%d col%d value%d", mode, nullable, batchRows, row, col, v.Values[i])
							}
						}
						row++
					}
					batch.ClearColumns()
				}
				if err = stream.Close(); err != nil {
					t.Fatal(err)
				}
				if row != n {
					t.Fatalf("rows%d", row)
				}
				if opts.MemoryManager.Used() != 0 {
					t.Fatal("retained query memory")
				}
			}
			opts := engine.DefaultOptions()
			opts.Workers = 1
			opts.KernelMode = mode
			session := lazy.NewSession(opts)
			scan, err := session.ScanParquet(path)
			if err != nil {
				t.Fatal(err)
			}
			result, err := scan.Filter(lazy.Col("ts0").Gt(lazy.Timestamp(0))).Select("ts0", "ts1", "ts2").Collect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			wantRows := 0
			for row := 131; row < n; row++ {
				if !nullable || row%7 != 0 {
					wantRows++
				}
			}
			if result.Height() != int64(wantRows) {
				t.Fatalf("query rows%d want%d", result.Height(), wantRows)
			}
			for col := 0; col < 3; col++ {
				s, err := result.Column(fmt.Sprintf("ts%d", col))
				if err != nil {
					t.Fatal(err)
				}
				v, valid, err := s.CopyInt64()
				if err != nil {
					t.Fatal(err)
				}
				i := 0
				for row := 131; row < n; row++ {
					if nullable && row%7 == 0 {
						continue
					}
					if v[i] != int64(row-130)*1000000000 || valid != nil && !memory.IsSet(valid, i) {
						t.Fatal("query value mismatch")
					}
					i++
				}
			}
			result.Close()
			session.Close()
		}
	}
}

func TestTimestampScaleParquetOverflowRow(t *testing.T) {
	for _, badAt := range []int{0, 1, 7, 8, 63, 64, 65, 128, 129} {
		path := scaleTimestampFixture(t, 131, true, badAt)
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			src, err := parquet.OpenFile(path)
			if err != nil {
				t.Fatal(err)
			}
			opts := source.ReadOptions{Kernels: kernel.New(mode), BatchRows: 131, BatchBytes: 1 << 20, MemoryManager: memory.NewBudgetManager(8 << 20)}
			s, err := source.OpenBatchStream(context.Background(), src, opts)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.Next(context.Background(), memory.NewBatch())
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("column \"ts0\" timestamp row %d overflows UTC nanosecond normalization", badAt)) {
				t.Fatalf("%s bad%d: %v", mode, badAt, err)
			}
			s.Close()
			if opts.MemoryManager.Used() != 0 {
				t.Fatal("failure leaked memory")
			}
		}
	}
}

func BenchmarkTimestampScaleParquetQuery(b *testing.B) {
	for _, nullable := range []bool{false, true} {
		path := scaleTimestampFixture(b, 65536, nullable, -1)
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			opts := engine.DefaultOptions()
			opts.KernelMode = mode
			opts.Workers = 1
			session := lazy.NewSession(opts)
			scan, err := session.ScanParquet(path)
			if err != nil {
				b.Fatal(err)
			}
			prepared, err := scan.Select("ts0", "ts1", "ts2").Prepare(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("nullable%v/%s", nullable, mode), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					result, err := prepared.Execute(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					if result.Height() != 65536 {
						b.Fatal("rows")
					}
					if err = result.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})
			prepared.Close()
			session.Close()
		}
	}
}
