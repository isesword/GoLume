package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

var unsignedGroupRows = []int{73, 5, 67}
var unsigned32Edges = []uint32{0, math.MaxInt32, 1 << 31, math.MaxUint32}
var unsigned64Edges = []uint64{0, 1<<53 + 1, math.MaxInt64, 1 << 63, math.MaxUint64}

// Real files exercise physical encoders, logical/legacy footer annotations,
// null scatter, reused buffers, validity-word boundaries, and group transitions.
func TestParquetUnsignedLogicalTypes(t *testing.T) {
	for _, enc := range []struct {
		name       string
		dictionary bool
		encoding   arrowparquet.Encoding
	}{
		{"plain", false, arrowparquet.Encodings.Plain},
		{"dictionary", true, arrowparquet.Encodings.Plain},
		{"delta", false, arrowparquet.Encodings.DeltaBinaryPacked},
	} {
		for _, legacy := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/legacy=%v", enc.name, legacy), func(t *testing.T) {
				path := writeUnsignedFixture(t, arrowparquet.NewWriterProperties(
					arrowparquet.WithDictionaryDefault(enc.dictionary), arrowparquet.WithEncoding(enc.encoding)))
				if legacy {
					stripLogicalAnnotations(t, path)
				}
				src, err := OpenFile(path)
				if err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"u32r", "u32n", "u64r", "u64n"} {
					field, ok := src.Schema().Field(name)
					want := memory.TypeUint32
					if strings.HasPrefix(name, "u64") {
						want = memory.TypeUint64
					}
					if !ok || field.Type != want || field.Nullable != strings.HasSuffix(name, "n") {
						t.Fatalf("field %s=%+v", name, field)
					}
				}
				// Ensure the intended page encoding was actually emitted.
				chunk, err := src.meta.RowGroup(0).ColumnChunk(0)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, actual := range chunk.Encodings() {
					if enc.dictionary {
						found = found || actual == arrowparquet.Encodings.RLEDict || actual == arrowparquet.Encodings.PlainDict
					} else {
						found = found || actual == enc.encoding
					}
				}
				if !found {
					t.Fatalf("missing %s encoding: %v", enc.name, chunk.Encodings())
				}
				for _, rows := range []int{1, 3, 65} {
					for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
						opts := source.ReadOptions{Kernels: kernel.New(mode), BatchRows: rows, BatchBytes: 4096, MemoryManager: memory.NewBudgetManager(4096)}
						verifyUnsignedStream(t, src, opts)
					}
				}
			})
		}
	}
}

func verifyUnsignedStream(t *testing.T, src *Source, opts source.ReadOptions) {
	t.Helper()
	ctx := context.Background()
	stream, err := src.NewBatchStream(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	// A partially initialized destination must also receive the Uint64 map.
	batch := memory.NewBatch()
	batch.Uint64s = nil
	group, offset, total := 0, 0, 0
	for {
		ok, err := stream.Next(ctx, batch)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for i := 0; i < batch.Rows; i++ {
			if offset == unsignedGroupRows[group] {
				group++
				offset = 0
			}
			want32, want64 := unsigned32Edges[offset%len(unsigned32Edges)], unsigned64Edges[offset%len(unsigned64Edges)]
			valid := group != 2 && offset%3 != 1
			for _, nullable := range []bool{false, true} {
				suffix := "r"
				if nullable {
					suffix = "n"
				}
				v32, v64 := batch.Uint32s["u32"+suffix], batch.Uint64s["u64"+suffix]
				if v32 == nil || v64 == nil {
					t.Fatal("missing unsigned vectors")
				}
				if v32.Encoding != memory.EncodingPlain || v64.Encoding != memory.EncodingPlain {
					t.Fatal("non-plain output")
				}
				if v32.IsValid(i) != (!nullable || valid) || v64.IsValid(i) != (!nullable || valid) {
					t.Fatalf("validity group=%d row=%d", group, offset)
				}
				x32, x64 := want32, want64
				if nullable && !valid {
					x32, x64 = 0, 0
				}
				if v32.Values[i] != x32 || v64.Values[i] != x64 {
					t.Fatalf("%s group=%d row=%d got=(%d,%d) want=(%d,%d)", suffix, group, offset, v32.Values[i], v64.Values[i], x32, x64)
				}
			}
			offset++
			total++
		}
		if batch.Rows > opts.BatchRows || memory.BatchBytes(batch) > opts.BatchBytes {
			t.Fatalf("oversized batch rows=%d bytes=%d", batch.Rows, memory.BatchBytes(batch))
		}
	}
	if total != 145 {
		t.Fatalf("rows=%d", total)
	}
	if batch.Rows != 0 || len(batch.Uint64s) != 0 || len(batch.Uint32s) != 0 {
		t.Fatal("EOF retains unsigned output")
	}
	if opts.MemoryManager.Used() != 0 {
		t.Fatalf("leaked %d bytes", opts.MemoryManager.Used())
	}
}

func TestParquetUnsignedPushdownPreservesExactValues(t *testing.T) {
	for _, stats := range []bool{true, false} {
		path := writeUnsignedFixture(t, arrowparquet.NewWriterProperties(arrowparquet.WithStats(stats)))
		src, err := OpenFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name      string
			predicate expr.Expr
			matches   func(uint32, uint64, bool) bool
		}{
			{"uint32_high_bit", expr.GT(expr.Col("u32r"), expr.I64(math.MaxInt32)), func(u uint32, _ uint64, _ bool) bool { return u > math.MaxInt32 }},
			{"uint32_max", expr.EQ(expr.Col("u32r"), expr.I64(math.MaxUint32)), func(u uint32, _ uint64, _ bool) bool { return u == math.MaxUint32 }},
			{"uint64_high_bit", expr.GT(expr.Col("u64r"), expr.I64(math.MaxInt64)), func(_ uint32, u uint64, _ bool) bool { return u > math.MaxInt64 }},
			{"uint64_exact_53", expr.GT(expr.Col("u64r"), expr.I64(1<<53)), func(_ uint32, u uint64, _ bool) bool { return u > 1<<53 }},
			{"uint64_not_negative", expr.Not(expr.LT(expr.Col("u64r"), expr.I64(0))), func(_ uint32, _ uint64, _ bool) bool { return true }},
			{"uint64_not_signed_max", expr.EQ(expr.Col("u64r"), expr.I64(-1)), func(_ uint32, _ uint64, _ bool) bool { return false }},
			{"uint64_not_rounded_max", expr.EQ(expr.Col("u64r"), expr.F64(0x1p64)), func(_ uint32, _ uint64, _ bool) bool { return false }},
			{"nullable_high_bit", expr.GT(expr.Col("u64n"), expr.I64(math.MaxInt64)), func(_ uint32, u uint64, valid bool) bool { return valid && u > math.MaxInt64 }},
			{"is_null", expr.IsNull(expr.Col("u64n")), func(_ uint32, _ uint64, valid bool) bool { return !valid }},
		} {
			t.Run(fmt.Sprintf("stats=%v/%s", stats, tc.name), func(t *testing.T) {
				pushed, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"u32r"}, Predicate: tc.predicate})
				if err != nil {
					t.Fatal(err)
				}
				want := 0
				for g, rows := range unsignedGroupRows {
					for i := 0; i < rows; i++ {
						if tc.matches(unsigned32Edges[i%4], unsigned64Edges[i%5], g != 2 && i%3 != 1) {
							want++
						}
					}
				}
				if stats && tc.name == "nullable_high_bit" && report.PrunedPartitions != 1 {
					t.Fatalf("all-null group not pruned: %+v", report)
				}
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
					for _, input := range []source.Source{src, pushed} {
						if got := countExactFloatMatches(t, input, tc.predicate, mode); got != want {
							t.Fatalf("%s matched=%d want=%d report=%+v", mode, got, want, report)
						}
					}
				}
			})
		}
	}
}

func TestParquetUnsignedBudgetsIncludeScratchAndPayload(t *testing.T) {
	path := writeUnsignedFixture(t, arrowparquet.NewWriterProperties())
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name           string
		batch, payload int64
		fail           bool
	}{
		{"one_row_exact", 54, 40, false}, // 24 output + 16 validity + 12 dense scratch + 2 defs.
		{"scratch_short", 53, 0, true},
		{"payload_short", 54, 39, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := testReadOptions()
			opts.BatchRows = 65
			opts.BatchBytes = tc.batch
			opts.BatchPayloadBytes = tc.payload
			opts.MemoryManager = memory.NewBudgetManager(tc.batch)
			stream, err := src.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			batch := memory.NewBatch()
			ok, err := stream.Next(context.Background(), batch)
			if tc.fail {
				if ok || !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatalf("ok=%v err=%v", ok, err)
				}
			} else if !ok || err != nil || batch.Rows != 1 || opts.MemoryManager.Used() != 54 {
				t.Fatalf("ok=%v rows=%d used=%d err=%v", ok, batch.Rows, opts.MemoryManager.Used(), err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if opts.MemoryManager.Used() != 0 {
				t.Fatal("leaked reservation")
			}
		})
	}
}

func writeUnsignedFixture(t *testing.T, props *arrowparquet.WriterProperties) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "unsigned.parquet")
	var fields schema.FieldList
	for _, bits := range []int8{32, 64} {
		for _, nullable := range []bool{false, true} {
			physical, repetition, suffix := arrowparquet.Types.Int32, arrowparquet.Repetitions.Required, "r"
			if bits == 64 {
				physical = arrowparquet.Types.Int64
			}
			if nullable {
				repetition, suffix = arrowparquet.Repetitions.Optional, "n"
			}
			node, err := schema.NewPrimitiveNodeLogical(fmt.Sprintf("u%d%s", bits, suffix), repetition, schema.NewIntLogicalType(bits, false), physical, -1, -1)
			if err != nil {
				t.Fatal(err)
			}
			fields = append(fields, node)
		}
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, fields, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	for g, rows := range unsignedGroupRows {
		rg := w.AppendRowGroup()
		for c := 0; c < 4; c++ {
			column, err := rg.NextColumn()
			if err != nil {
				t.Fatal(err)
			}
			var defs []int16
			if c%2 != 0 {
				defs = make([]int16, rows)
			}
			var v32 []int32
			var v64 []int64
			for i := 0; i < rows; i++ {
				if defs != nil {
					if g == 2 || i%3 == 1 {
						continue
					}
					defs[i] = 1
				}
				v32 = append(v32, int32(unsigned32Edges[i%4]))
				v64 = append(v64, int64(unsigned64Edges[i%5]))
			}
			if c < 2 {
				_, err = column.(*file.Int32ColumnChunkWriter).WriteBatch(v32, defs, nil)
			} else {
				_, err = column.(*file.Int64ColumnChunkWriter).WriteBatch(v64, defs, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := column.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// Arrow emits both modern and converted annotations. Rewrite only the footer
// to exercise genuine legacy ConvertedType-only input, preserving data pages.
func stripLogicalAnnotations(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	length := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	start := len(data) - 8 - length
	meta, err := metadata.NewFileMetaData(data[start:len(data)-8], nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range meta.FileMetaData.Schema {
		field.LogicalType = nil
	}
	footer, err := meta.Serialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := append(data[:start:start], footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	out = append(out, "PAR1"...)
	if err := os.WriteFile(path, out, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestParquetRejectsUnsupportedLogicalAnnotations(t *testing.T) {
	for _, tc := range []struct {
		name     string
		logical  schema.LogicalType
		physical arrowparquet.Type
		length   int
		legacy   bool
	}{
		{"decimal256", schema.NewDecimalLogicalType(39, 4), arrowparquet.Types.ByteArray, -1, true},
		{"time_millis_utc", schema.NewTimeLogicalType(true, schema.TimeUnitMillis), arrowparquet.Types.Int32, -1, true},
		{"time_micros_utc", schema.NewTimeLogicalType(true, schema.TimeUnitMicros), arrowparquet.Types.Int64, -1, true},
		{"time_nanos_local", schema.NewTimeLogicalType(false, schema.TimeUnitNanos), arrowparquet.Types.Int64, -1, false},
		{"unknown_int32", schema.NullLogicalType{}, arrowparquet.Types.Int32, -1, false},
		{"unknown_int64", schema.NullLogicalType{}, arrowparquet.Types.Int64, -1, false},
		{"unknown_bool", schema.NullLogicalType{}, arrowparquet.Types.Boolean, -1, false},
		{"unknown_float", schema.NullLogicalType{}, arrowparquet.Types.Float, -1, false},
		{"unknown_double", schema.NullLogicalType{}, arrowparquet.Types.Double, -1, false},
		{"enum", schema.EnumLogicalType{}, arrowparquet.Types.ByteArray, -1, true},
		{"json", schema.JSONLogicalType{}, arrowparquet.Types.ByteArray, -1, true},
		{"bson", schema.BSONLogicalType{}, arrowparquet.Types.ByteArray, -1, true},
		{"uuid", schema.UUIDLogicalType{}, arrowparquet.Types.FixedLenByteArray, 16, false},
		{"interval", schema.IntervalLogicalType{}, arrowparquet.Types.FixedLenByteArray, 12, true},
		{"float16", schema.Float16LogicalType{}, arrowparquet.Types.FixedLenByteArray, 2, false},
	} {
		for _, legacy := range []bool{false, true} {
			if legacy && !tc.legacy {
				continue
			}
			t.Run(fmt.Sprintf("%s/legacy=%v", tc.name, legacy), func(t *testing.T) {
				node, err := schema.NewPrimitiveNodeLogical("value", arrowparquet.Repetitions.Optional, tc.logical, tc.physical, tc.length, -1)
				if err != nil {
					t.Fatal(err)
				}
				path := writeAnnotatedFixture(t, node)
				if legacy {
					stripLogicalAnnotations(t, path)
				}
				_, err = OpenFile(path)
				if err == nil || !strings.Contains(err.Error(), `column "value" logical type`) || !strings.Contains(err.Error(), "not supported") {
					t.Fatalf("expected explicit annotation error, got %v", err)
				}
				if strings.HasPrefix(tc.name, "decimal") && !strings.Contains(err.Error(), "precision") {
					t.Fatalf("decimal error omits semantics: %v", err)
				}
			})
		}
	}
	t.Run("legacy_unknown", func(t *testing.T) {
		node, err := schema.NewPrimitiveNodeConverted("value", arrowparquet.Repetitions.Optional, arrowparquet.Types.Int32, schema.ConvertedTypes.NA, -1, -1, -1, -1)
		if err != nil {
			t.Fatal(err)
		}
		path := writeAnnotatedFixture(t, node)
		if _, err := OpenFile(path); err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Fatalf("legacy unknown accepted: %v", err)
		}
	})
}

func writeAnnotatedFixture(t *testing.T, node *schema.PrimitiveNode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "annotated.parquet")
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{node}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithDictionaryDefault(false), arrowparquet.WithStats(false))))
	rg := w.AppendRowGroup()
	c, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	defs := []int16{1, 0, 1, 1}
	count := 3
	switch node.LogicalType().(type) {
	case schema.NullLogicalType, schema.UnknownLogicalType:
		clear(defs)
		count = 0
	}
	switch col := c.(type) {
	case *file.Int32ColumnChunkWriter:
		_, err = col.WriteBatch([]int32{12345, -12345, 0}[:count], defs, nil)
	case *file.Int64ColumnChunkWriter:
		_, err = col.WriteBatch([]int64{12345, -12345, 0}[:count], defs, nil)
	case *file.BooleanColumnChunkWriter:
		_, err = col.WriteBatch([]bool{true, false, true}[:count], defs, nil)
	case *file.Float32ColumnChunkWriter:
		_, err = col.WriteBatch([]float32{1, -1, 0}[:count], defs, nil)
	case *file.Float64ColumnChunkWriter:
		_, err = col.WriteBatch([]float64{1, -1, 0}[:count], defs, nil)
	case *file.ByteArrayColumnChunkWriter:
		_, err = col.WriteBatch([]arrowparquet.ByteArray{{0x30, 0x39}, {0xcf, 0xc7}, {0}}[:count], defs, nil)
	case *file.FixedLenByteArrayColumnChunkWriter:
		values := make([]arrowparquet.FixedLenByteArray, count)
		for i := range values {
			values[i] = make([]byte, node.TypeLength())
			values[i][len(values[i])-1] = byte(i)
		}
		_, err = col.WriteBatch(values, defs, nil)
	default:
		t.Fatalf("unexpected fixture writer %T", c)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParquetRequiredUnsignedProjectionScratch(t *testing.T) {
	path := writeUnsignedFixture(t, arrowparquet.NewWriterProperties())
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		width int64
	}{{"u32r", 4}, {"u64r", 8}} {
		t.Run(tc.name, func(t *testing.T) {
			input, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{tc.name}})
			if err != nil {
				t.Fatal(err)
			}
			for _, short := range []bool{false, true} {
				limit := tc.width * 2 // One output value and one physical decode scratch value.
				if short {
					limit--
				}
				opts := testReadOptions()
				opts.BatchRows = 65
				opts.BatchBytes = limit
				opts.MemoryManager = memory.NewBudgetManager(limit)
				stream, err := input.NewBatchStream(context.Background(), opts)
				if err != nil {
					t.Fatal(err)
				}
				batch := memory.NewBatch()
				group, offset, total := 0, 0, 0
				for {
					ok, err := stream.Next(context.Background(), batch)
					if short {
						if ok || !errors.Is(err, memory.ErrBudgetExceeded) {
							t.Fatalf("short scratch budget ok=%v err=%v", ok, err)
						}
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					if batch.Rows != 1 || opts.MemoryManager.Used() != limit {
						t.Fatalf("rows=%d used=%d want=%d", batch.Rows, opts.MemoryManager.Used(), limit)
					}
					if offset == unsignedGroupRows[group] {
						group++
						offset = 0
					}
					if tc.width == 4 {
						v := batch.Uint32s[tc.name]
						if v.Validity != nil || v.Values[0] != unsigned32Edges[offset%4] {
							t.Fatalf("row %d uint32=%+v", total, v)
						}
					} else {
						v := batch.Uint64s[tc.name]
						if v.Validity != nil || v.Values[0] != unsigned64Edges[offset%5] {
							t.Fatalf("row %d uint64=%+v", total, v)
						}
					}
					offset++
					total++
				}
				if !short && total != 145 {
					t.Fatalf("rows=%d", total)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				if opts.MemoryManager.Used() != 0 {
					t.Fatal("leaked required-only scratch")
				}
			}
		})
	}
}
