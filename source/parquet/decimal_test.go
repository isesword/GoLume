package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
)

var decimalGroupRows = []int{0, 73, 3, 67}

func decimalValues(precision int32) []memory.Decimal128 {
	max := decimal128.GetScaleMultiplier(int(precision)).Sub(decimal128.FromI64(1))
	return []memory.Decimal128{{}, decimal128.FromI64(1), decimal128.FromI64(-1), max, max.Negate(), max.Sub(decimal128.FromI64(2))}
}
func decimalBytes(value memory.Decimal128, length int) []byte {
	if length < 16 {
		return decimalBytes(value, 16)[16-length:]
	}
	b := make([]byte, length)
	if value.Sign() < 0 {
		for i := range b {
			b[i] = 0xff
		}
	}
	binary.BigEndian.PutUint64(b[length-16:], uint64(value.HighBits()))
	binary.BigEndian.PutUint64(b[length-8:], value.LowBits())
	return b
}
func writeDecimalFixture(t *testing.T, physical arrowparquet.Type, length int, precision, scale int32, nullable bool, groups []int, values []memory.Decimal128, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	rep := arrowparquet.Repetitions.Required
	if nullable {
		rep = arrowparquet.Repetitions.Optional
	}
	node, err := schema.NewPrimitiveNodeLogical("amount", rep, schema.NewDecimalLogicalType(precision, scale), physical, length, -1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{node}, -1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "decimal.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	props = append([]arrowparquet.WriterProperty{arrowparquet.WithDataPageSize(64), arrowparquet.WithBatchSize(7)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(props...)))
	for group, n := range groups {
		rg := w.AppendRowGroup()
		c, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		var defs []int16
		if nullable {
			defs = make([]int16, n)
		}
		var i32 []int32
		var i64 []int64
		var ba []arrowparquet.ByteArray
		var fixed []arrowparquet.FixedLenByteArray
		for row := 0; row < n; row++ {
			valid := !nullable || group != 3 && row%3 != 1
			if !valid {
				continue
			}
			if nullable {
				defs[row] = 1
			}
			value := values[row%len(values)]
			i32 = append(i32, int32(value.LowBits()))
			i64 = append(i64, int64(value.LowBits()))
			ba = append(ba, decimalBytes(value, 17))
			if length > 0 {
				fixed = append(fixed, decimalBytes(value, length))
			}
		}
		switch col := c.(type) {
		case *file.Int32ColumnChunkWriter:
			_, err = col.WriteBatch(i32, defs, nil)
		case *file.Int64ColumnChunkWriter:
			_, err = col.WriteBatch(i64, defs, nil)
		case *file.ByteArrayColumnChunkWriter:
			_, err = col.WriteBatch(ba, defs, nil)
		case *file.FixedLenByteArrayColumnChunkWriter:
			_, err = col.WriteBatch(fixed, defs, nil)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = c.Close(); err != nil {
			t.Fatal(err)
		}
		if err = rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParquetDecimal128AllPhysicalEncodings(t *testing.T) {
	for _, tc := range []struct {
		physical         arrowparquet.Type
		length           int
		precision, scale int32
	}{
		{arrowparquet.Types.FixedLenByteArray, 1, 2, 0}, {arrowparquet.Types.FixedLenByteArray, 2, 4, 1}, {arrowparquet.Types.FixedLenByteArray, 4, 9, 3}, {arrowparquet.Types.FixedLenByteArray, 8, 18, 6}, {arrowparquet.Types.Int32, -1, 9, 2}, {arrowparquet.Types.Int64, -1, 18, 4}, {arrowparquet.Types.ByteArray, -1, 38, 18}, {arrowparquet.Types.FixedLenByteArray, 16, 38, 38}, {arrowparquet.Types.FixedLenByteArray, 32, 38, 0},
	} {
		for _, nullable := range []bool{false, true} {
			for _, dictionary := range []bool{false, true} {
				for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
					for _, legacy := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/len=%d/null=%v/dict=%v/page=%v/legacy=%v", tc.physical, tc.length, nullable, dictionary, version, legacy), func(t *testing.T) {
							values := decimalValues(tc.precision)
							path := writeDecimalFixture(t, tc.physical, tc.length, tc.precision, tc.scale, nullable, decimalGroupRows, values, arrowparquet.WithDictionaryDefault(dictionary), arrowparquet.WithDataPageVersion(version))
							if legacy {
								stripLogicalAnnotations(t, path)
							}
							src, err := OpenFile(path)
							if err != nil {
								t.Fatal(err)
							}
							field := src.Schema().Fields[0]
							if field.Type != memory.TypeDecimal || field.DecimalPrecision != tc.precision || field.DecimalScale != tc.scale || field.Nullable != nullable {
								t.Fatalf("schema=%+v", field)
							}
							for _, batchRows := range []int{1, 65} {
								opts := testReadOptions()
								opts.BatchRows = batchRows
								opts.BatchBytes = 1200
								opts.BatchPayloadBytes = 1100
								opts.MemoryManager = memory.NewBudgetManager(1200)
								st, err := src.NewBatchStream(context.Background(), opts)
								if err != nil {
									t.Fatal(err)
								}
								batch := memory.NewBatch()
								batch.Decimals = nil
								group, row, total := 1, 0, 0
								for {
									ok, err := st.Next(context.Background(), batch)
									if err != nil {
										t.Fatal(err)
									}
									if !ok {
										break
									}
									v := batch.Decimals["amount"]
									if err := v.ValidateField(field, batch.Rows); err != nil {
										t.Fatal(err)
									}
									if memory.BatchBytes(batch) > opts.BatchPayloadBytes || batch.Rows > batchRows || opts.MemoryManager.Used() > 1200 {
										t.Fatal("unbounded native decimal batch")
									}
									record, err := memory.ArrowRecord(arrowmem.DefaultAllocator, src.Schema(), batch)
									if err != nil {
										t.Fatal(err)
									}
									arr := record.Column(0).(*array.Decimal128)
									for i := 0; i < batch.Rows; i++ {
										if row == decimalGroupRows[group] {
											group++
											row = 0
										}
										valid := !nullable || group != 3 && row%3 != 1
										want := values[row%len(values)]
										if !valid {
											want = memory.Decimal128{}
										}
										if v.IsValid(i) != valid || v.Values[i] != want || arr.IsValid(i) != valid || valid && arr.Value(i) != want {
											t.Fatalf("group=%d row=%d got=%v valid=%v want=%v valid=%v", group, row, v.Values[i], v.IsValid(i), want, valid)
										}
										row++
										total++
									}
									record.Release()
								}
								if total != 143 {
									t.Fatalf("rows=%d", total)
								}
								if opts.MemoryManager.Used() != 0 || len(batch.Decimals) != 0 {
									t.Fatal("EOF leaked decimal buffers")
								}
								if err := st.Close(); err != nil {
									t.Fatal(err)
								}
							}
						})
					}
				}
			}
		}
	}
}

func TestParquetDecimal128BudgetFailureAndOverflow(t *testing.T) {
	for _, physical := range []arrowparquet.Type{arrowparquet.Types.Int32, arrowparquet.Types.Int64, arrowparquet.Types.ByteArray, arrowparquet.Types.FixedLenByteArray} {
		length := -1
		if physical == arrowparquet.Types.FixedLenByteArray {
			length = 16
		}
		path := writeDecimalFixture(t, physical, length, 2, 1, true, []int{3}, []memory.Decimal128{decimal128.FromI64(100)}, arrowparquet.WithDictionaryDefault(false))
		src, err := OpenFile(path)
		if err != nil {
			t.Fatal(err)
		}
		opts := testReadOptions()
		opts.BatchRows = 3
		opts.BatchBytes = 128
		opts.MemoryManager = memory.NewBudgetManager(128)
		st, err := src.NewBatchStream(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		dst := memory.NewBatch()
		ok, err := st.Next(context.Background(), dst)
		if ok || err == nil || !strings.Contains(err.Error(), "precision") {
			t.Fatalf("physical=%v overflow=%v", physical, err)
		}
		if dst.Rows != 0 || len(dst.Decimals) != 0 || opts.MemoryManager.Used() != 0 {
			t.Fatal("overflow leaked output/reservation")
		}
		st.Close()
	}
	path := writeDecimalFixture(t, arrowparquet.Types.Int32, -1, 9, 2, true, []int{3}, decimalValues(9))
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int64{1, 23, 24} {
		opts := testReadOptions()
		opts.BatchRows = 65
		opts.BatchBytes = limit
		opts.MemoryManager = memory.NewBudgetManager(limit)
		st, err := src.NewBatchStream(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		batch := memory.NewBatch()
		ok, err := st.Next(context.Background(), batch)
		if limit < 24 {
			if ok || !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("limit=%d ok=%v error=%v", limit, ok, err)
			}
		} else if !ok || err != nil || batch.Rows != 1 || opts.MemoryManager.Used() != 24 {
			t.Fatalf("exact row budget: ok=%v rows=%d used=%d err=%v", ok, batch.Rows, opts.MemoryManager.Used(), err)
		}
		st.Close()
		if opts.MemoryManager.Used() != 0 {
			t.Fatal("close leaked decimal buffer")
		}
	}
	// Until typed comparison is implemented, a predicate may not treat an
	// unscaled physical coefficient as an ordinary int64 or prune by its stats.
	if _, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: expr.GT(expr.Col("amount"), expr.I64(1))}); err == nil {
		t.Fatal("unsupported decimal comparison accepted")
	}
}

func TestParquetDecimal128DeltaPageEncodings(t *testing.T) {
	for _, tc := range []struct {
		physical  arrowparquet.Type
		encoding  arrowparquet.Encoding
		precision int32
		length    int
	}{
		{arrowparquet.Types.Int32, arrowparquet.Encodings.DeltaBinaryPacked, 9, -1},
		{arrowparquet.Types.Int64, arrowparquet.Encodings.DeltaBinaryPacked, 18, -1},
		{arrowparquet.Types.ByteArray, arrowparquet.Encodings.DeltaLengthByteArray, 38, -1},
		{arrowparquet.Types.ByteArray, arrowparquet.Encodings.DeltaByteArray, 38, -1},
		{arrowparquet.Types.FixedLenByteArray, arrowparquet.Encodings.ByteStreamSplit, 2, 1},
		{arrowparquet.Types.FixedLenByteArray, arrowparquet.Encodings.ByteStreamSplit, 4, 2},
		{arrowparquet.Types.FixedLenByteArray, arrowparquet.Encodings.ByteStreamSplit, 9, 4},
		{arrowparquet.Types.FixedLenByteArray, arrowparquet.Encodings.ByteStreamSplit, 18, 8},
		{arrowparquet.Types.FixedLenByteArray, arrowparquet.Encodings.ByteStreamSplit, 38, 16},
		{arrowparquet.Types.FixedLenByteArray, arrowparquet.Encodings.ByteStreamSplit, 38, 32},
	} {
		for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
			t.Run(fmt.Sprintf("%s/%s/%v", tc.physical, tc.encoding, version), func(t *testing.T) {
				values := decimalValues(tc.precision)
				path := writeDecimalFixture(t, tc.physical, tc.length, tc.precision, 2, true, []int{73}, values, arrowparquet.WithDictionaryDefault(false), arrowparquet.WithEncoding(tc.encoding), arrowparquet.WithDataPageVersion(version))
				src, err := OpenFile(path)
				if err != nil {
					t.Fatal(err)
				}
				chunk, err := src.meta.RowGroup(0).ColumnChunk(0)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, enc := range chunk.Encodings() {
					found = found || enc == tc.encoding
				}
				if !found {
					t.Fatalf("encoding %s not actually emitted: %v", tc.encoding, chunk.Encodings())
				}
				opts := testReadOptions()
				opts.BatchRows = 7
				opts.BatchBytes = 256
				st, err := src.NewBatchStream(context.Background(), opts)
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				batch := memory.NewBatch()
				row := 0
				for {
					ok, err := st.Next(context.Background(), batch)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					v := batch.Decimals["amount"]
					for i := 0; i < batch.Rows; i++ {
						valid := row%3 != 1
						if v.IsValid(i) != valid || valid && v.Values[i] != values[row%len(values)] {
							t.Fatalf("row=%d", row)
						}
						row++
					}
				}
				if row != 73 {
					t.Fatalf("rows=%d", row)
				}
			})
		}
	}
}

func TestParquetDecimal128RejectsUnsupportedPageEncoding(t *testing.T) {
	path := writeDecimalFixture(t, arrowparquet.Types.FixedLenByteArray, 16, 38, 2, false, []int{3}, decimalValues(38), arrowparquet.WithDictionaryDefault(false))
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Footer encodings are a public Thrift enum of an internal package; use
	// reflection only in the test to change its value without importing internals.
	encodings := src.meta.FileMetaData.RowGroups[0].Columns[0].MetaData.Encodings
	reflect.ValueOf(&encodings[0]).Elem().SetInt(int64(arrowparquet.Encodings.DeltaByteArray))
	if _, err := bindingAt(src.meta, 0, "amount"); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("unsupported decimal encoding accepted: %v", err)
	}
}

func TestParquetDecimal128UnlistedUnsupportedPageFailsCleanly(t *testing.T) {
	// DELTA_BYTE_ARRAY bodies encode byte lengths themselves. Retag the fixed
	// 17-byte coefficients as FLBA, then omit the unsupported encoding from the
	// footer to exercise the actual-page check before a decoder is constructed.
	path := writeDecimalFixture(t, arrowparquet.Types.ByteArray, -1, 38, 2, false, []int{3}, decimalValues(38), arrowparquet.WithDictionaryDefault(false), arrowparquet.WithEncoding(arrowparquet.Encodings.DeltaByteArray))
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
	field := meta.FileMetaData.Schema[1]
	reflect.ValueOf(field.Type).Elem().SetInt(int64(arrowparquet.Types.FixedLenByteArray))
	width := int32(17)
	field.TypeLength = &width
	column := meta.FileMetaData.RowGroups[0].Columns[0].MetaData
	reflect.ValueOf(&column.Type).Elem().SetInt(int64(arrowparquet.Types.FixedLenByteArray))
	for i := range column.Encodings {
		if int64(column.Encodings[i]) == int64(arrowparquet.Encodings.DeltaByteArray) {
			reflect.ValueOf(&column.Encodings[i]).Elem().SetInt(int64(arrowparquet.Encodings.Plain))
		}
	}
	footer, err := meta.Serialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := append(data[:start:start], footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	out = append(out, "PAR1"...)
	if err = os.WriteFile(path, out, 0600); err != nil {
		t.Fatal(err)
	}
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	opts := testReadOptions()
	opts.BatchRows = 3
	opts.BatchBytes = 128
	opts.MemoryManager = memory.NewBudgetManager(128)
	st, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	batch := memory.NewBatch()
	ok, err := st.Next(context.Background(), batch)
	if ok || err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("unlisted encoding ok=%v err=%v", ok, err)
	}
	if batch.Rows != 0 || len(batch.Decimals) != 0 || opts.MemoryManager.Used() != 0 {
		t.Fatal("unsupported decoder left output or reservation")
	}
	if ok, nextErr := st.Next(context.Background(), batch); ok || !errors.Is(nextErr, err) {
		t.Fatalf("terminal error not retained: ok=%v err=%v", ok, nextErr)
	}
}

func rewriteDecimalFooter(t *testing.T, path string, change func(*metadata.FileMetaData)) {
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
	change(meta)
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

func TestParquetDecimal128RejectsNumericByteStreamSplit(t *testing.T) {
	for _, tc := range []struct {
		physical  arrowparquet.Type
		precision int32
	}{{arrowparquet.Types.Int32, 9}, {arrowparquet.Types.Int64, 18}} {
		for _, nullable := range []bool{false, true} {
			for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
				t.Run(fmt.Sprintf("%s/nullable=%v/page=%v", tc.physical, nullable, version), func(t *testing.T) {
					path := writeDecimalFixture(t, tc.physical, -1, tc.precision, 2, nullable, []int{73}, decimalValues(tc.precision), arrowparquet.WithDictionaryDefault(false), arrowparquet.WithEncoding(arrowparquet.Encodings.ByteStreamSplit), arrowparquet.WithDataPageVersion(version))
					// This restriction is CPU-independent: a file admitted on a
					// vector-capable machine must not later panic on fallback.
					if _, err := OpenFile(path); err == nil || !strings.Contains(err.Error(), "BYTE_STREAM_SPLIT") || !strings.Contains(err.Error(), "not supported") {
						t.Fatalf("numeric BSS footer accepted: %v", err)
					}
					rewriteDecimalFooter(t, path, func(meta *metadata.FileMetaData) {
						found := false
						for _, rg := range meta.FileMetaData.RowGroups {
							column := rg.Columns[0].MetaData
							for i := range column.Encodings {
								if int64(column.Encodings[i]) == int64(arrowparquet.Encodings.ByteStreamSplit) {
									found = true
									// RLE is valid in the footer for definition levels,
									// but cannot authorize a BSS coefficient decoder.
									reflect.ValueOf(&column.Encodings[i]).Elem().SetInt(int64(arrowparquet.Encodings.RLE))
								}
							}
						}
						if !found {
							t.Fatal("BYTE_STREAM_SPLIT was not actually emitted")
						}
					})
					src, err := OpenFile(path)
					if err != nil {
						t.Fatal(err)
					}
					opts := testReadOptions()
					opts.BatchRows, opts.BatchBytes = 1, 24
					opts.MemoryManager = memory.NewBudgetManager(24)
					st, err := src.NewBatchStream(context.Background(), opts)
					if err != nil {
						t.Fatal(err)
					}
					defer st.Close()
					batch := memory.NewBatch()
					ok, err := st.Next(context.Background(), batch)
					if ok || err == nil || !strings.Contains(err.Error(), "page") || !strings.Contains(err.Error(), "BYTE_STREAM_SPLIT") || !strings.Contains(err.Error(), "not supported") {
						t.Fatalf("unlisted numeric BSS page accepted: ok=%v err=%v", ok, err)
					}
					if batch.Rows != 0 || len(batch.Decimals) != 0 || opts.MemoryManager.Used() != 0 {
						t.Fatal("unsupported page left output or reservation")
					}
					if ok, nextErr := st.Next(context.Background(), batch); ok || !errors.Is(nextErr, err) {
						t.Fatalf("terminal page error not retained: ok=%v err=%v", ok, nextErr)
					}
				})
			}
		}
	}
}

func TestParquetDecimal128FixedByteStreamSplitAllNull(t *testing.T) {
	for _, width := range []int{1, 2, 4, 8, 16, 32} {
		for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
			t.Run(fmt.Sprintf("width=%d/page=%v", width, version), func(t *testing.T) {
				// The fixture's fourth row group contains only NULLs. These
				// exercise zero-value Decode on the distinct FLBA implementation.
				path := writeDecimalFixture(t, arrowparquet.Types.FixedLenByteArray, width, 2, 2, true, []int{1, 1, 1, 73}, decimalValues(2), arrowparquet.WithDictionaryDefault(false), arrowparquet.WithEncoding(arrowparquet.Encodings.ByteStreamSplit), arrowparquet.WithDataPageVersion(version))
				src, err := OpenFile(path)
				if err != nil {
					t.Fatal(err)
				}
				chunk, err := src.meta.RowGroup(3).ColumnChunk(0)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, encoding := range chunk.Encodings() {
					found = found || encoding == arrowparquet.Encodings.ByteStreamSplit
				}
				if !found {
					t.Fatal("all-NULL group did not emit BYTE_STREAM_SPLIT")
				}
				opts := testReadOptions()
				opts.BatchRows, opts.BatchBytes = 1, 24
				opts.MemoryManager = memory.NewBudgetManager(24)
				st, err := src.NewBatchStream(context.Background(), opts)
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				batch := memory.NewBatch()
				rows := 0
				for {
					ok, err := st.Next(context.Background(), batch)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					v := batch.Decimals["amount"]
					if batch.Rows != 1 || v.IsValid(0) != (rows < 3) || v.Values[0] != (memory.Decimal128{}) || opts.MemoryManager.Used() != 24 {
						t.Fatal("all-NULL fixed BSS row or reservation changed")
					}
					rows++
				}
				if rows != 76 || opts.MemoryManager.Used() != 0 {
					t.Fatalf("rows=%d retained=%d", rows, opts.MemoryManager.Used())
				}
			})
		}
	}
}

func TestParquetDecimal128PagePreflightLifecycle(t *testing.T) {
	plain := writeDecimalFixture(t, arrowparquet.Types.Int32, -1, 9, 2, true, []int{73}, decimalValues(9), arrowparquet.WithDictionaryDefault(false))
	bss := writeDecimalFixture(t, arrowparquet.Types.Int32, -1, 9, 2, true, []int{73}, decimalValues(9), arrowparquet.WithDictionaryDefault(false), arrowparquet.WithEncoding(arrowparquet.Encodings.ByteStreamSplit))
	badCodec := writeDecimalFixture(t, arrowparquet.Types.Int32, -1, 9, 2, true, []int{73}, decimalValues(9), arrowparquet.WithDictionaryDefault(false))
	rewriteDecimalFooter(t, badCodec, func(meta *metadata.FileMetaData) {
		reflect.ValueOf(&meta.FileMetaData.RowGroups[0].Columns[0].MetaData.Codec).Elem().SetInt(127)
	})
	for _, tc := range []struct {
		name      string
		path      string
		cancel    bool
		cap       int64
		wantError bool
	}{
		{"streaming success", plain, false, 0, false},
		{"unsupported encoding", bss, false, 0, true},
		{"reader initialization error", badCodec, false, 0, true},
		{"canceled", plain, true, 0, true},
		{"page caps", plain, false, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer allocator.AssertSize(t, 0)
			props := arrowparquet.NewReaderProperties(allocator)
			props.BufferedStreamEnabled, props.PageStreamingEnabled = true, true
			if tc.cap > 0 {
				props.MaxCompressedPageSize, props.MaxUncompressedPageSize = tc.cap, tc.cap
			}
			r, err := file.OpenParquetFile(tc.path, false, file.WithReadProps(props))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			err = validateDecimalPages(ctx, r.RowGroup(0), columnBinding{Name: "amount", Index: 0}, arrowparquet.Types.Int32)
			if (err != nil) != tc.wantError || tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("preflight=%v wantError=%v", err, tc.wantError)
			}
			// The preflight owns no Arrow page or buffer after returning, even
			// while its parent file reader is still open for the actual scan.
			allocator.AssertSize(t, 0)
		})
	}
}
