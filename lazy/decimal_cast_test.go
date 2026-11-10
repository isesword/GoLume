package lazy

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func decimalCastQuery(scan LazyFrame) LazyFrame {
	d := CastDecimal128(Col("id"), 8, 2)
	text := expr.CastTo(d, memory.TypeString)
	return scan.Filter(Col("id").Ge(Lit(2))).SelectExprs(
		Col("id").Alias("id"), d.Alias("decimal"), Named(text, "text"),
		CastDecimal128(text, 8, 2).Alias("parsed"),
		Named(expr.CastTo(d, memory.TypeInt64), "integer"),
		Named(expr.TryCastTo(d, memory.TypeInt8), "small"),
		TryCastDecimal128(expr.Str("bad"), 8, 2).Alias("invalid"),
		CastDecimal128(NullDecimal128(38, 38), 8, 2).Alias("null"),
	).Filter(expr.GE(Col("decimal"), Decimal128(decimal128.FromI64(7000), 8, 2))).Limit(71)
}

func checkDecimalCastQuery(t *testing.T, column func(string) (Series, error), offset int) int {
	t.Helper()
	ids, err := column("id")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := ids.ToAnySlice()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"decimal", "parsed", "invalid", "null"} {
		series, err := column(name)
		if err != nil {
			t.Fatal(err)
		}
		chunks, err := series.Decimal128Chunks()
		if err != nil {
			t.Fatal(err)
		}
		i := 0
		for _, chunk := range chunks {
			if chunk.Precision() != 8 || chunk.Scale() != 2 {
				t.Fatal("metadata", name, chunk.Precision(), chunk.Scale())
			}
			for j := 0; j < chunk.Len(); j++ {
				row := rows[i].(int64)
				got, valid, err := chunk.Value(j)
				wantValid := name != "invalid" && name != "null"
				if row != int64(70+offset+i) || err != nil || valid != wantValid || valid && got != decimal128.FromI64(row*100) {
					t.Fatal(name, row, got, valid, err)
				}
				i++
			}
		}
		if i != len(rows) {
			t.Fatal("length", name, i)
		}
	}
	for _, name := range []string{"text", "integer", "small"} {
		series, err := column(name)
		if err != nil {
			t.Fatal(err)
		}
		values, err := series.ToAnySlice()
		if err != nil {
			t.Fatal(err)
		}
		if len(values) != len(rows) {
			t.Fatal("length", name)
		}
		for i, value := range values {
			row := rows[i].(int64)
			var want any
			switch name {
			case "text":
				want = fmt.Sprintf("%d.00", row)
			case "integer":
				want = row
			case "small":
				if row <= 127 {
					want = int8(row)
				}
			}
			if value != want {
				t.Fatalf("%s row=%d got=%v (%T) want=%v (%T)", name, row, value, value, want, want)
			}
		}
	}
	return len(rows)
}

func TestParquetDecimal128CastQueryPaths(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	for _, bounded := range []bool{false, true} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("bounded=%v/mode=%v", bounded, mode), func(t *testing.T) {
				opts := decimalExpressionOptions(bounded)
				opts.KernelMode = mode
				session := NewSession(opts)
				defer session.Close()
				scan, err := session.ScanParquet(path)
				if err != nil {
					t.Fatal(err)
				}
				query := decimalCastQuery(scan)
				frame, err := query.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if n := checkDecimalCastQuery(t, frame.Column, 0); n != 71 {
					t.Fatal("collect rows", n)
				}
				frame.Close()
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("collect leaked")
				}
				reader, err := query.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 4096})
				if err != nil {
					t.Fatal(err)
				}
				n := 0
				for reader.Next(context.Background()) {
					view := reader.Batch()
					n += checkDecimalCastQuery(t, view.Column, n)
				}
				if err := reader.Err(); err != nil {
					t.Fatal(err)
				}
				reader.Close()
				if n != 71 || session.Engine().Memory().Used() != 0 {
					t.Fatal("stream count/leak", n)
				}
				output := filepath.Join(t.TempDir(), "cast.parquet")
				if _, err := query.SinkParquet(context.Background(), output, ParquetSinkOptions{}); err != nil {
					t.Fatal(err)
				}
				roundtrip, err := session.ScanParquet(output)
				if err != nil {
					t.Fatal(err)
				}
				frame, err = roundtrip.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if n := checkDecimalCastQuery(t, frame.Column, 0); n != 71 {
					t.Fatal("roundtrip rows", n)
				}
				frame.Close()
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("roundtrip leaked")
				}
			})
		}
	}
}

func TestParquetDecimal128CastErrorDomain(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	for _, bounded := range []bool{false, true} {
		session := NewSession(decimalExpressionOptions(bounded))
		scan, err := session.ScanParquet(path)
		if err != nil {
			t.Fatal(err)
		}
		for name, bad := range map[string]expr.Expr{
			"inexact":        CastDecimal128(Col("amount"), 38, 11),
			"syntax":         CastDecimal128(expr.Str("999.001!"), 3, 2),
			"string_inexact": CastDecimal128(expr.Str("999.001"), 3, 2),
			"overflow":       CastDecimal128(Col("amount"), 38, 13),
		} {
			predicate := expr.EQ(bad, Decimal128(memory.Decimal128{}, 1, 0))
			for label, query := range map[string]LazyFrame{
				"earlier_empty_filter": scan.Filter(Col("id").Lt(Lit(0))).Filter(predicate),
				"guarded":              scan.Filter(If(Bool(false), predicate, Bool(false))),
				"unused":               scan.WithColumns(Named(bad, "unused")).Select("id"),
			} {
				t.Run(fmt.Sprintf("safe/%s/%s/bounded=%v", name, label, bounded), func(t *testing.T) {
					frame, err := query.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					frame.Close()
					if session.Engine().Memory().Used() != 0 {
						t.Fatal("safe path leaked")
					}
				})
			}
			for label, query := range map[string]LazyFrame{
				"later_empty_filter":    scan.Filter(predicate).Filter(Col("id").Lt(Lit(0))),
				"eager_and":             scan.Filter(expr.And(Bool(false), predicate)),
				"demanded":              scan.SelectExprs(Named(bad, "value")),
				"demanded_before_false": scan.WithColumns(Named(bad, "value")).Filter(Col("id").Lt(Lit(0))),
			} {
				t.Run(fmt.Sprintf("error/%s/%s/bounded=%v", name, label, bounded), func(t *testing.T) {
					want := "loses fractional digits at row 0"
					if name == "overflow" {
						want = "overflows at row 0"
					}
					if name == "syntax" {
						want = "has invalid decimal syntax at row 0"
					}
					frame, err := query.Collect(context.Background())
					frame.Close()
					if err == nil || !strings.Contains(err.Error(), want) {
						t.Fatal("error suppressed or changed", err)
					}
					if session.Engine().Memory().Used() != 0 {
						t.Fatal("failed collect leaked")
					}
					reader, err := query.CollectBatches(context.Background(), BatchOptions{})
					if err == nil {
						for reader.Next(context.Background()) {
						}
						err = reader.Err()
						reader.Close()
					}
					if err == nil || !strings.Contains(err.Error(), want) {
						t.Fatal("stream error changed", err)
					}
					if session.Engine().Memory().Used() != 0 {
						t.Fatal("failed stream leaked")
					}
				})
			}
		}
		session.Close()
	}
}

func TestDecimalCastStreamLaterBatchErrorAndClose(t *testing.T) {
	field, _ := memory.Decimal128Field("d", false, 3, 1)
	batch := memory.NewBatch()
	batch.Rows = 140
	v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, batch.Rows), Precision: 3, Scale: 1}
	for i := range v.Values {
		v.Values[i] = decimal128.FromI64(10)
	}
	batch.Decimals["d"] = v
	for _, bounded := range []bool{false, true} {
		for _, mode := range []string{"late_error", "early_close", "cancel"} {
			t.Run(fmt.Sprintf("%s/bounded=%v", mode, bounded), func(t *testing.T) {
				v.Values[100] = decimal128.FromI64(10)
				if mode == "late_error" {
					v.Values[100] = decimal128.FromI64(11)
				}
				src, err := inmemory.New(memory.NewSchema(field), batch)
				if err != nil {
					t.Fatal(err)
				}
				opts := decimalExpressionOptions(bounded)
				session := NewSession(opts)
				defer session.Close()
				var converted expr.Expr = expr.CastTo(Col("d"), memory.TypeInt64)
				if mode == "cancel" {
					converted = expr.TryCastTo(Col("d"), memory.TypeInt64)
				}
				query := session.FromSource(src).SelectExprs(TryCastDecimal128(converted, 1, 0).Alias("d"))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				reader, err := query.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 512, MaxBatchBytes: 1024})
				if err != nil {
					t.Fatal(err)
				}
				rows := 0
				for reader.Next(ctx) {
					view := reader.Batch()
					series, err := view.Column("d")
					if err != nil {
						t.Fatal(err)
					}
					chunks, err := series.Decimal128Chunks()
					if err != nil {
						t.Fatal(err)
					}
					for _, chunk := range chunks {
						for i := 0; i < chunk.Len(); i++ {
							got, valid, err := chunk.Value(i)
							if err != nil || !valid || got != decimal128.FromI64(1) {
								t.Fatal("early batch value", got, valid, err)
							}
							rows++
						}
					}
					if mode == "early_close" {
						break
					}
					if mode == "cancel" {
						cancel()
					}
				}
				if mode == "late_error" {
					if rows == 0 || rows >= batch.Rows || reader.Err() == nil || !strings.Contains(reader.Err().Error(), "loses fractional digits at row ") {
						t.Fatal("late error contract", rows, reader.Err())
					}
				} else if mode == "early_close" && reader.Err() != nil {
					t.Fatal(reader.Err())
				} else if mode == "cancel" && reader.Err() == nil {
					t.Fatal("cancel suppressed")
				}
				reader.Close()
				reader.Close()
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("stream close leaked")
				}
			})
		}
	}
}

func TestDecimalCastStringsInMemoryAndParquet(t *testing.T) {
	tokens := []string{"+12.3000", "-.5", "1.", ".1", "-0.00", "bad", "1.234", "99999", "poison", " 1", "1e2"}
	expected := []any{"12.30", "-0.50", "1.00", "0.10", "0.00", nil, nil, nil, nil, nil, nil}
	b := memory.NewBatch()
	b.Rows = len(tokens)
	v := &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView, Validity: memory.EnsureBitmap(nil, len(tokens))}
	for i, token := range tokens {
		v.Data = append(v.Data, token...)
		v.Offsets = append(v.Offsets, uint32(len(v.Data)))
		if i != 8 {
			memory.Set(v.Validity, i)
		}
	}
	b.Strings["text"] = v
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "text", Type: memory.TypeString, Nullable: true}), b)
	if err != nil {
		t.Fatal(err)
	}
	for _, bounded := range []bool{false, true} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("bounded=%v/mode=%v", bounded, mode), func(t *testing.T) {
				opts := decimalExpressionOptions(bounded)
				opts.KernelMode = mode
				session := NewSession(opts)
				defer session.Close()
				scan := session.FromSource(src)
				path := filepath.Join(t.TempDir(), "strings.parquet")
				if _, err := scan.SinkParquet(context.Background(), path, ParquetSinkOptions{}); err != nil {
					t.Fatal(err)
				}
				disk, err := session.ScanParquet(path)
				if err != nil {
					t.Fatal(err)
				}
				for _, input := range []LazyFrame{scan, disk} {
					q := input.SelectExprs(Named(expr.CastTo(TryCastDecimal128(Col("text"), 4, 2), memory.TypeString), "result"))
					frame, err := q.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					series, err := frame.Column("result")
					if err != nil {
						t.Fatal(err)
					}
					values, err := series.ToAnySlice()
					if err != nil {
						t.Fatal(err)
					}
					if len(values) != len(expected) {
						t.Fatal("length", len(values))
					}
					for i, want := range expected {
						if values[i] != want {
							t.Fatalf("row %d: got=%v want=%v", i, values[i], want)
						}
					}
					frame.Close()
				}
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("leaked")
				}
			})
		}
	}
}

func TestDecimalCastTryPreservesChildFailureAndBinding(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	for _, bounded := range []bool{false, true} {
		session := NewSession(decimalExpressionOptions(bounded))
		scan, err := session.ScanParquet(path)
		if err != nil {
			t.Fatal(err)
		}
		for name, value := range map[string]expr.Expr{
			"strict_child":          TryCastDecimal128(CastDecimal128(expr.Str("bad"), 4, 2), 4, 2),
			"integer_try_child":     expr.TryCastTo(CastDecimal128(expr.Str("bad"), 4, 2), memory.TypeInt64),
			"invalid_dead_metadata": If(Bool(false), TryCastDecimal128(Col("id"), 0, 0), NullDecimal128(1, 0)),
		} {
			t.Run(fmt.Sprintf("%s/bounded=%v", name, bounded), func(t *testing.T) {
				frame, err := scan.SelectExprs(Named(value, "value")).Collect(context.Background())
				frame.Close()
				if err == nil {
					t.Fatal("TRY suppressed child/binding error")
				}
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("leaked")
				}
			})
		}
		session.Close()
	}
}

// Direct public materialization must preserve native widths, not require an
// application-side widening cast to work around Series storage assumptions.
func TestDecimalCastIntegerPublicMaterialization(t *testing.T) {
	coefficients := []int64{-12800, 12700, 25500, 3276700, 6553500, 100, 101, 0}
	field, _ := memory.Decimal128Field("d", true, 8, 2)
	batch := memory.NewBatch()
	batch.Rows = len(coefficients)
	vector := &memory.Decimal128Vector{Values: make([]memory.Decimal128, len(coefficients)), Precision: 8, Scale: 2, Validity: memory.Bitmap{0x7f}}
	for i, v := range coefficients {
		vector.Values[i] = decimal128.FromI64(v)
	}
	batch.Decimals["d"] = vector
	src, err := inmemory.New(memory.NewSchema(field), batch)
	if err != nil {
		t.Fatal(err)
	}
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64}
	expected := map[memory.DataType][]any{
		memory.TypeInt32:  {int32(-128), int32(127), int32(255), int32(32767), int32(65535), int32(1), nil, nil},
		memory.TypeInt64:  {int64(-128), int64(127), int64(255), int64(32767), int64(65535), int64(1), nil, nil},
		memory.TypeUint32: {nil, uint32(127), uint32(255), uint32(32767), uint32(65535), uint32(1), nil, nil},
		memory.TypeUint64: {nil, uint64(127), uint64(255), uint64(32767), uint64(65535), uint64(1), nil, nil},
		memory.TypeInt8:   {int8(-128), int8(127), nil, nil, nil, int8(1), nil, nil},
		memory.TypeInt16:  {int16(-128), int16(127), int16(255), int16(32767), nil, int16(1), nil, nil},
		memory.TypeUint8:  {nil, uint8(127), uint8(255), nil, nil, uint8(1), nil, nil},
		memory.TypeUint16: {nil, uint16(127), uint16(255), uint16(32767), uint16(65535), uint16(1), nil, nil},
	}
	for _, bounded := range []bool{false, true} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("bounded=%v/mode=%v", bounded, mode), func(t *testing.T) {
				opts := decimalExpressionOptions(bounded)
				opts.KernelMode = mode
				session := NewSession(opts)
				defer session.Close()
				var expressions []expr.NamedExpr
				for _, typ := range types {
					expressions = append(expressions, Named(expr.TryCastTo(Col("d"), typ), typ.String()))
				}
				query := session.FromSource(src).SelectExprs(expressions...)
				// Strict casts also materialize every width directly. Guard all
				// other inputs to NULL so no inexact/range failure is demanded.
				var strictExpressions []expr.NamedExpr
				exact := If(expr.EQ(Col("d"), Decimal128(decimal128.FromI64(100), 8, 2)), Col("d"), NullDecimal128(8, 2))
				for _, typ := range types {
					strictExpressions = append(strictExpressions, Named(expr.CastTo(exact, typ), typ.String()))
				}
				strict, err := session.FromSource(src).SelectExprs(strictExpressions...).Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				for _, typ := range types {
					series, err := strict.Column(typ.String())
					if err != nil {
						t.Fatal(err)
					}
					values, err := series.ToAnySlice()
					if err != nil {
						t.Fatal(err)
					}
					if len(values) != len(coefficients) || series.NullCount() != int64(len(coefficients)-1) {
						t.Fatal("strict shape/nulls", typ)
					}
					for i, value := range values {
						var want any
						if i == 5 {
							want = expected[typ][5]
						}
						if value != want {
							t.Fatal("strict value", typ, i, value, want)
						}
					}
				}
				strict.Close()
				check := func(column func(string) (Series, error), offset, count int) {
					t.Helper()
					for _, typ := range types {
						s, err := column(typ.String())
						if err != nil {
							t.Fatal(err)
						}
						values, err := s.ToAnySlice()
						if err != nil {
							t.Fatal(err)
						}
						want := expected[typ][offset : offset+count]
						if len(values) != len(want) {
							t.Fatal("length", typ, len(values), len(want))
						}
						var nulls int64
						for i, x := range want {
							if x == nil {
								nulls++
							}
							if values[i] != x {
								t.Fatalf("%s row %d: got %v (%T), want %v (%T)", typ, i, values[i], values[i], x, x)
							}
						}
						if s.NullCount() != nulls {
							t.Fatal("null count", typ, s.NullCount(), nulls)
						}
					}
				}
				frame, err := query.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				check(frame.Column, 0, len(coefficients))
				view, err := frame.Slice(1, 5)
				if err != nil {
					t.Fatal(err)
				}
				check(view.Column, 1, 5)
				borrowed, err := frame.Column(types[0].String())
				if err != nil {
					t.Fatal(err)
				}
				frame.Close()
				if _, err := borrowed.ToAnySlice(); err == nil {
					t.Fatal("closed owner accepted")
				}
				reader, err := query.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 32, MaxBatchBytes: 128})
				if err != nil {
					t.Fatal(err)
				}
				offset := 0
				for reader.Next(context.Background()) {
					view := reader.Batch()
					s, err := view.Column(types[0].String())
					if err != nil {
						t.Fatal(err)
					}
					n := int(s.Len())
					check(view.Column, offset, n)
					offset += n
				}
				if err := reader.Err(); err != nil {
					t.Fatal(err)
				}
				reader.Close()
				if offset != len(coefficients) {
					t.Fatal("stream rows", offset)
				}
				path := filepath.Join(t.TempDir(), "narrow.parquet")
				if _, err := query.SinkParquet(context.Background(), path, ParquetSinkOptions{}); err != nil {
					t.Fatal(err)
				}
				disk, err := session.ScanParquet(path)
				if err != nil {
					t.Fatal(err)
				}
				frame, err = disk.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				check(frame.Column, 0, len(coefficients))
				frame.Close()
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("leaked")
				}
			})
		}
	}
}
