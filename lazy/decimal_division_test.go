package lazy

import (
	"context"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func divisionByOne(value expr.Expr, precision, scale int32) expr.Decimal128DivideExpr {
	return DivideDecimal128(value, Decimal128(decimal128.FromI64(1), 1, 0), precision, scale)
}

func decimalDivideByOneQuery(scan LazyFrame) LazyFrame {
	up := divisionByOne(Col("other_scale"), 20, 12)
	timesOne := expr.Mul(Col("other_scale"), Decimal128(decimal128.FromI64(100), 3, 2))
	return scan.Filter(Col("id").Ge(Lit(2))).SelectExprs(
		Col("id").Alias("id"), up.Alias("up"),
		divisionByOne(up, 20, 11).Alias("back"),
		divisionByOne(timesOne, 20, 11).Alias("down"),
		Named(If(Col("condition"), up, divisionByOne(Col("other_precision"), 20, 12)), "unified"),
		divisionByOne(NullDecimal128(38, 38), 1, 0).Alias("null"),
	).Filter(expr.GE(Col("up"), Decimal128(memory.Decimal128{}, 1, 0))).Limit(71)
}

func checkDecimalDivideByOneQuery(t *testing.T, column func(string) (Series, error), offset int) int {
	t.Helper()
	ids, err := column("id")
	if err != nil {
		t.Fatal(err)
	}
	values, err := ids.ToAnySlice()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"up", "back", "down", "unified", "null"} {
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
			for j := 0; j < chunk.Len(); j++ {
				row := int(values[i].(int64))
				if row != 70+offset+i {
					t.Fatalf("row order got=%d want=%d", row, 70+offset+i)
				}
				want := big.NewInt(int64(row - 70))
				precision, scale := int32(20), int32(11)
				switch name {
				case "up":
					scale = 12
					want.Mul(want, big.NewInt(10))
				case "unified":
					scale = 12
					if decimalExpressionCondition(row) {
						want.Mul(want, big.NewInt(10))
					}
				case "null":
					precision, scale = 1, 0
				}
				got, valid, err := chunk.Value(j)
				if err != nil || valid != (name != "null") || valid && got.BigInt().Cmp(want) != 0 || chunk.Precision() != precision || chunk.Scale() != scale {
					t.Fatalf("%s row=%d value=%s valid=%v p/s=%d/%d want=%s err=%v", name, row, got.BigInt(), valid, chunk.Precision(), chunk.Scale(), want, err)
				}
				i++
			}
		}
		if i != len(values) {
			t.Fatal("column length changed")
		}
	}
	return len(values)
}

func TestParquetDecimal128DivideByOneQueryPaths(t *testing.T) {
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
				query := decimalDivideByOneQuery(scan)
				frame, err := query.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if n := checkDecimalDivideByOneQuery(t, frame.Column, 0); n != 71 {
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
					n += checkDecimalDivideByOneQuery(t, view.Column, n)
				}
				if err := reader.Err(); err != nil {
					t.Fatal(err)
				}
				reader.Close()
				if n != 71 || session.Engine().Memory().Used() != 0 {
					t.Fatal("stream count/leak", n)
				}
				output := filepath.Join(t.TempDir(), "division.parquet")
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
				if n := checkDecimalDivideByOneQuery(t, frame.Column, 0); n != 71 {
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

func TestParquetDecimal128DivideByOneErrorDomain(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	for _, bounded := range []bool{false, true} {
		session := NewSession(decimalExpressionOptions(bounded))
		scan, err := session.ScanParquet(path)
		if err != nil {
			t.Fatal(err)
		}
		for name, bad := range map[string]expr.Expr{
			"inexact":  divisionByOne(Col("amount"), 38, 11),
			"overflow": divisionByOne(Col("amount"), 38, 13),
			"zero":     DivideDecimal128(Col("amount"), Decimal128(memory.Decimal128{}, 1, 0), 38, 12),
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
					} else if name == "zero" {
						want = "by zero at row 0"
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

func TestDecimalDivideByOneStreamLaterBatchErrorAndClose(t *testing.T) {
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
				query := session.FromSource(src).SelectExprs(divisionByOne(Col("d"), 1, 0).Alias("d"))
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

func decimalDivideTwoColumnsQuery(scan LazyFrame) LazyFrame {
	ratio := DivideDecimal128(Col("other_scale"), Col("other_precision"), 8, 2)
	reverse := DivideDecimal128(Col("other_precision"), Col("other_scale"), 8, 2)
	denominator := If(Col("condition"), Decimal128(decimal128.FromI64(-4), 1, 0), Decimal128(decimal128.FromI64(2), 1, 0))
	nonzero := expr.GT(expr.Mul(Col("other_precision"), Col("other_precision")), Decimal128(memory.Decimal128{}, 1, 0))
	return scan.Filter(Col("id").Ge(Lit(2))).SelectExprs(
		Col("id").Alias("id"),
		Named(If(nonzero, ratio, NullDecimal128(8, 2)), "ratio"),
		Named(If(nonzero, reverse, NullDecimal128(8, 2)), "reverse"),
		DivideDecimal128(Col("other_scale"), denominator, 20, 13).Alias("variable"),
		DivideDecimal128(Col("other_scale"), If(expr.IsNull(Col("amount")), NullDecimal128(1, 0), denominator), 20, 13).Alias("nullable"),
	).Limit(71)
}

func checkDecimalDivideTwoColumnsQuery(t *testing.T, column func(string) (Series, error), offset int) int {
	t.Helper()
	ids, err := column("id")
	if err != nil {
		t.Fatal(err)
	}
	values, err := ids.ToAnySlice()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ratio", "reverse", "variable", "nullable"} {
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
			for j := 0; j < chunk.Len(); j++ {
				row := int(values[i].(int64))
				if row != 2+offset+i {
					t.Fatal("row order", row)
				}
				p, s := int32(8), int32(2)
				valid := row != 70
				want := big.NewInt(1000)
				if name == "reverse" {
					want.SetInt64(10)
				}
				if name == "variable" || name == "nullable" {
					p, s, valid = 20, 13, true
					divisor := int64(2)
					if decimalExpressionCondition(row) {
						divisor = -4
					}
					want.Quo(big.NewInt(int64(row-70)*100), big.NewInt(divisor))
					if name == "nullable" {
						_, valid = decimalExpressionValue(row, "amount")
					}
				}
				got, ok, err := chunk.Value(j)
				if err != nil || valid != ok || valid && got.BigInt().Cmp(want) != 0 || chunk.Precision() != p || chunk.Scale() != s {
					t.Fatalf("%s row=%d got=%s/%v p/s=%d/%d want=%s/%v: %v", name, row, got.BigInt(), ok, chunk.Precision(), chunk.Scale(), want, valid, err)
				}
				i++
			}
		}
		if i != len(values) {
			t.Fatal("length mismatch")
		}
	}
	return len(values)
}

func TestParquetDecimal128DivideTwoColumnsQueryPaths(t *testing.T) {
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
				query := decimalDivideTwoColumnsQuery(scan)
				frame, err := query.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if n := checkDecimalDivideTwoColumnsQuery(t, frame.Column, 0); n != 71 {
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
					n += checkDecimalDivideTwoColumnsQuery(t, view.Column, n)
				}
				if err := reader.Err(); err != nil {
					t.Fatal(err)
				}
				reader.Close()
				if n != 71 || session.Engine().Memory().Used() != 0 {
					t.Fatal("stream count/leak", n)
				}
				output := filepath.Join(t.TempDir(), "division.parquet")
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
				if n := checkDecimalDivideTwoColumnsQuery(t, frame.Column, 0); n != 71 {
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
