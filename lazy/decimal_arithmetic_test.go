package lazy

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func decimalArithmeticQuery(scan LazyFrame) LazyFrame {
	zero := Decimal128(memory.Decimal128{}, 1, 0)
	one := Decimal128(decimal128.FromI64(1), 1, 0)
	sum := expr.Add(Col("other_scale"), Col("other_precision"))
	return scan.Filter(Col("id").Ge(Lit(2))).SelectExprs(
		Col("id").Alias("id"),
		Named(expr.Add(Col("amount"), zero), "add"),
		Named(expr.Sub(Col("amount"), Col("amount")), "sub"),
		Named(expr.Mul(Col("amount"), one), "mul"),
		Named(sum, "cross_scale"),
		Named(expr.Mul(Col("other_scale"), Col("other_precision")), "product"),
		Named(If(Col("condition"), expr.Add(Col("amount"), zero), expr.Sub(Col("amount"), zero)), "guarded"),
		Named(expr.Add(Col("amount"), NullDecimal128(38, 12)), "null"),
	).Filter(expr.GE(Col("cross_scale"), zero)).Limit(71)
}

func checkDecimalArithmeticQuery(t *testing.T, column func(string) (Series, error), offset int) int {
	t.Helper()
	ids, err := column("id")
	if err != nil {
		t.Fatal(err)
	}
	values, err := ids.ToAnySlice()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"add", "sub", "mul", "cross_scale", "product", "guarded", "null"} {
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
				if row != offset+i+70 {
					t.Fatal("source order/limit changed", row, offset+i+70)
				}
				amount, valid := decimalExpressionValue(row, "amount")
				want := amount.BigInt()
				scale := int32(12)
				switch name {
				case "sub":
					want = big.NewInt(0)
				case "cross_scale":
					want, valid = big.NewInt(int64(row-70)*11), true
				case "product":
					want, valid, scale = new(big.Int).Mul(big.NewInt(int64(row-70)), big.NewInt(int64(row-70))), true, 23
				case "null":
					valid = false
				}
				got, ok, err := chunk.Value(j)
				if err != nil || ok != valid || ok && got.BigInt().Cmp(want) != 0 || chunk.Precision() != 38 || chunk.Scale() != scale {
					t.Fatalf("%s row=%d got=%s valid=%v p=%d s=%d want=%s valid=%v err=%v", name, row, got.BigInt(), ok, chunk.Precision(), chunk.Scale(), want, valid, err)
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

func TestParquetDecimal128ArithmeticQueryPaths(t *testing.T) {
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
				query := decimalArithmeticQuery(scan)
				frame, err := query.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if n := checkDecimalArithmeticQuery(t, frame.Column, 0); n != 71 {
					t.Fatal("row count", n)
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
					n += checkDecimalArithmeticQuery(t, view.Column, n)
				}
				if err := reader.Err(); err != nil {
					t.Fatal(err)
				}
				reader.Close()
				if n != 71 || session.Engine().Memory().Used() != 0 {
					t.Fatal("stream row count or leak", n)
				}
				outPath := filepath.Join(t.TempDir(), "arithmetic.parquet")
				if _, err := query.SinkParquet(context.Background(), outPath, ParquetSinkOptions{}); err != nil {
					t.Fatal(err)
				}
				roundtrip, err := session.ScanParquet(outPath)
				if err != nil {
					t.Fatal(err)
				}
				frame, err = roundtrip.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if n := checkDecimalArithmeticQuery(t, frame.Column, 0); n != 71 {
					t.Fatal("roundtrip rows", n)
				}
				frame.Close()
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("sink/readback leaked")
				}
			})
		}
	}
}

func TestParquetDecimal128ArithmeticErrorDomain(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	zero := Decimal128(memory.Decimal128{}, 38, 12)
	one := Decimal128(decimal128.FromI64(1), 12, 12)
	bad := expr.Add(Col("amount"), one)
	predicate := expr.EQ(bad, zero)
	for _, bounded := range []bool{false, true} {
		opts := decimalExpressionOptions(bounded)
		session := NewSession(opts)
		scan, err := session.ScanParquet(path)
		if err != nil {
			t.Fatal(err)
		}
		for name, query := range map[string]LazyFrame{
			"earlier_empty_filter": scan.Filter(Col("id").Lt(Lit(0))).Filter(predicate),
			"guarded":              scan.Filter(If(Bool(false), predicate, Bool(false))),
			"unused_projection":    scan.WithColumns(Named(bad, "unused")).Select("id"),
			"filtered_overflow":    scan.Filter(expr.LT(Col("amount"), Decimal128(max, 38, 12))).SelectExprs(Named(bad, "sum")),
		} {
			t.Run(fmt.Sprintf("safe/%s/bounded=%v", name, bounded), func(t *testing.T) {
				frame, err := query.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				frame.Close()
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("safe query leaked")
				}
			})
		}
		for name, query := range map[string]LazyFrame{
			"error_before_false_filter": scan.Filter(predicate).Filter(Col("id").Lt(Lit(0))),
			"eager_and":                 scan.Filter(expr.And(Bool(false), predicate)),
			"demanded_projection":       scan.SelectExprs(Named(bad, "sum")),
		} {
			t.Run(fmt.Sprintf("error/%s/bounded=%v", name, bounded), func(t *testing.T) {
				frame, err := query.Collect(context.Background())
				frame.Close()
				if err == nil || !strings.Contains(err.Error(), "decimal128(38,12) overflows at row 0") {
					t.Fatal("demanded overflow suppressed", err)
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
				if err == nil || !strings.Contains(err.Error(), "overflows at row 0") {
					t.Fatal("stream error mismatch", err)
				}
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("failed stream leaked")
				}
			})
		}
		session.Close()
	}
}

func TestParquetDecimal128ArithmeticStreamLifecycle(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	for _, mode := range []string{"early_close", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			session := NewSession(decimalExpressionOptions(true))
			defer session.Close()
			scan, err := session.ScanParquet(path)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := decimalArithmeticQuery(scan).CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 512, MaxBatchBytes: 2048})
			if err != nil {
				t.Fatal(err)
			}
			if !reader.Next(context.Background()) {
				t.Fatal("first batch missing", reader.Err())
			}
			view := reader.Batch()
			series, err := view.Column("add")
			if err != nil {
				t.Fatal(err)
			}
			chunks, err := series.Decimal128Chunks()
			if err != nil || len(chunks) == 0 {
				t.Fatal(err)
			}
			if mode == "cancel" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if reader.Next(ctx) || !errors.Is(reader.Err(), context.Canceled) {
					t.Fatal("cancel mismatch", reader.Err())
				}
			}
			if err := reader.Close(); err != nil && !(mode == "cancel" && errors.Is(err, context.Canceled)) {
				t.Fatal(err)
			}
			if view.Valid() {
				t.Fatal("borrowed result survived close")
			}
			if _, _, err := chunks[0].Value(0); !errors.Is(err, ErrDataFrameClosed) {
				t.Fatal("chunk survived close", err)
			}
			if session.Engine().Memory().Used() != 0 {
				t.Fatal("lifecycle leaked")
			}
		})
	}
}
