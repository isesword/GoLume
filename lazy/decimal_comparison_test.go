package lazy

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func decimalParquetComparisonOracle(a memory.Decimal128, as int64, b memory.Decimal128, bs int64) int {
	left := new(big.Int).Mul(a.BigInt(), new(big.Int).Exp(big.NewInt(10), big.NewInt(bs), nil))
	right := new(big.Int).Mul(b.BigInt(), new(big.Int).Exp(big.NewInt(10), big.NewInt(as), nil))
	return left.Cmp(right)
}

func TestParquetDecimal128ExactComparisonQueries(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	nearOne := Decimal128(max, 38, 38)
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
				gt := expr.GT(Col("amount"), nearOne)
				selected := scan.SelectExprs(
					Col("id").Alias("id"), Col("amount").Alias("renamed"), Col("other_scale").Alias("other"),
					Named(expr.LT(Col("amount"), Col("other_scale")), "less"),
					Named(expr.EQ(Col("amount"), Col("fallback")), "equal"),
					Named(gt, "greater"),
					Named(In(Col("amount"), Col("other_scale"), NullDecimal128(38, 38), Decimal128(memory.Decimal128{}, 1, 0)), "in"),
					Named(expr.Not(In(Col("amount"), Col("other_scale"), NullDecimal128(38, 38), Decimal128(memory.Decimal128{}, 1, 0))), "not_in"),
					nearOne.Alias("literal"), NullDecimal128(38, 38).Alias("typed_null"),
				).Filter(Col("id").Ge(Lit(2))).Limit(91)
				frame, err := selected.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				got := make(map[string][]any)
				for _, name := range []string{"id", "less", "equal", "greater", "in", "not_in"} {
					series, err := frame.Column(name)
					if err != nil {
						t.Fatal(err)
					}
					got[name], err = series.ToAnySlice()
					if err != nil {
						t.Fatal(err)
					}
				}
				if frame.Height() != 91 {
					t.Fatal("unexpected row count", frame.Height())
				}
				for i, id := range got["id"] {
					row := int(id.(int64))
					if row != i+2 {
						t.Fatal("stable source/limit order changed")
					}
					a, av := decimalExpressionValue(row, "amount")
					b, bv := decimalExpressionValue(row, "other_scale")
					fallback, fv := decimalExpressionValue(row, "fallback")
					var less, equal, greater, in, notin any
					if av && bv {
						less = decimalParquetComparisonOracle(a, 12, b, 11) < 0
					}
					if av && fv {
						equal = decimalParquetComparisonOracle(a, 12, fallback, 12) == 0
					}
					if av {
						greater = decimalParquetComparisonOracle(a, 12, max, 38) > 0
					}
					if av && (a == (memory.Decimal128{}) || bv && decimalParquetComparisonOracle(a, 12, b, 11) == 0) {
						in = true
						notin = false
					}
					for name, want := range map[string]any{"less": less, "equal": equal, "greater": greater, "in": in, "not_in": notin} {
						if !reflect.DeepEqual(got[name][i], want) {
							t.Fatalf("%s id=%d got=%v want=%v", name, row, got[name][i], want)
						}
					}
				}
				for _, name := range []string{"renamed", "other", "typed_null"} {
					series, err := frame.Column(name)
					if err != nil {
						t.Fatal(err)
					}
					chunks, err := series.Decimal128Chunks()
					if err != nil {
						t.Fatal(err)
					}
					index := 0
					for _, chunk := range chunks {
						for i := 0; i < chunk.Len(); i++ {
							row := int(got["id"][index].(int64))
							var want memory.Decimal128
							valid := false
							if name == "renamed" {
								want, valid = decimalExpressionValue(row, "amount")
							}
							if name == "other" {
								want, valid = decimalExpressionValue(row, "other_scale")
							}
							value, ok, err := chunk.Value(i)
							if err != nil || ok != valid || valid && value != want {
								t.Fatalf("%s payload row %d mismatch", name, row)
							}
							index++
						}
					}
					if index != 91 {
						t.Fatal("payload row count")
					}
				}
				literal, _ := frame.Column("literal")
				chunks, err := literal.Decimal128Chunks()
				if err != nil {
					t.Fatal(err)
				}
				for _, chunk := range chunks {
					if chunk.Precision() != 38 || chunk.Scale() != 38 {
						t.Fatal("literal metadata lost")
					}
					for i := 0; i < chunk.Len(); i++ {
						v, valid, err := chunk.Value(i)
						if err != nil || !valid || v != max {
							t.Fatal("literal rounded")
						}
					}
				}
				frame.Close()
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("projected comparisons leaked")
				}
				// The same predicate works as a residual filter, after aliases, and as a
				// projected Boolean. Selected execution must preserve source row order.
				aliases := scan.SelectExprs(Col("id").Alias("id"), Col("amount").Alias("renamed"))
				queries := []LazyFrame{
					scan.Filter(gt).Select("id"),
					scan.Filter(expr.LT(nearOne, Col("amount"))).Select("id"),
					aliases.Filter(expr.GT(Col("renamed"), nearOne)).Select("id"),
					scan.SelectExprs(Col("id").Alias("id"), Named(gt, "keep")).Filter(Col("keep")).Select("id"),
				}
				var want []any
				for row := 0; row < decimalExpressionRows; row++ {
					a, valid := decimalExpressionValue(row, "amount")
					if valid && decimalParquetComparisonOracle(a, 12, max, 38) > 0 {
						want = append(want, int64(row))
					}
				}
				for index, query := range queries {
					result, err := query.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					series, err := result.Column("id")
					if err != nil {
						t.Fatal(err)
					}
					output, err := series.ToAnySlice()
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(output, want) {
						t.Fatalf("query=%d ids=%v want=%v", index, output, want)
					}
					result.Close()
					if session.Engine().Memory().Used() != 0 {
						t.Fatal("filtered comparisons leaked")
					}
				}
			})
		}
	}
}

func TestParquetDecimal128ComparisonErrorDomain(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	session := NewSession(decimalExpressionOptions(false))
	defer session.Close()
	scan, err := session.ScanParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	zero := Decimal128(memory.Decimal128{}, 1, 0)
	bad := If(expr.EQ(Cast(Str("invalid"), memory.TypeInt64), Lit(0)), zero, zero)
	predicate := expr.EQ(Col("amount"), bad)
	for name, query := range map[string]LazyFrame{
		"earlier_empty_filter": scan.Filter(Col("id").Lt(Lit(0))).Filter(predicate),
		"guarded":              scan.Filter(If(Bool(false), predicate, Bool(false))),
		"unused_projection":    scan.WithColumns(Named(predicate, "unused")).Select("id"),
	} {
		t.Run(name, func(t *testing.T) {
			out, err := query.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			out.Close()
			if session.Engine().Memory().Used() != 0 {
				t.Fatal("safe query leaked")
			}
		})
	}
	for name, query := range map[string]LazyFrame{
		"error_before_false_filter": scan.Filter(predicate).Filter(Col("id").Lt(Lit(0))),
		"eager_and":                 scan.Filter(expr.And(Bool(false), predicate)),
	} {
		t.Run(name, func(t *testing.T) {
			out, err := query.Collect(context.Background())
			out.Close()
			if err == nil {
				t.Fatal("demanded error was suppressed")
			}
			if session.Engine().Memory().Used() != 0 {
				t.Fatal("failed query leaked")
			}
		})
	}
}

func TestParquetDecimal128ComparisonStreamLifecycle(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	for _, mode := range []string{"eof", "early_close", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			session := NewSession(decimalExpressionOptions(true))
			defer session.Close()
			scan, err := session.ScanParquet(path)
			if err != nil {
				t.Fatal(err)
			}
			budget, err := session.Engine().BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx := memory.WithQueryBudget(context.Background(), budget)
			query := scan.Filter(expr.GT(Col("amount"), Decimal128(memory.Decimal128{}, 1, 0))).SelectExprs(Col("id").Alias("id"), Col("amount").Alias("amount"), Named(expr.LE(Col("amount"), Col("fallback")), "comparison"))
			reader, err := query.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 512, MaxBatchBytes: 2048})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if !reader.Next(ctx) {
				t.Fatal("first result missing", reader.Err())
			}
			view := reader.Batch()
			series, err := view.Column("amount")
			if err != nil {
				t.Fatal(err)
			}
			chunks, err := series.Decimal128Chunks()
			if err != nil || len(chunks) == 0 {
				t.Fatal("missing decimal payload", err)
			}
			switch mode {
			case "eof":
				for reader.Next(ctx) {
				}
				if err := reader.Err(); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				if reader.Next(canceled) || !errors.Is(reader.Err(), context.Canceled) {
					t.Fatal("cancellation lost", reader.Err())
				}
			}
			if err := reader.Close(); err != nil && !(mode == "cancel" && errors.Is(err, context.Canceled)) {
				t.Fatal(err)
			}
			if view.Valid() {
				t.Fatal("borrowed comparison output survived close")
			}
			if _, _, err := chunks[0].Value(0); !errors.Is(err, ErrDataFrameClosed) {
				t.Fatal("decimal payload survived close", err)
			}
			if snapshot := budget.Snapshot(); budget.Used() != 0 || snapshot.PeakBytes > budget.Limit() || snapshot.Closed {
				t.Fatalf("query budget lifecycle=%+v", snapshot)
			}
		})
	}
}
