package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var lazyRoundedDivideInputs = [][2]int64{{1, 20}, {-1, 20}, {1, -20}, {-1, -20}, {3, 20}, {5, 20}, {7, -20}, {1, 30}, {-1, 30}, {0, 10}, {9007199254740993, 30}, {-9007199254740993, 30}}

func lazyRoundedDivideModes() []expr.DecimalRoundingMode {
	return []expr.DecimalRoundingMode{expr.DecimalRoundTowardZero, expr.DecimalRoundAwayFromZero, expr.DecimalRoundFloor, expr.DecimalRoundCeil, expr.DecimalRoundHalfEven, expr.DecimalRoundHalfAwayFromZero}
}
func lazyRoundedDivideOracle(a, b int64, as, bs, scale int32, mode expr.DecimalRoundingMode) *big.Int {
	numerator, denominator := big.NewInt(a), big.NewInt(b)
	exponent := int64(scale + bs - as)
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(absLazyRoundedExponent(exponent)), nil)
	if exponent >= 0 {
		numerator.Mul(numerator, factor)
	} else {
		denominator.Mul(denominator, factor)
	}
	sign := numerator.Sign() * denominator.Sign()
	numerator.Abs(numerator)
	denominator.Abs(denominator)
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(numerator, denominator, r)
	inc := false
	if r.Sign() != 0 {
		switch mode {
		case expr.DecimalRoundTowardZero:
		case expr.DecimalRoundAwayFromZero:
			inc = true
		case expr.DecimalRoundFloor:
			inc = sign < 0
		case expr.DecimalRoundCeil:
			inc = sign > 0
		case expr.DecimalRoundHalfEven, expr.DecimalRoundHalfAwayFromZero:
			cmp := new(big.Int).Lsh(r, 1).Cmp(denominator)
			inc = cmp > 0 || cmp == 0 && (mode == expr.DecimalRoundHalfAwayFromZero || q.Bit(0) == 1)
		}
	}
	if inc {
		q.Add(q, big.NewInt(1))
	}
	if sign < 0 {
		q.Neg(q)
	}
	return q
}
func absLazyRoundedExponent(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
func lazyRoundedDivideSource(t *testing.T, s *Session, as, bs int32) LazyFrame {
	t.Helper()
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	t.Cleanup(func() { s.Close(); alloc.AssertSize(t, 0) })
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "a", Type: &arrow.Decimal128Type{Precision: 20, Scale: as}, Nullable: true}, {Name: "b", Type: &arrow.Decimal128Type{Precision: 6, Scale: bs}, Nullable: true}}, nil)
	var records []arrow.RecordBatch
	for start := 0; start < 130; start += 31 {
		rb := array.NewRecordBuilder(alloc, schema)
		for r := start; r < min(start+31, 130); r++ {
			pair := lazyRoundedDivideInputs[r%len(lazyRoundedDivideInputs)]
			rb.Field(0).(*array.Int64Builder).Append(int64(r))
			if r%7 == 0 {
				rb.Field(1).(*array.Decimal128Builder).AppendNull()
			} else {
				rb.Field(1).(*array.Decimal128Builder).Append(decimal128.FromI64(pair[0]))
			}
			if r%11 == 0 {
				rb.Field(2).(*array.Decimal128Builder).AppendNull()
			} else {
				rb.Field(2).(*array.Decimal128Builder).Append(decimal128.FromI64(pair[1]))
			}
		}
		records = append(records, rb.NewRecordBatch())
		rb.Release()
	}
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, records...)
	for _, r := range records {
		r.Release()
	}
	if e != nil {
		t.Fatal(e)
	}
	return q
}
func lazyRoundedDivideCheck(t *testing.T, column func(string) (Series, error), rows []int, as, bs int32) {
	t.Helper()
	for k, m := range lazyRoundedDivideModes() {
		for _, name := range []string{fmt.Sprintf("q%d", k), fmt.Sprintf("copy%d", k)} {
			c, e := column(name)
			if e != nil {
				t.Fatal(e)
			}
			chunks, e := c.Decimal128Chunks()
			if e != nil {
				t.Fatal(e)
			}
			seen := 0
			for _, v := range chunks {
				if v.Precision() != 25 || v.Scale() != 2 {
					t.Fatal("metadata")
				}
				for j := 0; j < v.Len(); j++ {
					if seen >= len(rows) {
						t.Fatal("too many")
					}
					r := rows[seen]
					pair := lazyRoundedDivideInputs[r%len(lazyRoundedDivideInputs)]
					want := lazyRoundedDivideOracle(pair[0], pair[1], as, bs, 2, m)
					x, valid, e := v.Value(j)
					if e != nil || valid != (r%7 != 0 && r%11 != 0) || valid && x.BigInt().Cmp(want) != 0 {
						t.Fatalf("%s row%d got%v valid%v want%v err%v", name, r, x, valid, want, e)
					}
					seen++
				}
			}
			if seen != len(rows) {
				t.Fatal("length", seen, len(rows))
			}
		}
	}
}

func TestDecimalDivideRoundArrowPipeline(t *testing.T) {
	for _, scales := range [][2]int32{{2, 1}, {0, 3}, {5, 0}} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("%d_%d/%d", scales[0], scales[1], mode), func(t *testing.T) {
				as, bs := scales[0], scales[1]
				opts := engine.DefaultOptions()
				opts.KernelMode = mode
				opts.PreparedCacheBytes = 0
				s := NewSession(opts)
				defer func() {
					s.Close()
					if s.Engine().Memory().Used() != 0 {
						t.Error("leak")
					}
				}()
				scan := lazyRoundedDivideSource(t, s, as, bs)
				outputs := []expr.NamedExpr{Col("id").Alias("id")}
				for k, m := range lazyRoundedDivideModes() {
					q := DivideRoundDecimal128(Col("a"), Col("b"), 25, 2, m)
					outputs = append(outputs, q.Alias(fmt.Sprintf("q%d", k)), q.Alias(fmt.Sprintf("copy%d", k)))
				}
				q := scan.Filter(expr.GE(Col("id"), expr.I64(1))).SelectExprs(outputs...)
				rows := make([]int, 129)
				for i := range rows {
					rows[i] = i + 1
				}
				schema, e := q.Schema()
				if e != nil {
					t.Fatal(e)
				}
				for _, f := range schema.Fields[1:] {
					if f.DecimalPrecision != 25 || f.DecimalScale != 2 || !f.Nullable {
						t.Fatal(f)
					}
				}
				df, e := q.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				lazyRoundedDivideCheck(t, df.Column, rows, as, bs)
				df.Close()
				filtered := q.Filter(expr.GT(Col("q4"), expr.Decimal128(decimal128.FromI64(0), 1, 0)))
				var keep []int
				for _, r := range rows {
					pair := lazyRoundedDivideInputs[r%len(lazyRoundedDivideInputs)]
					if r%7 != 0 && r%11 != 0 && lazyRoundedDivideOracle(pair[0], pair[1], as, bs, 2, expr.DecimalRoundHalfEven).Sign() > 0 {
						keep = append(keep, r)
					}
				}
				df, e = filtered.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				lazyRoundedDivideCheck(t, df.Column, keep, as, bs)
				df.Close()
				stream, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
				if e != nil {
					t.Fatal(e)
				}
				seen := 0
				for stream.Next(context.Background()) {
					v := stream.Batch()
					ids, e := v.Column("id")
					if e != nil {
						t.Fatal(e)
					}
					n := int(ids.Len())
					lazyRoundedDivideCheck(t, v.Column, rows[seen:seen+n], as, bs)
					seen += n
				}
				if e := stream.Err(); e != nil {
					t.Fatal(e)
				}
				stream.Close()
				if seen != len(rows) {
					t.Fatal(seen)
				}
				for _, format := range []string{"ipc", "parquet"} {
					path := filepath.Join(t.TempDir(), "rounded."+format)
					var round LazyFrame
					if format == "parquet" {
						if _, e := q.SinkParquet(context.Background(), path, ParquetSinkOptions{}); e != nil {
							t.Fatal(e)
						}
						round, e = s.ScanParquet(path)
					} else {
						if _, e := q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
							t.Fatal(e)
						}
						file, err := os.Open(path)
						if err != nil {
							t.Fatal(err)
						}
						reader, err := ipc.NewFileReader(file)
						if err != nil {
							t.Fatal(err)
						}
						var records []arrow.RecordBatch
						seen := 0
						for n := 0; n < reader.NumRecords(); n++ {
							rec, err := reader.RecordBatchAt(n)
							if err != nil {
								t.Fatal(err)
							}
							for k, m := range lazyRoundedDivideModes() {
								for _, idx := range []int{1 + 2*k, 2 + 2*k} {
									a := rec.Column(idx).(*array.Decimal128)
									dt := a.DataType().(*arrow.Decimal128Type)
									if dt.Precision != 25 || dt.Scale != 2 {
										t.Fatal(dt)
									}
									for j := 0; j < a.Len(); j++ {
										r := rows[seen+j]
										pair := lazyRoundedDivideInputs[r%len(lazyRoundedDivideInputs)]
										valid := r%7 != 0 && r%11 != 0
										want := lazyRoundedDivideOracle(pair[0], pair[1], as, bs, 2, m)
										if a.IsValid(j) != valid || valid && a.Value(j).BigInt().Cmp(want) != 0 {
											t.Fatal("IPC exact coefficient/NULL")
										}
									}
								}
							}
							seen += int(rec.NumRows())
							records = append(records, rec)
						}
						if seen != len(rows) {
							t.Fatal(seen)
						}
						round, e = s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, records...)
						for _, rec := range records {
							rec.Release()
						}
						reader.Close()
						file.Close()
					}
					if e != nil {
						t.Fatal(e)
					}
					df, e = round.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					lazyRoundedDivideCheck(t, df.Column, rows, as, bs)
					df.Close()
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				df, e = q.Collect(ctx)
				if !errors.Is(e, context.Canceled) {
					if e == nil {
						df.Close()
					}
					t.Fatal(e)
				}
				early, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
				if e != nil {
					t.Fatal(e)
				}
				if !early.Next(context.Background()) {
					t.Fatal(early.Err())
				}
				early.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("early/cancel leak")
				}
			})
		}
	}
}

func TestDecimalDivideRoundErrorsGuardsAndExact(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	scan := lazyRoundedDivideSource(t, s, 2, 1)
	one := expr.Decimal128(decimal128.FromI64(1), 1, 0)
	zero := expr.Decimal128(decimal128.FromI64(0), 1, 0)
	for _, mode := range lazyRoundedDivideModes() {
		for _, tc := range []struct {
			name string
			a, b expr.Expr
			want string
		}{{"divide_zero", one, zero, "zero"}, {"zero_zero", zero, zero, "zero"}, {"overflow", expr.Decimal128(decimal128.FromI64(999), 3, 0), one, "overflow"}} {
			t.Run(fmt.Sprintf("%d/%s", mode, tc.name), func(t *testing.T) {
				bad := expr.DivideRoundDecimal128(tc.a, tc.b, 2, 0, mode)
				df, e := scan.SelectExprs(bad.Alias("bad")).Collect(context.Background())
				if e == nil {
					df.Close()
					t.Fatal("demanded error accepted")
				}
				if !strings.Contains(e.Error(), tc.want) {
					t.Fatal(e)
				}
				safe := expr.If(expr.LT(Col("id"), expr.I64(0)), bad, expr.Decimal128(decimal128.FromI64(7), 2, 0))
				df, e = scan.SelectExprs(Named(safe, "safe")).Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				c, e := df.Column("safe")
				if e != nil {
					t.Fatal(e)
				}
				chunks, e := c.Decimal128Chunks()
				if e != nil {
					t.Fatal(e)
				}
				for _, v := range chunks {
					if v.Precision() != 2 || v.Scale() != 0 {
						t.Fatal("metadata")
					}
					for j := 0; j < v.Len(); j++ {
						x, ok, e := v.Value(j)
						if e != nil || !ok || x.BigInt().Cmp(big.NewInt(7)) != 0 {
							t.Fatal("guard")
						}
					}
				}
				df.Close()
			})
		}
	}
	// A true dynamic guard selects actual non-NULL quotient rows and NULL fallback.
	guarded := scan.SelectExprs(Named(expr.If(expr.LT(Col("id"), expr.I64(65)), expr.DivideRoundDecimal128(Col("a"), Col("b"), 25, 2, expr.DecimalRoundHalfEven), expr.NullDecimal128(25, 2)), "g"))
	df, e := guarded.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	c, e := df.Column("g")
	if e != nil {
		t.Fatal(e)
	}
	chunks, e := c.Decimal128Chunks()
	if e != nil {
		t.Fatal(e)
	}
	row := 0
	for _, v := range chunks {
		for j := 0; j < v.Len(); j++ {
			pair := lazyRoundedDivideInputs[row%len(lazyRoundedDivideInputs)]
			want := lazyRoundedDivideOracle(pair[0], pair[1], 2, 1, 2, expr.DecimalRoundHalfEven)
			x, ok, e := v.Value(j)
			valid := row < 65 && row%7 != 0 && row%11 != 0
			if e != nil || ok != valid || ok && x.BigInt().Cmp(want) != 0 {
				t.Fatal("dynamic guard row", row)
			}
			row++
		}
	}
	if row != 130 {
		t.Fatal(row)
	}
	df.Close()
	// NULL operand masks zero as defined by the binary Decimal NULL contract.
	nullq := expr.DivideRoundDecimal128(expr.NullDecimal128(1, 0), zero, 2, 0, expr.DecimalRoundHalfEven)
	df, e = scan.SelectExprs(nullq.Alias("n")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	c, e = df.Column("n")
	if e != nil {
		t.Fatal(e)
	}
	if c.NullCount() != 130 {
		t.Fatal("NULL/zero")
	}
	df.Close()
	df, e = scan.SelectExprs(expr.DivideDecimal128(one, expr.Decimal128(decimal128.FromI64(3), 1, 0), 3, 2).Alias("exact")).Collect(context.Background())
	if e == nil {
		df.Close()
		t.Fatal("old exact rounded")
	}
	if !strings.Contains(e.Error(), "loses fractional digits") {
		t.Fatal(e)
	}
	for _, mode := range []expr.DecimalRoundingMode{0, 255} {
		_, e := scan.SelectExprs(expr.DivideRoundDecimal128(one, one, 3, 2, mode).Alias("bad")).Schema()
		if e == nil {
			t.Fatal("invalid mode bound")
		}
	}
	tinyOpts := opts
	tinyOpts.MemoryLimitBytes = 1
	tiny := NewSession(tinyOpts)
	defer tiny.Close()
	tinyScan := lazyRoundedDivideSource(t, tiny, 2, 1)
	df, e = tinyScan.SelectExprs(expr.DivideRoundDecimal128(Col("a"), Col("b"), 25, 2, expr.DecimalRoundHalfEven).Alias("r")).Collect(context.Background())
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		if e == nil {
			df.Close()
		}
		t.Fatal(e)
	}
	if tiny.Engine().Memory().Used() != 0 || s.Engine().Memory().Used() != 0 {
		t.Fatal("error leak")
	}
}

func TestDecimalDivideRoundBindingAndChildErrors(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	scan := lazyRoundedDivideSource(t, s, 2, 1)
	one := expr.Decimal128(decimal128.FromI64(1), 1, 0)
	zero := expr.Decimal128(decimal128.FromI64(0), 1, 0)
	null := expr.NullDecimal128(1, 0)
	for _, operands := range [][2]expr.Expr{{zero, null}, {null, zero}} {
		node := expr.DivideRoundDecimal128(operands[0], operands[1], 2, 0, expr.DecimalRoundHalfEven)
		df, e := scan.SelectExprs(node.Alias("n")).Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		c, e := df.Column("n")
		if e != nil || c.NullCount() != 130 {
			t.Fatal("NULL order", e)
		}
		df.Close()
	}
	// NULL propagation is not permission to suppress evaluation of a failing child.
	child := expr.DivideRoundDecimal128(one, zero, 1, 0, expr.DecimalRoundHalfEven)
	df, e := scan.SelectExprs(expr.DivideRoundDecimal128(null, child, 2, 0, expr.DecimalRoundHalfEven).Alias("bad")).Collect(context.Background())
	if e == nil {
		df.Close()
		t.Fatal("child error swallowed")
	}
	if !strings.Contains(e.Error(), "zero") {
		t.Fatal(e)
	}
	for _, bad := range []expr.Expr{expr.DivideRoundDecimal128(one, one, 2, 0, 0), expr.DivideRoundDecimal128(one, one, 0, 0, expr.DecimalRoundHalfEven), expr.DivideRoundDecimal128(one, one, 2, 3, expr.DecimalRoundHalfEven)} {
		_, e := scan.SelectExprs(Named(expr.If(expr.Bool(false), bad, expr.Decimal128(decimal128.FromI64(7), 2, 0)), "guard")).Schema()
		if e == nil {
			t.Fatal("dead invalid mode/meta bound")
		}
	}
	_, e = scan.SelectExprs(Named(expr.Div(one, one), "plain")).Schema()
	if e == nil {
		t.Fatal("ordinary Decimal Div broadened")
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("error leak")
	}
}
