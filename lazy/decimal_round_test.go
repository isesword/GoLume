package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var decimalRoundCoefficients = []int64{-350, -250, -150, -149, -101, -100, -99, -50, -1, 0, 1, 49, 50, 99, 100, 101, 149, 150, 250, 350, 9999, -9999}

func decimalRoundModes() []expr.DecimalRoundingMode {
	return []expr.DecimalRoundingMode{expr.DecimalRoundTowardZero, expr.DecimalRoundAwayFromZero, expr.DecimalRoundFloor, expr.DecimalRoundCeil, expr.DecimalRoundHalfEven, expr.DecimalRoundHalfAwayFromZero}
}

// Independent signed integer quotient/remainder oracle, with one rounding decision.
func decimalRoundOracle(coefficient int64, mode expr.DecimalRoundingMode) *big.Int {
	x := big.NewInt(coefficient)
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(x, big.NewInt(100), r)
	if r.Sign() == 0 {
		return q
	}
	sign := x.Sign()
	inc := false
	switch mode {
	case expr.DecimalRoundTowardZero:
	case expr.DecimalRoundAwayFromZero:
		inc = true
	case expr.DecimalRoundFloor:
		inc = sign < 0
	case expr.DecimalRoundCeil:
		inc = sign > 0
	case expr.DecimalRoundHalfEven, expr.DecimalRoundHalfAwayFromZero:
		twice := new(big.Int).Lsh(new(big.Int).Abs(r), 1)
		cmp := twice.Cmp(big.NewInt(100))
		inc = cmp > 0 || cmp == 0 && (mode == expr.DecimalRoundHalfAwayFromZero || q.Bit(0) == 1)
	}
	if inc {
		q.Add(q, big.NewInt(int64(sign)))
	}
	return q
}
func decimalRoundSource(t *testing.T) *inmemory.Source {
	t.Helper()
	f, e := memory.Decimal128Field("d", true, 6, 2)
	if e != nil {
		t.Fatal(e)
	}
	var batches []*memory.Batch
	for start := 0; start < 130; start += 31 {
		n := min(31, 130-start)
		b := memory.NewBatch()
		b.Reset(n)
		ids := make([]int64, n)
		v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, n), Validity: memory.EnsureBitmap(nil, n), Precision: 6, Scale: 2}
		for i := 0; i < n; i++ {
			r := start + i
			ids[i] = int64(r)
			v.Values[i] = decimal128.FromI64(decimalRoundCoefficients[r%len(decimalRoundCoefficients)])
			if r%7 != 0 {
				memory.Set(v.Validity, i)
			}
		}
		b.Int64s["id"] = &memory.Int64Vector{Values: ids}
		b.Decimals["d"] = v
		batches = append(batches, b)
	}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, f), batches...)
	if e != nil {
		t.Fatal(e)
	}
	return src
}
func decimalRoundCheck(t *testing.T, column func(string) (Series, error), rows []int) {
	t.Helper()
	for k, mode := range decimalRoundModes() {
		for _, name := range []string{fmt.Sprintf("r%d", k), fmt.Sprintf("copy%d", k)} {
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
				if v.Precision() != 4 || v.Scale() != 0 {
					t.Fatal("metadata")
				}
				for j := 0; j < v.Len(); j++ {
					if seen >= len(rows) {
						t.Fatal("too many")
					}
					row := rows[seen]
					got, valid, e := v.Value(j)
					want := decimalRoundOracle(decimalRoundCoefficients[row%len(decimalRoundCoefficients)], mode)
					if e != nil || valid != (row%7 != 0) || valid && got.BigInt().Cmp(want) != 0 {
						t.Fatalf("%s row%d got%v valid%v want%s err%v", name, row, got, valid, want, e)
					}
					seen++
				}
			}
			if seen != len(rows) {
				t.Fatal("count", seen, len(rows))
			}
		}
	}
}
func TestDecimalRoundQueryOracle(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.KernelMode = mode
			opts.PreparedCacheBytes = 0
			s := NewSession(opts)
			defer func() {
				s.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("leak")
				}
			}()
			src := decimalRoundSource(t)
			scan := s.FromSource(src)
			var outputs []expr.NamedExpr
			outputs = append(outputs, expr.Col("id").Alias("id"))
			for k, m := range decimalRoundModes() {
				r := RoundDecimal128(Col("d"), 4, 0, m)
				outputs = append(outputs, r.Alias(fmt.Sprintf("r%d", k)), r.Alias(fmt.Sprintf("copy%d", k)))
			}
			q := scan.Filter(expr.GE(expr.Col("id"), expr.I64(1))).SelectExprs(outputs...)
			rows := make([]int, 129)
			for i := range rows {
				rows[i] = i + 1
			}
			schema, e := q.Schema()
			if e != nil {
				t.Fatal(e)
			}
			for _, f := range schema.Fields[1:] {
				if f.DecimalPrecision != 4 || f.DecimalScale != 0 || !f.Nullable {
					t.Fatal(f)
				}
			}
			df, e := q.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			decimalRoundCheck(t, df.Column, rows)
			df.Close()
			// Post-round Decimal predicate must observe the rounded value rather than input digits.
			positive := q.Filter(expr.GT(expr.Col("r4"), expr.Decimal128(decimal128.FromI64(0), 1, 0)))
			var keep []int
			for _, r := range rows {
				if r%7 != 0 && decimalRoundOracle(decimalRoundCoefficients[r%len(decimalRoundCoefficients)], expr.DecimalRoundHalfEven).Sign() > 0 {
					keep = append(keep, r)
				}
			}
			df, e = positive.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			decimalRoundCheck(t, df.Column, keep)
			df.Close()
			stream, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
			if e != nil {
				t.Fatal(e)
			}
			seen := 0
			for stream.Next(context.Background()) {
				batch := stream.Batch()
				ids, e := batch.Column("id")
				if e != nil {
					t.Fatal(e)
				}
				n := int(ids.Len())
				decimalRoundCheck(t, batch.Column, rows[seen:seen+n])
				seen += n
			}
			if e := stream.Err(); e != nil {
				t.Fatal(e)
			}
			stream.Close()
			if seen != 129 {
				t.Fatal(seen)
			}
			p := filepath.Join(t.TempDir(), "rounded.parquet")
			if _, e := q.SinkParquet(context.Background(), p, ParquetSinkOptions{}); e != nil {
				t.Fatal(e)
			}
			round, e := s.ScanParquet(p)
			if e != nil {
				t.Fatal(e)
			}
			df, e = round.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			decimalRoundCheck(t, df.Column, rows)
			df.Close()
			p = filepath.Join(t.TempDir(), "rounded.arrow")
			if _, e := q.SinkIPC(context.Background(), p, IPCSinkOptions{}); e != nil {
				t.Fatal(e)
			}
			file, e := os.Open(p)
			if e != nil {
				t.Fatal(e)
			}
			reader, e := ipc.NewFileReader(file)
			if e != nil {
				t.Fatal(e)
			}
			defer file.Close()
			defer reader.Close()
			seen = 0
			for n := 0; n < reader.NumRecords(); n++ {
				rec, e := reader.RecordBatchAt(n)
				if e != nil {
					t.Fatal(e)
				}
				for k, m := range decimalRoundModes() {
					for _, idx := range []int{1 + k*2, 2 + k*2} {
						a := rec.Column(idx).(*array.Decimal128)
						dt := a.DataType().(*arrow.Decimal128Type)
						if dt.Precision != 4 || dt.Scale != 0 {
							t.Fatal(dt)
						}
						for j := 0; j < a.Len(); j++ {
							r := rows[seen+j]
							if a.IsValid(j) != (r%7 != 0) || a.IsValid(j) && a.Value(j).BigInt().Cmp(decimalRoundOracle(decimalRoundCoefficients[r%len(decimalRoundCoefficients)], m)) != 0 {
								t.Fatal("IPC coefficient/null")
							}
						}
					}
				}
				seen += int(rec.NumRows())
				rec.Release()
			}
			if seen != len(rows) {
				t.Fatal(seen)
			}
			// Baseline Arrow scan remains opaque; IPC transport is not native Decimal re-entry.
			rec, e := reader.RecordBatchAt(0)
			if e != nil {
				t.Fatal(e)
			}
			opaque, e := s.ScanArrow(rec)
			rec.Release()
			if e != nil {
				t.Fatal(e)
			}
			df, e = opaque.SelectExprs(expr.RoundDecimal128(expr.Col("r0"), 4, 0, expr.DecimalRoundHalfEven).Alias("bad")).Collect(context.Background())
			if e == nil {
				df.Close()
				t.Fatal("legacy Arrow unexpectedly native")
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
func TestDecimalRoundErrorsGuardsExactCompatibility(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	scan := s.FromSource(decimalRoundSource(t))
	for _, mode := range decimalRoundModes() {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			v := expr.Decimal128(decimal128.FromI64(99999), 5, 2)
			bad := expr.RoundDecimal128(v, 2, 0, mode)
			df, e := scan.SelectExprs(bad.Alias("bad")).Collect(context.Background())
			if e == nil {
				df.Close()
				t.Fatal("overflow accepted")
			}
			if !strings.Contains(e.Error(), "overflow") {
				t.Fatal(e)
			}
			safe := expr.If(expr.Bool(false), bad, expr.Decimal128(decimal128.FromI64(7), 2, 0))
			df, e = scan.SelectExprs(safe.Alias("safe")).Collect(context.Background())
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
				for i := 0; i < v.Len(); i++ {
					x, ok, e := v.Value(i)
					if e != nil || !ok || x.BigInt().Cmp(big.NewInt(7)) != 0 {
						t.Fatal("guard")
					}
				}
			}
			df.Close()
		})
	}
	for name, e := range map[string]expr.Expr{"rescale": expr.RescaleDecimal128(expr.Decimal128(decimal128.FromI64(125), 3, 2), 3, 1), "cast": expr.CastDecimal128(expr.Decimal128(decimal128.FromI64(125), 3, 2), 3, 1), "divide": expr.DivideDecimal128(expr.Decimal128(decimal128.FromI64(1), 1, 0), expr.Decimal128(decimal128.FromI64(3), 1, 0), 3, 2)} {
		t.Run(name, func(t *testing.T) {
			df, err := scan.SelectExprs(Named(e, "exact")).Collect(context.Background())
			if err == nil {
				df.Close()
				t.Fatal("exact operation silently rounded")
			}
			if !strings.Contains(err.Error(), "loses fractional digits") {
				t.Fatal(err)
			}
		})
	}
	for _, mode := range []expr.DecimalRoundingMode{0, 255} {
		_, e := scan.SelectExprs(expr.RoundDecimal128(expr.Col("d"), 4, 0, mode).Alias("bad")).Schema()
		if e == nil {
			t.Fatal("invalid mode bound")
		}
	}
	tinyOpts := opts
	tinyOpts.MemoryLimitBytes = 1
	tiny := NewSession(tinyOpts)
	defer tiny.Close()
	df, e := tiny.FromSource(decimalRoundSource(t)).SelectExprs(expr.RoundDecimal128(expr.Col("d"), 4, 0, expr.DecimalRoundHalfEven).Alias("r")).Collect(context.Background())
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

func TestDecimalRoundScalePrecisionAndDynamicGuard(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	scan := s.FromSource(decimalRoundSource(t))
	for _, mode := range decimalRoundModes() {
		for _, sign := range []int64{-1, 1} {
			t.Run(fmt.Sprintf("%d/%d", mode, sign), func(t *testing.T) {
				// Target precision is checked after rounding, not against original coefficient.
				c := int64(9999) * sign
				want := decimalRoundOracle(c, mode)
				node := expr.RoundDecimal128(expr.Decimal128(decimal128.FromI64(c), 4, 2), 2, 0, mode)
				df, e := scan.SelectExprs(node.Alias("r")).Collect(context.Background())
				over := new(big.Int).Abs(new(big.Int).Set(want)).Cmp(big.NewInt(100)) >= 0
				if over {
					if e == nil {
						df.Close()
						t.Fatal("carry overflow accepted")
					}
					if !strings.Contains(e.Error(), "overflow") {
						t.Fatal(e)
					}
					return
				}
				if e != nil {
					t.Fatal(e)
				}
				defer df.Close()
				col, e := df.Column("r")
				if e != nil {
					t.Fatal(e)
				}
				chunks, e := col.Decimal128Chunks()
				if e != nil {
					t.Fatal(e)
				}
				for _, v := range chunks {
					if v.Precision() != 2 || v.Scale() != 0 {
						t.Fatal("precision")
					}
					for i := 0; i < v.Len(); i++ {
						x, valid, e := v.Value(i)
						if e != nil || !valid || x.BigInt().Cmp(want) != 0 {
							t.Fatal("post-round precision")
						}
					}
				}
			})
		}
	}
	for _, mode := range decimalRoundModes() {
		t.Run(fmt.Sprintf("exact-scale%d", mode), func(t *testing.T) {
			q := scan.SelectExprs(expr.RoundDecimal128(expr.Col("d"), 7, 3, mode).Alias("up"), expr.RoundDecimal128(expr.Col("d"), 6, 2, mode).Alias("same"), expr.If(expr.LT(expr.Col("id"), expr.I64(65)), expr.RoundDecimal128(expr.Col("d"), 4, 0, mode), expr.NullDecimal128(4, 0)).Alias("guard"))
			df, e := q.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			for _, name := range []string{"up", "same", "guard"} {
				c, e := df.Column(name)
				if e != nil {
					t.Fatal(e)
				}
				chunks, e := c.Decimal128Chunks()
				if e != nil {
					t.Fatal(e)
				}
				row := 0
				for _, v := range chunks {
					p, scale := int32(6), int32(2)
					if name == "up" {
						p, scale = 7, 3
					}
					if name == "guard" {
						p, scale = 4, 0
					}
					if v.Precision() != p || v.Scale() != scale {
						t.Fatal("metadata")
					}
					for j := 0; j < v.Len(); j++ {
						valid := row%7 != 0
						want := big.NewInt(decimalRoundCoefficients[row%len(decimalRoundCoefficients)])
						if name == "up" {
							want.Mul(want, big.NewInt(10))
						}
						if name == "guard" {
							valid = valid && row < 65
							want = decimalRoundOracle(decimalRoundCoefficients[row%len(decimalRoundCoefficients)], mode)
						}
						x, ok, e := v.Value(j)
						if e != nil || ok != valid || ok && x.BigInt().Cmp(want) != 0 {
							t.Fatalf("%s row%d", name, row)
						}
						row++
					}
				}
				if row != 130 {
					t.Fatal(row)
				}
			}
		})
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("leak")
	}
}
