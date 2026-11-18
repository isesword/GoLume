package lazy_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/lazy"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func decimalArrowCheck(t *testing.T, column func(string) (lazy.Series, error), name string, precision, scale int32, start, count int, expected func(int) (*big.Int, bool)) {
	t.Helper()
	s, err := column(name)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := s.Decimal128Chunks()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, c := range chunks {
		if c.Precision() != precision || c.Scale() != scale {
			t.Fatalf("metadata %d,%d", c.Precision(), c.Scale())
		}
		for i := 0; i < c.Len(); i++ {
			want, valid := expected(start + n)
			got, ok, err := c.Value(i)
			if err != nil || ok != valid || ok && got.BigInt().Cmp(want) != 0 {
				t.Fatalf("row %d coefficient=%v valid=%v want=%v/%v err=%v", start+n, got, ok, want, valid, err)
			}
			n++
		}
	}
	if n != count {
		t.Fatalf("rows %d want %d", n, count)
	}
}

func decimalArrowField(t *testing.T, q lazy.LazyFrame, name string, p, s int32, nullable bool) {
	t.Helper()
	schema, err := q.Schema()
	if err != nil {
		t.Fatal(err)
	}
	f, ok := schema.Field(name)
	if !ok || f.Type != memory.TypeDecimal || f.DecimalPrecision != p || f.DecimalScale != s || f.Nullable != nullable {
		t.Fatalf("field %+v", f)
	}
}

func TestDecimalArrowIPCNativeReentry(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, bounded := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode%d/bounded%v", mode, bounded), func(t *testing.T) {
				s := decimalArrowDownstreamSession(t, mode, bounded)
				ctx := context.Background()
				path := filepath.Join(t.TempDir(), "native.arrow")
				if _, err := decimalArrowDownstreamInput(t, s).SinkIPC(ctx, path, lazy.IPCSinkOptions{}); err != nil {
					t.Fatal(err)
				}
				f, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				reader, err := ipc.NewFileReader(f)
				if err != nil {
					f.Close()
					t.Fatal(err)
				}
				var records []arrow.RecordBatch
				for k := 0; k < reader.NumRecords(); k++ {
					rec, err := reader.RecordBatchAt(k)
					if err != nil {
						t.Fatal(err)
					}
					// Multiple records include nonzero array offsets and cross 64-bit validity words.
					for start := int64(0); start < rec.NumRows(); start += 31 {
						records = append(records, rec.NewSlice(start, min(start+31, rec.NumRows())))
					}
					rec.Release()
				}
				if len(records) < 2 {
					t.Fatal("fixture must exercise multirecord slices")
				}
				legacy, err := s.ScanArrow(records...)
				if err != nil {
					t.Fatal(err)
				}
				zero, err := s.ScanArrowWithOptions(lazy.ArrowScanOptions{}, records...)
				if err != nil {
					t.Fatal(err)
				}
				native, err := s.ScanArrowWithOptions(lazy.ArrowScanOptions{NativeDecimal128: true}, records...)
				if err != nil {
					t.Fatal(err)
				}
				for _, rec := range records {
					rec.Release()
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				decimalArrowField(t, native, "amount", 21, 0, true)
				for _, old := range []lazy.LazyFrame{legacy, zero} {
					decimalArrowField(t, old, "amount", 0, 0, true)
					df, err := old.SelectExprs(expr.CastDecimal128(expr.Col("amount"), 21, 0).Alias("d")).Collect(ctx)
					if err == nil {
						df.Close()
						t.Fatal("legacy became native")
					}
					if !strings.Contains(err.Error(), "requires native decimal128 precision") {
						t.Fatal(err)
					}
				}
				// Independent expected coefficients derive from original integer inputs,
				// not from the production Decimal result or a floating-point intermediate.
				want := func(row int) (*big.Int, bool) {
					v := new(big.Int).Mul(decimalArrowDownstreamWant(row), big.NewInt(100))
					v.Add(v, big.NewInt(500))
					return v, row%7 != 0
				}
				q := native.Filter(expr.GE(expr.Col("id"), expr.I64(0))).SelectExprs(expr.Add(expr.RescaleDecimal128(expr.Col("amount"), 24, 2), expr.Decimal128(decimal128.FromI64(5), 1, 0)).Alias("out"))
				decimalArrowField(t, q, "out", 25, 2, true)
				for repeat := 0; repeat < 2; repeat++ {
					df, err := q.Collect(ctx)
					if err != nil {
						t.Fatal(err)
					}
					decimalArrowCheck(t, df.Column, "out", 25, 2, 0, decimalArrowDownstreamRows, want)
					df.Close()
				}
				selected := make([]int, 0, decimalArrowDownstreamRows)
				for row := 0; row < decimalArrowDownstreamRows; row++ {
					if row%7 != 0 {
						selected = append(selected, row)
					}
				}
				filtered := native.Filter(expr.GT(expr.Col("amount"), expr.Decimal128(decimal128.FromI64(0), 1, 0))).SelectExprs(expr.Col("amount").Alias("out"))
				filteredDF, err := filtered.Collect(ctx)
				if err != nil {
					t.Fatal(err)
				}
				decimalArrowCheck(t, filteredDF.Column, "out", 21, 0, 0, len(selected), func(i int) (*big.Int, bool) { return decimalArrowDownstreamWant(selected[i]), true })
				filteredDF.Close()
				// Early close must release a partially consumed query without losing source ownership.
				early, err := q.CollectBatches(ctx, lazy.BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
				if err != nil {
					t.Fatal(err)
				}
				if !early.Next(ctx) {
					t.Fatal("missing initial batch", early.Err())
				}
				if err := early.Close(); err != nil {
					t.Fatal(err)
				}
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("early-close reservation leak")
				}
				batches, err := q.CollectBatches(ctx, lazy.BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
				if err != nil {
					t.Fatal(err)
				}
				seen := 0
				for batches.Next(ctx) {
					series, err := batches.Batch().Column("out")
					if err != nil {
						t.Fatal(err)
					}
					chunks, err := series.Decimal128Chunks()
					if err != nil {
						t.Fatal(err)
					}
					n := 0
					for _, c := range chunks {
						n += c.Len()
					}
					decimalArrowCheck(t, batches.Batch().Column, "out", 25, 2, seen, n, want)
					seen += n
				}
				if err := batches.Err(); err != nil {
					t.Fatal(err)
				}
				batches.Close()
				if seen != decimalArrowDownstreamRows {
					t.Fatal(seen)
				}
				parquet := filepath.Join(t.TempDir(), "native.parquet")
				if _, err := q.SinkParquet(ctx, parquet, lazy.ParquetSinkOptions{RowGroupRows: 31}); err != nil {
					t.Fatal(err)
				}
				round, err := s.ScanParquet(parquet)
				if err != nil {
					t.Fatal(err)
				}
				decimalArrowField(t, round, "out", 25, 2, true)
				df, err := round.Collect(ctx)
				if err != nil {
					t.Fatal(err)
				}
				decimalArrowCheck(t, df.Column, "out", 25, 2, 0, decimalArrowDownstreamRows, want)
				df.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("pipeline reservation leak")
				}
			})
		}
	}
}

func decimalArrowBoundaryRecord(t *testing.T, alloc arrowmem.Allocator, scale int32) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: scale}, Nullable: true}}, nil)
	b := array.NewRecordBuilder(alloc, schema)
	d := b.Field(0).(*array.Decimal128Builder)
	max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
	d.Append(decimal128.FromBigInt(max))
	d.Append(decimal128.FromBigInt(new(big.Int).Neg(new(big.Int).Set(max))))
	d.Append(decimal128.FromI64(0))
	d.AppendNull()
	rec := b.NewRecordBatch()
	b.Release()
	return rec
}

func TestDecimalArrowPrecision38AndSessionOwnership(t *testing.T) {
	for _, scale := range []int32{0, 38} {
		t.Run(fmt.Sprint(scale), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			rec := decimalArrowBoundaryRecord(t, alloc, scale)
			opts := engine.DefaultOptions()
			opts.PreparedCacheBytes = 0
			s := lazy.NewSession(opts)
			q, err := s.ScanArrowWithOptions(lazy.ArrowScanOptions{NativeDecimal128: true}, rec)
			rec.Release()
			if err != nil {
				s.Close()
				t.Fatal(err)
			}
			defer func() { s.Close(); alloc.AssertSize(t, 0) }()
			decimalArrowField(t, q, "d", 38, scale, true)
			// Multiply by exact one exercises native arithmetic at both metadata limits.
			computed := q.SelectExprs(expr.Mul(expr.Col("d"), expr.Decimal128(decimal128.FromI64(1), 1, 0)).Alias("out"))
			max := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
			want := func(i int) (*big.Int, bool) {
				switch i {
				case 0:
					return new(big.Int).Set(max), true
				case 1:
					return new(big.Int).Neg(new(big.Int).Set(max)), true
				case 2:
					return new(big.Int), true
				default:
					return nil, false
				}
			}
			df, err := computed.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			decimalArrowCheck(t, df.Column, "out", 38, scale, 0, 4, want)
			df.Close()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if df, err := computed.Collect(ctx); !errors.Is(err, context.Canceled) {
				if err == nil {
					df.Close()
				}
				t.Fatalf("cancel=%v", err)
			}
			if scale == 0 {
				overflow := q.SelectExprs(expr.Add(expr.Col("d"), expr.Decimal128(decimal128.FromI64(1), 1, 0)).Alias("out"))
				df, err := overflow.Collect(context.Background())
				if err == nil {
					df.Close()
					t.Fatal("overflow accepted")
				}
				if !strings.Contains(err.Error(), "overflow") {
					t.Fatal(err)
				}
			}
			sorted, err := q.Sort(lazy.Asc("d")).Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			sortedMax := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
			decimalArrowCheck(t, sorted.Column, "d", 38, scale, 0, 4, func(row int) (*big.Int, bool) {
				switch row {
				case 0:
					return new(big.Int).Neg(new(big.Int).Set(sortedMax)), true
				case 1:
					return big.NewInt(0), true
				case 2:
					return sortedMax, true
				default:
					return big.NewInt(0), false
				}
			})
			sorted.Close()
			// Native opt-in must not remove existing downstream admission boundaries.
			for _, tc := range []struct {
				q    lazy.LazyFrame
				want string
			}{
				{q.Agg(lazy.Sum("d", "sum")), "decimal does not support numeric reductions"},

				{q.WithWindows(lazy.NewWindow(nil, lazy.OrderBy(lazy.Asc("d"))), lazy.RowNumber("n")), "not orderable"},
				{q.Join(q, lazy.JoinOptions{Type: lazy.InnerJoin, LeftOn: []string{"d"}, RightOn: []string{"d"}, Suffix: "_r"}), "native decimal execution is not supported"},
			} {
				df, err := tc.q.Collect(context.Background())
				if err == nil {
					df.Close()
					t.Fatal("native consumer boundary removed")
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatal(err)
				}
			}
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("error/cancel reservation leak")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			alloc.AssertSize(t, 0)
			if df, err := q.Collect(context.Background()); err == nil {
				df.Close()
				t.Fatal("closed session query accepted")
			}
		})
	}
}

func TestDecimalArrowBudgetFailureReleasesRecords(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	rec := decimalArrowBoundaryRecord(t, alloc, 0)
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1
	opts.PreparedCacheBytes = 0
	s := lazy.NewSession(opts)
	q, err := s.ScanArrowWithOptions(lazy.ArrowScanOptions{NativeDecimal128: true}, rec)
	rec.Release()
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	df, err := q.SelectExprs(expr.RescaleDecimal128(expr.Col("d"), 38, 0).Alias("out")).Collect(context.Background())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		if err == nil {
			df.Close()
		}
		s.Close()
		t.Fatalf("expected budget error: %v", err)
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("budget reservation leak")
	}
	s.Close()
	alloc.AssertSize(t, 0)
}

func TestDecimalArrowConstructorFailureSessionOwnership(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := lazy.NewSession(opts)
	defer func() { s.Close(); alloc.AssertSize(t, 0) }()
	for _, tc := range []struct {
		name           string
		p, scale       int32
		nullable, null bool
		coefficient    int64
		want           string
	}{
		{"coefficient", 2, 0, true, false, 100, "precision"},
		{"metadata", 38, -1, true, false, 1, "scale"},
		{"nonnull", 2, 0, false, true, 0, "null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: &arrow.Decimal128Type{Precision: tc.p, Scale: tc.scale}, Nullable: tc.nullable}}, nil)
			b := array.NewRecordBuilder(alloc, schema)
			d := b.Field(0).(*array.Decimal128Builder)
			if tc.null {
				d.AppendNull()
			} else {
				d.Append(decimal128.FromI64(tc.coefficient))
			}
			rec := b.NewRecordBatch()
			b.Release()
			_, err := s.ScanArrowWithOptions(lazy.ArrowScanOptions{NativeDecimal128: true}, rec)
			rec.Release()
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("constructor error %v", err)
			}
			// No caller reference remains: a failed constructor must not retain this record.
			alloc.AssertSize(t, 0)
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("failed constructor query reservations")
			}
		})
	}
	// A failed scan must not poison subsequent successful registration and Close.
	rec := decimalArrowBoundaryRecord(t, alloc, 0)
	q, err := s.ScanArrowWithOptions(lazy.ArrowScanOptions{NativeDecimal128: true}, rec)
	rec.Release()
	if err != nil {
		t.Fatal(err)
	}
	df, err := q.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	df.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	alloc.AssertSize(t, 0)
}

// Fixtures copied from decimal_downstream_recipe_test.go for isolated coverage.
const decimalArrowDownstreamRows = 130

func decimalArrowDownstreamInput(t *testing.T, s *lazy.Session) lazy.LazyFrame {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = decimalArrowDownstreamRows
	ids, is, us := make([]int64, decimalArrowDownstreamRows), make([]int64, decimalArrowDownstreamRows), make([]uint64, decimalArrowDownstreamRows)
	valid := make(memory.Bitmap, memory.WordsFor(decimalArrowDownstreamRows))
	for r := range ids {
		ids[r] = int64(r)
		is[r] = decimalArrowDownstreamSigned(r)
		us[r] = math.MaxUint64 - uint64(r)
		if r%7 != 0 {
			valid[r/64] |= 1 << uint(r%64)
		}
	}
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	b.Int64s["i"] = &memory.Int64Vector{Values: is, Validity: valid}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: us}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "u", Type: memory.TypeUint64}), b)
	if err != nil {
		t.Fatal(err)
	}
	return s.FromSource(src).SelectExprs(expr.Named(expr.Col("id"), "id"), expr.Named(expr.Sub(expr.CastDecimal128(expr.Col("u"), 20, 0), expr.CastDecimal128(expr.Col("i"), 20, 0)), "amount"))
}

func decimalArrowDownstreamSigned(r int) int64 {
	if r%2 == 0 {
		return math.MinInt64 + int64(r)
	}
	return 9007199254740993 + int64(r)
}

func decimalArrowDownstreamWant(r int) *big.Int {
	return new(big.Int).Sub(new(big.Int).SetUint64(math.MaxUint64-uint64(r)), big.NewInt(decimalArrowDownstreamSigned(r)))
}

func decimalArrowDownstreamSession(t *testing.T, mode kernel.Mode, bounded bool) *lazy.Session {
	t.Helper()
	opts := engine.DefaultOptions()
	opts.Workers = 1
	opts.KernelMode = mode
	opts.PreparedCacheBytes = 0
	if bounded {
		opts.ExecutionPolicy = engine.ExecutionBounded
		opts.MemoryLimitBytes = 16 << 20
		opts.Spill = memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
	}
	s := lazy.NewSession(opts)
	t.Cleanup(func() { _ = s.Close() })
	return s
}
