package lazy_test

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/lazy"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

const downstreamRows = 130

func downstreamInput(t *testing.T, s *lazy.Session) lazy.LazyFrame {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = downstreamRows
	ids, is, us := make([]int64, downstreamRows), make([]int64, downstreamRows), make([]uint64, downstreamRows)
	valid := make(memory.Bitmap, memory.WordsFor(downstreamRows))
	for r := range ids {
		ids[r] = int64(r)
		is[r] = downstreamSigned(r)
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
func downstreamSigned(r int) int64 {
	if r%2 == 0 {
		return math.MinInt64 + int64(r)
	}
	return 9007199254740993 + int64(r)
}
func downstreamWant(r int) *big.Int {
	return new(big.Int).Sub(new(big.Int).SetUint64(math.MaxUint64-uint64(r)), big.NewInt(downstreamSigned(r)))
}
func downstreamCheck(t *testing.T, col func(string) (lazy.Series, error), offset int) int {
	t.Helper()
	s, err := col("amount")
	if err != nil {
		t.Fatal(err)
	}
	cs, err := s.Decimal128Chunks()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, c := range cs {
		if c.Precision() != 21 || c.Scale() != 0 {
			t.Fatal("lost decimal metadata")
		}
		for j := 0; j < c.Len(); j++ {
			r := offset + n
			v, valid, err := c.Value(j)
			if err != nil || valid != (r%7 != 0) || valid && v.BigInt().Cmp(downstreamWant(r)) != 0 {
				t.Fatalf("row %d value=%v valid=%v error=%v", r, v, valid, err)
			}
			n++
		}
	}
	return n
}
func downstreamSession(t *testing.T, mode kernel.Mode, bounded bool) *lazy.Session {
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
func TestExplicitDecimalDownstreamTransport(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, bounded := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode=%v/bounded=%v", mode, bounded), func(t *testing.T) {
				s := downstreamSession(t, mode, bounded)
				q := downstreamInput(t, s)
				ctx := context.Background()
				df, err := q.Collect(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if downstreamCheck(t, df.Column, 0) != downstreamRows {
					t.Fatal("collect rows")
				}
				_ = df.Close()
				reader, err := q.CollectBatches(ctx, lazy.BatchOptions{TargetBatchBytes: 4096, MaxBatchBytes: 32768})
				if err != nil {
					t.Fatal(err)
				}
				n := 0
				for reader.Next(ctx) {
					n += downstreamCheck(t, reader.Batch().Column, n)
				}
				if err := reader.Err(); err != nil {
					t.Fatal(err)
				}
				_ = reader.Close()
				if n != downstreamRows {
					t.Fatal("stream rows", n)
				}
				p := filepath.Join(t.TempDir(), "exact.parquet")
				result, err := q.SinkParquet(ctx, p, lazy.ParquetSinkOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if result.Rows != downstreamRows {
					t.Fatal(result.Rows)
				}
				round, err := s.ScanParquet(p)
				if err != nil {
					t.Fatal(err)
				}
				df, err = round.Collect(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if downstreamCheck(t, df.Column, 0) != downstreamRows {
					t.Fatal("parquet rows")
				}
				_ = df.Close()
				p = filepath.Join(t.TempDir(), "exact.arrow")
				result, err = q.SinkIPC(ctx, p, lazy.IPCSinkOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if result.Rows != downstreamRows {
					t.Fatal(result.Rows)
				}
				f, err := os.Open(p)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				ir, err := ipc.NewFileReader(f)
				if err != nil {
					t.Fatal(err)
				}
				defer ir.Close()
				n = 0
				for k := 0; k < ir.NumRecords(); k++ {
					rec, err := ir.RecordBatchAt(k)
					if err != nil {
						t.Fatal(err)
					}
					field := rec.Schema().Field(1)
					dt, ok := field.Type.(*arrow.Decimal128Type)
					if !ok || dt.Precision != 21 || dt.Scale != 0 || !field.Nullable {
						t.Fatal("Arrow metadata", field)
					}
					a, ok := rec.Column(1).(*array.Decimal128)
					if !ok {
						t.Fatal("Arrow type")
					}
					for j := 0; j < a.Len(); j++ {
						r := n + j
						if a.IsValid(j) != (r%7 != 0) || a.IsValid(j) && a.Value(j).BigInt().Cmp(downstreamWant(r)) != 0 {
							t.Fatal("Arrow coefficient/null", r)
						}
					}
					n += a.Len()
					if k == 0 {
						reentry, err := s.ScanArrow(rec)
						if err != nil {
							t.Fatal(err)
						}
						native := reentry.SelectExprs(expr.Named(expr.CastDecimal128(expr.Col("amount"), 21, 0), "d"))
						df, err := native.Collect(ctx)
						if err == nil {
							_ = df.Close()
							t.Fatal("unexpected native Decimal Arrow reentry acceptance")
						}
						if !strings.Contains(err.Error(), "requires native decimal128 precision") || !strings.Contains(err.Error(), "precision=0 scale=0") {
							t.Fatal("unexpected Arrow reentry error", err)
						}
						t.Logf("Arrow reentry: %v", err)
					}
					rec.Release()
				}
				if n != downstreamRows {
					t.Fatal("IPC rows", n)
				}
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("released query budget not zero")
				}
			})
		}
	}
}
func TestExplicitDecimalDownstreamConsumerBoundaries(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		s := downstreamSession(t, kernel.ModeScalar, bounded)
		q := downstreamInput(t, s)
		cases := map[string]lazy.LazyFrame{"sum": q.Agg(lazy.Sum("amount", "n")), "min": q.Agg(lazy.Min("amount", "n")), "max": q.Agg(lazy.Max("amount", "n")), "avg": q.Agg(lazy.Avg("amount", "n")), "count_value": q.Agg(lazy.CountColumn("amount", "n")), "group_decimal": q.GroupBy("amount").Agg(lazy.Count("n")), "sort_decimal": q.Sort(lazy.Asc("amount")), "sort_payload": q.Sort(lazy.Asc("id")), "join_decimal": q.Join(q, lazy.JoinOptions{Type: lazy.InnerJoin, LeftOn: []string{"amount"}, RightOn: []string{"amount"}, Suffix: "_r"}), "join_payload": q.Join(q, lazy.JoinOptions{Type: lazy.InnerJoin, LeftOn: []string{"id"}, RightOn: []string{"id"}, Suffix: "_r"})}
		for name, f := range cases {
			df, err := f.Collect(context.Background())
			t.Logf("bounded=%v consumer=%s err=%v", bounded, name, err)
			if name == "sum" {
				if err != nil {
					t.Fatal(err)
				}
				c, e := df.Column("n")
				if e != nil {
					t.Fatal(e)
				}
				chunks, e := c.Decimal128Chunks()
				if e != nil {
					t.Fatal(e)
				}
				want := new(big.Int)
				for i := 0; i < downstreamRows; i++ {
					if i%7 != 0 {
						want.Add(want, downstreamWant(i))
					}
				}
				seen := 0
				for _, ch := range chunks {
					if ch.Precision() != 38 || ch.Scale() != 0 {
						t.Fatal("SUM metadata")
					}
					for i := 0; i < ch.Len(); i++ {
						v, valid, e := ch.Value(i)
						if e != nil || !valid || v.BigInt().Cmp(want) != 0 {
							t.Fatal("SUM exact oracle", v, valid, e)
						}
						seen++
					}
				}
				if seen != 1 {
					t.Fatal("SUM rows", seen)
				}
				df.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("SUM leak")
				}
				continue
			}
			if name == "sort_decimal" || name == "sort_payload" {
				if err != nil {
					t.Fatal(err)
				}
				rows := make([]int, downstreamRows)
				for i := range rows {
					rows[i] = i
				}
				if name == "sort_decimal" {
					sort.SliceStable(rows, func(i, j int) bool {
						a, b := rows[i], rows[j]
						an, bn := a%7 == 0, b%7 == 0
						if an != bn {
							return !an
						}
						if an {
							return false
						}
						return downstreamWant(a).Cmp(downstreamWant(b)) < 0
					})
				}
				decimalArrowCheck(t, df.Column, "amount", 21, 0, 0, downstreamRows, func(i int) (*big.Int, bool) { r := rows[i]; return downstreamWant(r), r%7 != 0 })
				df.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("resident result leak")
				}
				continue
			}
			if err == nil {
				_ = df.Close()
				t.Fatalf("expected documented current boundary for %s", name)
			}
			want := "native decimal execution is not supported"
			switch name {
			case "sum", "min", "max", "avg":
				want = "decimal does not support numeric reductions"
			case "count_value", "group_decimal":
				want = "native decimal aggregation is not supported"
			}
			allowedSumBoundary := (name == "min" || name == "max" || name == "avg" || name == "count_value" || name == "group_decimal") && (strings.Contains(err.Error(), "native Decimal aggregation requires global SUM only") || strings.Contains(err.Error(), "native Decimal aggregation requires only global native Decimal SUM outputs") || strings.Contains(err.Error(), "native Decimal aggregation requires at least one native Decimal SUM"))
			if !strings.Contains(err.Error(), want) && !allowedSumBoundary && !(name == "group_decimal" && err.Error() == "native Decimal mixed aggregate requires at least one SUM") {
				t.Fatalf("%s: wanted %q or explicit global-SUM-only boundary got %v", name, want, err)
			}
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("rejection leaked query memory")
			}

		}
	}
}

// This tests the codec/store capability, not a spill-capable sort or join plan.
func TestExplicitDecimalDownstreamRealSpillCodec(t *testing.T) {
	s := downstreamSession(t, kernel.ModeScalar, false)
	df, err := downstreamInput(t, s).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	series, err := df.Column("amount")
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := series.Decimal128Chunks()
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = downstreamRows
	v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, downstreamRows), Validity: make(memory.Bitmap, memory.WordsFor(downstreamRows)), Precision: 21, Scale: 0}
	r := 0
	for _, c := range chunks {
		for j := 0; j < c.Len(); j++ {
			x, valid, e := c.Value(j)
			if e != nil {
				t.Fatal(e)
			}
			v.Values[r] = x
			if valid {
				v.Validity[r/64] |= 1 << uint(r%64)
			}
			r++
		}
	}
	b.Decimals["amount"] = v
	_ = df.Close()
	field, err := memory.Decimal128Field("amount", true, 21, 0)
	if err != nil {
		t.Fatal(err)
	}
	schema := memory.NewSchema(field)
	ctx := context.Background()
	m := memory.NewBudgetManager(1 << 20)
	encoded, err := memory.EncodeColumnarRun(ctx, schema, b, m)
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	store, err := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	spillDirectory := store.Directory()
	fingerprint := memory.SchemaFingerprint(schema)
	handle, err := store.SpillWithMetadata(ctx, memory.SpillObject{Kind: memory.SpillColumnarRun, Codec: memory.ColumnarRunCodec, SchemaFingerprint: fingerprint, Payload: encoded.Payload}, m)
	if err != nil {
		t.Fatal(err)
	}
	if store.Used() <= 0 || handle.Bytes() <= 0 {
		t.Fatal("no real spill bytes")
	}
	entries, err := os.ReadDir(store.Directory())
	if err != nil || len(entries) == 0 {
		t.Fatal("no spill file", err)
	}
	_ = encoded.Close()
	restored, err := store.Restore(ctx, handle, fingerprint, m)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	decoded, err := memory.DecodeColumnarRun(ctx, schema, restored, m)
	if err != nil {
		t.Fatal(err)
	}
	defer decoded.Close()
	got := decoded.Batch.Decimals["amount"]
	if got.Precision != 21 || got.Scale != 0 || decoded.Batch.Rows != downstreamRows {
		t.Fatal("decoded metadata")
	}
	for i := 0; i < downstreamRows; i++ {
		if got.IsValid(i) != (i%7 != 0) || got.IsValid(i) && got.Values[i].BigInt().Cmp(downstreamWant(i)) != 0 {
			t.Fatal("codec value/null", i)
		}
	}
	_ = decoded.Close()
	_ = restored.Close()
	if err := store.Delete(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(spillDirectory); !os.IsNotExist(err) {
		t.Fatalf("spill directory not removed: %v", err)
	}
	if store.Used() != 0 || m.Used() != 0 || s.Engine().Memory().Used() != 0 {
		t.Fatal("spill lifecycle leak")
	}
}

// CountRows consumes cardinality without requiring Decimal value aggregation.
func TestExplicitDecimalDownstreamCountRowsControl(t *testing.T) {
	s := downstreamSession(t, kernel.ModeScalar, false)
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	q := s.FromSource(src).SelectExprs(expr.Named(expr.Col("id"), "id"))
	assertDownstreamCountRows(t, s, q, 3)
	for _, bounded := range []bool{false, true} {
		ds := downstreamSession(t, kernel.ModeScalar, bounded)
		assertDownstreamCountRows(t, ds, downstreamInput(t, ds), downstreamRows)
	}
}

func assertDownstreamCountRows(t *testing.T, s *lazy.Session, q lazy.LazyFrame, want uint64) {
	t.Helper()
	df, err := q.Agg(lazy.Count("n")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	col, err := df.Column("n")
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := col.Uint64Chunks()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0].Len() != 1 {
		t.Fatalf("unexpected count shape: %v", chunks)
	}
	got, valid, err := chunks[0].Value(0)
	if err != nil || !valid || got != want {
		t.Fatalf("count got %d valid %v err %v want %d", got, valid, err, want)
	}
	if err := df.Close(); err != nil {
		t.Fatal(err)
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("count query budget leak")
	}
}
