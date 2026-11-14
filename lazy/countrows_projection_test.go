package lazy

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func countRowsInput(t *testing.T, s *Session, n int) LazyFrame {
	t.Helper()
	var batches []*memory.Batch
	for start := 0; start < n; start += 65 {
		end := min(start+65, n)
		b := memory.NewBatch()
		b.Rows = end - start
		xs := make([]int64, b.Rows)
		validity := make(memory.Bitmap, memory.WordsFor(b.Rows))
		for j := range xs {
			xs[j] = int64(start + j)
			if (start+j)%2 == 0 {
				validity[j/64] |= 1 << uint(j%64)
			}
		}
		b.Int64s["x"] = &memory.Int64Vector{Values: xs}
		b.Int64s["nullable"] = &memory.Int64Vector{Values: xs, Validity: validity}
		batches = append(batches, b)
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "nullable", Type: memory.TypeInt64, Nullable: true}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	return s.FromSource(src)
}
func assertProjectionCount(t *testing.T, q LazyFrame, want uint64) {
	t.Helper()
	df, err := q.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	bs := df.batches()
	if len(bs) != 1 || bs[0].Rows != 1 || bs[0].Uint64s["n"].Values[0] != want {
		t.Fatalf("want count %d, batches=%+v", want, bs)
	}
}
func TestCountRowsProjection(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, n := range []int{0, 3, 130} {
			t.Run(fmt.Sprintf("mode%d/rows%d", mode, n), func(t *testing.T) {
				opts := engine.DefaultOptions()
				opts.KernelMode = mode
				opts.Workers = 1
				opts.PreparedCacheBytes = 0
				s := NewSession(opts)
				defer s.Close()
				base := countRowsInput(t, s, n)
				p := base.SelectExprs(expr.Named(expr.Col("x"), "a"))
				cases := []struct {
					name string
					q    LazyFrame
					want uint64
				}{
					{"plain", p.Agg(Count("n")), uint64(n)},
					{"nested", p.SelectExprs(expr.Named(expr.Add(expr.Col("a"), expr.I64(1)), "b")).Agg(Count("n")), uint64(n)},
					{"limit", p.Limit(2).Agg(Count("n")), uint64(min(n, 2))},
					{"filter", p.Filter(expr.GT(expr.Col("a"), expr.I64(1))).Agg(Count("n")), uint64(max(n-2, 0))},
					{"filter_before", base.Filter(expr.GT(expr.Col("x"), expr.I64(1))).SelectExprs(expr.Named(expr.Col("x"), "a")).Agg(Count("n")), uint64(max(n-2, 0))},
					{"count_column", base.SelectExprs(expr.Named(expr.Col("nullable"), "a")).Agg(CountColumn("a", "n")), uint64((n + 1) / 2)},
					{"dead_runtime_error", base.SelectExprs(expr.Named(expr.CastTo(expr.Add(expr.Col("x"), expr.I64(1000)), memory.TypeInt8), "bad")).Agg(Count("n")), uint64(n)},
				}
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) { assertProjectionCount(t, tc.q, tc.want) })
				}
				// Group keys remain demanded even though CountRows has no value dependency.
				g := base.SelectExprs(expr.Named(expr.Col("nullable"), "key")).GroupBy("key").Agg(Count("n"))
				df, err := g.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				var total uint64
				rows := 0
				for _, b := range df.batches() {
					rows += b.Rows
					for _, v := range b.Uint64s["n"].Values {
						total += v
					}
				}
				df.Close()
				wantGroups := (n + 1) / 2
				if n > 1 {
					wantGroups++
				}
				if total != uint64(n) || rows != wantGroups {
					t.Fatalf("groups=%d total=%d", rows, total)
				}
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("query memory leak")
				}
			})
		}
	}
}
func TestCountRowsProjectionErrorBoundaries(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	base := countRowsInput(t, s, 3)
	// Binding errors are never removed, even for otherwise dead outputs.
	bad := base.SelectExprs(expr.Named(expr.Col("missing"), "bad")).Agg(Count("n"))
	if df, err := bad.Collect(context.Background()); err == nil {
		df.Close()
		t.Fatal("missing dependency accepted")
	} else if !strings.Contains(err.Error(), "missing") {
		t.Fatal(err)
	}
	fallible := base.SelectExprs(expr.Named(expr.CastTo(expr.Add(expr.Col("x"), expr.I64(1000)), memory.TypeInt8), "bad"))
	// A demanded aggregate input, filter or group key must still evaluate and fail.
	for name, q := range map[string]LazyFrame{
		"count_column": fallible.Agg(CountColumn("bad", "n")),
		"filter":       fallible.Filter(expr.GT(expr.Col("bad"), expr.I64(0))).Agg(Count("n")),
		"group":        fallible.GroupBy("bad").Agg(Count("n")),
	} {
		t.Run(name, func(t *testing.T) {
			df, err := q.Collect(context.Background())
			if err == nil {
				df.Close()
				t.Fatal("demanded narrowing error suppressed")
			}
			if !strings.Contains(err.Error(), "cast int64 to int8 overflows at row 0") {
				t.Fatalf("wrong failure: %v", err)
			}
			t.Log(err)
		})
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("error memory leak")
	}
}

// A cardinality-only scan retains source columns. Elimination must not expose
// its hidden native Decimal field to aggregate admission.
func TestCountRowsProjectionHiddenDecimal(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	field, err := memory.Decimal128Field("hidden", false, 9, 2)
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	b.Decimals["hidden"] = &memory.Decimal128Vector{Values: make([]memory.Decimal128, 3), Precision: 9, Scale: 2}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, field), b)
	if err != nil {
		t.Fatal(err)
	}
	assertProjectionCount(t, s.FromSource(src).SelectExprs(expr.Named(expr.Col("x"), "a")).Agg(Count("n")), 3)
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("memory leak")
	}
}

func TestCountRowsProjectionOuterConstant(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	q := countRowsInput(t, s, 130).SelectExprs(expr.Named(expr.CastTo(expr.Add(expr.Col("x"), expr.I64(1000)), memory.TypeInt8), "dead")).SelectExprs(expr.Named(expr.I64(7), "visible"))
	df, err := q.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, b := range df.batches() {
		total += b.Rows
		if len(b.Int64s) != 1 {
			t.Fatal("hidden output leaked")
		}
		for _, v := range b.Int64s["visible"].Values {
			if v != 7 {
				t.Fatal(v)
			}
		}
	}
	df.Close()
	if total != 130 {
		t.Fatal(total)
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("memory leak")
	}
}

// This fix does not add general zero-column physical projection support.
func TestCountRowsProjectionEmptySelectBoundary(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	df, err := countRowsInput(t, s, 3).Select().Collect(context.Background())
	if err == nil {
		df.Close()
		t.Fatal("expected existing zero-column physical projection boundary")
	}
	if !strings.Contains(err.Error(), "physical projection requires expressions") {
		t.Fatal(err)
	}
}

func TestCountRowsProjectionDeadDecimal(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(fmt.Sprint(bounded), func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.PreparedCacheBytes = 0
			if bounded {
				opts.ExecutionPolicy = engine.ExecutionBounded
				opts.MemoryLimitBytes = 16 << 20
				opts.Spill = memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
			}
			s := NewSession(opts)
			defer s.Close()
			b := memory.NewBatch()
			b.Rows = 3
			b.Int64s["i"] = &memory.Int64Vector{Values: []int64{-1, 2, 3}}
			b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{^uint64(0), 5, 6}}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64}), b)
			if err != nil {
				t.Fatal(err)
			}
			p := s.FromSource(src).SelectExprs(expr.Named(expr.Sub(expr.CastDecimal128(expr.Col("u"), 20, 0), expr.CastDecimal128(expr.Col("i"), 20, 0)), "amount"))
			assertProjectionCount(t, p.Agg(Count("n")), 3)
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("memory leak")
			}
		})
	}
}
