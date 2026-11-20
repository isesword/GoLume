package physical

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

type n43OracleRow struct {
	ts, value int64
	valid     bool
}
type n43OracleResult struct {
	rows, count int64
	sum         *big.Int
	avg         float64
}

func n43Expected(rows []n43OracleRow, width int64) []n43OracleResult {
	out := make([]n43OracleResult, len(rows))
	for i, row := range rows {
		hi := big.NewInt(row.ts)
		lo := new(big.Int).Sub(new(big.Int).Set(hi), big.NewInt(width))
		total := new(big.Int)
		for _, candidate := range rows {
			ts := big.NewInt(candidate.ts)
			if ts.Cmp(lo) < 0 || ts.Cmp(hi) > 0 {
				continue
			}
			out[i].rows++
			if candidate.valid {
				out[i].count++
				total.Add(total, big.NewInt(candidate.value))
			}
		}
		if out[i].count > 0 {
			out[i].sum = total
			out[i].avg, _ = new(big.Rat).SetFrac(total, big.NewInt(out[i].count)).Float64()
		}
	}
	return out
}
func n43OracleInput(t *testing.T, rows []n43OracleRow, chunk int) source.Source {
	t.Helper()
	schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Encoding: memory.EncodingPlain}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true, Encoding: memory.EncodingPlain})
	var pages []*memory.Batch
	for start := 0; start < len(rows); start += chunk {
		end := min(start+chunk, len(rows))
		b := memory.NewBatch()
		b.Rows = end - start
		ts := &memory.Int64Vector{Values: make([]int64, b.Rows)}
		value := &memory.Int64Vector{Values: make([]int64, b.Rows), Validity: make(memory.Bitmap, memory.WordsFor(b.Rows))}
		for i, row := range rows[start:end] {
			ts.Values[i] = row.ts
			value.Values[i] = row.value
			if row.valid {
				memory.Set(value.Validity, i)
			}
		}
		b.Int64s["ts"] = ts
		b.Int64s["v"] = value
		pages = append(pages, b)
	}
	src, err := inmemory.New(schema, pages...)
	if err != nil {
		t.Fatal(err)
	}
	return &n43CountedInput{Source: src.WithOrdering(source.OrderingColumn{Name: "ts"})}
}
func n43OracleSpec(width int64) TimestampRangeSpec {
	return TimestampRangeSpec{OrderBy: "ts", Column: "v", Frame: TimestampRangeFrame{Kind: WindowFrameRange, PrecedingNS: width}, CountRowsAlias: "cr", CountAlias: "cv", SumAlias: "sum", AvgAlias: "avg"}
}
func n43OracleOpen(t *testing.T, input source.Source, spec TimestampRangeSpec, limit int64) (context.Context, source.BatchStream, *memory.QueryBudget) {
	t.Helper()
	src, err := NewTimestampRangeSource(input, spec)
	if err != nil {
		t.Fatalf("range constructor: %v", err)
	}
	if limit >= 0 {
		src, err = NewLimitSource(src, limit)
		if err != nil {
			t.Fatal(err)
		}
	}
	root := memory.NewEngineBudget(8 << 20)
	q, err := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, err := source.OpenBatchStream(ctx, src, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 2, MemoryManager: q})
	if err != nil {
		opens := -1
		if counted, ok := input.(*n43CountedInput); ok {
			opens = counted.opens
		}
		t.Logf("execution open error: %v; actual source opens=%d queryUsed=%d", err, opens, q.Used())
		if q.Used() != 0 {
			t.Errorf("failed open leaked query bytes")
		}
		q.Close()
		t.Fatalf("RANGE EXECUTION BARRIER: %v", err)
	}
	t.Cleanup(func() {
		st.Close()
		if q.Used() != 0 {
			t.Errorf("range leaked %d", q.Used())
		}
		q.Close()
	})
	return ctx, st, q
}
func n43CheckResults(t *testing.T, ctx context.Context, st source.BatchStream, rows []n43OracleRow, want []n43OracleResult, spec TimestampRangeSpec) {
	t.Helper()
	b := memory.NewBatch()
	at := 0
	for {
		ok, err := st.Next(ctx, b)
		if err != nil {
			t.Fatalf("RANGE NEXT BARRIER: %v", err)
		}
		if !ok {
			break
		}
		for i := 0; i < b.Rows; i++ {
			if at >= len(want) {
				t.Fatal("extra row")
			}
			if b.Int64s["ts"].Values[i] != rows[at].ts {
				t.Fatal("input order/cardinality changed")
			}
			for name, w := range map[string]int64{"cr": want[at].rows, "cv": want[at].count} {
				v := b.Int64s[name]
				if v == nil || !v.IsValid(i) || v.Values[i] != w {
					t.Fatalf("row%d %s got%v want%d", at, name, v, w)
				}
			}
			if spec.SumAlias != "" {
				v := b.Int64s[spec.SumAlias]
				if v == nil {
					t.Fatal("missing sum")
				}
				if want[at].sum == nil {
					if v.IsValid(i) {
						t.Fatal("allNULL sum valid")
					}
				} else if !want[at].sum.IsInt64() {
					t.Fatal("oracle expected overflow not error")
				} else if !v.IsValid(i) || v.Values[i] != want[at].sum.Int64() {
					t.Fatalf("row%d sum got%v want%s", at, v, want[at].sum)
				}
			}
			v := b.Float64s["avg"]
			if v == nil {
				t.Fatal("missing avg")
			}
			if want[at].sum == nil {
				if v.IsValid(i) {
					t.Fatal("allNULL avg valid")
				}
			} else if !v.IsValid(i) || math.Float64bits(v.Values[i]) != math.Float64bits(want[at].avg) {
				t.Fatalf("row%d avg got%v want%.17g", at, v, want[at].avg)
			}
			at++
		}
	}
	if at != len(want) {
		t.Fatalf("rows%d want%d", at, len(want))
	}
}
func TestN43TimestampRangeOracle(t *testing.T) {
	cases := []struct {
		name  string
		rows  []n43OracleRow
		width int64
	}{
		{"peers-zero", []n43OracleRow{{0, 5, true}, {0, 99, false}, {0, -2, true}, {1, 9, false}, {1, 7, false}, {2, 4, true}}, 0},
		{"inclusive-boundary", []n43OracleRow{{-3, 3, true}, {-1, 2, true}, {0, 8, true}, {0, -5, true}, {2, 9, false}, {3, -1, true}}, 3},
		{"underflow", []n43OracleRow{{math.MinInt64, 2, true}, {math.MinInt64 + 1, 3, true}, {math.MinInt64 + 1, -2, true}, {-1, 1, true}, {0, 4, true}, {math.MaxInt64, 2, true}}, math.MaxInt64},
		{"peer-overflow-cancellation", []n43OracleRow{{4, math.MaxInt64, true}, {4, math.MaxInt64, true}, {4, -math.MaxInt64, true}, {5, -math.MaxInt64, true}}, 0},
		{"eviction-exact", []n43OracleRow{{0, -math.MaxInt64, true}, {1, math.MaxInt64, true}, {2, -math.MaxInt64, true}, {3, math.MaxInt64, true}}, 1},
	}
	for _, tc := range cases {
		for _, chunk := range []int{1, 2, 4} {
			t.Run(fmt.Sprintf("%s_chunk%d", tc.name, chunk), func(t *testing.T) {
				spec := n43OracleSpec(tc.width)
				ctx, st, _ := n43OracleOpen(t, n43OracleInput(t, tc.rows, chunk), spec, -1)
				n43CheckResults(t, ctx, st, tc.rows, n43Expected(tc.rows, tc.width), spec)
			})
		}
	}
}
func TestN43TimestampRangeOracleLimitCompletesPeers(t *testing.T) {
	rows := []n43OracleRow{{0, math.MaxInt64, true}, {0, math.MaxInt64, true}, {0, -math.MaxInt64, true}, {1, 8, true}}
	spec := n43OracleSpec(0)
	ctx, st, _ := n43OracleOpen(t, n43OracleInput(t, rows, 1), spec, 1)
	n43CheckResults(t, ctx, st, rows[:1], n43Expected(rows, 0)[:1], spec)
}

type n43CountedInput struct {
	source.Source
	opens int
}

func (s *n43CountedInput) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "ts"}}
}
func (s *n43CountedInput) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	return source.OpenBatchStream(ctx, s.Source, opts)
}

type n43PoisonInput struct {
	source.Source
	invalidations int
}

func (s *n43PoisonInput) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "ts"}}
}
func (s *n43PoisonInput) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	in, err := source.OpenBatchStream(ctx, s.Source, opts)
	if err != nil {
		return nil, err
	}
	return &n43PoisonStream{BatchStream: in, owner: s}, nil
}

type n43PoisonStream struct {
	source.BatchStream
	owner *n43PoisonInput
	old   []*memory.Int64Vector
}

func (s *n43PoisonStream) poison() {
	for _, v := range s.old {
		for i := range v.Values {
			v.Values[i] = math.MinInt64
		}
		s.owner.invalidations++
	}
	s.old = nil
}
func (s *n43PoisonStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.poison()
	ok, err := s.BatchStream.Next(ctx, b)
	if ok && err == nil {
		s.old = []*memory.Int64Vector{b.Int64s["ts"], b.Int64s["v"]}
	}
	return ok, err
}
func (s *n43PoisonStream) Close() error { s.poison(); return s.BatchStream.Close() }
func TestN43TimestampRangeOracleEmptyNullAndPoison(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []n43OracleRow
	}{
		{"empty", nil},
		{"allNULL", []n43OracleRow{{0, math.MaxInt64, false}, {0, math.MinInt64, false}, {1, 999, false}}},
		{"poison", []n43OracleRow{{-1, 4, true}, {0, math.MaxInt64, true}, {0, -math.MaxInt64, true}, {0, 6, true}, {1, 2, true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := &n43PoisonInput{Source: n43OracleInput(t, tc.rows, 1)}
			spec := n43OracleSpec(1)
			ctx, st, _ := n43OracleOpen(t, input, spec, -1)
			n43CheckResults(t, ctx, st, tc.rows, n43Expected(tc.rows, 1), spec)
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if len(tc.rows) > 0 && input.invalidations == 0 {
				t.Fatal("source invalidation not exercised")
			}
			t.Logf("source leaf invalidations=%d", input.invalidations)
		})
	}
}
