package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/memory"
)

// The fixture owns only test input. All production state/output is charged to
// the independent manager passed to the public bounded entry point.
type n32RefRow struct {
	key      int64
	keyValid bool
	value    memory.Decimal128
	valid    bool
}
type n32RefInput struct {
	rows                          []n32RefRow
	pos, calls, closed, batchRows int
	precision, scale              int32
	emptyBatches                  int
}

func (s *n32RefInput) Next(ctx context.Context, dst *DecimalBoundedInputBatch) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.calls++
	*dst = DecimalBoundedInputBatch{}
	dst.Precision, dst.Scale = s.precision, s.scale
	if s.emptyBatches > 0 {
		s.emptyBatches--
		return true, nil
	}
	if s.pos == len(s.rows) {
		return false, nil
	}
	n := s.batchRows
	if n == 0 {
		n = 256
	}
	if n > len(s.rows)-s.pos {
		n = len(s.rows) - s.pos
	}
	for i := 0; i < n; i++ {
		r := s.rows[s.pos+i]
		dst.Records[i].Key = r.key
		dst.Records[i].KeyValid = r.keyValid
		dst.Records[i].Value = r.value
		dst.Records[i].ValueValid = r.valid
	}
	dst.Rows = n
	s.pos += n
	return true, nil
}
func (s *n32RefInput) Close() error { s.closed++; return nil }
func n32RefNewInput(rows []n32RefRow, batch int) *n32RefInput {
	return &n32RefInput{rows: rows, batchRows: batch, precision: 38, scale: 9}
}
func n32RefCoeff(v memory.Decimal128) *big.Int {
	n := new(big.Int).SetUint64(uint64(v.HighBits()))
	n.Lsh(n, 64)
	n.Add(n, new(big.Int).SetUint64(v.LowBits()))
	if v.HighBits() < 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	return n
}
func n32RefOptions(t *testing.T, capacity int) (DecimalBoundedGroupedOptions, *memory.BudgetManager, *memory.QuerySpillStore) {
	t.Helper()
	m := memory.NewBudgetManager(16 << 20)
	store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := store.Close(); e != nil {
			t.Error(e)
		}
		if m.Used() != 0 || store.Used() != 0 {
			t.Errorf("retained memory/disk %d/%d", m.Used(), store.Used())
		}
	})
	return DecimalBoundedGroupedOptions{Manager: m, Store: store, Precision: 38, Scale: 9, TableCapacity: capacity}, m, store
}
func n32RefOracle(t *testing.T, rows []n32RefRow, inputBatch, capacity int, minRuns int) {
	t.Helper()
	want := map[n30RefKey]*n30RefValue{}
	for _, r := range rows {
		k := n30RefKey{r.keyValid, r.key}
		if !k.valid {
			k.key = 0
		}
		v := want[k]
		if v == nil {
			v = &n30RefValue{sum: new(big.Int)}
			want[k] = v
		}
		v.rows++
		if r.valid {
			v.valid++
			v.sum.Add(v.sum, n32RefCoeff(r.value))
		}
	}
	keys := make([]n30RefKey, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return n30RefLess(keys[i], keys[j]) })
	opts, _, store := n32RefOptions(t, capacity)
	input := n32RefNewInput(rows, inputBatch)
	result, e := ExecuteDecimalBoundedGrouped(context.Background(), input, opts)
	if result != nil {
		t.Cleanup(func() {
			if e := result.Close(context.Background()); e != nil {
				t.Error(e)
			}
		})
	}
	if e != nil {
		t.Fatal(e)
	}
	if result == nil {
		t.Fatal("nil successful result")
	}
	if input.closed != 1 {
		t.Fatalf("input Close count %d", input.closed)
	}
	stats := result.Stats
	if stats.InputRows != len(rows) || stats.Groups != len(keys) {
		t.Fatalf("stats %+v rows=%d groups=%d", stats, len(rows), len(keys))
	}
	if stats.Flushes < minRuns {
		t.Fatalf("expected actual >=%d flushes: %+v", minRuns, stats)
	}
	if minRuns > 8 && stats.MergePasses < 2 {
		t.Fatalf("not actual multipass: %+v", stats)
	}
	if len(rows) > 0 && store.Used() == 0 {
		t.Fatal("no actual disk-backed result")
	}
	seen := 0
	for {
		batch, ok, err := result.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if batch == nil || batch.Rows < 1 || batch.Rows > 256 {
			t.Fatal("invalid output page")
		}
		if batch.Precision != 38 || batch.Scale != 9 {
			t.Fatalf("output metadata %d/%d", batch.Precision, batch.Scale)
		}
		for i := 0; i < batch.Rows; i++ {
			if seen >= len(keys) {
				t.Fatal("extra output")
			}
			r := batch.Records[i]
			k := keys[seen]
			v := want[k]
			if r.KeyValid != k.valid || k.valid && r.Key != k.key {
				t.Fatalf("key row%d got %d/%v want %+v", seen, r.Key, r.KeyValid, k)
			}
			if !r.KeyValid && r.Key != 0 {
				t.Fatal("noncanonical NULL key")
			}
			if r.CountValid != v.valid || r.CountRows != v.rows || r.SumValid != (v.valid != 0) {
				t.Fatalf("count/NULL mismatch row%d: %+v want %+v", seen, r, v)
			}
			if r.SumValid && n32RefCoeff(r.Sum).Cmp(v.sum) != 0 {
				t.Fatalf("sum row%d got %s want %s", seen, n32RefCoeff(r.Sum), v.sum)
			}
			seen++
		}
	}
	if seen != len(keys) {
		t.Fatalf("missing groups got%d want%d", seen, len(keys))
	}
	if e := result.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
}

func TestN32BoundedReferencePipelineOracle(t *testing.T) {
	for _, tc := range []struct {
		name                                    string
		count, groups, capacity, batch, minRuns int
	}{
		{"budget_selected_capacity", 273, 273, 0, 19, 1},
		{"nine_runs", 273, 273, 32, 19, 9}, {"sixtyfour_runs", 16384, 16384, 256, 251, 64}, {"shared_groups", 1280, 97, 32, 113, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]n32RefRow, tc.count)
			for i := range rows {
				k := int64((i*37)%tc.groups) - int64(tc.groups/2)
				rows[i] = n32RefRow{k, true, decimal128.FromI64(int64(i%1009) - 504), i%11 != 0}
			}
			n32RefOracle(t, rows, tc.batch, tc.capacity, tc.minRuns)
		})
	}
}
func TestN32BoundedReferenceNullEmptyExtremes(t *testing.T) {
	t.Run("empty", func(t *testing.T) { n32RefOracle(t, nil, 1, 1, 0) })
	t.Run("null_and_signed_extremes", func(t *testing.T) {
		rows := []n32RefRow{{99, false, decimal128.FromI64(5), true}, {-99, false, decimal128.FromI64(-2), true}, {0, false, memory.Decimal128{}, false}, {math.MinInt64, true, memory.Decimal128{}, false}, {math.MaxInt64, true, decimal128.FromI64(7), true}, {0, true, decimal128.FromI64(-8), true}, {math.MinInt64, true, memory.Decimal128{}, false}}
		n32RefOracle(t, rows, 2, 1, 1)
	})
	t.Run("all_null_value", func(t *testing.T) {
		rows := make([]n32RefRow, 290)
		for i := range rows {
			rows[i] = n32RefRow{int64(i % 17), true, decimal128.New(math.MinInt64, 0), false}
		}
		n32RefOracle(t, rows, 7, 16, 1)
	})
}
func TestN32BoundedReferenceWideOnlyFinalizedAtEnd(t *testing.T) {
	max, e := decimal128.FromString("99999999999999999999999999999999999999", 38, 0)
	if e != nil {
		t.Fatal(e)
	}
	// One group exceeds p38 before each flush; intervening keys force 16 runs.
	rows := make([]n32RefRow, 0, 64)
	for i := 0; i < 16; i++ {
		v := max
		if i >= 8 {
			v = v.Negate()
		}
		rows = append(rows, n32RefRow{0, true, v, true}, n32RefRow{0, true, v, true}, n32RefRow{int64(i + 1), true, memory.Decimal128{}, false})
	}
	t.Run("exact_cancellation", func(t *testing.T) { n32RefOracle(t, rows, 5, 2, 9) })
	t.Run("final_overflow_no_result", func(t *testing.T) {
		positive := append([]n32RefRow(nil), rows...)
		for i := range positive {
			if positive[i].valid {
				positive[i].value = max
			}
		}
		opts, _, _ := n32RefOptions(t, 2)
		out, e := ExecuteDecimalBoundedGrouped(context.Background(), n32RefNewInput(positive, 5), opts)
		if out != nil {
			defer out.Close(context.Background())
		}
		if !errors.Is(e, aggregate.ErrDecimalSumOverflow) {
			t.Fatalf("want final sum overflow got %v", e)
		}
		if out != nil {
			if _, ok, err := out.Next(context.Background()); ok || err == nil {
				t.Fatal("failed finalize exposed consumable result")
			}
		}
	})
}
func TestN32BoundedReferenceHardLimits(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		rows, capacity, empty int
	}{
		{"run65", 65, 1, 0}, {"row65537", 65537, 256, 0}, {"batch4097", 0, 1, 4097},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]n32RefRow, tc.rows)
			for i := range rows {
				k := int64(0)
				if tc.name == "run65" {
					k = int64(i)
				}
				rows[i] = n32RefRow{k, true, decimal128.FromI64(1), true}
			}
			in := n32RefNewInput(rows, 256)
			in.emptyBatches = tc.empty
			opts, _, _ := n32RefOptions(t, tc.capacity)
			out, e := ExecuteDecimalBoundedGrouped(context.Background(), in, opts)
			if out != nil {
				defer out.Close(context.Background())
			}
			if e == nil {
				t.Fatal("accepted exceeded hard limit")
			}
			if in.closed != 1 {
				t.Fatalf("input not closed: %d", in.closed)
			}
		})
	}
	for _, cap := range []int{-1, 257} {
		t.Run(fmt.Sprintf("capacity%d", cap), func(t *testing.T) {
			opts, _, _ := n32RefOptions(t, cap)
			out, e := ExecuteDecimalBoundedGrouped(context.Background(), n32RefNewInput(nil, 1), opts)
			if out != nil {
				defer out.Close(context.Background())
			}
			if e == nil {
				t.Fatal("accepted invalid capacity")
			}
		})
	}
}
