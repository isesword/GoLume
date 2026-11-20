package engine

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/memory"
)

// Independent rational oracle: scale the numerator/denominator before doing
// exactly one quotient/remainder rounding. No production partial/finalizer is used.
func n33RefRound(sum *big.Int, count uint64, inputScale, p, scale int32, mode aggregate.DecimalAverageRoundingMode) (*big.Int, bool) {
	if count == 0 {
		return nil, false
	}
	n := new(big.Int).Abs(sum)
	d := new(big.Int).SetUint64(count)
	delta := scale - inputScale
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(absN33(delta))), nil)
	if delta >= 0 {
		n.Mul(n, factor)
	} else {
		d.Mul(d, factor)
	}
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(n, d, r)
	increment := false
	if r.Sign() != 0 {
		switch mode {
		case aggregate.DecimalAverageAwayFromZero:
			increment = true
		case aggregate.DecimalAverageFloor:
			increment = sum.Sign() < 0
		case aggregate.DecimalAverageCeil:
			increment = sum.Sign() > 0
		case aggregate.DecimalAverageHalfEven, aggregate.DecimalAverageHalfAwayFromZero:
			c := new(big.Int).Lsh(new(big.Int).Set(r), 1).Cmp(d)
			increment = c > 0 || c == 0 && (mode == aggregate.DecimalAverageHalfAwayFromZero || q.Bit(0) != 0)
		}
	}
	if increment {
		q.Add(q, big.NewInt(1))
	}
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(p)), nil)
	overflow := q.Cmp(limit) >= 0
	if sum.Sign() < 0 {
		q.Neg(q)
	}
	return q, overflow
}
func absN33(x int32) int32 {
	if x < 0 {
		return -x
	}
	return x
}

func n33RefCheck(t *testing.T, rows []n32RefRow, inputScale, p, scale int32, round aggregate.DecimalAverageRoundingMode, mode DecimalBoundedAggregateMode, capacity, minFlush int) {
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
	overflow := false
	for k, v := range want {
		keys = append(keys, k)
		_, bad := n33RefRound(v.sum, v.valid, inputScale, p, scale, round)
		overflow = overflow || bad
	}
	sort.Slice(keys, func(i, j int) bool { return n30RefLess(keys[i], keys[j]) })
	opts, _, store := n32RefOptions(t, capacity)
	opts.Scale = inputScale
	opts.Mode = mode
	opts.AveragePrecision = p
	opts.AverageScale = scale
	opts.AverageRounding = round
	input := n32RefNewInput(rows, 13)
	input.scale = inputScale
	out, e := ExecuteDecimalBoundedGrouped(context.Background(), input, opts)
	if out != nil {
		t.Cleanup(func() {
			if e := out.Close(context.Background()); e != nil {
				t.Error(e)
			}
		})
	}
	if overflow {
		if !errors.Is(e, aggregate.ErrDecimalAverageOverflow) {
			t.Fatalf("oracle overflow got %v", e)
		}
		if out != nil {
			if _, ok, err := out.Next(context.Background()); ok || err == nil {
				t.Fatal("overflow result consumable")
			}
		}
		return
	}
	if e != nil {
		t.Fatal(e)
	}
	if out == nil {
		t.Fatal("missing result")
	}
	if input.closed != 1 {
		t.Fatal("input not closed exactly once")
	}
	if out.Stats.Flushes < minFlush || minFlush > 8 && out.Stats.MergePasses < 2 {
		t.Fatalf("not actual requested spill/multipass: %+v", out.Stats)
	}
	if len(rows) > 0 && store.Used() == 0 {
		t.Fatal("no physical spill bytes")
	}
	seen := 0
	for {
		b, ok, e := out.Next(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		if b == nil || b.Rows < 1 || b.Rows > 256 {
			t.Fatal("invalid page")
		}
		if b.Mode != mode || b.AveragePrecision != p || b.AverageScale != scale || b.Precision != 38 || b.Scale != inputScale {
			t.Fatal("metadata lost")
		}
		for i := 0; i < b.Rows; i++ {
			if seen >= len(keys) {
				t.Fatal("extra group")
			}
			r := b.Records[i]
			k := keys[seen]
			v := want[k]
			avg, _ := n33RefRound(v.sum, v.valid, inputScale, p, scale, round)
			if r.KeyValid != k.valid || k.valid && r.Key != k.key {
				t.Fatalf("key order row%d", seen)
			}
			if r.CountValid != v.valid || r.CountRows != v.rows || r.AverageValid != (v.valid != 0) {
				t.Fatalf("count/NULL row%d: %+v", seen, r)
			}
			if r.AverageValid && n32RefCoeff(r.Average).Cmp(avg) != 0 {
				t.Fatalf("AVG key%+v got%s want%s", k, n32RefCoeff(r.Average), avg)
			}
			if mode == DecimalBoundedAverageCounts {
				if r.SumValid || r.Sum != (memory.Decimal128{}) {
					t.Fatal("AVG-only exposed SUM")
				}
			} else {
				if r.SumValid != (v.valid != 0) || r.SumValid && n32RefCoeff(r.Sum).Cmp(v.sum) != 0 {
					t.Fatal("mixed SUM changed")
				}
			}
			seen++
		}
	}
	if seen != len(keys) || out.Stats.Groups != len(keys) {
		t.Fatalf("group count got%d want%d stats%+v", seen, len(keys), out.Stats)
	}
}
func TestN33BoundedAverageReferenceUnequalPartials(t *testing.T) {
	var rows []n32RefRow
	// Per-key partial counts 1..20 differ. Averaging partial averages would give
	// the unweighted midpoint instead of the explicitly computed weighted ratio.
	for part := 0; part < 20; part++ {
		for j := 0; j <= part; j++ {
			rows = append(rows, n32RefRow{0, true, decimal128.FromI64(int64(part*7 - 40)), true})
		}
		rows = append(rows, n32RefRow{int64(part + 1), true, decimal128.FromI64(int64(part)), true})
	}
	for mode := aggregate.DecimalAverageTowardZero; mode <= aggregate.DecimalAverageHalfAwayFromZero; mode++ {
		t.Run(fmt.Sprint(mode), func(t *testing.T) { n33RefCheck(t, rows, 2, 38, 5, mode, DecimalBoundedSumAverageCounts, 2, 9) })
	}
}
func TestN33BoundedAverageReferenceRoundingScales(t *testing.T) {
	values := []int64{25, 35, -25, -35, 24, 26, -24, -26, 1, -1, 0}
	var rows []n32RefRow
	for i, v := range values {
		rows = append(rows, n32RefRow{int64(i), true, decimal128.FromI64(v), true})
	}
	rows = append(rows, n32RefRow{777, false, decimal128.FromI64(1), true}, n32RefRow{-777, false, decimal128.FromI64(2), true}, n32RefRow{888, true, memory.Decimal128{}, false})
	for mode := aggregate.DecimalAverageTowardZero; mode <= aggregate.DecimalAverageHalfAwayFromZero; mode++ {
		for _, scales := range [][2]int32{{1, 0}, {2, 3}, {38, 0}, {0, 38}, {0, 0}} {
			t.Run(fmt.Sprintf("mode%d_%d_to_%d", mode, scales[0], scales[1]), func(t *testing.T) {
				n33RefCheck(t, rows, scales[0], 38, scales[1], mode, DecimalBoundedAverageCounts, 2, 1)
			})
		}
	}
}
func TestN33BoundedAverageReferenceWideAndCancellation(t *testing.T) {
	max, e := decimal128.FromString("99999999999999999999999999999999999999", 38, 0)
	if e != nil {
		t.Fatal(e)
	}
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel%v", cancel), func(t *testing.T) {
			var rows []n32RefRow
			for i := 0; i < 20; i++ {
				v := max
				if cancel && i >= 10 {
					v = v.Negate()
				}
				for j := 0; j < 3; j++ {
					rows = append(rows, n32RefRow{0, true, v, true})
				}
				rows = append(rows, n32RefRow{int64(i + 1), true, memory.Decimal128{}, false})
			}
			n33RefCheck(t, rows, 0, 38, 0, aggregate.DecimalAverageHalfEven, DecimalBoundedAverageCounts, 2, 9)
			if !cancel {
				opts, _, _ := n32RefOptions(t, 2)
				opts.Scale = 0
				opts.Mode = DecimalBoundedSumAverageCounts
				opts.AveragePrecision = 38
				opts.AverageRounding = aggregate.DecimalAverageHalfEven
				in := n32RefNewInput(rows, 13)
				in.scale = 0
				out, e := ExecuteDecimalBoundedGrouped(context.Background(), in, opts)
				if out != nil {
					defer out.Close(context.Background())
				}
				if !errors.Is(e, aggregate.ErrDecimalSumOverflow) {
					t.Fatalf("mixed must retain SUM overflow: %v", e)
				}
			}
		})
	}
}
func TestN33BoundedAverageReferenceNullEmptyAndFinalRoundingOverflow(t *testing.T) {
	for _, rows := range [][]n32RefRow{nil, {{0, false, decimal128.New(-1, 0), false}, {1, true, decimal128.New(-1, 0), false}}} {
		n33RefCheck(t, rows, 9, 18, 7, aggregate.DecimalAverageHalfEven, DecimalBoundedAverageCounts, 2, 0)
	}
	// Exact coefficient 9.5 rounds to 10: output p1 overflow must be checked after rounding.
	for _, sign := range []int64{1, -1} {
		for mode := aggregate.DecimalAverageTowardZero; mode <= aggregate.DecimalAverageHalfAwayFromZero; mode++ {
			t.Run(fmt.Sprintf("sign%d_mode%d", sign, mode), func(t *testing.T) {
				rows := []n32RefRow{{0, true, decimal128.FromI64(sign * 9), true}, {0, true, decimal128.FromI64(sign * 10), true}}
				n33RefCheck(t, rows, 0, 1, 0, mode, DecimalBoundedAverageCounts, 1, 1)
			})
		}
	}
}

func TestN33BoundedAverageReferenceMultiPage(t *testing.T) {
	rows := make([]n32RefRow, 513)
	for i := range rows {
		rows[i] = n32RefRow{int64(512 - i), true, decimal128.FromI64(int64(i*3 - 701)), i%13 != 0}
	}
	n33RefCheck(t, rows, 3, 24, 8, aggregate.DecimalAverageHalfEven, DecimalBoundedSumAverageCounts, 16, 9)
}
