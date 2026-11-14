package parquet

import (
	"context"
	"math"
	"testing"

	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
)

type bloomCountingContext struct {
	context.Context
	calls, limit int
}

func (c *bloomCountingContext) Err() error {
	c.calls++
	if c.limit > 0 && c.calls >= c.limit {
		return context.Canceled
	}
	return nil
}

func TestOptimizedBloomIntervalEdges(t *testing.T) {
	for _, tc := range []struct {
		name  string
		r     []bloomInterval
		valid bool
	}{
		{"adjacent", []bloomInterval{{10, 20}, {4, 10}, {20, 30}}, true},
		{"same_start", []bloomInterval{{4, 10}, {4, 20}}, false},
		{"nested", []bloomInterval{{4, 30}, {10, 20}, {21, 25}}, false},
		{"cross_many", []bloomInterval{{4, 100}, {20, 21}, {30, 31}}, false},
		{"partial", []bloomInterval{{4, 20}, {10, 30}}, false},
		{"zero", []bloomInterval{{4, 4}}, false},
		{"overflow", []bloomInterval{{math.MaxInt64, math.MinInt64}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := bloomIntervalsDisjoint(context.Background(), tc.r); got != tc.valid {
				t.Fatalf("got %v want %v", got, tc.valid)
			}
		})
	}
}
func TestOptimizedBloomPreprocessingBudgetAndMetadata(t *testing.T) {
	for _, which := range []string{"unknown_length", "missing_offset", "negative_offset", "overflow", "zero_data", "nil_group", "nil_column", "too_many_chunks", "too_many_groups"} {
		t.Run(which, func(t *testing.T) {
			src := recoveryBloomFixture(t, 4096, true, 2)
			meta := src.meta
			c := meta.RowGroups[0].Columns[1].MetaData
			switch which {
			case "unknown_length":
				c.BloomFilterLength = nil
			case "missing_offset":
				c.BloomFilterOffset = nil
			case "negative_offset":
				v := int64(-1)
				c.BloomFilterOffset = &v
			case "overflow":
				v := int64(math.MaxInt64)
				c.BloomFilterOffset = &v
			case "zero_data":
				c.TotalCompressedSize = 0
			case "nil_group":
				meta.RowGroups[0] = nil
			case "nil_column":
				meta.RowGroups[0].Columns[0] = nil
			case "too_many_chunks":
				rg := meta.RowGroups[0]
				for len(rg.Columns) <= maxPruningBloomRanges/4 {
					rg.Columns = append(rg.Columns, rg.Columns[0])
				}
			case "too_many_groups":
				for len(meta.RowGroups) <= maxPruningBloomRanges {
					meta.RowGroups = append(meta.RowGroups, meta.RowGroups[0])
				}
			}
			if bloomFileRangesValid(context.Background(), meta) {
				t.Fatal("invalid metadata admitted")
			}
			if which == "too_many_chunks" || which == "too_many_groups" {
				if allocs := testing.AllocsPerRun(10, func() { bloomFileRangesValid(context.Background(), meta) }); allocs != 0 {
					t.Fatalf("over-limit preflight allocated: %g", allocs)
				}
			}
		})
	}
	// Fully absent filters are valid metadata and do not poison other ranges.
	src := recoveryBloomFixture(t, 4096, false, 2)
	if !bloomFileRangesValid(context.Background(), src.meta) {
		t.Fatal("absent filter mistaken for incomplete extent")
	}
}
func TestOptimizedBloomCrossGroupAliases(t *testing.T) {
	for _, which := range []string{"same_bloom", "unknown_length", "overlapping_data"} {
		t.Run(which, func(t *testing.T) {
			src := recoveryBloomConfiguredFixture(t, 4096, 3, true, 2, nil)
			target := src.meta.RowGroups[0].Columns[0].MetaData
			other := src.meta.RowGroups[2].Columns[0].MetaData
			switch which {
			case "same_bloom":
				other.BloomFilterOffset = target.BloomFilterOffset
				other.BloomFilterLength = target.BloomFilterLength
			case "unknown_length":
				other.BloomFilterLength = nil
			case "overlapping_data":
				other.DataPageOffset = *target.BloomFilterOffset
			}
			if bloomFileRangesValid(context.Background(), src.meta) {
				t.Fatal("cross-group alias admitted")
			}
			src = reopenLiteralInFooter(t, src)
			pushed, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: expr.EQ(expr.Col("x"), expr.I64(50))})
			if err != nil {
				t.Fatal(err)
			}
			// Only execute valid data layouts. The data-offset case is intentionally
			// corrupt and ordinary scanning may correctly report its corruption.
			if which != "overlapping_data" {
				rows, _ := recoveryBloomDecoded(t, pushed.(*Source), false)
				if rows != 3*4096 {
					t.Fatalf("alias pruned: %d", rows)
				}
			}
		})
	}
}
func TestOptimizedBloomCacheAndCancellation(t *testing.T) {
	src := recoveryBloomConfiguredFixture(t, 4096, 8, true, 2, nil)
	ctx := &bloomCountingContext{Context: context.Background()}
	var cache bloomRangeValidation
	if !cache.validate(ctx, src.meta) || !cache.attempted || !cache.valid {
		t.Fatal("valid initialization failed")
	}
	first := ctx.calls
	ctx.calls = 0
	for i := 0; i < 128; i++ {
		if !cache.validate(ctx, src.meta) {
			t.Fatal("cached validation failed")
		}
	}
	if ctx.calls != 256 {
		t.Fatalf("reused cache traversed metadata: %d", ctx.calls)
	}
	t.Logf("initial context polls=%d; subsequent polls=2/group", first)
	for _, limit := range []int{1, 5, 20, first - 2} {
		canceled := &bloomCountingContext{Context: context.Background(), limit: limit}
		var partial bloomRangeValidation
		if partial.validate(canceled, src.meta) {
			t.Fatalf("cancellation at %d admitted", limit)
		}
		if partial.valid {
			t.Fatal("partial validation cached as valid")
		}
		if partial.attempted && partial.validate(context.Background(), src.meta) {
			t.Fatal("failed initialization retried")
		}
	}
	// Every stream starts with its own zero cache; neither validated metadata
	// nor cancellation state escapes to another reader or shared Source.
	p, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: expr.EQ(expr.Col("x"), expr.I64(50))})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		input, e := p.(*Source).NewBatchStream(context.Background(), testReadOptions().Normalize())
		if e != nil {
			t.Fatal(e)
		}
		stream := input.(*batchStream)
		if stream.bloomRanges.attempted {
			t.Fatal("new stream inherited range cache")
		}
		if _, e = stream.Next(context.Background(), memory.NewBatch()); e != nil {
			t.Fatal(e)
		}
		if !stream.bloomRanges.attempted || !stream.bloomRanges.valid {
			t.Fatal("stream did not validate ranges")
		}
		stream.Close()
	}
}
func TestOptimizedBloomPreprocessingScaling(t *testing.T) {
	previous := 0
	for _, groups := range []int{8, 32, 128} {
		src := recoveryBloomConfiguredFixture(t, 1024, groups, true, 2, nil)
		ctx := &bloomCountingContext{Context: context.Background()}
		if !bloomFileRangesValid(ctx, src.meta) {
			t.Fatalf("groups=%d not admitted", groups)
		}
		if previous > 0 && ctx.calls > 8*previous {
			t.Fatalf("superlinear beyond sort bound: prev=%d current=%d", previous, ctx.calls)
		}
		t.Logf("groups=%d context polls=%d", groups, ctx.calls)
		previous = ctx.calls
	}
}
