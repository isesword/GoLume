package parquet

import (
	"context"
	"math"
	"testing"
)

func TestBloomCacheReviewIntervals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ranges []bloomInterval
		valid  bool
	}{
		{"adjacent_unsorted", []bloomInterval{{10, 20}, {4, 10}, {20, 21}}, true},
		{"nested", []bloomInterval{{4, 100}, {10, 20}, {30, 40}}, false},
		{"same_start", []bloomInterval{{4, 10}, {4, 20}}, false},
		{"identical", []bloomInterval{{4, 10}, {4, 10}}, false},
		{"partial_overlap", []bloomInterval{{10, 20}, {4, 11}}, false},
		{"empty", []bloomInterval{{4, 4}}, false},
		{"negative", []bloomInterval{{-1, 10}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := bloomIntervalsDisjoint(context.Background(), tc.ranges); got != tc.valid {
				t.Fatalf("got %v want %v", got, tc.valid)
			}
		})
	}
}

func TestBloomCacheReviewMetadataConservative(t *testing.T) {
	for _, kind := range []string{"no_filter_other_column", "unknown_length", "length_without_offset", "external_path", "data_overflow", "negative_bloom_offset"} {
		t.Run(kind, func(t *testing.T) {
			src := recoveryBloomFixture(t, 16384, true, 2)
			if !bloomFileRangesValid(context.Background(), src.meta) {
				t.Fatal("nonvacuous original metadata rejected")
			}
			other := src.meta.RowGroups[0].Columns[1]
			switch kind {
			case "no_filter_other_column":
				other.MetaData.BloomFilterOffset = nil
				other.MetaData.BloomFilterLength = nil
			case "unknown_length":
				other.MetaData.BloomFilterLength = nil
			case "length_without_offset":
				other.MetaData.BloomFilterOffset = nil
			case "external_path":
				p := "other.parquet"
				other.FilePath = &p
			case "data_overflow":
				other.MetaData.TotalCompressedSize = math.MaxInt64
			case "negative_bloom_offset":
				off := int64(-1)
				other.MetaData.BloomFilterOffset = &off
			}
			want := kind == "no_filter_other_column"
			if got := bloomFileRangesValid(context.Background(), src.meta); got != want {
				t.Fatalf("valid=%v want=%v", got, want)
			}
		})
	}
}

func TestBloomCacheReviewStateIsolationAndCancellation(t *testing.T) {
	src := recoveryBloomFixture(t, 16384, true, 2)
	var first, second bloomRangeValidation
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if first.validate(canceled, src.meta) {
		t.Fatal("canceled cache accepted")
	}
	if !second.validate(context.Background(), src.meta) {
		t.Fatal("independent valid cache was affected")
	}
	if !first.validate(context.Background(), src.meta) {
		t.Fatal("pre-canceled attempt should not prevent later validation")
	}
	// A failed initialized cache stays conservative, while a fresh stream can
	// validate its own immutable metadata. No partial interval storage survives.
	bad := recoveryBloomFixture(t, 16384, true, 2)
	bad.meta.RowGroups[0].Columns[1].MetaData.BloomFilterLength = nil
	var failed bloomRangeValidation
	if failed.validate(context.Background(), bad.meta) || !failed.attempted || failed.valid {
		t.Fatal("invalid initialized cache state")
	}
	if failed.validate(context.Background(), src.meta) {
		t.Fatal("failed cache must not become a partial success")
	}
	var fresh bloomRangeValidation
	if !fresh.validate(context.Background(), src.meta) {
		t.Fatal("fresh reader cache failed")
	}
}

type bloomReviewCancelOnPoll struct {
	context.Context
	cancel    context.CancelFunc
	calls, at int
}

func (c *bloomReviewCancelOnPoll) Err() error {
	c.calls++
	if c.calls >= c.at {
		c.cancel()
	}
	return c.Context.Err()
}
func TestBloomCacheReviewCancellationDuringBuild(t *testing.T) {
	for _, at := range []int{4, 8} {
		src := recoveryBloomFixture(t, 16384, true, 2)
		base, cancel := context.WithCancel(context.Background())
		c := &bloomReviewCancelOnPoll{Context: base, cancel: cancel, at: at}
		var state bloomRangeValidation
		got := state.validate(c, src.meta)
		cancel()
		if got || !state.attempted || state.valid || c.calls < at {
			t.Fatalf("poll=%d got=%v attempted=%v valid=%v calls=%d", at, got, state.attempted, state.valid, c.calls)
		}
		if state.validate(context.Background(), src.meta) {
			t.Fatal("canceled initialized cache reused partial success")
		}
	}
}
