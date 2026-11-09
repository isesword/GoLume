package memory

import (
	"context"
	"math"
	"strings"
	"testing"
)

func TestColumnarRunRangeEstimateCoversNestedPayload(t *testing.T) {
	schema, batch := nestedSpillFixture()
	batch.DictStrings["kind"].DictionaryDomain = strings.Repeat("domain", 2000)
	for start := 0; start <= batch.Rows; start++ {
		for rows := 0; rows <= batch.Rows-start; rows++ {
			estimate, err := EstimateColumnarRunRangeBytes(schema, batch, start, rows)
			if err != nil {
				t.Fatal(err)
			}
			if rows == 0 {
				// CloneBatchRange's empty struct has no materialized child vectors;
				// the estimator still includes dictionary/codec metadata for an
				// empty range without trying to encode that unrelated sparse clone.
				if estimate < columnarRunHeader+int64(len(batch.DictStrings["kind"].DictionaryDomain)) {
					t.Fatal(estimate)
				}
				continue
			}
			copy, err := CloneBatchRange(batch, start, rows)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := EncodeColumnarRun(context.Background(), schema, copy, NewBudgetManager(1<<20))
			if err != nil {
				copy.ReleaseOwned()
				t.Fatal(err)
			}
			if estimate < int64(len(encoded.Payload)) {
				t.Fatalf("range %d:%d estimate=%d payload=%d", start, rows, estimate, len(encoded.Payload))
			}
			encoded.Close()
			copy.ReleaseOwned()
		}
	}
}
func TestColumnarRunRangeEstimateFixedAndInvalid(t *testing.T) {
	b := NewBatch()
	b.Rows = 70
	b.Int64s["v"] = &Int64Vector{Values: make([]int64, 70), Validity: EnsureBitmap(nil, 70)}
	schema := NewSchema(Field{Name: "v", Type: TypeInt64, Nullable: true})
	estimate, err := EstimateColumnarRunRangeBytes(schema, b, 1, 65)
	if err != nil || estimate != 22+4+16+65*8 {
		t.Fatal(estimate, err)
	}
	for _, offset := range []int{-1, 71} {
		if _, err := EstimateColumnarRunRangeBytes(schema, b, offset, 1); err == nil {
			t.Fatal("invalid range accepted")
		}
	}
	if _, err := EstimateColumnarRunRangeBytes(schema, nil, 0, 0); err == nil {
		t.Fatal("nil batch accepted")
	}
}

func TestColumnarRunRangeEstimateMetadataLengthBoundary(t *testing.T) {
	for _, n := range []uint64{0, 1, math.MaxUint32} {
		got, err := columnarRunMetadataLength(n)
		if err != nil || got != int64(n) {
			t.Fatal(n, got, err)
		}
	}
	for _, n := range []uint64{math.MaxUint32 + 1, math.MaxUint64} {
		if _, err := columnarRunMetadataLength(n); err == nil {
			t.Fatal("accepted overflowing domain or offset count", n)
		}
	}
}
