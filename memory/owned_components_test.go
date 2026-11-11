package memory

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

type componentCountingArray struct {
	arrow.Array
	calls *int
}

func (a *componentCountingArray) Data() arrow.ArrayData { *a.calls++; return a.Array.Data() }

// Reconstruct the previous two-logical-walk formula without changing production
// code. This preserves native occurrence corrections independently of the legacy
// full-minus-native component, including inherited NULL bitmaps and zero ranges.
func previousNativeOwnedFormula(b *Batch, offset, length int) (NativeOwnedRangeEstimate, error) {
	state := nativeOwnedEstimateState{}
	metadata, err := ownedRangeMetadata(b, offset, length, &state, false, false, true, true)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	nativePayload, err := estimateBatchRangeBytesMode(b, offset, length, false, false)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	totalLogical, err := EstimateBatchRangeBytes(b, offset, length)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	copyPayload, err := ownedRangeAdd(nativePayload, state.nativePayloadExtra, 1)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	native, err := ownedRangeAdd(metadata, copyPayload, 4)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	opaque := totalLogical - nativePayload
	total, err := ownedRangeAdd(native, opaque, 1)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	return NativeOwnedRangeEstimate{Bytes: total, NativeBytes: native, LegacyOpaqueBytes: opaque, OpaquePresent: state.opaquePresent}, nil
}

func TestNativeOwnedSingleLogicalWalkKeepsComponents(t *testing.T) {
	builder := array.NewInt64Builder(arrowmem.DefaultAllocator)
	builder.AppendValues([]int64{1, 2, 3}, nil)
	original := builder.NewArray()
	builder.Release()
	defer original.Release()
	calls := 0
	counted := &componentCountingArray{Array: original, calls: &calls}
	child := NewBatch()
	child.Rows = 3
	child.Int8s["x"] = &Int8Vector{Values: []int8{1, 2, 3}}
	child.Opaque["child"] = &OpaqueVector{Rows: 3, Value: counted}
	f := Field{Name: "x", Type: TypeInt8}
	b := NewBatch()
	b.Rows = 3
	b.Structs["s"] = &StructVector{Rows: 3, Values: child, Fields: []Field{f, f}, Validity: Bitmap{5}}
	b.Opaque["root"] = &OpaqueVector{Rows: 3, Value: counted, Validity: Bitmap{7}}
	for offset := 0; offset <= 3; offset++ {
		for length := 0; length <= 3-offset; length++ {
			expected, err := previousNativeOwnedFormula(b, offset, length)
			if err != nil {
				t.Fatal(err)
			}
			calls = 0
			got, err := EstimateNativeOwnedBatchRangeBytes(b, offset, length)
			if err != nil {
				t.Fatal(err)
			}
			if got != expected {
				t.Fatalf("range %d/%d got %+v want %+v", offset, length, got, expected)
			}
			if calls != 2 {
				t.Fatalf("range %d/%d Opaque entry reads=%d want=2", offset, length, calls)
			}
			nativeLogical, err := estimateBatchRangeBytesMode(b, offset, length, false, false)
			if err != nil {
				t.Fatal(err)
			}
			var opaque int64
			total, err := estimateBatchRangeBytesComponents(b, offset, length, false, true, &opaque)
			if err != nil {
				t.Fatal(err)
			}
			if opaque != got.LegacyOpaqueBytes || total-nativeLogical != opaque {
				t.Fatal("recursive Opaque contribution was duplicated")
			}
		}
	}
	var err error
	if allocations := testing.AllocsPerRun(100, func() { _, err = EstimateNativeOwnedBatchRangeBytes(b, 0, 3) }); allocations != 0 || err != nil {
		t.Fatalf("allocations=%g error=%v", allocations, err)
	}
}

func TestLogicalOpaqueAccumulatorCheckedAndOptional(t *testing.T) {
	builder := array.NewInt8Builder(arrowmem.DefaultAllocator)
	builder.Append(1)
	a := builder.NewArray()
	builder.Release()
	defer a.Release()
	b := NewBatch()
	b.Rows = 1
	b.Opaque["a"] = &OpaqueVector{Rows: 1, Value: a}
	legacy, err := EstimateBatchRangeBytes(b, 0, 1)
	if err != nil || legacy <= 0 {
		t.Fatalf("legacy=%d err=%v", legacy, err)
	}
	accumulated := int64(math.MaxInt64)
	if _, err := estimateBatchRangeBytesComponents(b, 0, 1, false, true, &accumulated); err == nil {
		t.Fatal("overflowing Opaque component admitted")
	}
	if accumulated != math.MaxInt64 {
		t.Fatal("overflow mutated accumulator")
	}
	accumulated = 17
	n, err := estimateBatchRangeBytesComponents(b, 0, 1, false, false, &accumulated)
	if err != nil || n != 0 || accumulated != 17 {
		t.Fatalf("native-only traversal touched Opaque: n=%d opaque=%d err=%v", n, accumulated, err)
	}
	if got, err := EstimateBatchRangeBytes(b, 0, 1); err != nil || got != legacy {
		t.Fatal("public nil-accumulator behavior changed")
	}
}
