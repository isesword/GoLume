package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func borrowCancellationFixture(t *testing.T) (*Batch, *arrowmem.CheckedAllocator) {
	t.Helper()
	a := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	b := array.NewInt64Builder(a)
	b.AppendValues([]int64{10, 20, 30, 40}, nil)
	values := b.NewInt64Array()
	b.Release()
	src := NewBatch()
	src.Rows = 4
	src.Int64s["x"] = &Int64Vector{Values: []int64{1, 2, 3, 4}, Validity: Bitmap{15}}
	src.Strings["s"] = &StringViewVector{Offsets: []uint32{0, 1, 3, 6, 10}, Data: []byte("abbcccdddd"), Validity: Bitmap{15}}
	children := NewBatch()
	children.Rows = 4
	children.Int64s["v"] = &Int64Vector{Values: []int64{5, 6, 7, 8}, Validity: Bitmap{15}}
	src.Structs["n"] = &StructVector{Rows: 4, Fields: []Field{{Name: "v", Type: TypeInt64, Nullable: true}}, Values: children, Validity: Bitmap{15}}
	src.Opaque["a"] = &OpaqueVector{Rows: 4, Type: TypeInt64, Value: values, Owned: true}
	return src, a
}

func TestBorrowBatchRangeWithCheckCancellationAndArrowCleanup(t *testing.T) {
	src, a := borrowCancellationFixture(t)
	checks := 0
	view, err := BorrowBatchRangeWithCheck(src, 1, 2, func() error { checks++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	view.ClearColumns()
	src.ClearColumns()
	a.AssertSize(t, 0)
	if checks < 8 {
		t.Fatal("missing validation/column/return checks", checks)
	}
	for stop := 1; stop <= checks; stop++ {
		src, a := borrowCancellationFixture(t)
		calls := 0
		view, err := BorrowBatchRangeWithCheck(src, 1, 2, func() error {
			calls++
			if calls == stop {
				return context.Canceled
			}
			return nil
		})
		if !errors.Is(err, context.Canceled) || view != nil {
			t.Fatalf("check %d: view=%v error=%v", stop, view, err)
		}
		if src.Int64s["x"].Values[1] != 2 || src.Opaque["a"].Value.(*array.Int64).Value(1) != 20 {
			t.Fatal("cancel changed source")
		}
		// A canceled post-copy return must release the partial Arrow slice retain.
		src.ClearColumns()
		a.AssertSize(t, 0)
	}
}

func TestBorrowedRangeMetadataWithCheckMatchesAndCancels(t *testing.T) {
	src, a := borrowCancellationFixture(t)
	defer a.AssertSize(t, 0)
	defer src.ClearColumns()
	want, err := BorrowedRangeMetadataBytes(src, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	checks := 0
	got, err := BorrowedRangeMetadataBytesWithCheck(src, 1, 2, func() error { checks++; return nil })
	if err != nil || got != want {
		t.Fatal(got, want, err)
	}
	if got, err = BorrowedRangeMetadataBytesWithCheck(src, 1, 2, nil); err != nil || got != want {
		t.Fatal("nil check changed admission", got, want, err)
	}
	for stop := 1; stop <= checks; stop++ {
		calls := 0
		got, err := BorrowedRangeMetadataBytesWithCheck(src, 1, 2, func() error {
			calls++
			if calls == stop {
				return context.Canceled
			}
			return nil
		})
		if got != 0 || !errors.Is(err, context.Canceled) {
			t.Fatalf("check %d: %d %v", stop, got, err)
		}
	}
	if _, err := BorrowBatchRangeWithCheck(nil, 0, 0, func() error { return context.Canceled }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
