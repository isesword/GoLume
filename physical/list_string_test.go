package physical

import (
	"context"
	"errors"
	"testing"

	"gocolumn/memory"
)

func TestTakeRowsListStringPreservesNestedValidityAndCancellation(t *testing.T) {
	parent := memory.EnsureBitmap(nil, 3)
	memory.Set(parent, 0)
	memory.Set(parent, 2)
	child := memory.EnsureBitmap(nil, 3)
	memory.Set(child, 0)
	memory.Set(child, 2)
	src := memory.NewBatch()
	src.StringLists["tags"] = &memory.StringListVector{
		Offsets:  []uint32{0, 2, 2, 3},
		Validity: parent,
		Encoding: memory.EncodingView,
		Values: &memory.StringViewVector{
			Offsets:  []uint32{0, 1, 1, 4},
			Data:     []byte("a中"),
			Validity: child,
			Encoding: memory.EncodingView,
		},
	}
	src.Reset(3)
	dst := memory.NewBatch()
	if err := takeRows(context.Background(), dst, src, []int{2, 1, 0}); err != nil {
		t.Fatal(err)
	}
	v, err := dst.StringList("tags")
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Validate(3); err != nil {
		t.Fatal(err)
	}
	if start, end, ok := v.ValueRange(0); !ok || end-start != 1 || string(v.Values.ValueBytes(start)) != "中" {
		t.Fatalf("row 0 range=(%d,%d,%v)", start, end, ok)
	}
	if v.IsValid(1) {
		t.Fatal("row 1 should remain NULL")
	}
	if start, end, ok := v.ValueRange(2); !ok || end-start != 2 || string(v.Values.ValueBytes(start)) != "a" || v.Values.IsValid(start+1) {
		t.Fatalf("row 2 nested state range=(%d,%d,%v)", start, end, ok)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := takeRows(canceled, memory.NewBatch(), src, []int{0}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled take error=%v", err)
	}
}
