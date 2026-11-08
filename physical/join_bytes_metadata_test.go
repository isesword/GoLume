package physical

import (
	"context"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
	"unsafe"
)

func TestReviewGenericPartitionMetadataBudget(t *testing.T) {
	leftRows := make([]genericJoinRow, 1000)
	for i := range leftRows {
		leftRows[i].s = "a"
	}
	left := genericJoinSource(t, leftRows, false, 1)
	right := genericJoinSource(t, []genericJoinRow{{s: "a"}}, false, 1)
	m := memory.NewBudgetManager(4096)
	owner, err := NewJoinSource(context.Background(), left, right, join.Inner, []JoinKey{{"s", "s"}}, "", m, kernel.New(kernel.ModeScalar))
	if err != nil {
		return
	}
	defer owner.Close()
	metadata := int64(cap(owner.leftParts)+cap(owner.parts)) * int64(unsafe.Sizeof(source.Partition{}))
	if metadata > m.Used() {
		t.Fatalf("retained partition metadata=%d exceeds total charge=%d (limit=%d)", metadata, m.Used(), m.Limit())
	}
}
