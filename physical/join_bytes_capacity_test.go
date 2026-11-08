package physical

import (
	"context"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"strings"
	"testing"
)

func TestReviewGenericBuildPayloadCapacity(t *testing.T) {
	src := genericJoinSource(t, []genericJoinRow{{s: "a", x: strings.Repeat("x", 65537)}}, false, 1)
	m := memory.NewBudgetManager(70000)
	owner, err := NewJoinSource(context.Background(), src, src, join.Inner, []JoinKey{{"s", "s"}}, "", m, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	got := cap(owner.rightBatches[0].Strings["x"].Data)
	if int64(got) > m.Limit() || int64(got) > m.Used() {
		t.Fatalf("one retained payload capacity %d exceeds ALL reserved bytes %d and hard limit %d", got, m.Used(), m.Limit())
	}
}
