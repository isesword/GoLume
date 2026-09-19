package hashagg

import (
	"gocolumn/memory"
	"testing"
)

func TestPairAggregateSelectedNullableValues(t *testing.T) {
	tab := NewPairTable(8)
	tab.Reset()
	keys := []Key128{{1, 10}, {1, 99}, {2, 10}, {3, 30}}
	keyValid := []uint8{3, 1, 2, 0}
	vals := []int64{4, 8, 16, 32}
	valueValid := memory.Bitmap{0b1011}
	sel := memory.Bitmap{0b1111}
	s, err := tab.AggregateSelectedNullableValues(keys, keyValid, vals, valueValid, sel)
	if err != nil {
		t.Fatal(err)
	}
	if s.Rows != 4 || s.Groups != 4 || s.Checksum != 44 {
		t.Fatalf("summary=%+v", s)
	}
	if _, err := tab.AggregateSelectedNullableValues(keys[:2], keyValid, vals[:2], nil, sel); err == nil {
		t.Fatal("expected key validity length error")
	}
}
