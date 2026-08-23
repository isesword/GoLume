package hashagg

import (
	"gocolumn/memory"
	"testing"
)

func TestI32Table(t *testing.T) {
	tab := NewI32Table(4)
	tab.Reset()
	keys := []int32{1, 2, 1, 3}
	vals := []int64{10, 20, 5, 7}
	sel := memory.Bitmap{0b1111}
	s := tab.AggregateSelected(keys, vals, sel)
	if s.Groups != 3 || s.Checksum != 42 || s.Rows != 4 {
		t.Fatalf("summary=%+v", s)
	}
	rows := tab.Materialize(nil)
	m := map[int32]GroupI32{}
	for _, r := range rows {
		m[r.Key] = r
	}
	if m[1].Sum != 15 || m[1].Count != 2 {
		t.Fatalf("key1=%+v", m[1])
	}
}
