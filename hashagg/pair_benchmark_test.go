package hashagg

import (
	"gocolumn/memory"
	"testing"
)

func BenchmarkPairAggregateSelectedNullableValues(b *testing.B) {
	keys := make([]Key128, 1024)
	valid := make([]uint8, len(keys))
	vals := make([]int64, len(keys))
	for i := range keys {
		keys[i] = Key128{A: uint64(i & 255), B: uint64(i)}
		valid[i] = 3
		vals[i] = int64(i)
	}
	sel := make(memory.Bitmap, memory.WordsFor(len(keys)))
	for i := range keys {
		memory.Set(sel, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := NewPairTable(len(keys))
		t.Reset()
		if _, err := t.AggregateSelectedNullableValues(keys, valid, vals, nil, sel); err != nil {
			b.Fatal(err)
		}
	}
}
