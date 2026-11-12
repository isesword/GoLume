package engine

import (
	"gocolumn/memory"
	"testing"
)

func TestInt64ListCountColumnInfo(t *testing.T) {
	b := memory.NewBatch()
	b.Int64Lists["l"] = &memory.Int64ListVector{Offsets: []uint32{0, 0, 0, 1}, Validity: memory.Bitmap{5}, Values: &memory.Int64Vector{Values: []int64{1}}}
	n, bits, err := aggregateColumnInfo(b, "l", memory.TypeList, false)
	if err != nil || n != 3 || memory.IsSet(bits, 1) || !memory.IsSet(bits, 2) {
		t.Fatalf("n=%d bits=%v err=%v", n, bits, err)
	}
}
