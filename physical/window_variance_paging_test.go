package physical

import (
	"gocolumn/memory"
	"testing"
)

func TestVarianceLookaheadInternalPagePolicy(t *testing.T) {
	for _, tc := range []struct{ requested, want int }{{0, 1024}, {1, 1}, {64, 64}, {1024, 1024}, {4096, 1024}} {
		if got := varianceLookaheadPageRows(tc.requested); got != tc.want {
			t.Fatal(tc, got)
		}
	}
}

func TestVarianceLookaheadRangeAdmissionPreservesDictionary(t *testing.T) {
	b := memory.NewBatch()
	b.Reset(9)
	b.DictStrings["d"] = &memory.DictionaryStringVector{Indices: make([]uint32, 9), Offsets: []uint32{0, 8192}, Data: make([]byte, 8192), Encoding: memory.EncodingDictionary, DictionaryDomain: "unchanged"}
	full, err := futureCloneAdmissionBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	page, err := futureCloneRangeAdmissionBytes(b, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	if full-page != 4*4*(9-2) {
		t.Fatalf("range must reduce only copied indices, not dictionary backing: full=%d page=%d", full, page)
	}
	if page < 4*(8192+8+8) {
		t.Fatal("missing complete dictionary backing", page)
	}
	clone, err := memory.CloneBatchRange(b, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.ReleaseOwned()
	d := clone.DictStrings["d"]
	if len(d.Indices) != 2 || len(d.Data) != 8192 || len(d.Offsets) != 2 {
		t.Fatal("clone contract changed")
	}
	b.DictStrings["d"].Data[0] = 99
	if d.Data[0] != 0 {
		t.Fatal("borrowed dictionary backing")
	}
	if _, err := futureCloneRangeAdmissionBytes(b, 8, 2); err == nil {
		t.Fatal("accepted out-of-bounds range")
	}
}
