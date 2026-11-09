package memory

import "testing"

func TestEstimateBatchRangeEmptyListChildOffsets(t *testing.T) {
	for _, nulls := range []bool{false, true} {
		b := NewBatch()
		b.Rows = 2
		v := &StringListVector{Offsets: []uint32{0, 0, 0}, Values: &StringViewVector{}}
		if nulls {
			v.Validity = Bitmap{1} // A valid empty list followed by NULL.
		}
		b.StringLists["list"] = v
		if err := v.Validate(b.Rows); err != nil {
			t.Fatal(err)
		}
		for _, r := range [][2]int{{0, 2}, {1, 1}, {1, 0}} {
			got, err := EstimateBatchRangeBytes(b, r[0], r[1])
			want := int64(r[1]+1)*4 + 4 // Parent offsets and owned child sentinel.
			if nulls && r[1] > 0 {
				want += 8
			}
			if err != nil || got != want {
				t.Fatalf("nulls=%v range=%v got=%d want=%d err=%v", nulls, r, got, want, err)
			}
		}
		if v.Values.Offsets != nil || v.Values.Data != nil || len(v.Offsets) != 3 {
			t.Fatal("estimation changed source storage")
		}
		v.Offsets[2] = 1
		if _, err := EstimateBatchRangeBytes(b, 0, 2); err == nil {
			t.Fatal("nonempty span with missing child offsets accepted")
		}
	}
}
