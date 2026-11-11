package memory

import (
	"reflect"
	"testing"
)

// Bulk offsets allocate only their endpoint-sized output before validating the
// interior. A malformed interior must fail clone, but need not fail admission.
func TestBulkOffsetsDeferInteriorValidation(t *testing.T) {
	for _, kind := range []string{"string", "list-parent", "list-child"} {
		t.Run(kind, func(t *testing.T) {
			b := NewBatch()
			b.Rows = 4
			b.Int64s["earlier"] = &Int64Vector{Values: make([]int64, 4)}
			field := Field{Name: "v", Type: TypeString}
			bad := []uint32{0, 0, 65536, 1, 1}
			original := append([]uint32(nil), bad...)
			switch kind {
			case "string":
				b.Strings["v"] = &StringViewVector{Offsets: bad, Data: make([]byte, 65536)}
			case "list-parent":
				field = ListStringField("v", true, true)
				b.StringLists["v"] = &StringListVector{Offsets: bad, Values: &StringViewVector{Offsets: []uint32{0, 1}, Data: []byte("a")}}
			case "list-child":
				field = ListStringField("v", true, true)
				b.StringLists["v"] = &StringListVector{Offsets: []uint32{0, 0, 1, 3, 3}, Values: &StringViewVector{Offsets: bad, Data: make([]byte, 65536)}}
			}
			if _, err := EstimateNativeOwnedBatchRangeBytes(b, 1, 2); err != nil {
				t.Fatalf("bulk admission: %v", err)
			}
			if got, err := CloneBatchRange(b, 1, 2); err == nil {
				got.ReleaseOwned()
				t.Fatal("malformed interior cloned")
			} else if got != nil {
				t.Fatal("partial clone exposed")
			}
			if !reflect.DeepEqual(bad, original) {
				t.Fatal("source offsets changed")
			}
			// The identical offsets are unsafe for incremental row append, including
			// a NULL parent: keep preflight rejection for all nonempty Struct children.
			for _, validity := range []Bitmap{nil, {0}} {
				parent := NewBatch()
				parent.Rows = 4
				parent.Structs["s"] = &StructVector{Rows: 4, Values: b, Fields: []Field{field}, Validity: validity}
				if _, err := EstimateNativeOwnedBatchRangeBytes(parent, 1, 2); err == nil {
					t.Fatal("incremental malformed interior admitted")
				}
			}
			// Zero-row Struct recursively bulk-copies its child runtime maps. Malformed
			// offsets outside the selected empty range must not add a new rejection.
			parent := NewBatch()
			parent.Rows = 4
			parent.Structs["s"] = &StructVector{Rows: 4, Values: b, Fields: []Field{field}}
			if _, err := EstimateNativeOwnedBatchRangeBytes(parent, 1, 0); err != nil {
				t.Fatal(err)
			}
			got, err := CloneBatchRange(parent, 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			got.ReleaseOwned()
		})
	}
}
