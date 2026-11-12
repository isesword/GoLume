package memory

import (
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"testing"
)

// Independent audit: malformed native storage must not be silently exported.
func TestAuditInt64ListExportRejectsMalformed(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    *Int64ListVector
	}{
		{"terminal", &Int64ListVector{Offsets: []uint32{0, 1}, Values: &Int64Vector{Values: []int64{7, 8}}}},
		{"short_child_bitmap", &Int64ListVector{Offsets: []uint32{0, 1}, Values: &Int64Vector{Values: []int64{7}, Validity: Bitmap{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBatch()
			b.Rows = 1
			b.Int64Lists["l"] = tc.v
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("malformed input panicked: %v", p)
				}
			}()
			r, err := ArrowRecord(arrowmem.DefaultAllocator, Schema{Fields: []Field{ListInt64Field("l", true, true)}}, b)
			if r != nil {
				r.Release()
			}
			if err == nil {
				t.Error("malformed list accepted")
			}
		})
	}
}
