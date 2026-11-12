package memory

import (
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"testing"
)

func TestArrowInt64ListRejectsMalformedStorage(t *testing.T) {
	for name, mutate := range map[string]func(*Int64ListVector){
		"parent bitmap":     func(v *Int64ListVector) { v.Validity = Bitmap{} },
		"child bitmap":      func(v *Int64ListVector) { v.Values.Validity = Bitmap{} },
		"terminal mismatch": func(v *Int64ListVector) { v.Offsets[4] = 4 },
		"decreasing offset": func(v *Int64ListVector) { v.Offsets[2] = 2 },
		"nonzero base":      func(v *Int64ListVector) { v.Offsets[0] = 1 },
		"nil child":         func(v *Int64ListVector) { v.Values = nil },
	} {
		for _, nested := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/direct", true: "/struct"}[nested], func(t *testing.T) {
				b := NewBatch()
				b.Rows = 4
				v := testInt64ListVector()
				mutate(v)
				b.Int64Lists["x"] = v
				f := ListInt64Field("x", true, true)
				schema := NewSchema(f)
				if nested {
					child := b
					b = NewBatch()
					b.Rows = 4
					b.Structs["s"] = &StructVector{Rows: 4, Fields: []Field{f}, Values: child, Validity: Bitmap{0}}
					schema = NewSchema(StructField("s", true, f))
				}
				for _, r := range [][2]int{{0, 4}, {2, 0}, {2, 1}} {
					alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
					record, err := ArrowRecordRange(alloc, schema, b, r[0], r[1])
					if record != nil {
						record.Release()
					}
					if err == nil {
						t.Fatalf("accepted malformed range %v", r)
					}
					alloc.AssertSize(t, 0)
				}
			})
		}
	}
}
