package arrowrecord

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/array"
	"gocolumn/memory"
)

// arrowInt64ListVector normalizes parent offsets and child validity independently.
// Arrow child Values already accounts for the child's own data offset; list
// offsets address that logical child array, not its underlying buffer.
func arrowInt64ListVector(a *array.List, parentValidity memory.Bitmap) (*memory.Int64ListVector, error) {
	values, ok := a.ListValues().(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("list child array is %T, want *array.Int64", a.ListValues())
	}
	rows := a.Len()
	base, terminal := int64(0), int64(0)
	if rows > 0 {
		base, _ = a.ValueOffsets(0)
		_, terminal = a.ValueOffsets(rows - 1)
	}
	if base < 0 || terminal < base || terminal > int64(values.Len()) {
		return nil, fmt.Errorf("list child range [%d,%d) exceeds %d values", base, terminal, values.Len())
	}
	offsets := make([]uint32, rows+1)
	for row := 0; row < rows; row++ {
		start, end := a.ValueOffsets(row)
		if start < base || end < start || end > terminal {
			return nil, fmt.Errorf("list row %d offsets [%d,%d) outside [%d,%d)", row, start, end, base, terminal)
		}
		offsets[row], offsets[row+1] = uint32(start-base), uint32(end-base)
	}
	childRows := int(terminal - base)
	var validity memory.Bitmap
	for i := 0; i < childRows; i++ {
		if !values.IsValid(int(base) + i) {
			if validity == nil {
				validity = memory.EnsureBitmap(nil, childRows)
				for j := 0; j < i; j++ {
					memory.Set(validity, j)
				}
			}
		} else if validity != nil {
			memory.Set(validity, i)
		}
	}
	v := &memory.Int64ListVector{Offsets: offsets, Values: &memory.Int64Vector{Values: values.Values()[base:terminal], Validity: validity, Encoding: memory.EncodingView}, Validity: parentValidity, Encoding: memory.EncodingView}
	if err := v.Validate(rows); err != nil {
		return nil, err
	}
	return v, nil
}
