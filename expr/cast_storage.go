package expr

import (
	"fmt"
	"gocolumn/memory"
)

// castStringBytes distinguishes invalid physical storage from empty syntax.
// Check only demanded non-NULL rows so a NULL payload is never inspected.
func castStringBytes(in *scalarRegister, row int) ([]byte, error) {
	if in.strView != nil {
		v := in.strView
		if row < 0 || row >= len(v.Offsets)-1 {
			return nil, fmt.Errorf("cast string offsets missing at row %d", row)
		}
		lo, hi := v.Offsets[row], v.Offsets[row+1]
		if lo > hi || uint64(hi) > uint64(len(v.Data)) {
			return nil, fmt.Errorf("cast string offsets invalid at row %d", row)
		}
		return v.Data[lo:hi], nil
	}
	if in.strDict != nil {
		v := in.strDict
		if row < 0 || row >= len(v.Indices) {
			return nil, fmt.Errorf("cast dictionary index missing at row %d", row)
		}
		code := v.Indices[row]
		if uint64(code)+1 >= uint64(len(v.Offsets)) {
			return nil, fmt.Errorf("cast dictionary code invalid at row %d", row)
		}
		lo, hi := v.Offsets[code], v.Offsets[code+1]
		if lo > hi || uint64(hi) > uint64(len(v.Data)) {
			return nil, fmt.Errorf("cast dictionary offsets invalid at row %d", row)
		}
		return v.Data[lo:hi], nil
	}
	return registerString(in, row)
}

func invalidateCastRow(out *scalarRegister, rows, row int) {
	if out.validity == nil {
		valid := ensureValidity(out, rows)
		for i := range valid {
			valid[i] = ^uint64(0)
		}
		if len(valid) > 0 {
			valid[len(valid)-1] &= memory.TailMask(rows)
		}
	}
	out.validity[row>>6] &^= uint64(1) << uint(row&63)
}
