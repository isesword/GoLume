package expr

import (
	"fmt"
	"gocolumn/memory"
)

// Validate the logical prefix without requiring canonical zero-based storage:
// borrowed views can start at a nonzero child offset or have a physical tail.
// Structural errors (including hidden NULL payload) are not value errors and
// are not suppressed by a conditional's active mask.
func evalListLength(out, in *scalarRegister, childType memory.DataType, rows int) error {
	var offsets []uint32
	var parent, childValidity memory.Bitmap
	var children int
	var strings *memory.StringViewVector
	switch childType {
	case memory.TypeInt64:
		if in.intList == nil || in.intList.Values == nil {
			return fmt.Errorf("LIST_LENGTH requires native list<int64> storage")
		}
		offsets, parent, childValidity = in.intList.Offsets, in.intList.Validity, in.intList.Values.Validity
		children = in.intList.Values.Len()
	case memory.TypeString:
		if in.strList == nil || in.strList.Values == nil {
			return fmt.Errorf("LIST_LENGTH requires native list<string> storage")
		}
		offsets, parent, strings = in.strList.Offsets, in.strList.Validity, in.strList.Values
		children, childValidity = strings.Len(), strings.Validity
	default:
		return fmt.Errorf("LIST_LENGTH unsupported child type %s", childType)
	}
	if rows < 0 || len(offsets) == 0 || rows >= len(offsets) {
		return fmt.Errorf("LIST_LENGTH invalid parent offsets for %d rows", rows)
	}
	if parent != nil && len(parent) < memory.WordsFor(rows) {
		return fmt.Errorf("LIST_LENGTH short parent validity")
	}
	lo, hi := uint64(offsets[0]), uint64(offsets[rows])
	if lo > hi || hi > uint64(children) {
		return fmt.Errorf("LIST_LENGTH child range out of bounds")
	}
	if childValidity != nil && uint64(len(childValidity))*64 < hi {
		return fmt.Errorf("LIST_LENGTH short child validity")
	}
	for i := 0; i < rows; i++ {
		if out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		if offsets[i] > offsets[i+1] || uint64(offsets[i+1]) > hi {
			return fmt.Errorf("LIST_LENGTH malformed parent offsets at row %d", i)
		}
	}
	if strings != nil {
		if hi >= uint64(len(strings.Offsets)) {
			return fmt.Errorf("LIST_LENGTH missing child string offset")
		}
		for i := lo; i <= hi; i++ {
			if out.checkContext != nil {
				if err := out.checkContext(); err != nil {
					return err
				}
			}
			if uint64(strings.Offsets[i]) > uint64(len(strings.Data)) || i > lo && strings.Offsets[i] < strings.Offsets[i-1] {
				return fmt.Errorf("LIST_LENGTH malformed child string offsets")
			}
		}
	}
	copyRegisterValidity(out, in, rows)
	values := ensureI64(out, rows)
	for row := range values {
		if out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		values[row] = 0
		if isValidRegister(out, row) {
			values[row] = int64(offsets[row+1]) - int64(offsets[row])
		}
	}
	return nil
}
