package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

func (a *externalSortAccumulator) appendStructList(ctx context.Context, dst, src *memory.StructListVector, f memory.Field, row int, parentValid bool) error {
	if dst == nil || dst.Values == nil || src == nil || src.Values == nil || row < 0 || row >= src.Len() {
		return fmt.Errorf("missing StructList merge payload")
	}
	lo, hi := src.Offsets[row], src.Offsets[row+1]
	if lo > hi || uint64(hi) > uint64(src.Values.Rows) {
		return fmt.Errorf("invalid StructList merge offsets")
	}
	valid, err := externalSortValidity(src.Validity, row)
	if err != nil {
		return err
	}
	valid = valid && parentValid
	if valid && uint64(dst.Values.Rows)+uint64(hi-lo) >= math.MaxUint32 {
		return memory.ErrBudgetExceeded
	}
	if err = a.initOffsets(ctx, &dst.Offsets); err != nil {
		return err
	}
	if err = a.appendValidity(ctx, &dst.Validity, valid, false, dst.Len()); err != nil {
		return err
	}
	if valid {
		for e := int(lo); e < int(hi); e++ {
			if err = ctx.Err(); err != nil {
				return err
			}
			item, err := externalSortValidity(src.Values.Validity, e)
			if err != nil {
				return err
			}
			if err = a.appendValidity(ctx, &dst.Values.Validity, item, false, dst.Values.Rows); err != nil {
				return err
			}
			if err = a.appendFields(ctx, dst.Values.Values, src.Values.Values, f.Children[0].Children, e, item, true); err != nil {
				return err
			}
			dst.Values.Rows++
			dst.Values.Values.Rows = dst.Values.Rows
		}
	}
	return externalSortAppendValue(ctx, a, &dst.Offsets, uint32(dst.Values.Rows))
}
