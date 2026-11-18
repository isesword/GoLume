package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

func newExternalSortArray(f memory.Field) *memory.ArrayNode {
	v := &memory.ArrayNode{Kind: f.Type, Encoding: memory.EncodingView}
	switch f.Type {
	case memory.TypeList:
		v.Child = newExternalSortArray(f.Children[0])
	case memory.TypeInt64:
		v.Int64 = &memory.Int64Vector{Encoding: memory.EncodingPlain}
	case memory.TypeString:
		v.Strings = &memory.StringViewVector{Encoding: memory.EncodingView}
	}
	return v
}
func (a *externalSortAccumulator) appendArray(ctx context.Context, dst, src *memory.ArrayNode, row int, parentValid bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if dst == nil || src == nil || dst.Kind != src.Kind || row < 0 || row >= src.Len() {
		return fmt.Errorf("invalid array merge node or row")
	}
	switch src.Kind {
	case memory.TypeInt64:
		return externalSortAppendScalar(ctx, a, &dst.Int64.Values, &dst.Int64.Validity, src.Int64.Values, src.Int64.Validity, row, parentValid, true)
	case memory.TypeString:
		return a.appendString(ctx, dst.Strings, src.Strings, row, parentValid, true)
	case memory.TypeList:
		if src.Child == nil || len(src.Offsets) <= row+1 {
			return fmt.Errorf("invalid array merge offsets")
		}
		lo, hi := src.Offsets[row], src.Offsets[row+1]
		if lo > hi || uint64(hi) > uint64(src.Child.Len()) {
			return fmt.Errorf("array merge offsets outside child")
		}
		valid, err := externalSortValidity(src.Validity, row)
		if err != nil {
			return err
		}
		valid = valid && parentValid
		if valid && uint64(dst.Child.Len())+uint64(hi-lo) >= math.MaxUint32 {
			return memory.ErrBudgetExceeded
		}
		if err = a.initOffsets(ctx, &dst.Offsets); err != nil {
			return err
		}
		if err = a.appendValidity(ctx, &dst.Validity, valid, false, dst.Len()); err != nil {
			return err
		}
		if valid {
			for i := int(lo); i < int(hi); i++ {
				if err = a.appendArray(ctx, dst.Child, src.Child, i, true); err != nil {
					return err
				}
			}
		}
		return externalSortAppendValue(ctx, a, &dst.Offsets, uint32(dst.Child.Len()))
	}
	return fmt.Errorf("unsupported array merge terminal")
}
func (a *externalSortAccumulator) finishArrayOffsets(ctx context.Context, v *memory.ArrayNode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v.Kind == memory.TypeList {
		if err := a.initOffsets(ctx, &v.Offsets); err != nil {
			return err
		}
		return a.finishArrayOffsets(ctx, v.Child)
	}
	if v.Kind == memory.TypeString {
		return a.initOffsets(ctx, &v.Strings.Offsets)
	}
	return nil
}
func clearExternalSortArray(v *memory.ArrayNode) {
	if v == nil {
		return
	}
	clearExternalSortArray(v.Child)
	if v.Int64 != nil {
		*v.Int64 = memory.Int64Vector{}
	}
	if v.Strings != nil {
		*v.Strings = memory.StringViewVector{}
	}
	*v = memory.ArrayNode{}
}
