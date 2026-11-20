package memory

import (
	"context"
	"fmt"
)

// EstimateNestedListTakeRangeBytes returns the owned Take charge for a
// permutation of a contiguous range. Repeated selections require their own
// count; this estimate does not bound arbitrary repetition of the range.
func EstimateNestedListTakeRangeBytes(ctx context.Context, src *ArrayNode, f Field, d NestedListParentDomain, offset, length int) (int64, error) {
	if ctx == nil || src == nil || offset < 0 || length < 0 || offset > src.Len() || length > src.Len()-offset || !nestedListCountOK(length) {
		return 0, fmt.Errorf("invalid nested List estimate range")
	}
	if err := src.validateNestedStructure(f, src.Len(), d, ctx.Err); err != nil {
		return 0, err
	}
	for r := offset; r < offset+length; r++ {
		if err := src.validateNestedParent(f, d, r, ctx.Err); err != nil {
			return 0, err
		}
	}
	return nestedListRangeOwnedBytes(ctx, src, f, d, offset, length)
}
func nestedListRangeOwnedBytes(ctx context.Context, src *ArrayNode, f Field, d NestedListParentDomain, offset, length int) (int64, error) {
	c, err := measureRepeatedRows(ctx, src, f, d, length, func(i int) int { return offset + i })
	if err != nil {
		return 0, err
	}
	return repeatedOwnedCharge(f, c)
}

// EstimateNestedListsStructTakeRangeBytes conservatively bounds the shared
// owned tree Take, including all snapshots, parent masks and descendant owners.
func EstimateNestedListsStructTakeRangeBytes(ctx context.Context, src *StructVector, f Field, offset, length int) (int64, error) {
	if ctx == nil || src == nil || f.Type != TypeStruct || offset < 0 || length < 0 || offset > src.Rows || length > src.Rows-offset {
		return 0, fmt.Errorf("invalid array tree estimate range")
	}
	if err := f.ValidateArrayContainers(); err != nil {
		return 0, err
	}
	if err := checkArrayBatchGraph(src.Values, f.Children, 1, ctx.Err); err != nil {
		return 0, err
	}
	wrapper := Batch{Rows: src.Rows, Structs: map[string]*StructVector{f.Name: src}}
	if _, err := selectionRunFieldSize(&wrapper, f, nil, columnarSelectionDomain{check: ctx.Err}); err != nil {
		return 0, err
	}
	var one [1]int
	for r := offset; r < offset+length; r++ {
		one[0] = r
		if _, err := selectionRunFieldSize(&wrapper, f, one[:], columnarSelectionDomain{check: ctx.Err}); err != nil {
			return 0, err
		}
	}
	return estimateStructListsTreeTake(ctx, src, f, offset, length)
}
