package memory

import (
	"context"
	"fmt"
)

// EstimateStructListTakeRangeBytes returns the admitted bytes of the owned Take
// for any permutation of a contiguous parent range, without allocating indices.
func EstimateStructListTakeRangeBytes(ctx context.Context, src *StructListVector, f Field, d StructListParentDomain, offset, length int) (int64, error) {
	if ctx == nil || src == nil || offset < 0 || length < 0 || offset > src.Len() || length > src.Len()-offset || !structListCountOK(length) {
		return 0, fmt.Errorf("invalid StructList estimate range")
	}
	if err := src.validateStructure(f, src.Len(), d, ctx.Err); err != nil {
		return 0, err
	}
	for r := offset; r < offset+length; r++ {
		if err := src.validateParent(f, d, r, ctx.Err); err != nil {
			return 0, err
		}
	}
	_, n, err := structListOwnedBytes(src, f, d, length, func(i int) int { return offset + i }, ctx.Err)
	return n, err
}

// EstimateStructListsStructTakeRangeBytes conservatively bounds all owned tree
// leases for a permutation of a contiguous range. It includes temporary parent
// masks and repeated schema snapshots; it is not a minimal payload estimate.
func EstimateStructListsStructTakeRangeBytes(ctx context.Context, src *StructVector, f Field, offset, length int) (int64, error) {
	if ctx == nil || src == nil || offset < 0 || length < 0 || offset > src.Rows || length > src.Rows-offset {
		return 0, fmt.Errorf("invalid StructLists tree estimate range")
	}
	if err := checkStructListTree(f, 0, ctx.Err); err != nil {
		return 0, err
	}
	if f.Type != TypeStruct {
		return 0, fmt.Errorf("expected Struct estimate root")
	}
	if err := checkStructListsGraph(src.Values, f.Children, 1, ctx.Err); err != nil {
		return 0, err
	}
	wrapper := Batch{Rows: src.Rows, Structs: map[string]*StructVector{f.Name: src}}
	// Validate empty-range storage too. A stack row slot avoids an unadmitted
	// index array; per-row validation is conservative work, not a speed claim.
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
func estimateStructListsTreeTake(ctx context.Context, src *StructVector, f Field, offset, length int) (int64, error) {
	snapshot, err := structListsSnapshotBytes(f, ctx.Err)
	if err != nil {
		return 0, err
	}
	if fieldHasNestedLists(f, 0) {
		names, e := arraySnapshotStringBytes(f, ctx.Err)
		if e != nil {
			return 0, e
		}
		snapshot, e = addRunSizes(snapshot, names)
		if e != nil {
			return 0, e
		}
	}
	total, err := addRunSizes(8192, snapshot, int64(WordsFor(src.Rows)+WordsFor(length))*8)
	if err != nil {
		return 0, err
	}
	// The actual implementation groups ordinary children in one flat owner.
	// Estimating each child in its own single-field owner conservatively includes
	// that common base more than once, never omitting any payload or metadata.
	for _, c := range f.Children {
		if err = ctx.Err(); err != nil {
			return 0, err
		}
		var n int64
		if c.Type == TypeStruct {
			n, err = estimateStructListsTreeTake(ctx, src.Values.Structs[c.Name], c, offset, length)
		} else if IsNestedList(c) {
			n, err = nestedListRangeOwnedBytes(ctx, src.Values.Arrays[c.Name], c, NestedListParentDomain{}, offset, length)
		} else if IsStructList(c) {
			// Discarding ancestor masks overestimates hidden data and cannot omit bytes.
			// Already validated full-root required values; this is sizing, not admission.
			_, n, err = structListOwnedBytes(src.Values.StructLists[c.Name], c, StructListParentDomain{}, length, func(i int) int { return offset + i }, ctx.Err)
		} else {
			one := [1]Field{c}
			field := Field{Name: f.Name, Type: TypeStruct, Nullable: true, Encoding: EncodingView, Children: one[:]}
			v := *src
			v.Fields = one[:]
			var base int64
			base, err = recursiveOwnedEstimate(&v, field, 0, 0, ctx.Err)
			n = base
			if err == nil {
				for r := offset; r < offset+length; r++ {
					var k int64
					k, err = recursiveOwnedEstimate(&v, field, r, 1, ctx.Err)
					if err != nil {
						break
					}
					n, err = ownedRangeAdd(n, k-base, 1)
					if err != nil {
						break
					}
				}
			}
		}
		if err != nil {
			return 0, err
		}
		total, err = addRunSizes(total, n)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}
