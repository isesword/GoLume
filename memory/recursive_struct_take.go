package memory

import (
	"context"
	"fmt"
)

// TakeRecursiveStructRowsOwned copies a selected, possibly repeated permutation
// into a canonical owned tree. All source storage is checked; required values
// are checked only in the selected, ancestor-visible domain. Its lease includes
// metadata and conservative append growth and is released by Close.
func TakeRecursiveStructRowsOwned(ctx context.Context, manager Manager, src *StructVector, field Field, rows []int) (*OwnedRecursiveStruct, error) {
	if ctx == nil || manager == nil || src == nil {
		return nil, fmt.Errorf("recursive take requires context, manager and vector")
	}
	if err := checkRecursiveField(field, 0, ctx.Err); err != nil {
		return nil, err
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if row < 0 || row >= src.Rows {
			return nil, fmt.Errorf("recursive take row out of bounds")
		}
	}
	// The single root wrapper is stack-local; no output containers precede admission.
	batch := Batch{Rows: src.Rows, Structs: map[string]*StructVector{field.Name: src}}
	if _, err := selectionRunFieldSize(&batch, field, rows, columnarSelectionDomain{check: ctx.Err}); err != nil {
		return nil, err
	}
	base, err := recursiveOwnedEstimate(src, field, 0, 0, ctx.Err)
	if err != nil {
		return nil, err
	}
	total := base
	for _, row := range rows {
		n, err := recursiveOwnedEstimate(src, field, row, 1, ctx.Err)
		if err != nil {
			return nil, err
		}
		total, err = ownedRangeAdd(total, n-base, 1)
		if err != nil {
			return nil, err
		}
	}
	lease, err := manager.Reserve(ctx, total)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			lease.Release()
		}
	}()
	out, err := newRecursiveOwnedVector(field, ctx.Err)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if err := appendRecursiveOwnedRow(out, src, field, row, true, ctx.Err); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ok = true
	return &OwnedRecursiveStruct{Vector: out, Bytes: total, lease: lease}, nil
}

// EstimateRecursiveStructTakeRangeBytes bounds TakeRecursiveStructRowsOwned for
// any permutation of a contiguous range without allocating row indices. It
// validates the complete source tree and includes all owned metadata once.
func EstimateRecursiveStructTakeRangeBytes(ctx context.Context, src *StructVector, field Field, offset, length int) (int64, error) {
	if ctx == nil || src == nil || offset < 0 || length < 0 || offset > src.Rows || length > src.Rows-offset {
		return 0, fmt.Errorf("recursive take range out of bounds")
	}
	if err := src.ValidateRecursiveWithContext(field, src.Rows, ctx.Err); err != nil {
		return 0, err
	}
	base, err := recursiveOwnedEstimate(src, field, 0, 0, ctx.Err)
	if err != nil {
		return 0, err
	}
	total := base
	for row := offset; row < offset+length; row++ {
		n, e := recursiveOwnedEstimate(src, field, row, 1, ctx.Err)
		if e != nil {
			return 0, e
		}
		total, e = ownedRangeAdd(total, n-base, 1)
		if e != nil {
			return 0, e
		}
	}
	return total, nil
}
