package physical

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

func compactInt64ListSize(v *memory.Int64ListVector, rows []int, parents *compactParent) (int, error) {
	if v == nil || v.Values == nil {
		return 0, fmt.Errorf("nil compact int64 list")
	}
	if v.Values.Validity != nil && len(v.Values.Validity) < memory.WordsFor(len(v.Values.Values)) {
		return 0, fmt.Errorf("short compact int64 list child validity")
	}
	var children uint64
	for _, row := range rows {
		if row < 0 || row+1 >= len(v.Offsets) || v.Validity != nil && row>>6 >= len(v.Validity) {
			return 0, fmt.Errorf("compact int64 list row out of range")
		}
		lo, hi := v.Offsets[row], v.Offsets[row+1]
		if lo > hi || uint64(hi) > uint64(len(v.Values.Values)) {
			return 0, fmt.Errorf("invalid compact int64 list offsets")
		}
		if rowValid(v.Validity, row) && parents.valid(row) {
			children += uint64(hi - lo)
		}
		if children > math.MaxUint32 || children > uint64(int(^uint(0)>>1))/8 {
			return 0, fmt.Errorf("compact int64 list exceeds offset range")
		}
	}
	return int(children), nil
}

func joinGatherInt64ListSize(v *memory.Int64ListVector, rows []int, parent *memory.StructVector) (int, error) {
	return joinGatherInt64ListSizeSelected(v, joinGatherSelection{rows: rows}, parent)
}
func joinGatherInt64ListSizeSelected(v *memory.Int64ListVector, rows joinGatherSelection, parent *memory.StructVector) (int, error) {
	if v == nil || v.Values == nil {
		return 0, fmt.Errorf("nil join int64 list")
	}
	if v.Values.Validity != nil && len(v.Values.Validity) < memory.WordsFor(len(v.Values.Values)) {
		return 0, fmt.Errorf("short join int64 list child validity")
	}
	var elements uint64
	for i := 0; i < rows.len(); i++ {
		row := rows.row(i)
		if row < 0 || row+1 >= len(v.Offsets) || v.Validity != nil && len(v.Validity) <= row/64 {
			return 0, fmt.Errorf("invalid join int64 list row")
		}
		lo, hi := v.Offsets[row], v.Offsets[row+1]
		if lo > hi || uint64(hi) > uint64(len(v.Values.Values)) {
			return 0, fmt.Errorf("invalid join int64 list offsets")
		}
		if (parent == nil || parent.IsValid(row)) && v.IsValid(row) {
			elements += uint64(hi - lo)
		}
		if elements > math.MaxUint32 || elements > uint64(math.MaxInt)/8 {
			return 0, fmt.Errorf("join int64 list exceeds representation")
		}
	}
	return int(elements), nil
}
func plannedJoinInt64List(rows, elements int) *memory.Int64ListVector {
	return &memory.Int64ListVector{Offsets: make([]uint32, 1, rows+1), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Values: &memory.Int64Vector{Values: make([]int64, 0, elements), Validity: make(memory.Bitmap, memory.WordsFor(elements)), Encoding: memory.EncodingPlain}, Encoding: memory.EncodingView}
}

func joinGatherInt64ListPayload(total *int64, v *memory.Int64ListVector, rows int) error {
	if v == nil || v.Values == nil || len(v.Offsets) != rows+1 || v.Offsets[0] != 0 {
		return fmt.Errorf("malformed join int64 list")
	}
	if err := joinGatherOffsets(v.Offsets, len(v.Values.Values)); err != nil {
		return err
	}
	if uint64(v.Offsets[rows]) != uint64(len(v.Values.Values)) {
		return fmt.Errorf("invalid join int64 list terminal offset")
	}
	if err := joinGatherValidity(v.Validity, rows); err != nil {
		return err
	}
	if err := joinGatherValidity(v.Values.Validity, len(v.Values.Values)); err != nil {
		return err
	}
	if err := addJoinGatherProduct(total, uint64(len(v.Offsets)), 4); err != nil {
		return err
	}
	if err := addJoinGatherProduct(total, uint64(len(v.Validity)+len(v.Values.Validity)), 8); err != nil {
		return err
	}
	return addJoinGatherProduct(total, uint64(len(v.Values.Values)), 8)
}
func joinGatherInt64ListOwnedSelected(total *int64, v *memory.Int64ListVector, rows joinGatherSelection, parent *memory.StructVector) error {
	elements, err := joinGatherInt64ListSizeSelected(v, rows, parent)
	if err != nil {
		return err
	}
	if err := joinGatherStringOwned(total, rows.len(), 0, true); err != nil {
		return err
	}
	return joinGatherFixedOwned(total, elements, 8, true)
}
