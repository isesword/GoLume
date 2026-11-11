package batchview

import (
	"fmt"
	"gocolumn/memory"
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// A Boolean binding is kept packed until Bind selects its output range. Only
// actual Arrow Boolean arrays qualify; dictionary/extension/other opaque values
// keep their original representation and sizing contract.
func packedBoolean(v *memory.OpaqueVector) (*array.Boolean, bool) {
	if v == nil || v.Type != memory.TypeBool {
		return nil, false
	}
	a, ok := v.Value.(*array.Boolean)
	return a, ok
}

func addDecodedBooleanBytes(total int64, rows int) (int64, error) {
	if total < 0 || rows < 0 || int64(rows) > (math.MaxInt64-total)/4 {
		return 0, fmt.Errorf("decoded Boolean byte estimate overflows int64")
	}
	return total + int64(rows)*4, nil
}

// decodedPayloadBytes uses the representation exposed by Bind, not the packed
// Arrow buffer retained internally. Other opaque backing still counts in full.
func decodedPayloadBytes(src *memory.Batch, offset, length int) (int64, error) {
	// Validate packed backing before the generic Arrow estimator dereferences it.
	if err := validatePackedBooleanRanges(src, offset, length); err != nil {
		return 0, err
	}
	total, err := memory.EstimateBatchRangeBytes(src, offset, length)
	if err != nil {
		return 0, err
	}
	return replaceBooleanPayloadBytes(src, total, length)
}

func validatePackedBooleanRanges(src *memory.Batch, offset, rows int) error {
	if src == nil || offset < 0 || rows < 0 || offset > src.Rows || rows > src.Rows-offset {
		return fmt.Errorf("invalid decoded Boolean payload range")
	}
	for name, v := range src.Opaque {
		if a, ok := packedBoolean(v); ok && (a == nil || a.Len() < offset+rows) {
			return fmt.Errorf("Boolean column %q shorter than source range", name)
		}
	}
	for _, v := range src.Structs {
		if v == nil {
			return fmt.Errorf("nil struct in decoded Boolean payload")
		}
		if err := validatePackedBooleanRanges(v.Values, offset, rows); err != nil {
			return err
		}
	}
	return nil
}

func replaceBooleanPayloadBytes(src *memory.Batch, total int64, rows int) (int64, error) {
	for _, v := range src.Opaque {
		if a, ok := packedBoolean(v); ok {
			if a == nil {
				return 0, fmt.Errorf("nil Arrow Boolean backing")
			}
			size := a.Data().SizeInBytes()
			if size > uint64(total) {
				return 0, fmt.Errorf("invalid Arrow Boolean payload size")
			}
			total -= int64(size)
			var err error
			total, err = addDecodedBooleanBytes(total, rows)
			if err != nil {
				return 0, err
			}
		}
	}
	for _, v := range src.Structs {
		var err error
		total, err = replaceBooleanPayloadBytes(v.Values, total, rows)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

// Match the native source payload policy: a maximal prefix under the soft
// target, allowing one row above it but never above the hard limit.
func selectDecodedRangeRows(src *memory.Batch, offset, maxRows int, target, hard int64) (int, error) {
	if src == nil || offset < 0 || maxRows < 0 || offset > src.Rows || maxRows > src.Rows-offset || target < 0 || hard < 0 {
		return 0, fmt.Errorf("invalid batch payload range or limits")
	}
	if maxRows == 0 {
		return 0, nil
	}
	limit := target
	if limit == 0 || hard > 0 && hard < limit {
		limit = hard
	}
	if limit == 0 {
		return maxRows, nil
	}
	size, err := decodedPayloadBytes(src, offset, maxRows)
	if err != nil {
		return 0, err
	}
	if size <= limit {
		return maxRows, nil
	}
	one, err := decodedPayloadBytes(src, offset, 1)
	if err != nil {
		return 0, err
	}
	if hard > 0 && one > hard {
		return 0, &memory.BatchPayloadTooLargeError{Row: offset, Bytes: one, LimitBytes: hard}
	}
	if one > limit {
		return 1, nil
	}
	lo, hi := 1, maxRows-1
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		size, err := decodedPayloadBytes(src, offset, mid)
		if err != nil {
			return 0, err
		}
		if size <= limit {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo, nil
}
