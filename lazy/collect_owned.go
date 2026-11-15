package lazy

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

// Each retained entry has an 8-byte batch pointer and a 16-byte reservation
// interface. 256 per entry covers both slice growth/old-new overlap, allocator
// rounding, and reservation bookkeeping on supported 64-bit Go platforms.
// This is an allocation admission model, not an RSS or source-memory bound.
const collectEntryAllowance int64 = 256

func collectBatchAdmission(batch *memory.Batch) (int64, error) {
	bytes, err := memory.EstimateCollectBatchBytes(batch)
	if err != nil {
		return 0, err
	}
	if bytes > math.MaxInt64-collectEntryAllowance {
		return 0, fmt.Errorf("Collect output admission overflows int64")
	}
	return bytes + collectEntryAllowance, nil
}
