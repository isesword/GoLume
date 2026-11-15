package memory

import (
	"fmt"
	"math"
)

// EstimateCollectBatchBytes admits an owned full-row-range clone for Collect.
// Native values use the same allocation model as owned fan-out (maps, vector
// descriptors, capacities, and clone growth overlap), not just logical payload.
// Opaque retains Collect's legacy validity-only charge; its private backing is
// NOT covered. Unlike the fan-out estimate this does not inspect Arrow buffers
// or require a sizing contract from arbitrary Retain/Release values.
// Inputs must satisfy CloneBatchRange's valid acyclic runtime graph contract.
func EstimateCollectBatchBytes(src *Batch) (int64, error) {
	if src != nil && len(src.Decimals)+len(src.Strings)+len(src.DictStrings)+len(src.StringLists)+len(src.Int64Lists)+len(src.Structs)+len(src.Opaque) == 0 {
		return estimateCollectFixedWidth(src)
	}
	state := nativeOwnedEstimateState{collect: true}
	if src == nil {
		return 0, fmt.Errorf("cannot estimate nil Collect batch")
	}
	metadata, err := ownedRangeMetadata(src, 0, src.Rows, &state, false, false, true, true)
	if err != nil {
		return 0, err
	}
	estimate, err := nativeOwnedRangeFromMetadata(src, 0, src.Rows, state, metadata)
	if err != nil {
		return 0, err
	}
	return estimate.Bytes, nil
}

// CloneCollectBatch copies logical native rows while retaining full Opaque values
// using only their Retain/Release contract. It does not access Arrow private data.
// Call EstimateCollectBatchBytes and reserve before calling this function.
func CloneCollectBatch(src *Batch) (*Batch, error) {
	if src == nil {
		return nil, fmt.Errorf("cannot clone nil Collect batch")
	}
	return cloneBatchRange(src, 0, src.Rows, true)
}

// Collect-only metadata allowances on the supported 64-bit Go runtime.
// Batch is 144 bytes (allow 160), and 17 empty map headers are 48 each
// (allow 64 each): 1248, rounded here to 1280. Map buckets/growth belong
// to entry charges, not this container charge. Largest native descriptor is
// DictionaryStringVector at 120 bytes (allow 128); retain 512 map-growth
// and 256 per-backing rounding slack from the general fan-out model.
const collectContainerAdmission int64 = 1280
const collectVectorAdmission int64 = 896

// Flat fixed-width native output is common in Collect. Validate and accumulate
// metadata/payload in one traversal rather than repeat the generic range walk.
// Complex and Opaque batches use the unchanged general traversal above.
func estimateCollectFixedWidth(src *Batch) (int64, error) {
	hasNative := len(src.Int8s)+len(src.Int16s)+len(src.Int32s)+len(src.Int64s)+len(src.Uint8s)+len(src.Uint16s)+len(src.Uint32s)+len(src.Uint64s)+len(src.Float32s)+len(src.Float64s) > 0
	if src.Rows < 0 || (hasNative && int64(src.Rows) > math.MaxInt64/16) {
		return 0, fmt.Errorf("Collect rows out of byte range")
	}
	metadata, payload := collectContainerAdmission, int64(0)
	entry := func(name string, n int, validity Bitmap, width int64) error {
		if n < src.Rows {
			return fmt.Errorf("Collect column %q is shorter than rows", name)
		}
		if err := rangeValidityBounds(validity, 0, src.Rows); err != nil {
			return err
		}
		var err error
		metadata, err = ownedRangeAdd(metadata, collectVectorAdmission, 1)
		if err != nil {
			return err
		}
		metadata, err = ownedRangeAdd(metadata, int64(len(name)), 1)
		if err != nil {
			return err
		}
		payload, err = ownedRangeAdd(payload, int64(src.Rows), width)
		if err != nil {
			return err
		}
		if validity != nil && src.Rows > 0 {
			payload, err = ownedRangeAdd(payload, int64(WordsFor(src.Rows)), 8)
		}
		return err
	}
	for name, v := range src.Int8s {
		if v == nil {
			return 0, fmt.Errorf("Collect Int8s column %q is nil", name)
		}
		if err := entry(name, len(v.Values), v.Validity, 1); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Int16s {
		if v == nil {
			return 0, fmt.Errorf("Collect Int16s column %q is nil", name)
		}
		if err := entry(name, len(v.Values), v.Validity, 2); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Int32s {
		if v == nil {
			return 0, fmt.Errorf("Collect Int32s column %q is nil", name)
		}
		if err := entry(name, len(v.Values), v.Validity, 4); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Int64s {
		if v == nil {
			return 0, fmt.Errorf("Collect Int64s column %q is nil", name)
		}
		if err := entry(name, len(v.Values), v.Validity, 8); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Uint8s {
		if v == nil {
			return 0, fmt.Errorf("Collect Uint8s column %q is nil", name)
		}
		if err := entry(name, len(v.Values), v.Validity, 1); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Uint16s {
		if v == nil {
			return 0, fmt.Errorf("Collect Uint16s column %q is nil", name)
		}
		if err := entry(name, len(v.Values), v.Validity, 2); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Uint32s {
		if v == nil {
			return 0, fmt.Errorf("Collect Uint32s column %q is nil", name)
		}
		if err := entry(name, len(v.Values), v.Validity, 4); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Uint64s {
		if v == nil {
			return 0, fmt.Errorf("Collect Uint64s column %q is nil", name)
		}
		if err := entry(name, len(v.Values), v.Validity, 8); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Float32s {
		if v == nil {
			return 0, fmt.Errorf("Collect Float32s column %q is nil", name)
		}
		if err := entry(name, len(v.Values), v.Validity, 4); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Float64s {
		if v == nil {
			return 0, fmt.Errorf("Collect Float64s column %q is nil", name)
		}
		if err := entry(name, len(v.Values), v.Validity, 8); err != nil {
			return 0, err
		}
	}
	charge, err := nativeBulkAdmission(payload)
	if err != nil {
		return 0, err
	}
	return ownedRangeAdd(metadata, charge, 1)
}
