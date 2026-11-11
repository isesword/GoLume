package memory

import (
	"fmt"
	"math"
	"unsafe"
)

// NativeOwnedRangeEstimate separates native owned-copy admission from the
// historical Opaque logical charge. OpaquePresent remains true for zero-byte
// Opaque arrays: their datatype/private graph is outside the native guarantee.
type NativeOwnedRangeEstimate struct {
	Bytes             int64
	NativeBytes       int64
	LegacyOpaqueBytes int64
	OpaquePresent     bool
}

// EstimateNativeOwnedBatchRangeBytes models native clone allocation before
// CloneBatchRange: native Go payload, all native runtime maps (including Decimal),
// vector/list-child descriptors, copied Fields and directly visible strings,
// append spare capacity and simultaneous old/new growth buffers. Opaque is charged
// exactly once under EstimateBatchRangeBytes's legacy contract. It is NOT covered
// by this native allocation bound: no new sizing/clone contract, type whitelist,
// cache read, extension conversion or private-graph traversal is imposed on it.
// Mixed batches retain native admission instead of falling back as a whole.
//
// Direct CloneBatchRange payload uses checked n+ceil(n/4), plus the existing
// per-backing rounding slack below. Its pointer-free numeric (1/2/4/8/16-byte),
// string, dictionary and list buffers allocate once: append(nil, slice...) has
// oldCap=0, so Go 1.27 nextslicecap returns the requested length; offsets/validity
// use make. The Go 1.27 noscan size classes satisfy roundup(n)<=1.25*n+6.75;
// large allocations round to 8192-byte pages, adding <n/4 above 32768 bytes.
// The 32761..32768 transition rounds to 32768 and also satisfies the bound.
// Summing backings permits ceil on the total, with at least 64 bytes slack each.
// Non-empty Struct row append retains four logical copies: oldCap<newLen,
// nextslicecap<=2*newLen and allocator rounding cover simultaneous old+new backing
// below 3.5*logical bytes. This includes parent validity, missing/repeated fields
// and all conservative child payload. Zero-row Struct recurses through bulk copy.
// Pointer-bearing Fields storage is metadata, not part of the reduced bulk rate.
// Per native entry, 1024 covers map slots/growth (512), descriptor (256), and
// small-allocation rounding (256: up to four backings with 64 bytes each).
// Lists get a separate child entry. Fields receive 2*Sizeof(Field)+64 storage.
// Each native Batch container receives 4096 for Batch and its sixteen map headers.
// A pure Opaque root retains its original charge; a native Struct's child Batch
// is still a native container, even if its only runtime entries are Opaque.
//
// This is not an RSS bound: GC-delayed garbage, allocator arenas, hidden backing
// of Go substrings, source/caller memory, queue/setup and producer range lists
// remain outside this per-clone charge. Shared immutable names are charged by
// visible length. Constants track the current Go clone strategy, not every future
// runtime. Like CloneBatchRange, this API requires valid acyclic runtime Batch
// graphs; it imposes no fixed width/depth caps and allocates no traversal maps.
// Caller-owned shared Fields.Children backing graphs
// remain outside this native-copy guarantee and are not traversed.
// Struct Field occurrences are charged individually, preserving repeated-field
// append behavior without a duplicate-name set. Recursive call-stack workspace
// is not part of the retained clone allocation charge. Opaque keeps its existing behavior, including descriptor and
// validity allocation/retention, without any new owned-allocation guarantee.
func EstimateNativeOwnedBatchRangeBytes(src *Batch, offset, length int) (NativeOwnedRangeEstimate, error) {
	state := nativeOwnedEstimateState{}
	metadata, err := ownedRangeMetadata(src, offset, length, &state, false, false, true, true)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	return nativeOwnedRangeFromMetadata(src, offset, length, state, metadata)
}

func nativeOwnedRangeFromMetadata(src *Batch, offset, length int, state nativeOwnedEstimateState, metadata int64) (NativeOwnedRangeEstimate, error) {
	// One logical traversal produces both legacy total and Opaque contribution.
	// Public logical sizing and the local native-only Struct comparison retain
	// their original behavior by passing a nil component accumulator.
	var opaque, bulk int64
	logical, err := estimateBatchRangeBytesAccounting(src, offset, length, false, true, &opaque, &bulk, true)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	if logical < opaque {
		return NativeOwnedRangeEstimate{}, fmt.Errorf("opaque charge exceeds total logical charge")
	}
	nativePayload := logical - opaque
	nativePayloadExtra := state.nativePayloadExtra

	if bulk > nativePayload {
		return NativeOwnedRangeEstimate{}, fmt.Errorf("bulk payload exceeds native logical charge")
	}
	nativeWithAppends, err := ownedRangeAdd(nativePayload-bulk, nativePayloadExtra, 1)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	bulkCharge, err := nativeBulkAdmission(bulk)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	native, err := ownedRangeAdd(metadata, nativeWithAppends, 4)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	native, err = ownedRangeAdd(native, bulkCharge, 1)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	total, err := ownedRangeAdd(native, opaque, 1)
	if err != nil {
		return NativeOwnedRangeEstimate{}, err
	}
	return NativeOwnedRangeEstimate{Bytes: total, NativeBytes: native, LegacyOpaqueBytes: opaque, OpaquePresent: state.opaquePresent}, nil
}

// nativeBulkAdmission applies 5/4 with checked arithmetic and no multiplication
// before division. Small-backing allocator slack is already in vector metadata.
func nativeBulkAdmission(bytes int64) (int64, error) {
	extra := bytes / 4
	if bytes%4 != 0 {
		extra++
	}
	return ownedRangeAdd(bytes, extra, 1)
}

func ownedRangeAdd(total, count, scale int64) (int64, error) {
	if total < 0 || count < 0 || scale < 0 || (scale > 0 && count > (math.MaxInt64-total)/scale) {
		return 0, fmt.Errorf("owned clone admission overflows int64")
	}
	return total + count*scale, nil
}

// bulkMode tracks actual direct CloneBatchRange allocation independently of
// clonedStructs (which controls Struct Field occurrence accounting). Bulk offset
// normalization checks interiors before Data copying; its temporary offset
// backing on failure is already charged by endpoint-derived lengths. Incremental
// Struct append must preflight interiors before any potentially larger row copy.
func ownedRangeMetadata(b *Batch, offset, length int, state *nativeOwnedEstimateState, forceContainer, inheritedNulls, clonedStructs, bulkMode bool) (int64, error) {
	if b == nil {
		return 0, fmt.Errorf("native owned clone has nil batch")
	}
	if state.nativeOnly && len(b.Opaque) != 0 {
		return 0, errNativeSharedOpaque
	}
	hasNative := len(b.Int8s)+len(b.Int16s)+len(b.Int32s)+len(b.Int64s)+len(b.Uint8s)+len(b.Uint16s)+len(b.Uint32s)+len(b.Uint64s)+len(b.Float32s)+len(b.Float64s)+len(b.Decimals)+len(b.Strings)+len(b.DictStrings)+len(b.StringLists)+len(b.Structs) > 0
	if len(b.Opaque) > 0 {
		state.opaquePresent = true
	}
	if offset < 0 || length < 0 || offset > b.Rows || length > b.Rows-offset || ((hasNative || forceContainer) && length > math.MaxInt64/16) {
		return 0, fmt.Errorf("owned clone range offset=%d length=%d out of rows=%d or byte range", offset, length, b.Rows)
	}
	end := offset + length
	total := int64(0)
	if hasNative || forceContainer || len(b.Opaque) == 0 {
		total = 4096
	}
	add := func(n, scale int64) error { var err error; total, err = ownedRangeAdd(total, n, scale); return err }
	entry := func(name string, validity Bitmap) error {
		if err := rangeValidityBounds(validity, offset, length); err != nil {
			return err
		}
		if err := add(1, 1024); err != nil {
			return err
		}
		return add(int64(len(name)), 1)
	}
	for name, v := range b.Int8s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Int8s column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Int16s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Int16s column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Int32s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Int32s column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Int64s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Int64s column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Uint8s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Uint8s column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Uint16s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Uint16s column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Uint32s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Uint32s column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Uint64s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Uint64s column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Float32s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Float32s column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Float64s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Float64s column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Decimals {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("owned clone Decimals column %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
	}
	for name, v := range b.Strings {
		if v == nil {
			return 0, fmt.Errorf("owned clone string %q is nil", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
		if _, _, err := stringRangeBounds(v, offset, length); err != nil {
			return 0, err
		}
		if !bulkMode {
			if err := ownedRangeOffsets(v.Offsets, offset, length); err != nil {
				return 0, err
			}
		}
	}
	for name, v := range b.DictStrings {
		if v == nil || len(v.Indices) < end {
			return 0, fmt.Errorf("owned clone dictionary %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
		if err := add(int64(len(v.DictionaryDomain)), 1); err != nil {
			return 0, err
		}
	}
	for name, v := range b.StringLists {
		if v == nil || v.Values == nil {
			return 0, fmt.Errorf("owned clone list %q has nil child", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
		if err := add(1, 1024); err != nil {
			return 0, err
		}
		lo, hi, _, _, err := stringListRangeBounds(v, offset, length)
		if err != nil {
			return 0, err
		}
		if !bulkMode {
			if err := ownedRangeOffsets(v.Offsets, offset, length); err != nil {
				return 0, err
			}
		}
		if !bulkMode {
			if err := ownedRangeOffsets(v.Values.Offsets, int(lo), int(hi-lo)); err != nil {
				return 0, err
			}
		}
	}
	for name, v := range b.Structs {
		if v == nil || v.Values == nil || v.Rows < end {
			return 0, fmt.Errorf("owned clone struct %q is shorter than range", name)
		}
		if err := entry(name, v.Validity); err != nil {
			return 0, err
		}
		childNulls := inheritedNulls
		if v.Validity != nil {
			for row := offset; row < end && !childNulls; row++ {
				if !IsSet(v.Validity, row) {
					childNulls = true
				}
			}
		}
		// For shared admission, visit the runtime child graph before Field
		// dispatch so undeclared or unsupported Opaque children select fallback.
		var child int64
		var childErr error
		if state.nativeOnly {
			child, childErr = ownedRangeMetadata(v.Values, offset, length, state, true, childNulls, clonedStructs && length == 0, bulkMode && length == 0)
			if childErr != nil {
				return 0, childErr
			}
		}
		if clonedStructs && length > 0 && len(v.Fields) > 0 {
			appended, missingMetadata, err := nativeStructAppendEstimate(v, offset, length, childNulls)
			if err != nil {
				return 0, err
			}
			if err := add(missingMetadata, 1); err != nil {
				return 0, err
			}
			// Compare only this child Batch level. Recursing through unrelated
			// runtime Struct maps here would rescan every suffix of deep inputs.
			local := *v.Values
			local.Structs = nil
			mapsPayload, err := estimateBatchRangeBytesMode(&local, offset, length, childNulls, false)
			if err != nil {
				return 0, err
			}
			if appended > mapsPayload {
				state.nativePayloadExtra, err = ownedRangeAdd(state.nativePayloadExtra, appended-mapsPayload, 1)
				if err != nil {
					return 0, err
				}
			}
		}

		fields, err := ownedRangeFields(v.Fields)
		if err != nil {
			return 0, err
		}
		if err := add(fields, 1); err != nil {
			return 0, err
		}
		// Non-empty Struct cloning follows Fields and never recursively clones
		// undeclared child Struct maps. Empty cloning copies all runtime maps.
		if !state.nativeOnly {
			child, childErr = ownedRangeMetadata(v.Values, offset, length, state, true, childNulls, clonedStructs && length == 0, bulkMode && length == 0)
			if childErr != nil {
				return 0, childErr
			}
		}
		if err := add(child, 1); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func ownedRangeOffsets(offsets []uint32, offset, length int) error {
	if len(offsets) == 0 && offset == 0 && length == 0 {
		return nil
	}
	if offset < 0 || length < 0 || offset >= len(offsets) || length >= len(offsets)-offset {
		return fmt.Errorf("owned clone offsets shorter than range")
	}
	for i := offset + 1; i <= offset+length; i++ {
		if offsets[i] < offsets[i-1] {
			return fmt.Errorf("owned clone offsets are not monotonic")
		}
	}
	return nil
}

// CloneStructRange copies only the top-level Field slice. Children slice
// headers are copied, but their caller-owned shared graphs are not traversed or
// brought under this native-copy allocation guarantee. Charge the copied slots
// and directly visible strings, preserving arbitrary deep/cyclic shared schemas.
func ownedRangeFields(fields []Field) (int64, error) {
	total, err := ownedRangeAdd(64, int64(len(fields)), 2*int64(unsafe.Sizeof(Field{})))
	if err != nil {
		return 0, err
	}
	for _, field := range fields {
		for _, value := range []string{field.Name, field.DictionaryDomain, field.ArrowType} {
			total, err = ownedRangeAdd(total, int64(len(value)), 1)
			if err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

type nativeOwnedEstimateState struct {
	nativeOnly         bool
	opaquePresent      bool
	nativePayloadExtra int64
}
