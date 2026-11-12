package memory

import "errors"

var errNativeSharedOpaque = errors.New("native shared bindings require Opaque fallback")

// NativeSharedOwnerAllowance reserves bookkeeping space for one shared owner.
const NativeSharedOwnerAllowance int64 = 256

// NativeSharedBindingOverheadAllowance bounds each live binding's pending node.
const NativeSharedBindingOverheadAllowance int64 = 256

// NativeSharedRangeEstimate partitions clone admission into one shared owner
// and one independently releasable charge per live native binding.
// Supported is false when the runtime graph requires legacy Opaque handling.
type NativeSharedRangeEstimate struct {
	CloneBytes   int64
	OwnerBytes   int64
	BindingBytes int64
	Supported    bool
}

// EstimateNativeSharedBatchRange splits the existing native clone admission
// into shared payload/owner and per-live-branch binding charges. Opaque at any
// runtime level chooses the old clone path without inspecting Arrow values.
// Inputs obey the same finite acyclic runtime graph invariant as owned cloning.
//
// Go 1.27: Batch is 136 bytes (allow 160), each of sixteen runtime map headers
// is 48 bytes (allow 64). Thus 1184 bytes covers the base container allocation.
// From its 4096-byte charge, move 256 to the shared owner. The remaining 3840
// still covers the base plus a <=256-byte per-binding pending node. Per-entry
// map growth, vector headers and Fields retain their original allowances.
// The owner must NOT retain a separate clone binding graph: the initial clone
// is the first live binding, and every binding drops its graph at ACK. Shared
// Fields/payload stay reachable through surviving read-only bindings.
func EstimateNativeSharedBatchRange(src *Batch, offset, rows int) (NativeSharedRangeEstimate, error) {
	state := nativeOwnedEstimateState{nativeOnly: true}
	metadata, err := ownedRangeMetadata(src, offset, rows, &state, false, false, true, true)
	if err == errNativeSharedOpaque {
		return NativeSharedRangeEstimate{}, nil
	}
	if err != nil {
		return NativeSharedRangeEstimate{}, err
	}
	full, err := nativeOwnedRangeFromMetadata(src, offset, rows, state, metadata)
	if err != nil {
		return NativeSharedRangeEstimate{}, err
	}
	if metadata < NativeSharedOwnerAllowance || full.Bytes < metadata {
		return NativeSharedRangeEstimate{}, errors.New("invalid native shared cost decomposition")
	}
	return NativeSharedRangeEstimate{CloneBytes: full.Bytes, OwnerBytes: full.Bytes - metadata + NativeSharedOwnerAllowance, BindingBytes: metadata - NativeSharedOwnerAllowance, Supported: true}, nil
}

// BorrowNativeWholeBatch copies a successful CloneBatchRange's whole binding
// graph, not a Rows-sized slice. Each Batch/map/vector/List-child header is
// independent, preserving duplicate-Field append lengths and all len/cap/Rows.
// Payload and Fields are shared read-only under the existing stream contract.
// ClearColumns/root binding replacement is safe independently on each view.
// Opaque is unsupported at every runtime level and must use the legacy path.
// This does not transfer ownership or allow mutation of borrowed payloads.
func BorrowNativeWholeBatch(src *Batch) (*Batch, error) {
	if src == nil {
		return nil, errors.New("cannot borrow nil native batch")
	}
	if len(src.Opaque) != 0 {
		return nil, errNativeSharedOpaque
	}
	dst := NewBatch()
	dst.Rows = src.Rows
	for k, v := range src.Int8s {
		if v == nil {
			dst.Int8s[k] = nil
			continue
		}
		h := *v
		dst.Int8s[k] = &h
	}
	for k, v := range src.Int16s {
		if v == nil {
			dst.Int16s[k] = nil
			continue
		}
		h := *v
		dst.Int16s[k] = &h
	}
	for k, v := range src.Int32s {
		if v == nil {
			dst.Int32s[k] = nil
			continue
		}
		h := *v
		dst.Int32s[k] = &h
	}
	for k, v := range src.Int64s {
		if v == nil {
			dst.Int64s[k] = nil
			continue
		}
		h := *v
		dst.Int64s[k] = &h
	}
	for k, v := range src.Uint8s {
		if v == nil {
			dst.Uint8s[k] = nil
			continue
		}
		h := *v
		dst.Uint8s[k] = &h
	}
	for k, v := range src.Uint16s {
		if v == nil {
			dst.Uint16s[k] = nil
			continue
		}
		h := *v
		dst.Uint16s[k] = &h
	}
	for k, v := range src.Uint32s {
		if v == nil {
			dst.Uint32s[k] = nil
			continue
		}
		h := *v
		dst.Uint32s[k] = &h
	}
	for k, v := range src.Uint64s {
		if v == nil {
			dst.Uint64s[k] = nil
			continue
		}
		h := *v
		dst.Uint64s[k] = &h
	}
	for k, v := range src.Float32s {
		if v == nil {
			dst.Float32s[k] = nil
			continue
		}
		h := *v
		dst.Float32s[k] = &h
	}
	for k, v := range src.Float64s {
		if v == nil {
			dst.Float64s[k] = nil
			continue
		}
		h := *v
		dst.Float64s[k] = &h
	}
	for k, v := range src.Decimals {
		if v == nil {
			dst.Decimals[k] = nil
			continue
		}
		h := *v
		dst.Decimals[k] = &h
	}
	for k, v := range src.Strings {
		if v == nil {
			dst.Strings[k] = nil
			continue
		}
		h := *v
		dst.Strings[k] = &h
	}
	for k, v := range src.DictStrings {
		if v == nil {
			dst.DictStrings[k] = nil
			continue
		}
		h := *v
		dst.DictStrings[k] = &h
	}
	for k, v := range src.StringLists {
		if v == nil {
			dst.StringLists[k] = nil
			continue
		}
		h := *v
		if v.Values != nil {
			child := *v.Values
			h.Values = &child
		}
		dst.StringLists[k] = &h
	}
	for k, v := range src.Int64Lists {
		if v == nil {
			dst.Int64Lists[k] = nil
			continue
		}
		h := *v
		if v.Values != nil {
			child := *v.Values
			h.Values = &child
		}
		dst.Int64Lists[k] = &h
	}
	for k, v := range src.Structs {
		if v == nil {
			dst.Structs[k] = nil
			continue
		}
		h := *v
		if v.Values != nil {
			child, err := BorrowNativeWholeBatch(v.Values)
			if err != nil {
				return nil, err
			}
			h.Values = child
		}
		dst.Structs[k] = &h
	}
	return dst, nil
}
