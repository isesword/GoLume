package expr

import "gocolumn/memory"

func releaseRegisterExport(r *scalarRegister) {
	if r.exportBatch != nil {
		r.exportBatch.ClearColumns()
		r.exportBatch = nil
	}
	if r.exportReservation != nil {
		r.exportReservation.Release()
		r.exportReservation = nil
	}
}

// exportBorrowedValue crops all row-indexed headers to the logical prefix.
// Offset/validity rebasing storage is admitted before borrowing; input headers
// and payloads remain untouched. Duplicate output roots reuse the same view.
func (w *evaluationState) exportBorrowedValue(index, rows int) (ScalarValue, error) {
	r := w.regs[index]
	owned := w.ownedRegister(index)
	if owned.exportBatch == nil {
		source := memory.NewBatch()
		switch {
		case r.strView != nil:
			source.Strings["v"] = r.strView
			source.Rows = r.strView.Len()
		case r.strDict != nil:
			source.DictStrings["v"] = r.strDict
			source.Rows = r.strDict.Len()
		case r.strList != nil:
			source.StringLists["v"] = r.strList
			source.Rows = r.strList.Len()
		case r.strct != nil:
			source.Structs["v"] = r.strct
			source.Rows = r.strct.Len()
		case r.opaque != nil:
			source.Opaque["v"] = r.opaque
			source.Rows = r.opaque.Len()
		}
		size, err := memory.BorrowedRangeMetadataBytes(source, 0, rows)
		if err != nil {
			return ScalarValue{}, err
		}
		lease, err := w.acquire(size)
		if err != nil {
			return ScalarValue{}, err
		}
		view, err := memory.BorrowBatchRange(source, 0, rows)
		if err != nil {
			lease.Release()
			return ScalarValue{}, err
		}
		owned.exportBatch, owned.exportReservation = view, lease
	}
	view := owned.exportBatch
	result := ScalarValue{Type: r.typ, String: view.Strings["v"], DictionaryString: view.DictStrings["v"], StringList: view.StringLists["v"], Struct: view.Structs["v"], Opaque: view.Opaque["v"]}
	if result.Opaque != nil {
		header := *result.Opaque
		header.Owned = false
		result.Opaque = &header
	}
	if result.Struct != nil {
		header := *result.Struct
		header.Values = borrowedOwnerHeaders(header.Values)
		result.Struct = &header
	}
	return result, nil
}

func ownedBatchCapacityBytes(b *memory.Batch) int64 {
	if b == nil {
		return 0
	}
	var n int64
	for _, v := range b.Int8s {
		n += int64(cap(v.Values)) + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint8s {
		n += int64(cap(v.Values)) + int64(cap(v.Validity))*8
	}
	for _, v := range b.Int16s {
		n += int64(cap(v.Values))*2 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint16s {
		n += int64(cap(v.Values))*2 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Int32s {
		n += int64(cap(v.Values))*4 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint32s {
		n += int64(cap(v.Values))*4 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Int64s {
		n += int64(cap(v.Values))*8 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint64s {
		n += int64(cap(v.Values))*8 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Float32s {
		n += int64(cap(v.Values))*4 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Float64s {
		n += int64(cap(v.Values))*8 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Strings {
		n += int64(cap(v.Offsets))*4 + int64(cap(v.Data)) + int64(cap(v.Validity))*8
	}
	for _, v := range b.DictStrings {
		n += int64(cap(v.Indices)+cap(v.Offsets))*4 + int64(cap(v.Data)) + int64(cap(v.Validity))*8
	}
	for _, v := range b.StringLists {
		n += int64(cap(v.Offsets))*4 + int64(cap(v.Validity))*8
		if v.Values != nil {
			n += int64(cap(v.Values.Offsets))*4 + int64(cap(v.Values.Data)) + int64(cap(v.Values.Validity))*8
		}
	}
	for _, v := range b.Structs {
		if v != nil {
			n += int64(cap(v.Validity))*8 + ownedBatchCapacityBytes(v.Values)
		}
	}
	return n
}

// Keep release flags private to the owning export view. Consumers may clear
// their result Batch without releasing a workspace-owned Arrow slice shared
// by another output alias.
func borrowedOwnerHeaders(source *memory.Batch) *memory.Batch {
	if source == nil {
		return nil
	}
	result := *source
	if len(source.Opaque) > 0 {
		result.Opaque = make(map[string]*memory.OpaqueVector, len(source.Opaque))
		for name, value := range source.Opaque {
			if value == nil {
				continue
			}
			header := *value
			header.Owned = false
			result.Opaque[name] = &header
		}
	}
	if len(source.Structs) > 0 {
		result.Structs = make(map[string]*memory.StructVector, len(source.Structs))
		for name, value := range source.Structs {
			if value == nil {
				continue
			}
			header := *value
			header.Values = borrowedOwnerHeaders(value.Values)
			result.Structs[name] = &header
		}
	}
	return &result
}
