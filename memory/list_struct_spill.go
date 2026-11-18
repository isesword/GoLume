package memory

import (
	"math"
	"unsafe"
)

func columnarVersionForSchema(s Schema) uint16 {
	if s.HasNestedLists() {
		return 4
	}
	if s.HasStructLists() {
		return 3
	}
	return columnarRunVersion
}
func validateColumnarRootContext(b *Batch, f Field, rows int, check func() error) error {
	if IsNestedList(f) {
		return b.Arrays[f.Name].ValidateWithContext(f, rows, NestedListParentDomain{}, check)
	}
	if fieldHasNestedLists(f, 0) {
		return b.Structs[f.Name].ValidateArrayContainersWithContext(f, rows, check)
	}
	if IsStructList(f) {
		return b.StructLists[f.Name].ValidateWithContext(f, rows, StructListParentDomain{}, check)
	}
	if fieldHasStructLists(f, 0) {
		return b.Structs[f.Name].ValidateStructListsWithContext(f, rows, check)
	}
	return validateRunFieldContext(b, f, rows, check)
}
func structListRunSize(v *StructListVector, f Field, rows int, check func() error) (int64, error) {
	p, e := validitySize(v.Validity, rows)
	if e != nil {
		return 0, e
	}
	item, e := validitySize(v.Values.Validity, v.Values.Rows)
	if e != nil {
		return 0, e
	}
	total, e := addRunSizes(p, 4, int64(len(v.Offsets))*4, item)
	if e != nil {
		return 0, e
	}
	for _, c := range f.Children[0].Children {
		n, err := runFieldSizeContext(v.Values.Values, c, v.Values.Rows, check)
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
func encodeStructListRun(w *runWriter, v *StructListVector, f Field, rows int) error {
	encodeValidity(w, v.Validity, rows)
	w.u32(uint32(len(v.Offsets)))
	encodeOffsets(w, v.Offsets, 0)
	encodeValidity(w, v.Values.Validity, v.Values.Rows)
	for _, c := range f.Children[0].Children {
		if w.err != nil {
			return w.err
		}
		if err := encodeRunField(w, v.Values.Values, c, v.Values.Rows); err != nil {
			return err
		}
	}
	return w.err
}
func decodeStructListRun(r *runReader, b *Batch, f Field, rows int) error {
	v := &StructListVector{Encoding: EncodingView}
	b.StructLists[f.Name] = v
	v.Validity = decodeValidity(r, rows)
	v.Offsets = r.offsets(r.u32(), rows+1)
	if r.err != nil {
		return r.err
	}
	if len(v.Offsets) == 0 || v.Offsets[0] != 0 {
		r.fail("StructList offsets must start at zero")
		return r.err
	}
	for i := 1; i < len(v.Offsets); i++ {
		if err := recursiveCheck(r.check); err != nil {
			return err
		}
		if v.Offsets[i] < v.Offsets[i-1] {
			r.fail("StructList offsets decrease")
			return r.err
		}
	}
	n := uint64(v.Offsets[len(v.Offsets)-1])
	if n >= uint64(math.MaxInt-63) {
		r.fail("StructList element count exceeds host range")
		return r.err
	}
	// Every item has at least one fixed-width scalar or one String offset.
	// Bound row-dependent allocations by remaining input before constructing them.
	if !r.requireArray(int(n), 1, "StructList elements exceed remaining payload") {
		return r.err
	}
	item := f.Children[0]
	v.Values = &StructVector{Rows: int(n), Encoding: EncodingView, Fields: make([]Field, len(item.Children)), Values: newColumnarDecodeBatch(item.Children, int(n))}
	copy(v.Values.Fields, item.Children)
	v.Values.Validity = decodeValidity(r, int(n))
	for _, c := range item.Children {
		if r.err != nil {
			return r.err
		}
		if err := decodeRunField(r, v.Values.Values, c, int(n)); err != nil {
			return err
		}
	}
	return r.err
}
func structListDecodeMetadata(f Field, check func() error) (int64, error) {
	if err := checkListStructField(f, check); err != nil {
		return 0, err
	}
	n, err := columnarDecodeBatchBytesContext(f.Children[0].Children, true, check)
	if err != nil {
		return 0, err
	}
	fields, err := columnarDecodeMul(int64(len(f.Children[0].Children)), int64(unsafe.Sizeof(Field{})))
	if err != nil {
		return 0, err
	}
	fields, err = columnarDecodeRounded(fields)
	if err != nil {
		return 0, err
	}
	desc, err := columnarDecodeRounded(int64(unsafe.Sizeof(StructVector{})))
	if err != nil {
		return 0, err
	}
	return addRunSizes(n, fields, desc)
}

func selectionStructListRunSize(b *Batch, f Field, rows []int, parent columnarSelectionDomain) (int64, error) {
	v := b.StructLists[f.Name]
	d := StructListParentDomain{rows: b.Rows, count: parent.count}
	for i := 0; i < parent.count; i++ {
		d.validity[i] = parent.vectors[i].Validity
	}
	if err := v.validateStructure(f, b.Rows, d, parent.check); err != nil {
		return 0, err
	}
	for _, r := range rows {
		if err := v.validateParent(f, d, r, parent.check); err != nil {
			return 0, err
		}
	}
	n, err := structListSelectedElements(v, d, len(rows), func(i int) int { return rows[i] }, parent.check)
	if err != nil {
		return 0, err
	}
	pv, err := selectionValidityBytes(int64(len(rows)))
	if err != nil {
		return 0, err
	}
	ev, err := selectionValidityBytes(int64(n))
	if err != nil {
		return 0, err
	}
	total, err := addRunSizes(pv, 4, (int64(len(rows))+1)*4, ev)
	if err != nil {
		return 0, err
	}
	for _, c := range f.Children[0].Children {
		var bytes int64
		if c.Type == TypeString {
			data, e := structListStringBytes(v, d, c, len(rows), func(i int) int { return rows[i] }, parent.check)
			if e != nil {
				return 0, e
			}
			bytes, e = addRunSizes(ev, 4, (int64(n)+1)*4, 8, int64(data))
			if e != nil {
				return 0, e
			}
		} else {
			bytes, err = addRunSizes(ev, int64(n)*structListFixedWidth(c.Type))
			if err != nil {
				return 0, err
			}
		}
		total, err = addRunSizes(total, bytes)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}
