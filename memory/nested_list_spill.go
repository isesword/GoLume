package memory

import (
	"math"
	"unsafe"
)

// arrayRunSize sizes validated canonical nodes without revalidating descendants
// in the wrong parent coordinate domain.
func arrayRunSize(v *ArrayNode, check func() error) (int64, error) {
	if err := recursiveCheck(check); err != nil {
		return 0, err
	}
	var validity Bitmap
	switch v.Kind {
	case TypeList:
		validity = v.Validity
	case TypeInt64:
		validity = v.Int64.Validity
	case TypeString:
		validity = v.Strings.Validity
	}
	n, err := validitySize(validity, v.Len())
	if err != nil {
		return 0, err
	}
	switch v.Kind {
	case TypeList:
		child, err := arrayRunSize(v.Child, check)
		if err != nil {
			return 0, err
		}
		return addRunSizes(n, 8, 4*(int64(v.Len())+1), child)
	case TypeInt64:
		return addRunSizes(n, 8*int64(v.Len()))
	default:
		return addRunSizes(n, 4, 4*(int64(v.Len())+1), 8, int64(len(v.Strings.Data)))
	}
}
func encodeArrayRun(w *runWriter, v *ArrayNode) error {
	if err := recursiveCheck(w.check); err != nil {
		return err
	}
	switch v.Kind {
	case TypeList:
		encodeValidity(w, v.Validity, v.Len())
		w.u32(uint32(v.Child.Len()))
		w.u32(uint32(len(v.Offsets)))
		encodeOffsets(w, v.Offsets, 0)
		if w.err != nil {
			return w.err
		}
		return encodeArrayRun(w, v.Child)
	case TypeInt64:
		encodeValidity(w, v.Int64.Validity, v.Len())
		for _, x := range v.Int64.Values {
			w.u64(uint64(x))
			if w.err != nil {
				return w.err
			}
		}
	case TypeString:
		encodeValidity(w, v.Strings.Validity, v.Len())
		w.u32(uint32(len(v.Strings.Offsets)))
		encodeNormalizedString(w, v.Strings.Offsets, v.Strings.Data)
	}
	return w.err
}
func decodeArrayRun(r *runReader, f Field, rows int) (*ArrayNode, error) {
	if err := recursiveCheck(r.check); err != nil {
		return nil, err
	}
	v := &ArrayNode{Kind: f.Type, Encoding: EncodingView}
	mask := decodeValidity(r, rows)
	if r.err != nil {
		return nil, r.err
	}
	switch f.Type {
	case TypeList:
		v.Validity = mask
		n := uint64(r.u32())
		v.Offsets = r.offsets(r.u32(), rows+1)
		if r.err != nil {
			return nil, r.err
		}
		if n >= uint64(math.MaxInt-63) || len(v.Offsets) == 0 || v.Offsets[0] != 0 || uint64(v.Offsets[rows]) != n {
			r.fail("array child count or offsets mismatch")
			return nil, r.err
		}
		width := 4
		if f.Children[0].Type == TypeInt64 {
			width = 8
		}
		if !r.requireArray(int(n), width, "array child rows exceed remaining payload") {
			return nil, r.err
		}
		child, err := decodeArrayRun(r, f.Children[0], int(n))
		if err != nil {
			return nil, err
		}
		v.Child = child
	case TypeInt64:
		if !r.requireArray(rows, 8, "array Int64 values exceed remaining payload") {
			return nil, r.err
		}
		x := &Int64Vector{Validity: mask, Values: make([]int64, rows)}
		v.Int64 = x
		for i := range x.Values {
			x.Values[i] = int64(r.u64())
			if r.err != nil {
				return nil, r.err
			}
		}
	case TypeString:
		x := &StringViewVector{Validity: mask}
		v.Strings = x
		x.Offsets = r.offsets(r.u32(), rows+1)
		x.Data = r.data()
		if r.err != nil {
			return nil, r.err
		}
		if len(x.Offsets) == 0 || x.Offsets[0] != 0 || uint64(x.Offsets[rows]) != uint64(len(x.Data)) {
			r.fail("array String terminal mismatch")
			return nil, r.err
		}
	}
	return v, r.err
}
func arrayDecodeMetadata(f Field) (int64, error) {
	if err := f.ValidateNestedList(); err != nil {
		return 0, err
	}
	node, err := columnarDecodeRounded(int64(unsafe.Sizeof(ArrayNode{})))
	if err != nil {
		return 0, err
	}
	var leaf int64
	if f.Children[0].Children[0].Type == TypeInt64 {
		leaf = int64(unsafe.Sizeof(Int64Vector{}))
	} else {
		leaf = int64(unsafe.Sizeof(StringViewVector{}))
	}
	leaf, err = columnarDecodeRounded(leaf)
	if err != nil {
		return 0, err
	}
	// The root descriptor is already charged by the map classifier.
	// Each child node is allocated separately and must be rounded separately.
	return addRunSizes(node, node, leaf)
}
func clearDecodedArray(v *ArrayNode) {
	if v == nil {
		return
	}
	clearDecodedArray(v.Child)
	if v.Int64 != nil {
		*v.Int64 = Int64Vector{}
	}
	if v.Strings != nil {
		*v.Strings = StringViewVector{}
	}
	*v = ArrayNode{}
}

func selectionArrayRunSize(b *Batch, f Field, rows []int, parent columnarSelectionDomain) (int64, error) {
	v := b.Arrays[f.Name]
	d := NestedListParentDomain{rows: b.Rows, count: parent.count}
	for i := 0; i < parent.count; i++ {
		d.validity[i] = parent.vectors[i].Validity
	}
	if err := v.validateNestedStructure(f, b.Rows, d, parent.check); err != nil {
		return 0, err
	}
	ri, re, data := 0, 0, 0
	for _, r := range rows {
		if err := v.validateNestedParent(f, d, r, parent.check); err != nil {
			return 0, err
		}
		if !d.visible(r) || !v.IsValid(r) {
			continue
		}
		var err error
		ri, err = nestedListAddCount(ri, int(v.Offsets[r+1]-v.Offsets[r]))
		if err != nil {
			return 0, err
		}
		for i := int(v.Offsets[r]); i < int(v.Offsets[r+1]); i++ {
			if err := recursiveCheck(parent.check); err != nil {
				return 0, err
			}
			inner := v.Child
			if !inner.IsValid(i) {
				continue
			}
			re, err = nestedListAddCount(re, int(inner.Offsets[i+1]-inner.Offsets[i]))
			if err != nil {
				return 0, err
			}
			leaf := inner.Child
			if leaf.Kind == TypeString {
				for j := int(inner.Offsets[i]); j < int(inner.Offsets[i+1]); j++ {
					if err := recursiveCheck(parent.check); err != nil {
						return 0, err
					}
					if leaf.IsValid(j) {
						data, err = nestedListAddBytes(data, int(leaf.Strings.Offsets[j+1]-leaf.Strings.Offsets[j]))
						if err != nil {
							return 0, err
						}
					}
				}
			}
		}
	}
	pv, err := selectionValidityBytes(int64(len(rows)))
	if err != nil {
		return 0, err
	}
	iv, err := selectionValidityBytes(int64(ri))
	if err != nil {
		return 0, err
	}
	ev, err := selectionValidityBytes(int64(re))
	if err != nil {
		return 0, err
	}
	total, err := addRunSizes(pv, 8, 4*(int64(len(rows))+1), iv, 8, 4*(int64(ri)+1), ev)
	if err != nil {
		return 0, err
	}
	if v.Child.Child.Kind == TypeInt64 {
		return addRunSizes(total, 8*int64(re))
	}
	return addRunSizes(total, 4, 4*(int64(re)+1), 8, int64(data))
}
