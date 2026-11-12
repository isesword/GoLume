package physical

import (
	"fmt"
	"gocolumn/memory"
)

// topKFixedColumns recognizes payloads whose row storage has a fixed maximum
// width. Snapshots pre-admit one validity word per column even when their first
// row is all-valid, so a later NULL never needs an uncharged buffer expansion.
func topKFixedColumns(b *memory.Batch) (int, bool) {
	if len(b.Strings)+len(b.DictStrings)+len(b.StringLists)+len(b.Int64Lists)+len(b.Structs)+len(b.Opaque) != 0 {
		return 0, false
	}
	return len(b.Int8s) + len(b.Int16s) + len(b.Int32s) + len(b.Int64s) + len(b.Uint8s) + len(b.Uint16s) + len(b.Uint32s) + len(b.Uint64s) + len(b.Float32s) + len(b.Float64s), true
}

// reuseFixed keeps the already admitted maps, vectors, cells and reservation
// when an incoming fixed-width row replaces a winner. It allocates at most one
// validity word per column over the snapshot's entire lifetime, not per row.
// Variable-width, nested and opaque snapshots still use exact row admission.
func (s *topKSnapshotStream) reuseFixed(c *topKSnapshot, row int) (bool, error) {
	dst, src := c.batch, s.input
	if _, fixed := topKFixedColumns(dst); !fixed {
		return false, nil
	}
	if _, fixed := topKFixedColumns(src); !fixed {
		return false, nil
	}
	if len(dst.Int8s) != len(src.Int8s) ||
		len(dst.Int16s) != len(src.Int16s) ||
		len(dst.Int32s) != len(src.Int32s) ||
		len(dst.Int64s) != len(src.Int64s) ||
		len(dst.Uint8s) != len(src.Uint8s) ||
		len(dst.Uint16s) != len(src.Uint16s) ||
		len(dst.Uint32s) != len(src.Uint32s) ||
		len(dst.Uint64s) != len(src.Uint64s) ||
		len(dst.Float32s) != len(src.Float32s) ||
		len(dst.Float64s) != len(src.Float64s) {
		return false, nil
	}
	for name, out := range dst.Int8s {
		in := src.Int8s[name]
		if in == nil {
			return false, nil
		}
		if row >= len(in.Values) || (in.Validity != nil && len(in.Validity) < memory.WordsFor(row+1)) {
			return false, fmt.Errorf("top-k fixed column %q is shorter than row", name)
		}
		out.Values[0] = in.Values[row]
		out.Validity = topKCopyValidity(out.Validity, in.Validity, row)
		out.Encoding = in.Encoding
	}
	for name, out := range dst.Int16s {
		in := src.Int16s[name]
		if in == nil {
			return false, nil
		}
		if row >= len(in.Values) || (in.Validity != nil && len(in.Validity) < memory.WordsFor(row+1)) {
			return false, fmt.Errorf("top-k fixed column %q is shorter than row", name)
		}
		out.Values[0] = in.Values[row]
		out.Validity = topKCopyValidity(out.Validity, in.Validity, row)
		out.Encoding = in.Encoding
	}
	for name, out := range dst.Int32s {
		in := src.Int32s[name]
		if in == nil {
			return false, nil
		}
		if row >= len(in.Values) || (in.Validity != nil && len(in.Validity) < memory.WordsFor(row+1)) {
			return false, fmt.Errorf("top-k fixed column %q is shorter than row", name)
		}
		out.Values[0] = in.Values[row]
		out.Validity = topKCopyValidity(out.Validity, in.Validity, row)
		out.Encoding = in.Encoding
	}
	for name, out := range dst.Int64s {
		in := src.Int64s[name]
		if in == nil {
			return false, nil
		}
		if row >= len(in.Values) || (in.Validity != nil && len(in.Validity) < memory.WordsFor(row+1)) {
			return false, fmt.Errorf("top-k fixed column %q is shorter than row", name)
		}
		out.Values[0] = in.Values[row]
		out.Validity = topKCopyValidity(out.Validity, in.Validity, row)
		out.Encoding = in.Encoding
	}
	for name, out := range dst.Uint8s {
		in := src.Uint8s[name]
		if in == nil {
			return false, nil
		}
		if row >= len(in.Values) || (in.Validity != nil && len(in.Validity) < memory.WordsFor(row+1)) {
			return false, fmt.Errorf("top-k fixed column %q is shorter than row", name)
		}
		out.Values[0] = in.Values[row]
		out.Validity = topKCopyValidity(out.Validity, in.Validity, row)
		out.Encoding = in.Encoding
	}
	for name, out := range dst.Uint16s {
		in := src.Uint16s[name]
		if in == nil {
			return false, nil
		}
		if row >= len(in.Values) || (in.Validity != nil && len(in.Validity) < memory.WordsFor(row+1)) {
			return false, fmt.Errorf("top-k fixed column %q is shorter than row", name)
		}
		out.Values[0] = in.Values[row]
		out.Validity = topKCopyValidity(out.Validity, in.Validity, row)
		out.Encoding = in.Encoding
	}
	for name, out := range dst.Uint32s {
		in := src.Uint32s[name]
		if in == nil {
			return false, nil
		}
		if row >= len(in.Values) || (in.Validity != nil && len(in.Validity) < memory.WordsFor(row+1)) {
			return false, fmt.Errorf("top-k fixed column %q is shorter than row", name)
		}
		out.Values[0] = in.Values[row]
		out.Validity = topKCopyValidity(out.Validity, in.Validity, row)
		out.Encoding = in.Encoding
	}
	for name, out := range dst.Uint64s {
		in := src.Uint64s[name]
		if in == nil {
			return false, nil
		}
		if row >= len(in.Values) || (in.Validity != nil && len(in.Validity) < memory.WordsFor(row+1)) {
			return false, fmt.Errorf("top-k fixed column %q is shorter than row", name)
		}
		out.Values[0] = in.Values[row]
		out.Validity = topKCopyValidity(out.Validity, in.Validity, row)
		out.Encoding = in.Encoding
	}
	for name, out := range dst.Float32s {
		in := src.Float32s[name]
		if in == nil {
			return false, nil
		}
		if row >= len(in.Values) || (in.Validity != nil && len(in.Validity) < memory.WordsFor(row+1)) {
			return false, fmt.Errorf("top-k fixed column %q is shorter than row", name)
		}
		out.Values[0] = in.Values[row]
		out.Validity = topKCopyValidity(out.Validity, in.Validity, row)
		out.Encoding = in.Encoding
	}
	for name, out := range dst.Float64s {
		in := src.Float64s[name]
		if in == nil {
			return false, nil
		}
		if row >= len(in.Values) || (in.Validity != nil && len(in.Validity) < memory.WordsFor(row+1)) {
			return false, fmt.Errorf("top-k fixed column %q is shorter than row", name)
		}
		out.Values[0] = in.Values[row]
		out.Validity = topKCopyValidity(out.Validity, in.Validity, row)
		out.Encoding = in.Encoding
	}
	c.key.ordinal = s.rows
	for i, key := range s.owner.keys {
		cell, err := snapshotCell(dst, s.owner.Schema(), key.Column, 0)
		if err != nil {
			return false, err
		}
		c.key.cells[i] = cell
	}
	return true, nil
}

func topKCopyValidity(dst, src memory.Bitmap, row int) memory.Bitmap {
	if dst == nil && src == nil {
		return nil
	}
	if dst == nil {
		dst = make(memory.Bitmap, 1)
	}
	dst[0] = 0
	if src == nil || memory.IsSet(src, row) {
		dst[0] = 1
	}
	return dst
}
