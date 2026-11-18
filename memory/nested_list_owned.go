package memory

import (
	"context"
	"fmt"
	"math"
)

// OwnedNestedList owns a canonical independent array tree and its reservation.
// Bytes includes exact payload capacities and conservative metadata, not process RSS.
type OwnedNestedList struct {
	Vector *ArrayNode
	Field  Field
	Bytes  int64
	lease  Reservation
}

// Close releases this owner and invalidates Vector and Field; it is idempotent.
func (o *OwnedNestedList) Close() {
	if o == nil {
		return
	}
	o.Vector = nil
	o.Field = Field{}
	if o.lease != nil {
		o.lease.Release()
		o.lease = nil
	}
}

// CloneNestedListRangeOwned copies a parent range after checking all structure
// and only the selected values. Hidden NULL spans are canonicalized away.
func CloneNestedListRangeOwned(ctx context.Context, manager Manager, src *ArrayNode, f Field, d NestedListParentDomain, offset, length int) (*OwnedNestedList, error) {
	if src == nil || offset < 0 || length < 0 || offset > src.Len() || length > src.Len()-offset {
		return nil, fmt.Errorf("nested List range out of bounds")
	}
	return copyNestedListOwned(ctx, manager, src, f, d, length, func(i int) int { return offset + i })
}

// TakeNestedListRowsOwned copies reordered/repeated parent rows. Both List
// offsets are remapped; ancestor validity is indexed only by original parent rows.
func TakeNestedListRowsOwned(ctx context.Context, manager Manager, src *ArrayNode, f Field, d NestedListParentDomain, rows []int) (*OwnedNestedList, error) {
	return copyNestedListOwned(ctx, manager, src, f, d, len(rows), func(i int) int { return rows[i] })
}

func nestedListCountOK(n int) bool { return n >= 0 && n < math.MaxInt-63 && uint64(n) < math.MaxUint32 }
func nestedListAddCount(a, b int) (int, error) {
	if !nestedListCountOK(a) || b < 0 || b > math.MaxInt-a || !nestedListCountOK(a+b) {
		return 0, fmt.Errorf("nested List count overflow")
	}
	return a + b, nil
}
func nestedListAddBytes(a, b int) (int, error) {
	if a < 0 || b < 0 || b > math.MaxInt-a || uint64(a)+uint64(b) > math.MaxUint32 {
		return 0, fmt.Errorf("nested List String bytes overflow")
	}
	return a + b, nil
}
func nestedListOwnedCharge(f Field, p, i, e, data int) (int64, error) {
	if !nestedListCountOK(p) || !nestedListCountOK(i) || !nestedListCountOK(e) || data < 0 || uint64(data) > math.MaxUint32 {
		return 0, fmt.Errorf("nested List owned size overflow")
	}
	total := int64(3*4096 + 2*1024)
	add := func(n int64) error {
		if n < 0 || total > math.MaxInt64-n {
			return fmt.Errorf("nested List owned bytes overflow")
		}
		total += n
		return nil
	}
	// Field strings are immutable shared references, not copied byte payloads.
	// Charge their visible lengths conservatively for this owner's retained metadata.
	node := f
	for j := 0; j < 3; j++ {
		for _, s := range []string{node.Name, node.ArrowType, node.DictionaryDomain} {
			if err := add(int64(len(s))); err != nil {
				return 0, err
			}
		}
		if j < 2 {
			node = node.Children[0]
		}
	}
	for _, n := range []int{p, i} {
		if err := add((int64(n)+1)*4 + int64(WordsFor(n))*8); err != nil {
			return 0, err
		}
	}
	if err := add(int64(WordsFor(e)) * 8); err != nil {
		return 0, err
	}
	if node.Type == TypeInt64 {
		if err := add(int64(e) * 8); err != nil {
			return 0, err
		}
	} else {
		if err := add((int64(e)+1)*4 + int64(data)); err != nil {
			return 0, err
		}
	}
	return total, nil
}
func copyNestedListOwned(ctx context.Context, manager Manager, src *ArrayNode, f Field, d NestedListParentDomain, n int, row func(int) int) (*OwnedNestedList, error) {
	if ctx == nil || manager == nil || src == nil || !nestedListCountOK(n) {
		return nil, fmt.Errorf("nested List copy requires context, manager and bounded rows")
	}
	if err := src.validateNestedStructure(f, src.Len(), d, ctx.Err); err != nil {
		return nil, err
	}
	inner, leaf := src.Child, src.Child.Child
	ri, re, bytes := 0, 0, 0
	for k := 0; k < n; k++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r := row(k)
		if r < 0 || r >= src.Len() {
			return nil, fmt.Errorf("nested List selected row out of bounds")
		}
		if err := src.validateNestedParent(f, d, r, ctx.Err); err != nil {
			return nil, err
		}
		if !d.visible(r) || !src.IsValid(r) {
			continue
		}
		var err error
		ri, err = nestedListAddCount(ri, int(src.Offsets[r+1]-src.Offsets[r]))
		if err != nil {
			return nil, err
		}
		for i := int(src.Offsets[r]); i < int(src.Offsets[r+1]); i++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if !inner.IsValid(i) {
				continue
			}
			re, err = nestedListAddCount(re, int(inner.Offsets[i+1]-inner.Offsets[i]))
			if err != nil {
				return nil, err
			}
			if leaf.Kind == TypeString {
				for e := int(inner.Offsets[i]); e < int(inner.Offsets[i+1]); e++ {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					if leaf.IsValid(e) {
						bytes, err = nestedListAddBytes(bytes, int(leaf.Strings.Offsets[e+1]-leaf.Strings.Offsets[e]))
						if err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}
	charge, err := nestedListOwnedCharge(f, n, ri, re, bytes)
	if err != nil {
		return nil, err
	}
	lease, err := manager.Reserve(ctx, charge)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			lease.Release()
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	field, err := cloneRecursiveFieldContext(f, ctx.Err)
	if err != nil {
		return nil, err
	}
	if d.count > 0 {
		field.Nullable = true
	}
	out := &ArrayNode{Kind: TypeList, Offsets: make([]uint32, n+1), Validity: make(Bitmap, WordsFor(n)), Encoding: EncodingView}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	oi := &ArrayNode{Kind: TypeList, Offsets: make([]uint32, ri+1), Validity: make(Bitmap, WordsFor(ri)), Encoding: EncodingView}
	out.Child = oi
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	ol := &ArrayNode{Kind: leaf.Kind, Encoding: EncodingView}
	oi.Child = ol
	if leaf.Kind == TypeInt64 {
		ol.Int64 = &Int64Vector{Values: make([]int64, re), Validity: make(Bitmap, WordsFor(re)), Encoding: EncodingView}
	} else {
		ol.Strings = &StringViewVector{Offsets: make([]uint32, re+1), Data: make([]byte, bytes), Validity: make(Bitmap, WordsFor(re)), Encoding: EncodingView}
	}
	di, de, db := 0, 0, 0
	for k := 0; k < n; k++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		r := row(k)
		if d.visible(r) && src.IsValid(r) {
			Set(out.Validity, k)
			for i := int(src.Offsets[r]); i < int(src.Offsets[r+1]); i++ {
				if err = ctx.Err(); err != nil {
					return nil, err
				}
				if inner.IsValid(i) {
					Set(oi.Validity, di)
					for e := int(inner.Offsets[i]); e < int(inner.Offsets[i+1]); e++ {
						if err = ctx.Err(); err != nil {
							return nil, err
						}
						if leaf.IsValid(e) {
							if leaf.Kind == TypeInt64 {
								ol.Int64.Values[de] = leaf.Int64.Values[e]
								Set(ol.Int64.Validity, de)
							} else {
								a, b := leaf.Strings.Offsets[e], leaf.Strings.Offsets[e+1]
								value := leaf.Strings.Data[int(a):int(b)]
								if err = copyNestedListBytes(ctx, ol.Strings.Data[db:db+len(value)], value); err != nil {
									return nil, err
								}
								db += len(value)
								Set(ol.Strings.Validity, de)
							}
						}
						if leaf.Kind == TypeString {
							ol.Strings.Offsets[de+1] = uint32(db)
						}
						de++
					}
				}
				oi.Offsets[di+1] = uint32(de)
				di++
			}
		}
		out.Offsets[k+1] = uint32(di)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if di != ri || de != re || db != bytes {
		return nil, fmt.Errorf("nested List source changed during copy")
	}
	ok = true
	return &OwnedNestedList{Vector: out, Field: field, Bytes: charge, lease: lease}, nil
}
func copyNestedListBytes(ctx context.Context, dst, src []byte) error {
	for len(src) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := len(src)
		if n > 32768 {
			n = 32768
		}
		copy(dst[:n], src[:n])
		dst = dst[n:]
		src = src[n:]
	}
	return nil
}
