package memory

import (
	"context"
	"fmt"
)

// OwnedArray owns a primitive inner List extracted from the checked array profile.
// Its node can be adapted to existing primitive List vectors without copying.
type OwnedArray struct {
	Vector *ArrayNode
	Field  Field
	Bytes  int64
	lease  Reservation
}

// Close releases this owner's reservation and drops owned references once.
func (o *OwnedArray) Close() {
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

// TakeNullableArrayRowsOwned copies primitive inner Lists by row. A -1 index
// emits NULL; all other indices must exist. Storage is checked in full, required
// terminal values only in the selected valid Lists. The result is nullable.
func TakeNullableArrayRowsOwned(ctx context.Context, m Manager, src *ArrayNode, f Field, rows []int) (*OwnedArray, error) {
	if ctx == nil || m == nil || src == nil || !nestedListCountOK(len(rows)) {
		return nil, fmt.Errorf("invalid nullable inner List take")
	}
	if err := f.ValidateList(); err != nil {
		return nil, err
	}
	if len(f.Children) != 1 || (f.Children[0].Type != TypeInt64 && f.Children[0].Type != TypeString) || src.Kind != TypeList || src.Child == nil {
		return nil, fmt.Errorf("expected primitive inner List")
	}
	// A stack-local outer descriptor validates the same storage graph without a
	// temporary element-to-parent index array or full-domain required-value check.
	outerField := Field{Name: "array", Type: TypeList, Nullable: true, Encoding: EncodingView, Children: []Field{f}}
	outerField.Children[0].Name = "item"
	sentinel := [2]uint32{0, uint32(src.Len())}
	if !nestedListCountOK(src.Len()) {
		return nil, fmt.Errorf("inner row count overflow")
	}
	wrapper := ArrayNode{Kind: TypeList, Offsets: sentinel[:], Child: src, Encoding: EncodingView}
	if err := wrapper.validateNestedStructure(outerField, 1, NestedListParentDomain{}, ctx.Err); err != nil {
		return nil, err
	}
	elements, data := 0, 0
	for _, r := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if r == -1 {
			continue
		}
		if r < 0 || r >= src.Len() {
			return nil, fmt.Errorf("inner List selection out of bounds")
		}
		if !src.IsValid(r) {
			continue
		}
		var err error
		elements, err = nestedListAddCount(elements, int(src.Offsets[r+1]-src.Offsets[r]))
		if err != nil {
			return nil, err
		}
		for e := int(src.Offsets[r]); e < int(src.Offsets[r+1]); e++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if !src.Child.IsValid(e) {
				if !f.Children[0].Nullable {
					return nil, fmt.Errorf("required inner element is NULL")
				}
				continue
			}
			if src.Child.Kind == TypeString {
				v := src.Child.Strings
				data, err = nestedListAddBytes(data, int(v.Offsets[e+1]-v.Offsets[e]))
				if err != nil {
					return nil, err
				}
			}
		}
	}
	charge := int64(2*4096 + 1024)
	var err error
	for _, x := range []Field{f, f.Children[0]} {
		for _, text := range []string{x.Name, x.ArrowType, x.DictionaryDomain} {
			charge, err = ownedRangeAdd(charge, int64(len(text)), 1)
			if err != nil {
				return nil, err
			}
		}
	}
	charge, err = ownedRangeAdd(charge, int64(len(rows))+1, 4)
	if err != nil {
		return nil, err
	}
	charge, err = ownedRangeAdd(charge, int64(WordsFor(len(rows))+WordsFor(elements)), 8)
	if err != nil {
		return nil, err
	}
	if src.Child.Kind == TypeInt64 {
		charge, err = ownedRangeAdd(charge, int64(elements), 8)
	} else {
		charge, err = ownedRangeAdd(charge, int64(elements)+1, 4)
		if err == nil {
			charge, err = ownedRangeAdd(charge, int64(data), 1)
		}
	}
	if err != nil {
		return nil, err
	}
	lease, err := m.Reserve(ctx, charge)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			lease.Release()
		}
	}()
	snapshot, err := cloneRecursiveFieldContext(f, ctx.Err)
	if err != nil {
		return nil, err
	}
	snapshot.Nullable = true
	child := &ArrayNode{Kind: src.Child.Kind, Encoding: EncodingView}
	if child.Kind == TypeInt64 {
		child.Int64 = &Int64Vector{Values: make([]int64, elements), Validity: make(Bitmap, WordsFor(elements)), Encoding: EncodingView}
	} else {
		child.Strings = &StringViewVector{Offsets: make([]uint32, elements+1), Data: make([]byte, data), Validity: make(Bitmap, WordsFor(elements)), Encoding: EncodingView}
	}
	out := &ArrayNode{Kind: TypeList, Offsets: make([]uint32, len(rows)+1), Validity: make(Bitmap, WordsFor(len(rows))), Child: child, Encoding: EncodingView}
	at, bytes := 0, 0
	for i, r := range rows {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if r >= 0 && src.IsValid(r) {
			Set(out.Validity, i)
			for e := int(src.Offsets[r]); e < int(src.Offsets[r+1]); e++ {
				if err = ctx.Err(); err != nil {
					return nil, err
				}
				if src.Child.IsValid(e) {
					if child.Kind == TypeInt64 {
						Set(child.Int64.Validity, at)
						child.Int64.Values[at] = src.Child.Int64.Values[e]
					} else {
						Set(child.Strings.Validity, at)
						v := src.Child.Strings
						part := v.Data[v.Offsets[e]:v.Offsets[e+1]]
						if err = copyNestedListBytes(ctx, child.Strings.Data[bytes:bytes+len(part)], part); err != nil {
							return nil, err
						}
						bytes += len(part)
					}
				}
				at++
				if child.Kind == TypeString {
					child.Strings.Offsets[at] = uint32(bytes)
				}
			}
		}
		out.Offsets[i+1] = uint32(at)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if at != elements || bytes != data {
		return nil, fmt.Errorf("inner List changed during copy")
	}
	ok = true
	return &OwnedArray{Vector: out, Field: snapshot, Bytes: charge, lease: lease}, nil
}
