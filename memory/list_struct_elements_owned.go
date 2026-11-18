package memory

import (
	"context"
	"fmt"
	"math"
)

// TakeNullableStructListElementsOwned copies flat item rows selected by index.
// A -1 index emits a NULL record. It is an owned expression helper, not permission
// to execute nested expressions with a selected input batch.
func TakeNullableStructListElementsOwned(ctx context.Context, m Manager, src *StructVector, item Field, indices []int) (*OwnedRecursiveStruct, error) {
	if ctx == nil || m == nil || src == nil || src.Values == nil || !structListCountOK(len(indices)) {
		return nil, fmt.Errorf("invalid StructList element selection")
	}
	f := Field{Name: "elements", Type: TypeList, Encoding: EncodingView, Children: []Field{item}}
	f.Children[0].Name = "item"
	if err := checkListStructField(f, ctx.Err); err != nil {
		return nil, err
	}
	if src.Rows != src.Values.Rows || len(src.Fields) != len(item.Children) || !structValidityFits(src.Validity, src.Rows) {
		return nil, fmt.Errorf("element Struct shape mismatch")
	}
	b := src.Values
	physicalFields := len(b.Int8s) + len(b.Int16s) + len(b.Int32s) + len(b.Int64s) + len(b.Uint8s) + len(b.Uint16s) + len(b.Uint32s) + len(b.Uint64s) + len(b.Float32s) + len(b.Float64s) + len(b.Strings)
	if physicalFields != len(item.Children) || !structListEncoding(src.Encoding) {
		return nil, fmt.Errorf("undeclared or invalid element storage")
	}
	for i, c := range item.Children {
		eq, err := recursiveFieldEqual(c, src.Fields[i], 0, ctx.Err)
		if err != nil {
			return nil, err
		}
		if !eq {
			return nil, fmt.Errorf("element metadata mismatch")
		}
		if err = validateStructChildStorageContext(src.Values, c, src.Rows, ctx.Err); err != nil {
			return nil, err
		}
	}
	for _, r := range indices {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if r < -1 || r >= src.Rows {
			return nil, fmt.Errorf("element index out of range")
		}
		if r >= 0 && src.IsValid(r) {
			for _, c := range item.Children {
				if !c.Nullable && !batchFieldValid(src.Values, c, r) {
					return nil, fmt.Errorf("required element child is NULL")
				}
			}
		}
	}
	if len(src.Values.StructLists)+len(src.Values.Structs)+len(src.Values.StringLists)+len(src.Values.Int64Lists)+len(src.Values.DictStrings)+len(src.Values.Opaque)+len(src.Values.Decimals) != 0 {
		return nil, fmt.Errorf("item Struct must have flat native storage")
	}
	n := len(indices)
	bytes := int64(8192)
	add := func(k, w int64) error { var e error; bytes, e = ownedRangeAdd(bytes, k, w); return e }
	if err := add(int64(WordsFor(n)), 8); err != nil {
		return nil, err
	}
	stringBytes := func(c Field) (int, error) {
		total := uint64(0)
		v := src.Values.Strings[c.Name]
		if v == nil {
			return 0, fmt.Errorf("item String requires view")
		}
		for _, r := range indices {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if r >= 0 && src.IsValid(r) && v.IsValid(r) {
				total += uint64(len(v.ValueBytes(r)))
				if total > math.MaxUint32 || total > uint64(int(^uint(0)>>1)) {
					return 0, fmt.Errorf("element String bytes overflow")
				}
			}
		}
		return int(total), nil
	}
	for _, c := range item.Children {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := add(2048, 1); err != nil {
			return nil, err
		}
		if err := add(int64(WordsFor(n)), 8); err != nil {
			return nil, err
		}
		if c.Type == TypeString {
			k, e := stringBytes(c)
			if e != nil {
				return nil, e
			}
			if e = add(int64(n)+1, 4); e != nil {
				return nil, e
			}
			if e = add(int64(k), 1); e != nil {
				return nil, e
			}
		} else {
			if e := add(int64(n), structListFixedWidth(c.Type)); e != nil {
				return nil, e
			}
		}
	}
	lease, err := m.Reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			lease.Release()
		}
	}()
	snapshot, err := cloneRecursiveFieldContext(item, ctx.Err)
	if err != nil {
		return nil, err
	}
	out := &StructVector{Fields: snapshot.Children, Values: NewBatch(), Validity: make(Bitmap, WordsFor(n)), Encoding: EncodingView}
	for _, c := range item.Children {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if c.Type == TypeString {
			k, e := stringBytes(c)
			if e != nil {
				return nil, e
			}
			out.Values.Strings[c.Name] = &StringViewVector{Offsets: make([]uint32, 1, n+1), Data: make([]byte, 0, k), Validity: make(Bitmap, WordsFor(n)), Encoding: EncodingView}
		} else {
			allocateStructListFixed(out.Values, c, n)
		}
	}
	for _, r := range indices {
		if err = appendRecursiveOwnedRow(out, src, item, r, r >= 0, ctx.Err); err != nil {
			return nil, err
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	ok = true
	return &OwnedRecursiveStruct{Vector: out, Bytes: bytes, lease: lease}, nil
}
