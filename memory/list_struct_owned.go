package memory

import (
	"context"
	"fmt"
	"math"
)

// OwnedStructList holds an independent canonical vector and its reservation.
// Field describes the output, including ancestor NULLs folded into List validity.
// Source memory and process RSS are not part of Bytes.
type OwnedStructList struct {
	Vector *StructListVector
	Field  Field
	Bytes  int64
	lease  Reservation
}

// Close releases the reservation and invalidates Vector and Field. It is idempotent.
func (o *OwnedStructList) Close() {
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

// CloneStructListRangeOwned validates all input storage and the selected parent
// range's values, then admits all output before copying. Ancestor NULLs are
// folded into parent validity, never indexed using element coordinates.
func CloneStructListRangeOwned(ctx context.Context, manager Manager, src *StructListVector, f Field, d StructListParentDomain, offset, length int) (*OwnedStructList, error) {
	if src == nil || offset < 0 || length < 0 || offset > src.Len() || length > src.Len()-offset {
		return nil, fmt.Errorf("List<Struct> range out of bounds")
	}
	return copyStructListOwned(ctx, manager, src, f, d, length, func(i int) int { return offset + i })
}

// TakeStructListRowsOwned copies an ordered, possibly repeated selection of
// parent rows. Unselected values are not required to be non-NULL, but all storage
// is checked. On failure no output is published and new reservations are released.
func TakeStructListRowsOwned(ctx context.Context, manager Manager, src *StructListVector, f Field, d StructListParentDomain, rows []int) (*OwnedStructList, error) {
	return copyStructListOwned(ctx, manager, src, f, d, len(rows), func(i int) int { return rows[i] })
}
func structListCountOK(n int) bool {
	return n >= 0 && uint64(n) < uint64(math.MaxUint32) && n < int(^uint(0)>>1)-63
}
func structListSelectedElements(src *StructListVector, d StructListParentDomain, n int, row func(int) int, check func() error) (int, error) {
	total := uint64(0)
	for i := 0; i < n; i++ {
		if err := recursiveCheck(check); err != nil {
			return 0, err
		}
		r := row(i)
		if d.visible(r) && src.IsValid(r) {
			total += uint64(src.Offsets[r+1] - src.Offsets[r])
			if total >= math.MaxUint32 || total >= uint64(int(^uint(0)>>1)-63) {
				return 0, fmt.Errorf("List<Struct> element offsets overflow")
			}
		}
	}
	return int(total), nil
}
func structListStringBytes(src *StructListVector, d StructListParentDomain, c Field, n int, row func(int) int, check func() error) (int, error) {
	total := uint64(0)
	v := src.Values.Values.Strings[c.Name]
	for i := 0; i < n; i++ {
		if err := recursiveCheck(check); err != nil {
			return 0, err
		}
		r := row(i)
		if !d.visible(r) || !src.IsValid(r) {
			continue
		}
		for e := int(src.Offsets[r]); e < int(src.Offsets[r+1]); e++ {
			if err := recursiveCheck(check); err != nil {
				return 0, err
			}
			if src.Values.IsValid(e) && v.IsValid(e) {
				total += uint64(v.Offsets[e+1] - v.Offsets[e])
				if total > math.MaxUint32 || total > uint64(int(^uint(0)>>1)) {
					return 0, fmt.Errorf("List<Struct> String offsets overflow")
				}
			}
		}
	}
	return int(total), nil
}
func copyStructListOwned(ctx context.Context, manager Manager, src *StructListVector, f Field, d StructListParentDomain, n int, row func(int) int) (*OwnedStructList, error) {
	if ctx == nil || manager == nil || src == nil || !structListCountOK(n) {
		return nil, fmt.Errorf("List<Struct> copy requires context, manager and bounded rows")
	}
	if err := src.validateStructure(f, src.Len(), d, ctx.Err); err != nil {
		return nil, err
	}
	for i := 0; i < n; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r := row(i)
		if r < 0 || r >= src.Len() {
			return nil, fmt.Errorf("List<Struct> selected row out of bounds")
		}
		if err := src.validateParent(f, d, r, ctx.Err); err != nil {
			return nil, err
		}
	}
	elements, bytes, err := structListOwnedBytes(src, f, d, n, row, ctx.Err)
	if err != nil {
		return nil, err
	}
	lease, err := manager.Reserve(ctx, bytes)
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
	outField, err := cloneRecursiveFieldContext(f, ctx.Err)
	if err != nil {
		return nil, err
	}
	if d.count > 0 {
		outField.Nullable = true
	}
	item := outField.Children[0]
	elementFields := make([]Field, len(item.Children))
	for i, c := range item.Children {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		elementFields[i] = c
	}
	outElements := &StructVector{Fields: elementFields, Values: NewBatch(), Validity: make(Bitmap, WordsFor(elements)), Encoding: EncodingView}
	for _, c := range item.Children {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if c.Type == TypeString {
			size, e := structListStringBytes(src, d, c, n, row, ctx.Err)
			if e != nil {
				return nil, e
			}
			outElements.Values.Strings[c.Name] = &StringViewVector{Offsets: make([]uint32, 1, elements+1), Data: make([]byte, 0, size), Validity: make(Bitmap, WordsFor(elements)), Encoding: EncodingView}
		} else {
			allocateStructListFixed(outElements.Values, c, elements)
		}
	}
	out := &StructListVector{Offsets: make([]uint32, n+1), Validity: make(Bitmap, WordsFor(n)), Values: outElements, Encoding: EncodingView}
	for i := 0; i < n; i++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		r := row(i)
		if d.visible(r) && src.IsValid(r) {
			Set(out.Validity, i)
			for e := int(src.Offsets[r]); e < int(src.Offsets[r+1]); e++ {
				if err = appendRecursiveOwnedRow(outElements, src.Values, item, e, true, ctx.Err); err != nil {
					return nil, err
				}
			}
		}
		out.Offsets[i+1] = uint32(outElements.Rows)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	ok = true
	return &OwnedStructList{Vector: out, Field: outField, Bytes: bytes, lease: lease}, nil
}
func structListFixedWidth(t DataType) int64 {
	switch t {
	case TypeInt8, TypeUint8:
		return 1
	case TypeInt16, TypeUint16:
		return 2
	case TypeInt32, TypeUint32, TypeFloat32, TypeBool, TypeDate32:
		return 4
	default:
		return 8
	}
}

func allocateStructListFixed(b *Batch, f Field, n int) {
	switch f.Type {
	case TypeInt8:
		b.Int8s[f.Name] = &Int8Vector{Values: make([]int8, 0, n), Validity: make(Bitmap, WordsFor(n)), Encoding: f.Encoding}
	case TypeInt16:
		b.Int16s[f.Name] = &Int16Vector{Values: make([]int16, 0, n), Validity: make(Bitmap, WordsFor(n)), Encoding: f.Encoding}
	case TypeInt32, TypeDate32:
		b.Int32s[f.Name] = &Int32Vector{Values: make([]int32, 0, n), Validity: make(Bitmap, WordsFor(n)), Encoding: f.Encoding}
	case TypeInt64, TypeTimestamp:
		b.Int64s[f.Name] = &Int64Vector{Values: make([]int64, 0, n), Validity: make(Bitmap, WordsFor(n)), Encoding: f.Encoding}
	case TypeUint8:
		b.Uint8s[f.Name] = &Uint8Vector{Values: make([]uint8, 0, n), Validity: make(Bitmap, WordsFor(n)), Encoding: f.Encoding}
	case TypeUint16:
		b.Uint16s[f.Name] = &Uint16Vector{Values: make([]uint16, 0, n), Validity: make(Bitmap, WordsFor(n)), Encoding: f.Encoding}
	case TypeUint32, TypeBool:
		b.Uint32s[f.Name] = &Uint32Vector{Values: make([]uint32, 0, n), Validity: make(Bitmap, WordsFor(n)), Encoding: f.Encoding}
	case TypeUint64:
		b.Uint64s[f.Name] = &Uint64Vector{Values: make([]uint64, 0, n), Validity: make(Bitmap, WordsFor(n)), Encoding: f.Encoding}
	case TypeFloat32:
		b.Float32s[f.Name] = &Float32Vector{Values: make([]float32, 0, n), Validity: make(Bitmap, WordsFor(n)), Encoding: f.Encoding}
	case TypeFloat64:
		b.Float64s[f.Name] = &Float64Vector{Values: make([]float64, 0, n), Validity: make(Bitmap, WordsFor(n)), Encoding: f.Encoding}
	}
}

func structListOwnedBytes(src *StructListVector, f Field, d StructListParentDomain, n int, row func(int) int, check func() error) (int, int64, error) {
	elements, err := structListSelectedElements(src, d, n, row, check)
	if err != nil {
		return 0, 0, err
	}
	// Container and metadata envelopes cover maps, vectors, two field snapshots,
	// and the owner. Payload buffers below have exact preallocated capacities.
	bytes := int64(8192)
	add := func(count, width int64) error { var e error; bytes, e = ownedRangeAdd(bytes, count, width); return e }
	if err = add(int64(n)+1, 4); err != nil {
		return 0, 0, err
	}
	if err = add(int64(WordsFor(n)+WordsFor(elements)), 8); err != nil {
		return 0, 0, err
	}
	for _, c := range f.Children[0].Children {
		if err = check(); err != nil {
			return 0, 0, err
		}
		if err = add(1024+int64(len(c.Name))+int64(len(c.ArrowType)), 2); err != nil {
			return 0, 0, err
		}
		if err = add(int64(WordsFor(elements)), 8); err != nil {
			return 0, 0, err
		}
		if c.Type == TypeString {
			size, e := structListStringBytes(src, d, c, n, row, check)
			if e != nil {
				return 0, 0, e
			}
			if err = add(int64(elements)+1, 4); err != nil {
				return 0, 0, err
			}
			if err = add(int64(size), 1); err != nil {
				return 0, 0, err
			}
		} else {
			if err = add(int64(elements), structListFixedWidth(c.Type)); err != nil {
				return 0, 0, err
			}
		}
	}
	return elements, bytes, nil
}
