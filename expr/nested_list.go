package expr

import (
	"fmt"
	"gocolumn/memory"
)

func validateExpressionContainers(f memory.Field) error {
	if f.Name == "" {
		f.Name = "expression"
	}
	return f.ValidateArrayContainers()
}

type nestedArrayLease struct{ owned *memory.OwnedNestedList }

// Bytes returns the reservation charge of the owned nested List.
func (l *nestedArrayLease) Bytes() int64 { return l.owned.Bytes }

// Release closes the owned nested List and releases its reservation.
func (l *nestedArrayLease) Release() { l.owned.Close() }

type innerArrayLease struct{ owned *memory.OwnedArray }

// Bytes returns the reservation charge of the owned inner array.
func (l *innerArrayLease) Bytes() int64 { return l.owned.Bytes }

// Release closes the owned inner array and releases its reservation.
func (l *innerArrayLease) Release() { l.owned.Close() }

func nestedListEffective(out, in *scalarRegister, f memory.Field, rows int, active memory.Bitmap) (memory.Bitmap, memory.Reservation, error) {
	if in.array == nil || in.array.Len() != rows || rows < 0 {
		return nil, nil, fmt.Errorf("native array rows mismatch")
	}
	if in.validity != nil && len(in.validity) < memory.WordsFor(rows) || active != nil && len(active) < memory.WordsFor(rows) {
		return nil, nil, fmt.Errorf("short array mask")
	}
	ctx := recursiveCopyContext{out.checkContext}
	lease, err := (recursiveCopyManager{out.acquire}).Reserve(ctx, 1024+int64(memory.WordsFor(rows))*8)
	if err != nil {
		return nil, nil, err
	}
	effective := make(memory.Bitmap, memory.WordsFor(rows))
	for r := 0; r < rows; r++ {
		if err = ctx.Err(); err != nil {
			lease.Release()
			return nil, nil, err
		}
		if active != nil && !memory.IsSet(active, r) {
			continue
		}
		if !in.array.IsValid(r) {
			if !f.Nullable {
				lease.Release()
				return nil, nil, fmt.Errorf("required array List is NULL")
			}
			continue
		}
		if isValidRegister(in, r) {
			memory.Set(effective, r)
		}
	}
	v := *in.array
	v.Validity = effective
	f.Nullable = true
	if f.Name == "" {
		f.Name = "expression"
	}
	if err = v.ValidateWithContext(f, rows, memory.NestedListParentDomain{}, out.checkContext); err != nil {
		lease.Release()
		return nil, nil, err
	}
	return effective, lease, nil
}
func evalNestedListLength(out, in *scalarRegister, f memory.Field, rows int, active memory.Bitmap) error {
	bits, lease, err := nestedListEffective(out, in, f, rows, active)
	if err != nil {
		return err
	}
	defer lease.Release()
	copyRegisterValidity(out, in, rows)
	values := ensureI64(out, rows)
	for r := range values {
		if out.checkContext != nil {
			if err = out.checkContext(); err != nil {
				return err
			}
		}
		values[r] = 0
		if memory.IsSet(bits, r) {
			values[r] = int64(in.array.Offsets[r+1] - in.array.Offsets[r])
		}
	}
	return nil
}
func evalNestedListGet(out, in, index *scalarRegister, f memory.Field, rows int, active memory.Bitmap) error {
	bits, lease, err := nestedListEffective(out, in, f, rows, active)
	if err != nil {
		return err
	}
	defer lease.Release()
	if err = validateArrayIndexStorage(index, rows); err != nil {
		return err
	}
	if index.validity != nil && len(index.validity) < memory.WordsFor(rows) {
		return fmt.Errorf("short array index mask")
	}
	ctx := recursiveCopyContext{out.checkContext}
	m := recursiveCopyManager{out.acquire}
	scratch, err := m.Reserve(ctx, 1024+int64(rows)*8)
	if err != nil {
		return err
	}
	defer scratch.Release()
	indices := make([]int, rows)
	for r := range indices {
		if err = ctx.Err(); err != nil {
			return err
		}
		indices[r] = -1
		if !memory.IsSet(bits, r) {
			continue
		}
		lo, hi := in.array.Offsets[r], in.array.Offsets[r+1]
		n, ok, e := listGetIndex(index, r, uint64(hi-lo))
		if e != nil {
			return e
		}
		if ok {
			indices[r] = int(uint64(lo) + n)
		}
	}
	child := f.Children[0]
	child.Nullable = true
	owned, err := memory.TakeNullableArrayRowsOwned(ctx, m, in.array.Child, child, indices)
	if err != nil {
		return err
	}
	previous := out.structGetReservation
	node := owned.Vector
	out.array = nil
	out.structList = nil
	out.intList = nil
	out.strList = nil
	if node.Child.Kind == memory.TypeInt64 {
		out.intList = &memory.Int64ListVector{Offsets: node.Offsets, Validity: node.Validity, Values: node.Child.Int64, Encoding: memory.EncodingView}
	} else {
		out.strList = &memory.StringListVector{Offsets: node.Offsets, Validity: node.Validity, Values: node.Child.Strings, Encoding: memory.EncodingView}
	}
	out.ownedValidity = node.Validity
	out.validity = node.Validity
	out.structGetBytes = owned.Bytes
	out.structGetReservation = &innerArrayLease{owned}
	if previous != nil {
		previous.Release()
	}
	return nil
}
func evalNestedListChild(out *scalarRegister, child *memory.ArrayNode, f memory.Field, rows int, effective memory.Bitmap) error {
	domain, err := memory.NewNestedListParentDomain(rows, effective)
	if err != nil {
		return err
	}
	if f.Name == "" {
		f.Name = "expression"
	}
	owned, err := memory.CloneNestedListRangeOwned(recursiveCopyContext{out.checkContext}, recursiveCopyManager{out.acquire}, child, f, domain, 0, rows)
	if err != nil {
		return err
	}
	previous := out.structGetReservation
	out.array = owned.Vector
	out.structList = nil
	out.strList = nil
	out.intList = nil
	out.ownedValidity = owned.Vector.Validity
	out.validity = owned.Vector.Validity
	out.structGetBytes = owned.Bytes
	out.structGetReservation = &nestedArrayLease{owned}
	if previous != nil {
		previous.Release()
	}
	return nil
}

func validateArrayIndexStorage(r *scalarRegister, rows int) error {
	if r == nil {
		return fmt.Errorf("missing array index")
	}
	if r.constant {
		return nil
	}
	n := -1
	switch r.typ {
	case memory.TypeInt8:
		n = len(r.i8)
	case memory.TypeInt16:
		n = len(r.i16)
	case memory.TypeInt32:
		n = len(r.i32)
	case memory.TypeInt64:
		n = len(r.i64)
	case memory.TypeUint8:
		n = len(r.u8)
	case memory.TypeUint16:
		n = len(r.u16)
	case memory.TypeUint32:
		n = len(r.u32)
	case memory.TypeUint64:
		n = len(r.u64)
	}
	if n < rows {
		return fmt.Errorf("array index storage shorter than logical rows")
	}
	return nil
}
