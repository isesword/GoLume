package expr

import (
	"fmt"
	"gocolumn/memory"
)

type structListLease struct{ owned *memory.OwnedStructList }

// Bytes reports the owned admission.
func (l *structListLease) Bytes() int64 { return l.owned.Bytes }

// Release drops the owned list.
func (l *structListLease) Release() { l.owned.Close() }

func structListEffective(out, in *scalarRegister, f memory.Field, rows int, active memory.Bitmap) (memory.Bitmap, memory.Reservation, error) {
	if in.structList == nil || rows < 0 || in.structList.Len() != rows {
		return nil, nil, fmt.Errorf("native StructList rows mismatch")
	}
	if (in.validity != nil && len(in.validity) < memory.WordsFor(rows)) || (active != nil && len(active) < memory.WordsFor(rows)) {
		return nil, nil, fmt.Errorf("short StructList mask")
	}
	bytes := int64(memory.WordsFor(rows))*8 + 1024
	lease, err := (recursiveCopyManager{out.acquire}).Reserve(recursiveCopyContext{out.checkContext}, bytes)
	if err != nil {
		return nil, nil, err
	}
	effective := make(memory.Bitmap, memory.WordsFor(rows))
	for r := 0; r < rows; r++ {
		if out.checkContext != nil {
			if err = out.checkContext(); err != nil {
				lease.Release()
				return nil, nil, err
			}
		}
		if active != nil && !memory.IsSet(active, r) {
			continue
		}
		if !in.structList.IsValid(r) {
			if !f.Nullable {
				lease.Release()
				return nil, nil, fmt.Errorf("required StructList is NULL")
			}
			continue
		}
		if isValidRegister(in, r) {
			memory.Set(effective, r)
		}
	}
	view := *in.structList
	view.Validity = effective
	f.Nullable = true
	if err = view.ValidateWithContext(f, rows, memory.StructListParentDomain{}, out.checkContext); err != nil {
		lease.Release()
		return nil, nil, err
	}
	return effective, lease, nil
}
func evalStructListLength(out, in *scalarRegister, f memory.Field, rows int, active memory.Bitmap) error {
	effective, lease, err := structListEffective(out, in, f, rows, active)
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
		if memory.IsSet(effective, r) {
			values[r] = int64(in.structList.Offsets[r+1]) - int64(in.structList.Offsets[r])
		}
	}
	return nil
}
func evalStructListGet(out, in, index *scalarRegister, f memory.Field, rows int, active memory.Bitmap) error {
	effective, lease, err := structListEffective(out, in, f, rows, active)
	if err != nil {
		return err
	}
	defer lease.Release()
	if index.validity != nil && len(index.validity) < memory.WordsFor(rows) {
		return fmt.Errorf("short StructList index validity")
	}
	size := int64(rows)
	if size < 0 || size > (int64(^uint64(0)>>1)-1024)/8 {
		return fmt.Errorf("StructList indices overflow")
	}
	scratch, err := (recursiveCopyManager{out.acquire}).Reserve(recursiveCopyContext{out.checkContext}, size*8+1024)
	if err != nil {
		return err
	}
	defer scratch.Release()
	indices := make([]int, rows)
	for r := range indices {
		if out.checkContext != nil {
			if err = out.checkContext(); err != nil {
				return err
			}
		}
		indices[r] = -1
		if !memory.IsSet(effective, r) {
			continue
		}
		lo, hi := in.structList.Offsets[r], in.structList.Offsets[r+1]
		n, ok, e := listGetIndex(index, r, uint64(hi-lo))
		if e != nil {
			return e
		}
		if ok {
			indices[r] = int(uint64(lo) + n)
		}
	}
	item := f.Children[0]
	item.Nullable = true
	owned, err := memory.TakeNullableStructListElementsOwned(recursiveCopyContext{out.checkContext}, recursiveCopyManager{out.acquire}, in.structList.Values, item, indices)
	if err != nil {
		return err
	}
	previous := out.structGetReservation
	out.strct = owned.Vector
	out.ownedValidity = owned.Vector.Validity
	out.validity = owned.Vector.Validity
	out.structGetBytes = owned.Bytes
	out.structGetReservation = &recursiveStructLease{owned}
	if previous != nil {
		previous.Release()
	}
	return nil
}
func evalStructListChild(out *scalarRegister, child *memory.StructListVector, f memory.Field, rows int, effective memory.Bitmap) error {
	domain, err := memory.NewStructListParentDomain(rows, effective)
	if err != nil {
		return err
	}
	owned, err := memory.CloneStructListRangeOwned(recursiveCopyContext{out.checkContext}, recursiveCopyManager{out.acquire}, child, f, domain, 0, rows)
	if err != nil {
		return err
	}
	previous := out.structGetReservation
	out.structList = owned.Vector
	out.strList = nil
	out.intList = nil
	out.ownedValidity = owned.Vector.Validity
	out.validity = owned.Vector.Validity
	out.structGetBytes = owned.Bytes
	out.structGetReservation = &structListLease{owned}
	if previous != nil {
		previous.Release()
	}
	return nil
}
