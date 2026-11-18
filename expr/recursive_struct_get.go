package expr

import (
	"context"
	"gocolumn/memory"
	"time"
)

type recursiveCopyContext struct{ check func() error }

// Deadline leaves timing to the workspace cancellation callback.
func (c recursiveCopyContext) Deadline() (time.Time, bool) { return time.Time{}, false }

// Done is nil; this adapter uses synchronous Err checkpoints.
func (c recursiveCopyContext) Done() <-chan struct{} { return nil }

// Err delegates to the workspace cancellation callback.
func (c recursiveCopyContext) Err() error {
	if c.check != nil {
		return c.check()
	}
	return nil
}

// Value exposes no context values.
func (c recursiveCopyContext) Value(any) any { return nil }

type recursiveCopyManager struct {
	acquire func(int64) (memory.Reservation, error)
}

// Reserve delegates admission to the workspace owner.
func (m recursiveCopyManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.acquire != nil {
		return m.acquire(n)
	}
	return scalarNoopReservation{}, nil
}

// Used is not tracked by this callback adapter.
func (recursiveCopyManager) Used() int64 { return 0 }

// Limit is enforced by the underlying workspace admission owner.
func (recursiveCopyManager) Limit() int64 { return 0 }

type recursiveStructLease struct{ owned *memory.OwnedRecursiveStruct }

// Bytes returns the retained admission charge.
func (l *recursiveStructLease) Bytes() int64 { return l.owned.Bytes }

// Release idempotently releases the retained owned reservations.
func (l *recursiveStructLease) Release() { l.owned.Close() }

func evalRecursiveStructGet(out *scalarRegister, child *memory.StructVector, field memory.Field, rows int, effective memory.Bitmap) error {
	// The whole input tree has already been validated on the active domain.
	// Reuse the admitted effective bitmap as the selected child's visible mask.
	for row := 0; row < rows; row++ {
		if out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		if !child.IsValid(row) {
			effective[row/64] &^= uint64(1) << uint(row%64)
		}
	}
	view := *child
	view.Validity = effective
	field.Nullable = true
	clone := memory.CloneRecursiveStructRangeOwned
	if (memory.Schema{Fields: []memory.Field{field}}).HasStructLists() {
		clone = memory.CloneStructListsStructRangeOwned
	}
	if (memory.Schema{Fields: []memory.Field{field}}).HasNestedLists() {
		clone = memory.CloneNestedListsStructRangeOwned
	}
	if field.Name == "" {
		field.Name = "expression"
	}
	owned, err := clone(recursiveCopyContext{out.checkContext}, recursiveCopyManager{out.acquire}, &view, field, 0, rows)
	if err != nil {
		return err
	}
	vector := owned.Vector
	if vector.Validity == nil && rows > 0 {
		vector.Validity = make(memory.Bitmap, memory.WordsFor(rows))
		for row := 0; row < rows; row++ {
			memory.Set(vector.Validity, row)
		}
	}
	if out.checkContext != nil {
		if err := out.checkContext(); err != nil {
			owned.Close()
			return err
		}
	}
	previous := out.structGetReservation
	out.strct = vector
	out.ownedValidity = vector.Validity
	out.validity = vector.Validity
	out.structGetReservation = &recursiveStructLease{owned}
	out.structGetBytes = owned.Bytes
	if previous != nil {
		previous.Release()
	}
	return nil
}
