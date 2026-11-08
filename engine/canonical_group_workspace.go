package engine

import (
	"fmt"
	"math"
	"unsafe"

	"gocolumn/memory"
)

// canonicalGroupWorkspace owns charged descriptor storage reused across batches.
// Its returned bindings borrow components only until the next bind or clear.
type canonicalGroupWorkspace struct {
	binding    canonicalGroupBinding
	components []canonicalGroupComponent
	lease      memory.Reservation
}

func (w *canonicalGroupWorkspace) ensure(fields []memory.Field, reserve func(int64) (memory.Reservation, error)) error {
	if len(fields) <= len(w.components) {
		return nil
	}
	size := uint64(unsafe.Sizeof(canonicalGroupComponent{}))
	if uint64(len(fields)) > uint64(math.MaxInt)/size || uint64(len(fields)) > uint64(math.MaxInt64)/size {
		return fmt.Errorf("canonical group workspace size overflow")
	}
	if reserve == nil {
		return fmt.Errorf("canonical group workspace has no memory owner")
	}
	lease, err := reserve(int64(uint64(len(fields)) * size))
	if err != nil {
		return err
	}
	components := make([]canonicalGroupComponent, len(fields))
	w.close()
	w.components, w.lease = components, lease
	return nil
}

func (w *canonicalGroupWorkspace) bind(fields []memory.Field, batch *memory.Batch) (canonicalGroupBinding, error) {
	w.clear()
	if len(fields) > len(w.components) {
		return canonicalGroupBinding{}, fmt.Errorf("canonical group workspace was not reserved")
	}
	binding, err := bindCanonicalGroupComponents(fields, batch, w.components[:len(fields)])
	if err != nil {
		w.clear()
		return canonicalGroupBinding{}, err
	}
	w.binding = binding
	return binding, nil
}

func (w *canonicalGroupWorkspace) clear() {
	clear(w.components)
	w.binding = canonicalGroupBinding{}
}

func (w *canonicalGroupWorkspace) close() {
	w.clear()
	w.components = nil
	if w.lease != nil {
		w.lease.Release()
		w.lease = nil
	}
}
