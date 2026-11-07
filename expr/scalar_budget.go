package expr

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
	"reflect"
	"unsafe"
)

// Accounting covers retained engine-owned payload buffers and register/output
// backing arrays, plus explicit temporary transformation/parser allowances.
// It excludes typed result-view wrappers, reservation handles/tracking arrays,
// error objects, standard-library caches/pools (including regexp machines), and
// Go allocator size classes/bookkeeping. This is not a hard heap or RSS limit.
type scalarReservation struct {
	owner       *evaluationState
	reservation memory.Reservation
	index       int
	bytes       int64
}

// Bytes returns the reserved payload allowance.
func (r *scalarReservation) Bytes() int64 { return r.bytes }

// Release relinquishes the allowance exactly once.
func (r *scalarReservation) Release() {
	if r.owner == nil {
		return
	}
	w := r.owner
	last := len(w.reservations) - 1
	if r.index != last {
		w.reservations[r.index] = w.reservations[last]
		w.reservations[r.index].(*scalarReservation).index = r.index
	}
	w.reservations[last] = nil
	w.reservations = w.reservations[:last]
	r.owner = nil
	r.reservation.Release()
}

type scalarNoopReservation struct{}

// Bytes returns zero for an unbudgeted reservation.
func (scalarNoopReservation) Bytes() int64 { return 0 }

// Release is a no-op for unbudgeted work.
func (scalarNoopReservation) Release() {}
func (w *evaluationState) checkContext() error {
	if w.ctx != nil {
		return w.ctx.Err()
	}
	return nil
}
func (w *evaluationState) acquire(n int64) (memory.Reservation, error) {
	if err := w.checkContext(); err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, fmt.Errorf("scalar allocation size overflow")
	}
	if (w.manager == nil && w.reserveHook == nil) || n == 0 {
		return scalarNoopReservation{}, nil
	}
	ctx := w.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	var reservation memory.Reservation
	var err error
	if w.manager != nil {
		reservation, err = w.manager.Reserve(ctx, n)
	} else {
		reservation, err = w.reserveHook(n)
	}
	if err != nil {
		return nil, err
	}
	r := &scalarReservation{owner: w, reservation: reservation, index: len(w.reservations), bytes: n}
	w.reservations = append(w.reservations, r)
	return r, nil
}
func scalarBytes(n int, width uintptr) (int64, error) {
	if n < 0 || uint64(n) > uint64(math.MaxInt64)/uint64(width) {
		return 0, fmt.Errorf("scalar allocation size overflow")
	}
	return int64(n) * int64(width), nil
}
func scalarResize[S ~[]T, T any](dst *S, n int, slot *memory.Reservation, acquire func(int64) (memory.Reservation, error)) error {
	if n < 0 {
		return fmt.Errorf("negative scalar buffer length")
	}
	if cap(*dst) >= n {
		*dst = (*dst)[:n]
		return nil
	}
	var value T
	size, err := scalarBytes(n, unsafe.Sizeof(value))
	if err != nil {
		return err
	}
	reservation, err := acquire(size)
	if err != nil {
		return err
	}
	next := make(S, n)
	copy(next, (*dst)[:cap(*dst)])
	old := *slot
	*dst = next
	*slot = reservation
	if old != nil {
		old.Release()
	}
	return nil
}
func (w *evaluationState) ensureRegisters(n int) error {
	return w.ensureRegisterCounts(n, n)
}

func (w *evaluationState) ensureRegisterCounts(n, owners int) error {
	if err := scalarResize(&w.regs, n, &w.storage[0], w.acquire); err != nil {
		return err
	}
	if err := scalarResize(&w.owned, owners, &w.storage[1], w.acquire); err != nil {
		return err
	}
	return nil
}
func (w *evaluationState) ensureOutputs(n int) error {
	return scalarResize(&w.outputs, n, &w.storage[2], w.acquire)
}
func prepareScalarBuffers(r *scalarRegister, rows int) error {
	if rows < 0 || rows == int(^uint(0)>>1) {
		return fmt.Errorf("invalid scalar row count")
	}
	r.validity = nil
	if r.strList != nil {
		r.strList.Validity = nil
	}
	if err := scalarResize(&r.ownedValidity, memory.WordsFor(rows), &r.buffers[0], r.acquire); err != nil {
		return err
	}
	r.validity = r.ownedValidity
	if r.typ == memory.TypeBool {
		return scalarResize(&r.Truth, memory.WordsFor(rows), &r.buffers[14], r.acquire)
	}
	switch r.typ {
	case memory.TypeInt8:
		return scalarResize(&r.i8, rows, &r.buffers[1], r.acquire)
	case memory.TypeInt16:
		return scalarResize(&r.i16, rows, &r.buffers[2], r.acquire)
	case memory.TypeUint8:
		return scalarResize(&r.u8, rows, &r.buffers[3], r.acquire)
	case memory.TypeUint16:
		return scalarResize(&r.u16, rows, &r.buffers[4], r.acquire)
	case memory.TypeFloat32:
		return scalarResize(&r.f32, rows, &r.buffers[5], r.acquire)
	}
	switch familyOf(r.typ) {
	case familyI32:
		return scalarResize(&r.i32, rows, &r.buffers[6], r.acquire)
	case familyI64:
		return scalarResize(&r.i64, rows, &r.buffers[7], r.acquire)
	case familyU32:
		return scalarResize(&r.u32, rows, &r.buffers[8], r.acquire)
	case familyU64:
		return scalarResize(&r.u64, rows, &r.buffers[9], r.acquire)
	case familyF64:
		return scalarResize(&r.f64, rows, &r.buffers[10], r.acquire)
	case familyString:
		return scalarResize(&r.offsets, rows+1, &r.buffers[11], r.acquire)
	}
	return nil
}
func growScalarBytes(r *scalarRegister, length int) error {
	if err := r.checkContext(); err != nil {
		return err
	}
	if uint64(len(r.data))+uint64(length) > math.MaxUint32 {
		return fmt.Errorf("scalar string result exceeds 4 GiB")
	}
	n := len(r.data) + length
	if cap(r.data) < n {
		capacity := max(n, 2*cap(r.data))
		if capacity < 0 || uint64(capacity) > math.MaxUint32 {
			capacity = n
		}
		oldLen := len(r.data)
		if err := scalarResize(&r.data, capacity, &r.buffers[12], r.acquire); err != nil {
			return err
		}
		r.data = r.data[:oldLen]
	}
	return nil
}
func appendScalarBytes(r *scalarRegister, value []byte) error {
	if err := growScalarBytes(r, len(value)); err != nil {
		return err
	}
	r.data = append(r.data, value...)
	return nil
}
func appendScalarString(r *scalarRegister, value string) error {
	if err := growScalarBytes(r, len(value)); err != nil {
		return err
	}
	r.data = append(r.data, value...)
	return nil
}
func appendScalarResult(r *scalarRegister, value []byte, op Op, row int) error {
	if err := appendScalarBytes(r, value); err != nil {
		return fmt.Errorf("%s at row %d: %w", op, row, err)
	}
	return nil
}

// scratchReservation guards temporary stdlib transformations before they allocate.
func scratchReservation(r *scalarRegister, n int64) (memory.Reservation, error) {
	if r.acquire == nil {
		return scalarNoopReservation{}, nil
	}
	return r.acquire(n)
}

func sameScalarManager(a, b memory.Manager) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	typ := reflect.TypeOf(a)
	return typ == reflect.TypeOf(b) && typ.Comparable() && a == b
}

func checkedScalarScratch(parts ...int64) (int64, error) {
	var total int64
	for _, n := range parts {
		if n < 0 || total > math.MaxInt64-n {
			return 0, fmt.Errorf("scalar scratch estimate overflow")
		}
		total += n
	}
	return total, nil
}
func regexScratch(r *scalarRegister, input []byte, in instruction, all bool) (memory.Reservation, error) {
	count := int64(1)
	if all {
		count = int64(len(input)) + 1
	}
	width := int64(2*(in.Regex.NumSubexp()+1))*int64(unsafe.Sizeof(int(0))) + int64(unsafe.Sizeof([]int{}))
	if count > math.MaxInt64/width/2 {
		return nil, fmt.Errorf("regex scratch estimate overflow")
	}
	// Match result arrays may grow geometrically. Standard-library regexp
	// machine caches/pools are explicitly outside this payload allowance.
	n, err := checkedScalarScratch(count*width*2, 1024)
	if err != nil {
		return nil, err
	}
	return scratchReservation(r, n)
}
