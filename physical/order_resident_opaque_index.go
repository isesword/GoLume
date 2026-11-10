package physical

import (
	"fmt"
	"hash/maphash"
	"math"
	"reflect"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"gocolumn/memory"
)

// The table owns one reference per occupied slot. ArrayData identity, not equal
// contents or a shared child buffer, determines whether backing is already held.
// Explicit storage lets the caller admit the full old/new table overlap before
// allocating. Unlike a Go map, there is no hidden bucket or growth allocation.
type residentOpaqueSourceIndex struct {
	slots []residentOpaqueSourceSlot
	count int
	seed  maphash.Seed
}

type residentOpaqueSourceSlot struct {
	data arrow.ArrayData
	hash uint64
}

func (s *residentOpaqueSourceIndex) key(data arrow.ArrayData) (uint64, bool, error) {
	if residentOpaqueIsNil(data) {
		return 0, false, fmt.Errorf("resident opaque source data is nil")
	}
	value := reflect.ValueOf(data)
	// Value.Comparable also checks values inside interface fields. Type.Comparable
	// alone can claim a struct is comparable even when equality would panic.
	comparable := value.Type().Comparable()
	if comparable && (value.Kind() == reflect.Struct || value.Kind() == reflect.Array) {
		comparable = value.Comparable()
	}
	if comparable {
		return maphash.Comparable(s.seed, data), true, nil
	}
	// There is no general identity for a non-comparable custom implementation.
	// Conservatively retain and charge each occurrence, never compare it. Count is
	// only a placement key, not an assertion that distinct sources differ by value.
	return maphash.Comparable(s.seed, s.count), false, nil
}

// find returns an existing identity or an empty slot. At most half of a live
// table is occupied, including after growth, so probing always reaches an empty
// slot. The caller can disable equality when finding space for a new reference.
func (s *residentOpaqueSourceIndex) find(data arrow.ArrayData, hash uint64, comparable bool) (int, bool) {
	if len(s.slots) == 0 {
		return 0, false
	}
	mask := len(s.slots) - 1
	slot := int(hash & uint64(mask))
	for s.slots[slot].data != nil {
		entry := s.slots[slot]
		// A comparable incoming value makes this comparison safe: matching dynamic
		// types are comparable; different dynamic types compare false without reading
		// the non-comparable contents of the stored value.
		if comparable && entry.hash == hash && entry.data == data {
			return slot, true
		}
		slot = (slot + 1) & mask
	}
	return slot, false
}

// growth plans a new insertion without changing the index. It includes the full
// replacement allocation, not only its delta: the old table remains live while
// rehashing, and the owner keeps its high-water admission until close.
func (s *residentOpaqueSourceIndex) growth() (int, int64, error) {
	return residentOpaqueSourceGrowth(len(s.slots), s.count)
}

func residentOpaqueSourceGrowth(capacity, count int) (int, int64, error) {
	if capacity < 0 || count < 0 {
		return 0, 0, memory.ErrBudgetExceeded
	}
	if count < capacity/2 {
		return capacity, 0, nil
	}
	if capacity > math.MaxInt/2 {
		return 0, 0, memory.ErrBudgetExceeded
	}
	n := max(8, 2*capacity)
	width := int64(unsafe.Sizeof(residentOpaqueSourceSlot{}))
	if int64(n) > math.MaxInt64/width {
		return 0, 0, memory.ErrBudgetExceeded
	}
	return n, int64(n) * width, nil
}

// resize is called only after admission. Moving interfaces does not change the
// ownership count; only insertion and close Retain/Release the original source.
func (s *residentOpaqueSourceIndex) resize(n int) {
	slots := make([]residentOpaqueSourceSlot, n)
	for _, entry := range s.slots {
		if entry.data == nil {
			continue
		}
		at := int(entry.hash & uint64(n-1))
		for slots[at].data != nil {
			at = (at + 1) & (n - 1)
		}
		slots[at] = entry
	}
	s.slots = slots
}

func (s *residentOpaqueSourceIndex) close() {
	for i := range s.slots {
		if data := s.slots[i].data; data != nil {
			s.slots[i] = residentOpaqueSourceSlot{}
			data.Release()
		}
	}
	s.slots = nil
	s.count = 0
}

func residentOpaqueIsNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
