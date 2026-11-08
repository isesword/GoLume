//go:build goexperiment.simd

package kernel

import "unsafe"

// Exact input/output aliases are safe: both input vectors are loaded before
// storing a result. Partial aliases retain the scalar loop's sequential semantics.
func checkedIntegerPartialOverlap[T ~int32 | ~uint32 | ~int64 | ~uint64](left, right, out []T) bool {
	if len(left) == 0 {
		return false
	}
	out = out[:len(left)]
	start := uintptr(unsafe.Pointer(unsafe.SliceData(out)))
	size := unsafe.Sizeof(*new(T))
	end := start + uintptr(len(out))*size
	for _, in := range [2][]T{left, right} {
		other := uintptr(unsafe.Pointer(unsafe.SliceData(in)))
		if start != other && start < other+uintptr(len(in))*size && other < end {
			return true
		}
	}
	return false
}
