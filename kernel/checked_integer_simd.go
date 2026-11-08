//go:build goexperiment.simd

package kernel

import "unsafe"

// Exact input/output aliases are safe: both input vectors are loaded before
// storing a result. Partial aliases retain the scalar loop's sequential semantics.
func checkedIntegerPartialOverlap[T ~int64 | ~uint64](left, right, out []T) bool {
	if len(left) == 0 {
		return false
	}
	out = out[:len(left)]
	start := uintptr(unsafe.Pointer(unsafe.SliceData(out)))
	end := start + uintptr(len(out))*8
	for _, in := range [2][]T{left, right} {
		other := uintptr(unsafe.Pointer(unsafe.SliceData(in)))
		if start != other && start < other+uintptr(len(in))*8 && other < end {
			return true
		}
	}
	return false
}
