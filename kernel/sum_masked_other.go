//go:build goexperiment.simd && !amd64 && !arm64

package kernel

import "simd"

func installArchSumSIMD(*Set, Mode, int) {}

func reduceSumInt32(acc simd.Int32s) int32 {
	var partial [16]int32
	acc.Store(partial[:])
	var sum int32
	for i := 0; i < acc.Len(); i++ {
		sum += partial[i]
	}
	return sum
}

func reduceSumUint32(acc simd.Uint32s) uint32 {
	var partial [16]uint32
	acc.Store(partial[:])
	var sum uint32
	for i := 0; i < acc.Len(); i++ {
		sum += partial[i]
	}
	return sum
}

func reduceSumInt64(acc simd.Int64s) int64 {
	var partial [8]int64
	acc.Store(partial[:])
	var sum int64
	for i := 0; i < acc.Len(); i++ {
		sum += partial[i]
	}
	return sum
}

func reduceSumUint64(acc simd.Uint64s) uint64 {
	var partial [8]uint64
	acc.Store(partial[:])
	var sum uint64
	for i := 0; i < acc.Len(); i++ {
		sum += partial[i]
	}
	return sum
}
