//go:build goexperiment.simd && !amd64 && !arm64

package kernel

import "simd"

func reduceMinMaxUint32(lo, hi simd.Uint32s) (uint32, uint32) {
	var a, z [16]uint32
	lo.Store(a[:])
	hi.Store(z[:])
	l, h := a[0], z[0]
	for i := 1; i < lo.Len(); i++ {
		l = min(l, a[i])
		h = max(h, z[i])
	}
	return l, h
}

func reduceMinMaxUint64(lo, hi simd.Uint64s) (uint64, uint64) {
	var a, z [8]uint64
	lo.Store(a[:])
	hi.Store(z[:])
	l, h := a[0], z[0]
	for i := 1; i < lo.Len(); i++ {
		l = min(l, a[i])
		h = max(h, z[i])
	}
	return l, h
}
