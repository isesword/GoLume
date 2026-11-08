//go:build goexperiment.simd && !amd64

package kernel

import "simd"

func expectedNativeSIMD() bool { return !simd.Emulated() }
