//go:build goexperiment.simd

package kernel

import "simd"

func installSIMD(set *Set, mode Mode) {
	if mode == ModeScalar || !nativeSIMDAvailable() {
		return
	}
	vectorBits := simd.Int64s{}.Len() * 64
	installWidenSIMD(set, mode, vectorBits)
	installSumSIMD(set, mode, vectorBits)
	installMinMaxSIMD(set, mode, vectorBits)
	installArchSumSIMD(set, mode, vectorBits)
	installMathSIMD(set, mode, vectorBits)
	installCheckedIntegerSIMD(set, mode, vectorBits)
	installScaleSIMD(set, mode, vectorBits)
	installGeneratedPortableSIMD(set, mode, vectorBits)
	installArchSIMDHash(set, mode, vectorBits)
	installArchSIMDCompact(set, mode, vectorBits)
	installFloat32ColumnSIMD(set, mode, vectorBits)
	installInt32ColumnSIMD(set, mode, vectorBits)
	installUint32ColumnSIMD(set, mode, vectorBits)
	// Native overrides must follow every portable installer.
	installGeneratedArchSIMD(set, mode, vectorBits)
}

// The current experimental amd64 lowering uses AVX2 scalar broadcasts even at
// 128 bits. Runtime width selection alone is not a sufficient hardware guard.
func nativeSIMDAvailable() bool { return !simd.Emulated() && archSIMDSupported() }
