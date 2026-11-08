//go:build goexperiment.simd

package kernel

import "simd"

func installSIMD(set *Set, mode Mode) {
	if mode == ModeScalar || simd.Emulated() {
		return
	}
	vectorBits := simd.Int64s{}.Len() * 64
	installMathSIMD(set, mode, vectorBits)
	installGeneratedPortableSIMD(set, mode, vectorBits)
	installGeneratedArchSIMD(set, mode, vectorBits)
	installArchSIMDHash(set, mode, vectorBits)
	installArchSIMDCompact(set, mode, vectorBits)
	installFloat32ColumnSIMD(set, mode, vectorBits)
	installInt32ColumnSIMD(set, mode, vectorBits)
	installUint32ColumnSIMD(set, mode, vectorBits)
}
