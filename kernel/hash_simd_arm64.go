//go:build goexperiment.simd && arm64

package kernel

func installArchSIMDHash(set *Set, mode Mode, _ int) {
	if mode != ModeAuto {
		return
	}
	set.d.hash64 = unrolledHash64Batch
	set.setCapability(KernelHash64Batch, KernelCapability{Backend: "arm64-unrolled"})
}

func installArchSIMDCompact(*Set, Mode, int) {}
