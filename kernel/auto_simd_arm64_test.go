//go:build goexperiment.simd && arm64

package kernel

import (
	"gocolumn/memory"
	"simd"
	"testing"
)

func TestArm64AutoSelectsNativeComparisonBackends(t *testing.T) {
	set := New(ModeAuto)
	if simd.Emulated() {
		return
	} // The shared native-dispatch test checks fallback.
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32} {
		for op := CompareGT; op <= CompareEQ; op++ {
			id, _ := ComparisonID(typ, op)
			capability, _ := set.Capability(id)
			if typ == memory.TypeInt32 && op == CompareEQ {
				if capability.Backend != "scalar" {
					t.Fatalf("unimplemented %s=%+v", id, capability)
				}
				continue
			}
			if capability.Backend != "archsimd-arm64-mask" || !capability.HardwareSIMD || capability.VectorBits != 128 {
				t.Fatalf("comparison %s=%+v", id, capability)
			}
		}
	}
	capability, _ := set.Capability(KernelHash64Batch)
	if capability.Backend != "arm64-unrolled" || capability.HardwareSIMD || capability.VectorBits != 0 {
		t.Fatalf("hash capability=%+v", capability)
	}
}
