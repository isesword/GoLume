//go:build goexperiment.simd && arm64

package kernel

import (
	"gocolumn/memory"
	"simd"
	"testing"
)

func TestArm64AutoAllowlistSelectsMeasuredBackends(t *testing.T) {
	set := New(ModeAuto)
	if simd.Emulated() {
		for _, id := range KernelIDs() {
			capability, _ := set.Capability(id)
			if capability.Backend != "scalar" {
				t.Fatalf("emulated mode selected %s=%+v", id, capability)
			}
		}
		return
	}
	for _, id := range KernelIDs() {
		capability, ok := set.Capability(id)
		if !ok {
			t.Fatalf("missing capability for %s", id)
		}
		if !approvedArm64AutoID(id) {
			if capability.Backend != "scalar" {
				t.Fatalf("unapproved auto backend %s=%+v", id, capability)
			}
			continue
		}
		switch id {
		case KernelHash64Batch:
			if capability.Backend != "arm64-unrolled" || capability.HardwareSIMD || capability.VectorBits != 0 {
				t.Fatalf("hash capability=%+v", capability)
			}
		default:
			if id.kind == KernelComparison {
				if capability.Backend != "archsimd-arm64-mask" || !capability.HardwareSIMD || capability.VectorBits != 128 {
					t.Fatalf("comparison %s=%+v", id, capability)
				}
			} else if id.typ == memory.TypeFloat64 {
				if capability.Backend != "portable-simd" || !capability.HardwareSIMD || capability.VectorBits < 128 {
					t.Fatalf("float arithmetic %s=%+v", id, capability)
				}
			}
		}
	}
}
