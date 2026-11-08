//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"testing"
)

func TestMathNativeSIMDCapability(t *testing.T) {
	for _, mode := range []Mode{ModeAuto, ModeSIMD} {
		for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
			for _, op := range []MathUnaryOp{MathAbs, MathSqrt} {
				id, _ := MathUnaryID(typ, op)
				cap, ok := New(mode).Capability(id)
				if !ok {
					t.Fatal(id)
				}
				if !expectedNativeSIMD() {
					if cap.HardwareSIMD {
						t.Fatalf("emulated runtime claims hardware %s", id)
					}
					continue
				}
				if !cap.HardwareSIMD || cap.Backend != "portable-simd" || cap.VectorBits <= 0 {
					t.Fatalf("candidate not installed %s %+v", id, cap)
				}
			}
		}
	}
}
