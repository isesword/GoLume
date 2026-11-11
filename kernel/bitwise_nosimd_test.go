//go:build !goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"testing"
)

func TestBitwisePlainDispatch(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
			for op := BitwiseAnd; op <= BitwiseNot; op++ {
				id, _ := BitwiseID(typ, op)
				cap, ok := s.Capability(id)
				if !ok || cap.HardwareSIMD || cap.VectorBits != 0 || cap.Backend != "scalar" {
					t.Fatalf("%s %s: %+v", mode, id, cap)
				}
			}
		}
	}
}
