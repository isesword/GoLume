//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"runtime"
	"simd"
	"testing"
)

func TestCheckedInteger64NativeCapabilities(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
			for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
				id, _ := ArithmeticID(typ, op)
				got, ok := set.Capability(id)
				want := mode != ModeScalar && expectedNativeSIMD() && (runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64")
				if !ok || got.HardwareSIMD != want || got.Emulated {
					t.Fatalf("%s %s: %+v", mode, id, got)
				}
				if want && (got.Backend != "archsimd-"+runtime.GOARCH+"-checked" || got.VectorBits != simd.Int64s{}.Len()*64) {
					t.Fatalf("unexpected native capability %s: %+v", id, got)
				}
			}
		}
	}
}
