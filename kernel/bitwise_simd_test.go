//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"reflect"
	"runtime"
	"simd"
	"strings"
	"testing"
)

func TestBitwiseNativeDispatch(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		want := mode != ModeScalar && expectedNativeSIMD()
		for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
			for op := BitwiseAnd; op <= BitwiseNot; op++ {
				id, _ := BitwiseID(typ, op)
				cap, ok := s.Capability(id)
				if !ok || cap.HardwareSIMD != want {
					t.Fatalf("%s %s: %+v want native=%v", mode, id, cap, want)
				}
				if want && (cap.Backend != "portable-simd" || cap.VectorBits != simd.Int64s{}.Len()*64) {
					t.Fatal(cap)
				}
				if !want && (cap.Backend != "scalar" || cap.VectorBits != 0) {
					t.Fatal(cap)
				}
			}
		}
		for _, pair := range [][2]any{{s.d.bitwise.int32, simdBitwiseInt32}, {s.d.bitwise.uint32, simdBitwiseUint32}, {s.d.bitwise.int64, simdBitwiseInt64}, {s.d.bitwise.uint64, simdBitwiseUint64}} {
			// The experimental compiler can specialize the test itself and its
			// direct function references (@simd128/256/512). The dispatch
			// table correctly retains the runtime-width gateway.
			gotName := runtime.FuncForPC(reflect.ValueOf(pair[0]).Pointer()).Name()
			nativeName := runtime.FuncForPC(reflect.ValueOf(pair[1]).Pointer()).Name()
			installed := strings.Split(gotName, "@")[0] == strings.Split(nativeName, "@")[0]
			if installed != want {
				t.Fatalf("actual function pointer installed=%v want=%v mode=%s got=%s expected=%s", installed, want, mode, runtime.FuncForPC(reflect.ValueOf(pair[0]).Pointer()).Name(), runtime.FuncForPC(reflect.ValueOf(pair[1]).Pointer()).Name())
			}
		}
	}
}
