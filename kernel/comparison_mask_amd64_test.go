//go:build goexperiment.simd && amd64

package kernel

import (
	"fmt"
	"gocolumn/memory"
	"reflect"
	"runtime"
	"simd"
	"simd/archsimd"
	"strings"
	"testing"
)

func TestAMD64ComparisonMaskInstallation(t *testing.T) {
	actual := simd.Int64s{}.Len() * 64
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		for _, width := range []int{-1, 0, 64, 127, 128, 256, 512, 1024} {
			s, scalar := newScalarSet(mode), newScalarSet(ModeScalar)
			installGeneratedArchSIMD(&s, mode, width)
			supported := !simd.Emulated() && width == actual && ((width == 128 && archsimd.X86.AVX()) || (width == 256 && archsimd.X86.AVX2()) || (width == 512 && archsimd.X86.AVX512()))
			wantNative := mode != ModeScalar && supported
			if got := amd64ComparisonWidthSupported(width); got != supported {
				t.Fatalf("width=%d supported=%t want=%t", width, got, supported)
			}
			for op := CompareGT; op <= CompareEQ; op++ {
				check := func(typ memory.DataType, columns bool, got, fallback any, suffix string) {
					t.Helper()
					id, _ := ComparisonID(typ, op)
					cap, _ := s.Capability(id)
					if columns {
						cap, _ = s.ColumnComparisonCapability(typ, op)
					}
					ptr := reflect.ValueOf(got).Pointer()
					if !wantNative {
						if ptr != reflect.ValueOf(fallback).Pointer() || cap != (KernelCapability{Backend: "scalar"}) {
							t.Fatalf("mode=%s width=%d %s columns=%t unexpected override: %+v", mode, width, id, columns, cap)
						}
						return
					}
					wantName := fmt.Sprintf(".simdAMD%d%s%s", width, suffix, strings.ToUpper(op.String()))
					if name := runtime.FuncForPC(ptr).Name(); !strings.HasSuffix(name, wantName) {
						t.Fatalf("mode=%s width=%d %s columns=%t function=%s want suffix=%s", mode, width, id, columns, name, wantName)
					}
					if cap != (KernelCapability{HardwareSIMD: true, VectorBits: width, Backend: "archsimd-amd64-mask"}) {
						t.Fatalf("mode=%s width=%d %s columns=%t cap=%+v", mode, width, id, columns, cap)
					}
				}
				check(memory.TypeInt8, false, s.d.compareInt8[op], scalar.d.compareInt8[op], "Int8")
				check(memory.TypeInt32, false, s.d.compareInt32[op], scalar.d.compareInt32[op], "Int32")
				check(memory.TypeInt64, false, s.d.compareInt64[op], scalar.d.compareInt64[op], "Int64")
				check(memory.TypeUint32, false, s.d.compareUint32[op], scalar.d.compareUint32[op], "Uint32")
				check(memory.TypeUint64, false, s.d.compareUint64[op], scalar.d.compareUint64[op], "Uint64")
				check(memory.TypeFloat32, false, s.d.compareFloat32[op], scalar.d.compareFloat32[op], "Float32")
				check(memory.TypeFloat64, false, s.d.compareFloat64[op], scalar.d.compareFloat64[op], "Float64")
				check(memory.TypeInt32, true, s.d.compareInt32Columns[op], scalar.d.compareInt32Columns[op], "Int32Columns")
				check(memory.TypeUint32, true, s.d.compareUint32Columns[op], scalar.d.compareUint32Columns[op], "Uint32Columns")
				check(memory.TypeFloat32, true, s.d.compareFloat32Columns[op], scalar.d.compareFloat32Columns[op], "Float32Columns")
				check(memory.TypeFloat64, true, s.d.compareFloat64Columns[op], scalar.d.compareFloat64Columns[op], "Float64Columns")

			}
		}
	}
}
