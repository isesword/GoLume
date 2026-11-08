//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"reflect"
	"simd"
	"testing"
)

func TestExpandedNumericNativeDispatch(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		native := mode != ModeScalar && !simd.Emulated()
		check := func(typ memory.DataType, op CompareOp, got, portable, scalar any) {
			t.Helper()
			id, _ := ComparisonID(typ, op)
			cap, _ := s.Capability(id)
			if native && reflect.ValueOf(got).Pointer() == reflect.ValueOf(scalar).Pointer() {
				t.Fatalf("%s/%s native dispatch retained scalar function", mode, id)
			}
			want := scalar
			if native {
				want = portable
			}
			if reflect.ValueOf(got).Pointer() != reflect.ValueOf(want).Pointer() {
				t.Fatalf("%s/%s wrong function (cap=%+v)", mode, id, cap)
			}
			if cap.HardwareSIMD != native || cap.Emulated {
				t.Fatalf("%s/%s cap=%+v", mode, id, cap)
			}
			if native && (cap.Backend != "portable-simd" || cap.VectorBits != simd.Int64s{}.Len()*64) {
				t.Fatalf("%s/%s cap=%+v", mode, id, cap)
			}
		}
		for op := CompareGT; op <= CompareEQ; op++ {
			scalar := New(ModeScalar)
			// arm64 retains its existing specialized overrides for Int32/Uint32.
			portable := newScalarSet(mode)
			installGeneratedPortableSIMD(&portable, mode, simd.Int64s{}.Len()*64)
			check(memory.TypeInt8, op, s.d.compareInt8[op], portable.d.compareInt8[op], scalar.d.compareInt8[op])
			check(memory.TypeInt64, op, s.d.compareInt64[op], portable.d.compareInt64[op], scalar.d.compareInt64[op])
			check(memory.TypeUint64, op, s.d.compareUint64[op], portable.d.compareUint64[op], scalar.d.compareUint64[op])
			check(memory.TypeFloat64, op, s.d.compareFloat64[op], portable.d.compareFloat64[op], scalar.d.compareFloat64[op])
			equalColumns := reflect.ValueOf(s.d.compareFloat64Columns[op]).Pointer() == reflect.ValueOf(scalar.d.compareFloat64Columns[op]).Pointer()
			if equalColumns == native {
				t.Fatalf("%s/float64-columns/%s wrong dispatch identity native=%t", mode, op, native)
			}
			if runtimeArch() != "arm64" {
				check(memory.TypeInt32, op, s.d.compareInt32[op], portable.d.compareInt32[op], scalar.d.compareInt32[op])
				check(memory.TypeUint32, op, s.d.compareUint32[op], portable.d.compareUint32[op], scalar.d.compareUint32[op])
			}
		}
		for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
			for op := ArithmeticAdd; op <= ArithmeticDiv; op++ {
				id, _ := ArithmeticID(typ, op)
				scalar := New(ModeScalar)
				var got, fallback any
				if typ == memory.TypeFloat32 {
					got, fallback = s.d.arithmeticFloat32[op], scalar.d.arithmeticFloat32[op]
				} else {
					got, fallback = s.d.arithmeticFloat64[op], scalar.d.arithmeticFloat64[op]
				}
				equal := reflect.ValueOf(got).Pointer() == reflect.ValueOf(fallback).Pointer()
				if equal == native {
					t.Fatalf("%s/%s wrong dispatch identity native=%t", mode, id, native)
				}
				cap, _ := s.Capability(id)
				if cap.HardwareSIMD != native || cap.Emulated {
					t.Fatalf("%s/%s cap=%+v", mode, id, cap)
				}
				if native && (cap.Backend != "portable-simd" || cap.VectorBits != simd.Int64s{}.Len()*64) {
					t.Fatalf("%s/%s cap=%+v", mode, id, cap)
				}
			}
		}
	}
}
