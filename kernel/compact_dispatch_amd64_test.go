//go:build goexperiment.simd && amd64

package kernel

import (
	"reflect"
	"simd"
	"simd/archsimd"
	"testing"
)

func TestReviewCompactDispatchIdentity(t *testing.T) {
	widths := simd.Int64s{}.Len() * 64
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		native := mode != ModeScalar && !simd.Emulated() && archsimd.X86.AVX2() && archsimd.X86.AVX512() && widths >= 512
		entries := []struct {
			id                  KernelID
			got, scalar, vector any
			needVBMI2           bool
		}{
			{KernelCompactFloat32, s.d.compactFloat32, scalarFloat32Compact, compactFloat32X16, false},
			{KernelCompactInt8, s.d.compactInt8, scalarInt8Compact, compactInt8X64, true},
			{KernelCompactUint8, s.d.compactUint8, scalarUint8Compact, compactUint8X64, true},
			{KernelCompactInt16, s.d.compactInt16, scalarInt16Compact, compactInt16X32, true},
			{KernelCompactUint16, s.d.compactUint16, scalarUint16Compact, compactUint16X32, true},
		}
		for _, e := range entries {
			wantNative := native && (!e.needVBMI2 || archsimd.X86.AVX512VBMI2())
			want := e.scalar
			if wantNative {
				want = e.vector
			}
			c, _ := s.Capability(e.id)
			if c.HardwareSIMD != wantNative || reflect.ValueOf(e.got).Pointer() != reflect.ValueOf(want).Pointer() {
				t.Fatalf("mode%s id%s cap%+v wrong dispatch", mode, e.id, c)
			}
		}
	}
	s := newScalarSet(ModeAuto)
	installNarrowSIMDCompact(&s, false)
	for _, p := range [][2]any{{s.d.compactInt8, scalarInt8Compact}, {s.d.compactUint8, scalarUint8Compact}, {s.d.compactInt16, scalarInt16Compact}, {s.d.compactUint16, scalarUint16Compact}} {
		if reflect.ValueOf(p[0]).Pointer() != reflect.ValueOf(p[1]).Pointer() {
			t.Fatal("VBMI2 absent must retain scalar function")
		}
	}
}
