//go:build goexperiment.simd

package kernel

import "testing"

// Exercise portable column kernels directly: amd64 native overrides must not
// hide regressions in the fallback's wordwise combined-validity handling.
func TestPortableColumnComparisonMaskWords(t *testing.T) {
	scalar := New(ModeScalar)
	t.Run("int32", func(t *testing.T) { checkMaskOneHot[int32](t, scalar.CompareInt32, simdInt32Columns) })
	t.Run("uint32", func(t *testing.T) { checkMaskOneHot[uint32](t, scalar.CompareUint32, simdUint32Columns) })
	t.Run("float32", func(t *testing.T) { checkMaskOneHot[float32](t, scalar.CompareFloat32, simdFloat32Columns) })
}
