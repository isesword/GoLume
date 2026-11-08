package kernel

import "testing"

var minMaxSink [2]uint64

func benchmarkMaskedMinMax[T extremeNumber](b *testing.B, call func([]T, uint64) (T, T), mask uint64) {
	var v [64]T
	for i := range v {
		v[i] = T((i * 137) % 71)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a, z := call(v[:], mask)
		minMaxSink = [2]uint64{uint64(a), uint64(z)}
	}
}

func BenchmarkMaskedMinMax(b *testing.B) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		for _, m := range []struct {
			name string
			mask uint64
		}{{"dense", ^uint64(0)}, {"half", 0xaaaaaaaaaaaaaaaa}} {
			prefix := mode.String() + "/" + m.name + "/"
			b.Run(prefix+"int32", func(b *testing.B) { benchmarkMaskedMinMax(b, s.MinMaxInt32Masked, m.mask) })
			b.Run(prefix+"int64", func(b *testing.B) { benchmarkMaskedMinMax(b, s.MinMaxInt64Masked, m.mask) })
			b.Run(prefix+"uint32", func(b *testing.B) { benchmarkMaskedMinMax(b, s.MinMaxUint32Masked, m.mask) })
			b.Run(prefix+"uint64", func(b *testing.B) { benchmarkMaskedMinMax(b, s.MinMaxUint64Masked, m.mask) })
			b.Run(prefix+"float32", func(b *testing.B) { benchmarkMaskedMinMax(b, s.MinMaxFloat32Masked, m.mask) })
			b.Run(prefix+"float64", func(b *testing.B) { benchmarkMaskedMinMax(b, s.MinMaxFloat64Masked, m.mask) })
		}
	}
}
