package kernel

import (
	"fmt"
	"gocolumn/memory"
	"testing"
)

func compactBenchmark[T any](b *testing.B, run func([]T, memory.Bitmap, []T) int, n, pct int) {
	values, out := make([]T, n), make([]T, n)
	selection := make(memory.Bitmap, memory.WordsFor(n))
	for row := 0; row < n; row++ {
		if (row*37)%100 < pct {
			memory.Set(selection, row)
		}
	}
	if pct == 100 {
		selection = nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		run(values, selection, out)
	}
}

func BenchmarkNativeCompact(b *testing.B) {
	for _, mode := range []Mode{ModeScalar, ModeAuto} {
		set := New(mode)
		for _, n := range []int{63, 65539} {
			for _, pct := range []int{1, 50, 99, 100} {
				prefix := fmt.Sprintf("%s/n%d/pct%d/", mode, n, pct)
				b.Run(prefix+"Float32", func(b *testing.B) { compactBenchmark(b, set.CompactFloat32, n, pct) })
				b.Run(prefix+"Int8", func(b *testing.B) { compactBenchmark(b, set.CompactInt8, n, pct) })
				b.Run(prefix+"Uint8", func(b *testing.B) { compactBenchmark(b, set.CompactUint8, n, pct) })
				b.Run(prefix+"Int16", func(b *testing.B) { compactBenchmark(b, set.CompactInt16, n, pct) })
				b.Run(prefix+"Uint16", func(b *testing.B) { compactBenchmark(b, set.CompactUint16, n, pct) })
			}
		}
	}
}
