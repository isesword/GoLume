package kernel

import (
	"fmt"
	"gocolumn/memory"
	"testing"
	"unsafe"
)

var integerColumnBenchmarkCount int

// A bounded size/validity matrix. All five operators are measured on streaming
// data; GT covers tiny input, bitmap tails and nullable/all-null fast paths.
func integerColumnBenchmark[T integerColumnNumber](b *testing.B, bind func(Set) integerColumnCall[T]) {
	for _, n := range []int{7, 65, 1 << 16, 1 << 20} {
		for _, kind := range []string{"dense", "nullable", "allnull"} {
			b.Run(fmt.Sprintf("n=%d/validity=%s", n, kind), func(b *testing.B) {
				a, c := make([]T, n), make([]T, n)
				for i := range a {
					a[i], c[i] = T(uint64(i)*0x9e3779b97f4a7c15), T(uint64(i)*0xd6e8feb86659fd93)
					if i%3 == 0 {
						c[i] = a[i]
					}
				}
				var av, bv memory.Bitmap
				switch kind {
				case "nullable":
					av, bv = integerColumnValidity(n, "mixed"), integerColumnValidity(n, "even")
				case "allnull":
					av, bv = integerColumnValidity(n, "null"), integerColumnValidity(n, "null")
				}
				out := make(memory.Bitmap, (n+63)/64)
				ops := []CompareOp{CompareGT}
				if n == 1<<20 && kind == "dense" {
					ops = []CompareOp{CompareGT, CompareGE, CompareLT, CompareLE, CompareEQ}
				}
				for _, op := range ops {
					for _, runner := range integerColumnRunners(bind) {
						b.Run(fmt.Sprintf("op=%s/mode=%s", op, runner.name), func(b *testing.B) {
							b.ReportAllocs()
							b.SetBytes(int64(n) * int64(2*unsafe.Sizeof(T(0))))
							b.ResetTimer()
							count := 0
							for i := 0; i < b.N; i++ {
								count = runner.call(a, c, av, bv, out, op)
							}
							integerColumnBenchmarkCount = count
						})
					}
				}
			})
		}
	}
}
func BenchmarkIntegerColumnComparisons(b *testing.B) {
	b.Run("type=int8", func(b *testing.B) {
		integerColumnBenchmark(b, func(s Set) integerColumnCall[int8] { return s.CompareInt8Columns })
	})
	b.Run("type=int16", func(b *testing.B) {
		integerColumnBenchmark(b, func(s Set) integerColumnCall[int16] { return s.CompareInt16Columns })
	})
	b.Run("type=int64", func(b *testing.B) {
		integerColumnBenchmark(b, func(s Set) integerColumnCall[int64] { return s.CompareInt64Columns })
	})
	b.Run("type=uint8", func(b *testing.B) {
		integerColumnBenchmark(b, func(s Set) integerColumnCall[uint8] { return s.CompareUint8Columns })
	})
	b.Run("type=uint16", func(b *testing.B) {
		integerColumnBenchmark(b, func(s Set) integerColumnCall[uint16] { return s.CompareUint16Columns })
	})
	b.Run("type=uint64", func(b *testing.B) {
		integerColumnBenchmark(b, func(s Set) integerColumnCall[uint64] { return s.CompareUint64Columns })
	})
}
