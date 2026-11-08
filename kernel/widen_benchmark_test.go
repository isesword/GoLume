package kernel

import (
	"fmt"
	"gocolumn/memory"
	"testing"
)

func BenchmarkWideningKernel(b *testing.B) {
	for _, rows := range []int{63, 65539} {
		for _, nullable := range []bool{false, true} {
			f, g, d, i, u, out := make([]float32, rows), make([]float32, rows), make([]float64, rows), make([]int32, rows), make([]uint64, rows), make([]float64, rows)
			var valid memory.Bitmap
			if nullable {
				valid = make(memory.Bitmap, memory.WordsFor(rows))
			}
			for row := range f {
				f[row] = float32(row%251) + 0.25
				g[row] = float32(row%127) + 0.75
				d[row] = float64(row%251) + 0.25
				i[row] = int32(row%31) + 1
				u[row] = 1<<63 + uint64(row%127)
				if nullable && row%17 != 0 {
					memory.Set(valid, row)
				}
			}
			for _, mode := range []Mode{ModeScalar, ModeAuto} {
				set := New(mode)
				cases := []struct {
					name string
					run  func()
				}{
					{"float32-pair", func() { set.Float32WidenArithmetic(f, g, out, valid, ArithmeticSub) }},
					{"float64-float32", func() { set.Float64Float32Arithmetic(d, f, out, valid, ArithmeticMul, false) }},
					{"float64-int32", func() { set.Float64Int32Arithmetic(d, i, out, valid, ArithmeticDiv, false) }},
					{"float64-uint64", func() { set.Float64Uint64Arithmetic(d, u, out, valid, ArithmeticAdd, false) }},
				}
				for _, c := range cases {
					b.Run(fmt.Sprintf("%s/rows=%d/null=%t/mode=%s", c.name, rows, nullable, mode), func(b *testing.B) {
						b.ReportAllocs()
						for range b.N {
							c.run()
						}
					})
				}
			}
		}
	}
}
