package expr

import (
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func wideningFixture(rows int, right memory.DataType, nullable bool) (memory.Schema, *memory.Batch) {
	batch := memory.NewBatch()
	batch.Reset(rows)
	a, b := make([]float32, rows), make([]float32, rows)
	i, d, u := make([]int32, rows), make([]float64, rows), make([]uint64, rows)
	var valid memory.Bitmap
	if nullable {
		valid = make(memory.Bitmap, memory.WordsFor(rows))
	}
	for row := range a {
		a[row] = float32(row%251) + 0.25
		b[row] = float32(row%127) + 0.75
		i[row] = int32(row%31) + 1
		d[row] = float64(row%127) + 0.75
		u[row] = 1<<63 + uint64(row%127)
		if nullable && row%17 != 0 {
			memory.Set(valid, row)
		}
	}
	batch.Float32s["a"] = &memory.Float32Vector{Values: a, Validity: valid}
	switch right {
	case memory.TypeFloat32:
		batch.Float32s["b"] = &memory.Float32Vector{Values: b}
	case memory.TypeInt32:
		batch.Int32s["b"] = &memory.Int32Vector{Values: i}
	case memory.TypeFloat64:
		batch.Float64s["b"] = &memory.Float64Vector{Values: d}
	case memory.TypeUint64:
		batch.Uint64s["b"] = &memory.Uint64Vector{Values: u}
	}
	return memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeFloat32, Nullable: nullable}, memory.Field{Name: "b", Type: right}), batch
}

func BenchmarkWideningExpression(b *testing.B) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64, memory.TypeInt32, memory.TypeUint64} {
		for _, nullable := range []bool{false, true} {
			for _, rows := range []int{63, 65539} {
				schema, batch := wideningFixture(rows, typ, nullable)
				p, err := Compile(schema, Col("a").Sub(Col("b")).Mul(Col("a")).Div(Col("b")))
				if err != nil {
					b.Fatal(err)
				}
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
					b.Run(fmt.Sprintf("right=%s/null=%t/rows=%d/mode=%s", typ, nullable, rows, mode), func(b *testing.B) {
						set := kernel.New(mode)
						var w evaluationState
						defer w.Close()
						if _, err := evaluateValueTest(p, batch, &w, set); err != nil {
							b.Fatal(err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							if _, err := evaluateValueTest(p, batch, &w, set); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			}
		}
	}
}
