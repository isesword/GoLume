package expr

import (
	"math"
	"testing"

	"gocolumn/memory"
)

func TestProjectionNativeNumericColumnsAliasInput(t *testing.T) {
	tests := []struct {
		name  string
		typ   memory.DataType
		fill  func(*memory.Batch)
		check func(ScalarValue, *memory.Batch)
	}{
		{"int8", memory.TypeInt8, func(b *memory.Batch) { b.Int8s["v"] = &memory.Int8Vector{Values: []int8{-3, 7}} }, func(v ScalarValue, b *memory.Batch) {
			if v.Int8 == nil || &v.Int8.Values[0] != &b.Int8s["v"].Values[0] {
				t.Fatalf("int8 did not alias native input")
			}
		}},
		{"int16", memory.TypeInt16, func(b *memory.Batch) { b.Int16s["v"] = &memory.Int16Vector{Values: []int16{-300, 700}} }, func(v ScalarValue, b *memory.Batch) {
			if v.Int16 == nil || &v.Int16.Values[0] != &b.Int16s["v"].Values[0] {
				t.Fatalf("int16 did not alias native input")
			}
		}},
		{"uint8", memory.TypeUint8, func(b *memory.Batch) { b.Uint8s["v"] = &memory.Uint8Vector{Values: []uint8{3, 7}} }, func(v ScalarValue, b *memory.Batch) {
			if v.Uint8 == nil || &v.Uint8.Values[0] != &b.Uint8s["v"].Values[0] {
				t.Fatalf("uint8 did not alias native input")
			}
		}},
		{"uint16", memory.TypeUint16, func(b *memory.Batch) { b.Uint16s["v"] = &memory.Uint16Vector{Values: []uint16{300, 700}} }, func(v ScalarValue, b *memory.Batch) {
			if v.Uint16 == nil || &v.Uint16.Values[0] != &b.Uint16s["v"].Values[0] {
				t.Fatalf("uint16 did not alias native input")
			}
		}},
		{"float32", memory.TypeFloat32, func(b *memory.Batch) {
			b.Float32s["v"] = &memory.Float32Vector{Values: []float32{float32(math.Inf(1)), float32(math.NaN())}}
		}, func(v ScalarValue, b *memory.Batch) {
			if v.Float32 == nil || &v.Float32.Values[0] != &b.Float32s["v"].Values[0] {
				t.Fatalf("float32 did not alias native input")
			}
			if !math.IsInf(float64(v.Float32.Values[0]), 1) || !math.IsNaN(float64(v.Float32.Values[1])) {
				t.Fatalf("float32 special values changed: %v", v.Float32.Values)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: "v", Type: tc.typ})
			b := memory.NewBatch()
			tc.fill(b)
			b.Rows = 2
			p, err := CompileExpressions(schema, []NamedExpr{Named(Col("v"), "alias")})
			if err != nil {
				t.Fatal(err)
			}
			got, err := evaluateProjectionTest(p, b, &evaluationState{}, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			tc.check(got[0], b)
		})
	}
}

func TestProjectionFloat32CastRetainsNativeType(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64})
	b := memory.NewBatch()
	b.Float64s["v"] = &memory.Float64Vector{Values: []float64{1.25, -2.5}}
	b.Rows = 2
	p, err := CompileExpressions(schema, []NamedExpr{Named(CastTo(Col("v"), memory.TypeFloat32), "f32")})
	if err != nil {
		t.Fatal(err)
	}
	got, err := evaluateProjectionTest(p, b, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Float32 == nil || got[0].Type != memory.TypeFloat32 || got[0].Float32.Values[1] != -2.5 {
		t.Fatalf("cast result=%+v", got[0])
	}
}
