package expr

import (
	"gocolumn/memory"
	"math"
	"testing"
)

func TestFillNullAndNaNStayDistinct(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true})
	batch := memory.NewBatch()
	batch.Rows = 4
	batch.Float64s["v"] = &memory.Float64Vector{Values: []float64{math.NaN(), 123, -0.0, 4}, Validity: memory.Bitmap{13}}
	for _, tc := range []struct {
		e   Expr
		nan bool
	}{{FillNull(Col("v"), F64(8)), false}, {FillNaN(Col("v"), F64(8)), true}} {
		p, err := Compile(schema, tc.e)
		if err != nil {
			t.Fatal(err)
		}
		var w evaluationState
		got, err := evaluateValueTest(p, batch, &w, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		v := got.Float64
		if tc.nan {
			if v.Values[0] != 8 || v.IsValid(1) {
				t.Fatal(v)
			}
		} else {
			if !math.IsNaN(v.Values[0]) || !v.IsValid(1) || v.Values[1] != 8 {
				t.Fatal(v)
			}
		}
		w.Close()
	}
}

func TestFillBindingWidthAndUnusedReplacement(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat32})
	for _, makeFill := range []func(Expr, Expr) Expr{FillNull, FillNaN} {
		bound, err := Bind(schema, makeFill(Col("v"), F64(8)))
		if err != nil || bound.Field.Type != memory.TypeFloat64 {
			t.Fatalf("existing widening policy: %+v %v", bound, err)
		}
		bound, err = Bind(schema, makeFill(Col("v"), CastTo(F64(8), memory.TypeFloat32)))
		if err != nil || bound.Field.Type != memory.TypeFloat32 {
			t.Fatalf("explicit width: %+v %v", bound, err)
		}
		p, err := Compile(schema, makeFill(Col("v"), CastTo(Str("bad"), memory.TypeFloat32)))
		if err != nil {
			t.Fatal(err)
		}
		batch := memory.NewBatch()
		batch.Rows = 1
		batch.Float32s["v"] = &memory.Float32Vector{Values: []float32{3}}
		var w evaluationState
		got, err := evaluateValueTest(p, batch, &w, testKernelSet())
		if err != nil || got.Float32.Values[0] != 3 {
			t.Fatalf("unused replacement: %v %v", got, err)
		}
		w.Close()
	}
}
