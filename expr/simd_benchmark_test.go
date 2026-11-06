package expr

import (
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

// BenchmarkFloat64ExpressionKernelMode is the operation-relevant end-to-end
// gate for the Float64 Sub/Mul/Div Auto entries. It includes expression
// dispatch, workspace reuse, validity propagation, and vector materialization.
func BenchmarkFloat64ExpressionKernelMode(b *testing.B) {
	const rows = 1 << 16
	schema := memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeFloat64},
		memory.Field{Name: "b", Type: memory.TypeFloat64},
		memory.Field{Name: "c", Type: memory.TypeFloat64},
		memory.Field{Name: "d", Type: memory.TypeFloat64},
	)
	batch := memory.NewBatch()
	for _, name := range []string{"a", "b", "c", "d"} {
		values := make([]float64, rows)
		for row := range values {
			values[row] = float64(row%251) + 1.25
		}
		batch.Float64s[name] = &memory.Float64Vector{Values: values}
	}
	batch.Reset(rows)
	program, err := Compile(schema, Col("a").Sub(Col("b")).Mul(Col("c")).Div(Col("d")))
	if err != nil {
		b.Fatal(err)
	}
	for _, choice := range []struct {
		name string
		mode kernel.Mode
	}{{"mode=scalar", kernel.ModeScalar}, {"mode=auto", kernel.ModeAuto}} {
		b.Run(choice.name, func(b *testing.B) {
			set := kernel.New(choice.mode)
			var workspace evaluationState
			if _, err := evaluateValueTest(program, batch, &workspace, set); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				value, err := evaluateValueTest(program, batch, &workspace, set)
				if err != nil {
					b.Fatal(err)
				}
				if len(value.Float64.Values) != rows {
					b.Fatal("wrong result length")
				}
			}
		})
	}
}

func BenchmarkUTF8LengthExpressionKernelMode(b *testing.B) {
	profiles := []struct {
		name  string
		value string
	}{
		{"long-ascii", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789--abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789--"},
		{"mixed", "Go语言🙂Go语言🙂Go语言🙂Go语言🙂Go语言🙂Go语言🙂Go语言🙂Go语言🙂"},
	}
	const rows = 1 << 14
	for _, profile := range profiles {
		offsets := make([]uint32, rows+1)
		data := make([]byte, 0, rows*len(profile.value))
		for row := 0; row < rows; row++ {
			data = append(data, profile.value...)
			offsets[row+1] = uint32(len(data))
		}
		batch := memory.NewBatch()
		batch.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data, Encoding: memory.EncodingView}
		batch.Reset(rows)
		schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingView})
		program, _, err := CompilePredicate(schema, GT(Length(Col("s")), I64(32)))
		if err != nil {
			b.Fatal(err)
		}
		for _, choice := range []struct {
			name string
			mode kernel.Mode
		}{{"mode=scalar", kernel.ModeScalar}, {"mode=auto", kernel.ModeAuto}} {
			b.Run(profile.name+"/"+choice.name, func(b *testing.B) {
				set := kernel.New(choice.mode)
				var workspace evaluationState
				if _, err := evaluateBoolTest(program, batch, &workspace, set); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if _, err := evaluateBoolTest(program, batch, &workspace, set); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkStringMatchExpressionKernelMode(b *testing.B) {
	const rows = 1 << 16
	offsets := make([]uint32, rows+1)
	data := make([]byte, 0, rows*12)
	for row := 0; row < rows; row++ {
		value := "alpha-value"
		if row%7 == 0 {
			value = "中文-😀-alpha"
		}
		data = append(data, value...)
		offsets[row+1] = uint32(len(data))
	}
	valid := memory.EnsureBitmap(nil, rows)
	for row := 0; row < rows; row++ {
		if row%13 != 0 {
			memory.Set(valid, row)
		}
	}
	batch := memory.NewBatch()
	batch.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: valid, Encoding: memory.EncodingView}
	batch.Reset(rows)
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingView})
	program, _, err := CompilePredicate(schema, Contains(Col("s"), Str("alpha")))
	if err != nil {
		b.Fatal(err)
	}
	for _, choice := range []struct {
		name string
		mode kernel.Mode
	}{{"mode=scalar", kernel.ModeScalar}, {"mode=auto", kernel.ModeAuto}} {
		b.Run(choice.name, func(b *testing.B) {
			set := kernel.New(choice.mode)
			var workspace evaluationState
			if _, err := evaluateBoolTest(program, batch, &workspace, set); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := evaluateBoolTest(program, batch, &workspace, set); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
